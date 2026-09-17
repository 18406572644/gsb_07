'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer } = require('ws');

const { ChatDB } = require('./db');
const { Hub, Connection } = require('./hub');
const defaultConfig = require('./config');
const {
  randomId,
  randomSecret,
  signToken,
  verifyToken,
  isNonEmptyString,
  parseFrame,
  now,
} = require('./util');

/** 业务错误：handler 抛出，统一转成 error 帧回给客户端 */
class ChatError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}
const fail = (code, message) => {
  throw new ChatError(code, message);
};

/** 令牌桶限流（按用户），防刷屏。闲置桶定时回收，避免长期运行后 key 无限增长 */
class TokenBucket {
  constructor(ratePerSec, burst) {
    this.rate = ratePerSec;
    this.burst = burst;
    this.buckets = new Map();
  }
  take(key) {
    const t = now();
    let b = this.buckets.get(key);
    if (!b) {
      b = { tokens: this.burst, updated: t };
      this.buckets.set(key, b);
    }
    b.tokens = Math.min(this.burst, b.tokens + ((t - b.updated) / 1000) * this.rate);
    b.updated = t;
    if (b.tokens < 1) return false;
    b.tokens -= 1;
    return true;
  }

  /** 删除闲置超过 idleMs 的桶 */
  prune(idleMs) {
    const t = now();
    for (const [key, b] of this.buckets) {
      if (t - b.updated > idleMs) this.buckets.delete(key);
    }
  }
}

/** 数据库消息行 -> 下发帧 */
function msgFrame(m) {
  return {
    type: 'msg',
    roomId: m.roomId,
    seq: m.seq,
    clientMsgId: m.clientMsgId,
    from: m.from,
    fromName: m.fromName,
    content: m.content,
    ts: m.ts,
  };
}

function createChatServer(overrides = {}) {
  const config = { ...defaultConfig, ...overrides };
  const db = new ChatDB(config.dbPath);
  const hub = new Hub(config);
  const limiter = new TokenBucket(config.rateLimitPerSec, config.rateLimitBurst);
  const publicDir = path.join(__dirname, '..', 'public');

  // ---------------------------------------------------------------- 消息处理

  /** 断线补发：把 roomId 中 seq > fromSeq 的消息按序推给连接，分批，客户端按 sync_done 续拉 */
  function replayRoom(conn, roomId, fromSeq) {
    const batch = db.getMessagesAfter(roomId, fromSeq, config.syncBatchSize + 1);
    const hasMore = batch.length > config.syncBatchSize;
    const slice = hasMore ? batch.slice(0, config.syncBatchSize) : batch;
    for (const m of slice) hub.send(conn, msgFrame(m), { track: true, roomId, seq: m.seq });
    const lastSeq = slice.length ? slice[slice.length - 1].seq : fromSeq;
    hub.send(conn, { type: 'sync_done', roomId, lastSeq, hasMore });
  }

  function requireMember(conn, roomId) {
    const member = db.getMember(roomId, conn.userId);
    if (!member) fail('NOT_MEMBER', 'not a member of this room');
    return member;
  }

  function requireAdmin(conn, roomId) {
    const member = requireMember(conn, roomId);
    if (member.role !== 'admin') fail('FORBIDDEN', 'admin role required');
    return member;
  }

  const handlers = {
    ping(conn, msg) {
      hub.send(conn, { type: 'pong', t: msg.t });
    },

    create_room(conn, msg) {
      if (!isNonEmptyString(msg.name, 64)) fail('BAD_REQUEST', 'invalid room name');
      if (db.getRoomByName(msg.name)) fail('ROOM_EXISTS', 'room name already taken');
      const room = db.createRoom(randomId('r_'), msg.name, conn.userId);
      hub.joinRoom(conn, room.id);
      hub.send(conn, {
        type: 'joined',
        roomId: room.id,
        name: room.name,
        role: 'admin',
        mutedUntil: 0,
        lastSeq: 0,
      });
    },

    join(conn, msg) {
      if (!isNonEmptyString(msg.room, 128)) fail('BAD_REQUEST', 'invalid room');
      const room = db.getRoom(msg.room) || db.getRoomByName(msg.room);
      if (!room) fail('NO_SUCH_ROOM', 'room not found');
      db.joinRoom(room.id, conn.userId);
      hub.joinRoom(conn, room.id);
      const member = db.getMember(room.id, conn.userId);
      hub.send(conn, {
        type: 'joined',
        roomId: room.id,
        name: room.name,
        role: member.role,
        mutedUntil: member.muted_until,
        lastSeq: room.last_seq,
      });
      // 补发：优先用客户端上报的进度，否则用服务端游标（新设备则从游标开始）
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(room.id, conn.userId);
      if (fromSeq < room.last_seq) replayRoom(conn, room.id, fromSeq);
    },

    leave(conn, msg) {
      hub.leaveRoom(conn, msg.roomId);
      hub.send(conn, { type: 'left', roomId: msg.roomId });
    },

    msg(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!isNonEmptyString(msg.clientMsgId, 64)) fail('BAD_REQUEST', 'invalid clientMsgId');
      if (!isNonEmptyString(msg.content, config.maxContentLength)) {
        fail('BAD_REQUEST', `content must be 1..${config.maxContentLength} chars`);
      }
      const member = requireMember(conn, msg.roomId);
      if (member.muted_until > now()) {
        fail('MUTED', `you are muted until ${new Date(member.muted_until).toISOString()}`);
      }
      if (!limiter.take(conn.userId)) fail('RATE_LIMITED', 'sending too fast, slow down');

      // 先落库（同事务分配 seq），再 ACK，再广播 —— 崩溃也不丢已确认消息
      const { message, duplicate } = db.insertMessage({
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        senderId: conn.userId,
        content: msg.content,
      });
      hub.send(conn, {
        type: 'ack',
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        seq: message.seq,
        ts: message.ts,
      });
      if (!duplicate) {
        // 重复提交（客户端重试）只回 ACK，不再广播 —— 发送幂等
        hub.broadcast(msg.roomId, msgFrame(message), { track: true, seq: message.seq });
      }
    },

    // 客户端累积 ACK：清除未确认队列 + 持久化游标（断线补发的兜底依据）
    ack(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) return;
      if (!conn.rooms.has(msg.roomId)) return; // 只处理本连接已加入的房间
      conn.ack(msg.roomId, msg.seq);
      db.saveCursor(msg.roomId, conn.userId, msg.seq);
    },

    sync(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(msg.roomId, conn.userId);
      replayRoom(conn, msg.roomId, fromSeq);
    },

    history(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const limit = Math.min(Math.max(1, msg.limit || 50), config.historyMaxLimit);
      const before = Number.isInteger(msg.beforeSeq) ? msg.beforeSeq : Number.MAX_SAFE_INTEGER;
      const messages = db.getMessagesBefore(msg.roomId, before, limit);
      hub.send(conn, { type: 'history', roomId: msg.roomId, messages, hasMore: messages.length === limit });
    },

    rooms(conn) {
      hub.send(conn, { type: 'rooms', rooms: db.listRoomsForUser(conn.userId) });
    },

    members(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const online = new Set(hub.onlineUserIds(msg.roomId));
      const members = db.listMembers(msg.roomId).map((m) => ({ ...m, online: online.has(m.userId) }));
      hub.send(conn, { type: 'members', roomId: msg.roomId, members });
    },

    mute(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      if (target.role === 'admin') fail('FORBIDDEN', 'cannot mute an admin');
      const minutes = Number(msg.minutes);
      if (!Number.isFinite(minutes) || minutes < 1 || minutes > 1440) {
        fail('BAD_REQUEST', 'minutes must be 1..1440');
      }
      const until = now() + Math.round(minutes * 60_000);
      db.setMuted(msg.roomId, msg.userId, until);
      hub.broadcast(msg.roomId, {
        type: 'notice',
        roomId: msg.roomId,
        event: 'muted',
        userId: msg.userId,
        until,
        by: conn.userId,
      });
    },

    unmute(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      db.setMuted(msg.roomId, msg.userId, 0);
      hub.broadcast(msg.roomId, {
        type: 'notice',
        roomId: msg.roomId,
        event: 'unmuted',
        userId: msg.userId,
        by: conn.userId,
      });
    },
  };

  function onFrame(conn, raw) {
    const msg = parseFrame(raw);
    if (!msg) {
      hub.send(conn, { type: 'error', code: 'BAD_FRAME', message: 'invalid JSON frame' });
      return;
    }
    const handler = handlers[msg.type];
    if (!handler) {
      hub.send(conn, { type: 'error', code: 'UNKNOWN_TYPE', message: `unknown type: ${msg.type}` });
      return;
    }
    try {
      handler(conn, msg);
    } catch (err) {
      if (err instanceof ChatError) {
        hub.send(conn, {
          type: 'error',
          code: err.code,
          message: err.message,
          ref: msg.clientMsgId || msg.roomId || undefined,
        });
      } else {
        console.error('[handler error]', msg.type, err);
        hub.send(conn, { type: 'error', code: 'INTERNAL', message: 'internal error' });
      }
    }
  }

  // ---------------------------------------------------------------- HTTP 层

  const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css' };

  function readBody(req, limit = 4096) {
    return new Promise((resolve, reject) => {
      let size = 0;
      const chunks = [];
      req.on('data', (c) => {
        size += c.length;
        if (size > limit) {
          reject(new Error('body too large'));
          req.destroy();
          return;
        }
        chunks.push(c);
      });
      req.on('end', () => resolve(Buffer.concat(chunks).toString()));
      req.on('error', reject);
    });
  }

  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const json = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(obj));
    };

    if (req.method === 'POST' && url.pathname === '/api/login') {
      // 演示级登录：按用户名创建/复用账号，返回签名 token
      try {
        const body = JSON.parse(await readBody(req));
        if (!isNonEmptyString(body.name, 32)) return json(400, { error: 'invalid name' });
        let user = db.getUserByName(body.name);
        if (!user) user = db.createUser(randomId('u_'), body.name, randomSecret());
        const token = signToken(user.id, user.token_random, config.authSecret);
        return json(200, { userId: user.id, name: user.name, token });
      } catch {
        return json(400, { error: 'bad request' });
      }
    }

    if (req.method === 'GET' && url.pathname === '/healthz') {
      return json(200, { ok: true, ...hub.stats() });
    }

    if (req.method === 'GET') {
      const rel = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
      const file = path.resolve(publicDir, rel);
      if (!file.startsWith(publicDir) || !MIME[path.extname(file)]) {
        res.writeHead(404).end('not found');
        return;
      }
      fs.readFile(file, (err, data) => {
        if (err) {
          res.writeHead(404).end('not found');
          return;
        }
        res.writeHead(200, { 'content-type': MIME[path.extname(file)] });
        res.end(data);
      });
      return;
    }

    res.writeHead(404).end('not found');
  });

  // ---------------------------------------------------------------- WS 层

  const wss = new WebSocketServer({ noServer: true });

  /**
   * 注册一条新连接：进 Hub 索引 + 绑定事件监听器。
   * 集中在一处完成，使「索引」与「带 close 清理的监听器」永远成对出现，
   * 杜绝升级回调异常时产生没有清理者的僵尸连接。
   */
  function registerConnection(ws, user) {
    const conn = new Connection(ws, user);
    hub.add(conn);
    conn.bindListeners({
      pong: () => { conn.lastPong = now(); },
      message: (raw) => onFrame(conn, raw),
      // 所有终止路径（正常关闭、error 后随附的 close、心跳/重发超时 terminate）
      // 都汇聚到 hub.remove —— 内部 destroyed 守卫保证只清理一次
      close: () => hub.remove(conn),
      error: () => { /* error 后必随 close，统一在 close 清理 */ },
    });
    return conn;
  }

  httpServer.on('upgrade', (req, socket, head) => {
    const reject = (code, text) => {
      try {
        socket.write(`HTTP/1.1 ${code} ${text}\r\nConnection: close\r\n\r\n`);
      } catch { /* 对端可能已关闭，destroy 兜底 */ }
      socket.destroy();
    };
    let url;
    try {
      url = new URL(req.url, 'http://localhost');
    } catch {
      return reject(400, 'Bad Request');
    }
    if (url.pathname !== '/ws') return reject(404, 'Not Found');

    const userId = verifyToken(url.searchParams.get('token'), config.authSecret);
    const user = userId && db.getUserById(userId);
    if (!user) return reject(401, 'Unauthorized');

    const denied = hub.checkAdmission(user.id);
    if (denied) return reject(503, denied);

    // 升级失败（回调内异常）时绝不能留下「已入索引但无人清理」的连接
    try {
      wss.handleUpgrade(req, socket, head, (ws) => {
        let conn;
        try {
          conn = registerConnection(ws, user);
        } catch (err) {
          console.error('[upgrade] connection init failed', err);
          try { ws.terminate(); } catch { /* 已关闭 */ }
          return;
        }
        hub.send(conn, { type: 'welcome', userId: user.id, name: user.name, serverTime: now() });
      });
    } catch (err) {
      console.error('[upgrade] handleUpgrade failed', err);
      reject(500, 'Internal Server Error');
    }
  });

  // ---------------------------------------------------------------- 定时任务

  const timers = [
    setInterval(() => hub.heartbeatSweep(), config.heartbeatIntervalMs),
    setInterval(() => hub.resendSweep(), config.ackResendIntervalMs),
    setInterval(() => limiter.prune(config.rateLimitIdleMs), config.rateLimitPruneIntervalMs),
  ];
  for (const t of timers) t.unref();

  // ---------------------------------------------------------------- 生命周期

  let stopped = false;

  function start() {
    return new Promise((resolve, reject) => {
      httpServer.listen(config.port, config.host, () => {
        const addr = httpServer.address();
        console.log(`[chat] listening on http://${addr.address}:${addr.port}  (db: ${config.dbPath})`);
        resolve(addr);
      });
      httpServer.once('error', reject);
    });
  }

  /**
   * 停止服务（幂等，可被 SIGINT/SIGTERM/测试重复调用）。
   * 清理顺序：停定时器 → 逐连接通知并同步摘除（Hub 索引、ws 监听器、unacked、
   * ws 引用一次清完）→ 关 WS/HTTP 服务 → 关数据库。
   */
  function stop() {
    if (stopped) return;
    stopped = true;
    for (const t of timers) clearInterval(t);
    timers.length = 0;

    for (const conn of [...hub.all]) {
      try { hub.send(conn, { type: 'server_shutdown' }); } catch { /* 忽略 */ }
      const ws = conn.ws; // release() 会清空 ws 引用，terminate 要先留一份
      hub.remove(conn); // 同步完成全部连接级清理（幂等）
      if (ws) {
        try { ws.terminate(); } catch { /* 已关闭 */ }
      }
    }

    wss.close();
    if (httpServer.listening) httpServer.close();
    db.close();
  }

  return { config, db, hub, limiter, timers, httpServer, wss, start, stop };
}

// 直接运行：node src/server.js
if (require.main === module) {
  const server = createChatServer();
  server.start();
  const shutdown = () => {
    console.log('\n[chat] shutting down...');
    server.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = { createChatServer };
