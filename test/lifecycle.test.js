'use strict';

/**
 * 连接生命周期清理测试：
 * - 服务端：正常关闭/异常断开/心跳终止/stop 各路径下，Hub 映射表、unacked、
 *   ws 事件监听器、ws 引用均被释放；清理幂等；升级失败不留残余；高频重连不累积。
 * - 客户端：ReconnectingSocket 重连定时器唯一可取消、代际守卫、监听器成对解绑；
 *   AckBatcher/TimerGroup 可释放；ChatSession 断线重连不重建定时器，shutdown 一次清完。
 */
const { test } = require('node:test');
const assert = require('node:assert/strict');
const WebSocket = require('ws');
const { createChatServer } = require('../src/server');
const { Hub, Connection } = require('../src/hub');
const {
  TimerGroup,
  AckBatcher,
  ReconnectingSocket,
  ChatSession,
} = require('../public/client.js');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function startServer(overrides = {}) {
  const server = createChatServer({
    port: 0,
    host: '127.0.0.1',
    dbPath: ':memory:',
    heartbeatIntervalMs: 60_000,
    ackResendAfterMs: 60_000,
    ...overrides,
  });
  const addr = await server.start();
  return { server, port: addr.port };
}

async function login(port, name) {
  const res = await fetch(`http://127.0.0.1:${port}/api/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({ name }),
  });
  assert.equal(res.status, 200);
  return res.json();
}

async function openClient(port, token) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
  // 监听器必须在 open 之前挂好：welcome 在升级完成时即下发
  const welcome = new Promise((res) => ws.once('message', () => res()));
  await new Promise((res, rej) => {
    ws.once('open', res);
    ws.once('error', rej);
  });
  await welcome;
  return ws;
}

// ============================================================ 服务端

test('服务端：正常关闭后连接的全部资源被释放', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const clientWs = await openClient(port, u.token);

    assert.equal(server.hub.all.size, 1);
    const conn = [...server.hub.all][0];
    const serverWs = conn.ws;
    assert.equal(serverWs.listenerCount('message'), 1);
    assert.equal(serverWs.listenerCount('close'), 2); // 1 个 ws 库内部监听器 + 我们的
    assert.equal(serverWs.listenerCount('pong'), 1);
    assert.equal(serverWs.listenerCount('error'), 1);

    // 房间与 unacked 也应有清理路径
    clientWs.send(JSON.stringify({ type: 'create_room', name: 'r1' }));
    await sleep(50);
    assert.equal(server.hub.byRoom.size, 1);

    await new Promise((res) => { clientWs.on('close', res); clientWs.close(); });
    await sleep(50); // 等服务端 close 事件处理完

    assert.equal(server.hub.all.size, 0, 'all 索引清空');
    assert.equal(server.hub.byUser.size, 0, 'byUser 空集合应被删除');
    assert.equal(server.hub.byRoom.size, 0, 'byRoom 空集合应被删除');
    assert.equal(conn.destroyed, true);
    assert.equal(conn.ws, null, 'conn 不应再持有 WebSocket 对象');
    assert.equal(conn.unacked.size, 0);
    assert.equal(conn.rooms.size, 0);
    assert.equal(serverWs.listenerCount('message'), 0, 'message 监听器应解绑');
    assert.equal(serverWs.listenerCount('close'), 1, '仅剩 ws 库内部 close 监听器（不持有 conn）');
    assert.equal(serverWs.listenerCount('pong'), 0, 'pong 监听器应解绑');
    assert.equal(serverWs.listenerCount('error'), 0, 'error 监听器应解绑');
    assert.equal(server.wss.clients.size, 0);
  } finally {
    server.stop();
  }
});

test('服务端：清理入口幂等，重复 remove/事件迟到均无副作用', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'bob');
    const clientWs = await openClient(port, u.token);
    const conn = [...server.hub.all][0];

    clientWs.close();
    await sleep(50);

    // 重复清理不抛错、不重复操作
    assert.doesNotThrow(() => server.hub.remove(conn));
    assert.doesNotThrow(() => server.hub.remove(conn));
    assert.doesNotThrow(() => server.hub.remove(null));
    // 迟到的 pong 帧在监听器已解绑后也不应产生影响
    assert.equal(server.hub.all.size, 0);
  } finally {
    server.stop();
  }
});

test('服务端：异常 terminate（心跳判死）走同一条清理路径', async () => {
  const { server, port } = await startServer({ heartbeatTimeoutMs: 100 });
  try {
    const u = await login(port, 'carol');
    const clientWs = await openClient(port, u.token);
    const conn = [...server.hub.all][0];
    const serverWs = conn.ws;

    conn.lastPong = 0; // 模拟心跳超时
    server.hub.heartbeatSweep();

    await new Promise((res) => clientWs.on('close', res));
    await sleep(50);

    assert.equal(conn.destroyed, true);
    assert.equal(server.hub.all.size, 0);
    assert.equal(server.hub.byUser.size, 0);
    assert.equal(conn.ws, null);
    assert.equal(serverWs.listenerCount('close'), 1, '仅剩 ws 库内部监听器');

    // 清理后再跑两个 sweep 也不应触碰已释放连接
    assert.doesNotThrow(() => {
      server.hub.heartbeatSweep();
      server.hub.resendSweep();
    });
  } finally {
    server.stop();
  }
});

test('服务端：高频连接/断开循环下注册中心不累积任何状态', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'dave');
    for (let i = 0; i < 30; i++) {
      const clientWs = await openClient(port, u.token);
      assert.equal(server.hub.all.size, 1, `第 ${i} 轮应只有 1 条连接`);
      const conn = [...server.hub.all][0];
      clientWs.send(JSON.stringify({ type: 'create_room', name: `room${i}` }));
      await sleep(20);
      assert.equal(server.hub.byRoom.size, 1);
      await new Promise((res) => { clientWs.on('close', res); clientWs.close(); });
      // 等服务端处理 close（忙等上限）
      for (let k = 0; k < 20 && server.hub.all.size; k++) await sleep(10);
      assert.equal(server.hub.all.size, 0, `第 ${i} 轮关闭后应清空`);
      assert.equal(server.hub.byUser.size, 0);
      assert.equal(server.hub.byRoom.size, 0);
      assert.equal(conn.destroyed, true);
      assert.equal(conn.ws, null);
    }
  } finally {
    server.stop();
  }
});

test('服务端：升级失败（坏 token / 错误路径）不留下连接或监听器', async () => {
  const { server, port } = await startServer();
  try {
    // 错误路径
    const badPath = await new Promise((resolve) => {
      const req = require('node:http').get({ host: '127.0.0.1', port, path: '/nope' }, (res) => {
        res.resume();
        res.on('end', () => resolve(res.statusCode));
      });
      // HTTP 请求不会走 upgrade；这里只验证普通 404，WS 拒绝用下面两例
      req.on('error', () => resolve(0));
    });
    assert.equal(badPath, 404);

    // 坏 token —— 服务端在 upgrade 内拒绝并销毁 socket
    await assert.rejects(
      () => new Promise((resolve, reject) => {
        const ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=bogus`);
        ws.on('open', resolve);
        ws.on('error', reject);
      }),
      /401|Unexpected server response|Handshake|Authorization/i
    );
    await sleep(20);
    assert.equal(server.hub.all.size, 0, '拒绝的握手不得进入连接索引');
    assert.equal(server.wss.clients.size, 0);

    // 错误的 WS 路径
    await assert.rejects(
      () => new Promise((resolve, reject) => {
        const ws = new WebSocket(`ws://127.0.0.1:${port}/other`);
        ws.on('open', resolve);
        ws.on('error', reject);
      }),
      /404|Unexpected server response/i
    );
    await sleep(20);
    assert.equal(server.hub.all.size, 0);
  } finally {
    server.stop();
  }
});

test('服务端：stop() 幂等，定时器/连接/监听器全部释放', async () => {
  const { server, port } = await startServer();
  const u1 = await login(port, 'erin');
  const u2 = await login(port, 'frank');
  const c1 = await openClient(port, u1.token);
  const c2 = await openClient(port, u2.token);
  const conns = [...server.hub.all];
  const serverSockets = conns.map((c) => c.ws);

  // stop() 内同步摘除连接并 terminate，客户端随之收到 close
  const bothClosed = Promise.all([
    new Promise((res) => c1.on('close', res)),
    new Promise((res) => c2.on('close', res)),
  ]);
  assert.doesNotThrow(() => server.stop());
  assert.doesNotThrow(() => server.stop(), '重复 stop 必须无副作用');
  assert.doesNotThrow(() => server.stop());
  await bothClosed;

  assert.equal(server.hub.all.size, 0);
  assert.equal(server.timers.length, 0, '定时器句柄数组应清空');
  for (const c of conns) {
    assert.equal(c.destroyed, true);
    assert.equal(c.ws, null);
  }
  for (const ws of serverSockets) {
    assert.equal(ws.listenerCount('close'), 1, '仅剩 ws 库内部监听器');
    assert.equal(ws.listenerCount('message'), 0, '业务监听器已解绑');
    assert.equal(ws.listenerCount('pong'), 0);
    assert.equal(ws.listenerCount('error'), 0);
  }
  assert.equal(server.wss.clients.size, 0);
});

test('服务端：限流器闲置桶可被回收', async () => {
  const { server } = await startServer({ rateLimitIdleMs: 100 });
  try {
    assert.equal(server.limiter.take('u-x'), true);
    assert.ok(server.limiter.buckets.has('u-x'));
    await sleep(120);
    server.limiter.prune(100);
    assert.ok(!server.limiter.buckets.has('u-x'), '闲置桶应被删除');
  } finally {
    server.stop();
  }
});

test('服务端：背压断开同样走统一清理', async () => {
  const { server, port } = await startServer({
    maxUnackedPerConn: 2,
    ackResendIntervalMs: 60_000,
  });
  try {
    const ua = await login(port, 'gina');
    const ub = await login(port, 'hubert');
    const a = await openClient(port, ua.token);
    const b = await openClient(port, ub.token);

    // 发送方 A 也会收到自己的广播回声：自动 ACK，避免 A 自己被背压断开
    a.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      if (m.type === 'msg') a.send(JSON.stringify({ type: 'ack', roomId: m.roomId, seq: m.seq }));
    });

    const bClosed = new Promise((res) => b.on('close', res));
    a.send(JSON.stringify({ type: 'create_room', name: 'x' }));
    await sleep(50);
    b.send(JSON.stringify({ type: 'join', room: 'x', lastSeq: 0 }));
    await sleep(50);
    const room = server.db.getRoomByName('x');

    // B 不 ACK：第 3 条 tracked 消息触发背压断开（maxUnackedPerConn=2）
    for (let i = 0; i < 3; i++) {
      a.send(JSON.stringify({ type: 'msg', roomId: room.id, clientMsgId: `m${i}`, content: 'x' }));
      await sleep(20);
    }
    await bClosed;
    for (let k = 0; k < 20 && server.hub.all.size > 1; k++) await sleep(10);

    assert.equal(server.hub.all.size, 1, 'B 被清理，A 保留');
    const remaining = [...server.hub.all][0];
    assert.equal(remaining.userId, ua.userId);
    assert.equal(server.hub.byRoom.get(room.id).size, 1);
    a.close();
    await sleep(30);
  } finally {
    server.stop();
  }
});

test('Hub 单元：连接释放后 send/joinRoom/ack 全部为安全空操作', () => {
  const hub = new Hub({ maxConnections: 10, maxConnectionsPerUser: 3, maxUnackedPerConn: 5 });
  const fakeWs = {
    readyState: 1,
    send() {}, close() {}, terminate() {}, ping() {},
    on() {}, removeListener() {},
  };
  const conn = new Connection(fakeWs, { id: 'u1', name: 'n' });
  hub.add(conn);
  conn.bindListeners({ close: () => {} });
  hub.remove(conn);

  assert.equal(hub.send(conn, { x: 1 }), false);
  assert.doesNotThrow(() => hub.joinRoom(conn, 'r'));
  assert.doesNotThrow(() => hub.leaveRoom(conn, 'r'));
  assert.equal(conn.ack('r', 9), 0);
  assert.doesNotThrow(() => hub.broadcast('r', { x: 1 }));
});

// ============================================================ 客户端假设施

/** 手动推进的假时钟：advance(ms) 按到期顺序触发 timeout/interval */
function makeClock() {
  const clock = {
    t: 0,
    _seq: 1,
    _entries: new Map(),
    timeoutCreated: 0,
    intervalCreated: 0,
    activeTimers() { return clock._entries.size; },
  };
  clock.setTimeout = (fn, ms) => {
    const id = clock._seq++;
    clock.timeoutCreated++;
    clock._entries.set(id, { fn, runAt: clock.t + ms, period: null });
    return id;
  };
  clock.clearTimeout = (id) => clock._entries.delete(id);
  clock.setInterval = (fn, ms) => {
    const id = clock._seq++;
    clock.intervalCreated++;
    clock._entries.set(id, { fn, runAt: clock.t + ms, period: ms });
    return id;
  };
  clock.clearInterval = (id) => clock._entries.delete(id);
  clock.now = () => clock.t;
  clock.uuid = (() => { let n = 0; return () => `cid-${++n}`; })();

  clock.advance = function (ms) {
    const end = this.t + ms;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      let next = null;
      for (const [id, e] of this._entries) if (!next || e.runAt < next.runAt) next = { id, ...e };
      if (!next || next.runAt > end) break;
      this.t = next.runAt;
      const e = this._entries.get(next.id);
      if (!e) continue;
      if (e.period == null) this._entries.delete(next.id);
      else e.runAt = this.t + e.period;
      e.fn();
    }
    this.t = end;
  };
  return clock;
}

/** 最小 WebSocket mock：事件监听器计数、发送记录、可服务端驱动的 close/error */
class MockWebSocket {
  constructor(url) {
    this.url = url;
    this.readyState = 0;
    this.sent = [];
    this._listeners = new Map();
    MockWebSocket.instances.push(this);
    // 构造后异步开不通——测试里统一手动 emitOpen()
  }
  addEventListener(type, fn) {
    if (!this._listeners.has(type)) this._listeners.set(type, new Set());
    this._listeners.get(type).add(fn);
  }
  removeEventListener(type, fn) {
    this._listeners.get(type)?.delete(fn);
  }
  listenerCount(type) {
    return this._listeners.get(type)?.size || 0;
  }
  emit(type, ev) {
    for (const fn of [...(this._listeners.get(type) || [])]) fn(ev);
  }
  emitOpen() { this.readyState = 1; this.emit('open', {}); }
  emitMessage(data) { this.emit('message', { data }); }
  send(str) { this.sent.push(str); }
  close() {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.emit('close', {});
  }
}
MockWebSocket.instances = [];

// ============================================================ 客户端：定时器原语

test('客户端：TimerGroup 登记并一次释放全部定时器', () => {
  const clock = makeClock();
  const tg = new TimerGroup(clock);
  let n = 0;
  tg.setTimeout(() => n++, 100);
  tg.setInterval(() => n++, 50);
  assert.equal(clock.activeTimers(), 2);
  tg.clearAll();
  assert.equal(clock.activeTimers(), 0);
  clock.advance(200);
  assert.equal(n, 0, 'clearAll 后回调不得执行');
  assert.doesNotThrow(() => tg.clearAll(), 'clearAll 幂等');
  assert.equal(tg.setTimeout(() => n++, 1), null, 'clearAll 后拒绝新定时器');
});

test('客户端：AckBatcher 合并调度、只 flush 一次、cancel 后不再触发', () => {
  const clock = makeClock();
  let flushes = 0;
  const ack = new AckBatcher({ delay: 500, clock, flush: () => flushes++ });
  ack.schedule();
  ack.schedule();
  ack.schedule();
  assert.equal(clock.activeTimers(), 1, '多次 schedule 合并为一个定时器');
  clock.advance(500);
  assert.equal(flushes, 1);
  assert.equal(clock.activeTimers(), 0);

  ack.schedule();
  ack.cancel();
  clock.advance(600);
  assert.equal(flushes, 1, 'cancel 后挂起的批处理必须取消');
  ack.schedule();
  assert.equal(clock.activeTimers(), 0, 'cancel 为永久停止');

  // flushNow 立即执行
  let f2 = 0;
  const ack2 = new AckBatcher({ delay: 500, clock, flush: () => f2++ });
  ack2.schedule();
  ack2.flushNow();
  assert.equal(f2, 1);
  clock.advance(600);
  assert.equal(f2, 1);
});

// ============================================ 客户端：ReconnectingSocket

function makeSocket(opts = {}) {
  MockWebSocket.instances.length = 0;
  const clock = makeClock();
  const events = [];
  const rs = new ReconnectingSocket({
    url: 'ws://test/ws',
    WebSocketCtor: MockWebSocket,
    clock,
    initialDelay: opts.initialDelay || 500,
    maxDelay: opts.maxDelay || 10_000,
    onOpen: () => events.push('open'),
    onMessage: () => events.push('message'),
    onStatus: (s) => events.push(['status', s.connected, s.retryIn, s.intentional || false]),
  });
  return { rs, clock, events };
}

test('客户端：断线后只有一个重连定时器，重连后旧 ws 监听器全部解绑', () => {
  const { rs, clock } = makeSocket();
  rs.connect();
  const ws1 = MockWebSocket.instances.at(-1);
  ws1.emitOpen();
  assert.equal(rs.isOpen, true);

  ws1.close(); // 服务端断开
  assert.equal(clock.activeTimers(), 1, '恰好排一个重连定时器');
  assert.equal(ws1.listenerCount('close'), 0, '旧 ws 监听器已解绑');
  assert.equal(ws1.listenerCount('message'), 0);
  assert.equal(rs.ws, null, '包装器不再持有旧 ws');

  clock.advance(500);
  const ws2 = MockWebSocket.instances.at(-1);
  assert.notEqual(ws2, ws1);
  assert.equal(MockWebSocket.instances.length, 2);
  ws2.emitOpen();
  assert.equal(clock.activeTimers(), 0, '连上后重连定时器已取消');
  assert.equal(rs.ws, ws2);
});

test('客户端：error+close 双事件不会排两个重连定时器', () => {
  const { rs, clock } = makeSocket();
  rs.connect();
  const ws = MockWebSocket.instances.at(-1);
  ws.emitOpen();
  ws.emit('error', {}); // error 处理器会 ws.close() → close 事件
  assert.equal(clock.activeTimers(), 1);
  ws.emit('close', {}); // 再来一个迟到 close 也不翻倍
  assert.equal(clock.activeTimers(), 1);
});

test('客户端：旧连接的迟到事件不影响新连接（代际守卫）', () => {
  const { rs, clock, events } = makeSocket();
  rs.connect();
  const ws1 = MockWebSocket.instances.at(-1);
  ws1.emitOpen();
  const opensBefore = events.filter((e) => e === 'open').length;

  ws1.close();
  clock.advance(500);
  const ws2 = MockWebSocket.instances.at(-1);
  ws2.emitOpen();

  // 旧 ws 已解绑监听器，直接 emit 不会触达包装器；即便人工触达包装器处理器，
  // 代际不匹配也必须丢弃（通过旧 ws 重发 close 验证不产生新重连）
  ws1.emit('close', {});
  ws1.emit('message', { data: '{}' });
  assert.equal(clock.activeTimers(), 0, '旧连接 close 不得触发新重连');
  assert.equal(events.filter((e) => e === 'open').length, opensBefore + 1);
  assert.equal(rs.ws, ws2);
});

test('客户端：指数退避，连上后重置', () => {
  const clock = makeClock();
  const retries = [];
  const rs = new ReconnectingSocket({
    url: 'x', WebSocketCtor: MockWebSocket, clock,
    initialDelay: 100, maxDelay: 800,
    onStatus: (s) => { if (!s.connected && s.retryIn != null) retries.push(s.retryIn); },
  });
  rs.connect();
  lastWs().emitOpen(); // 首次连上，退避初始化为 100
  lastWs().close(); // 第 1 次断开：100 后重连
  // 重连建立的新 ws 不 emitOpen（模拟握手即失败），退避应逐次翻倍
  for (let i = 0; i < 4; i++) {
    clock.advance(retries.at(-1));
    lastWs().close();
  }
  assert.deepEqual(retries, [100, 200, 400, 800, 800]);

  // 最终重连成功：退避重置，再断时回到初始值
  clock.advance(retries.at(-1));
  lastWs().emitOpen();
  lastWs().close();
  assert.equal(retries.at(-1), 100);
  rs.close();
});

test('客户端：close() 永久关闭且幂等，取消待触发的重连', () => {
  const { rs, clock } = makeSocket();
  rs.connect();
  const ws1 = MockWebSocket.instances.at(-1);
  ws1.emitOpen();
  ws1.close();
  assert.equal(clock.activeTimers(), 1);

  rs.close();
  assert.equal(clock.activeTimers(), 0, '重连定时器必须取消');
  assert.equal(ws1.listenerCount('close'), 0);
  const count = MockWebSocket.instances.length;
  clock.advance(5000);
  assert.equal(MockWebSocket.instances.length, count, '不得再建立新连接');
  assert.doesNotThrow(() => rs.close());
});

// ============================================ 客户端：ChatSession 集成

function makeSession(overrides = {}) {
  MockWebSocket.instances.length = 0;
  const clock = makeClock();
  const uiEvents = [];
  const session = new ChatSession({
    url: 'ws://test/ws',
    me: { userId: 'u1', name: 'alice', token: 't' },
    saved: { r1: { name: 'room1', lastSeenSeq: 3 } },
    WebSocketCtor: MockWebSocket,
    clock,
    initialDelay: 500,
    resendIntervalMs: 1000,
    resendAfterMs: 1000,
    membersRefreshMs: 5000,
    onEvent: (ev, p) => uiEvents.push([ev, p]),
    ...overrides,
  });
  return { session, clock, uiEvents };
}

function lastWs() { return MockWebSocket.instances.at(-1); }

test('客户端会话：连上后按 saved 房间重放 join，消息触发批量 ACK', () => {
  const { session, clock } = makeSession();
  session.connect();
  const ws = lastWs();
  ws.emitOpen();

  const join = ws.sent.map(JSON.parse).find((f) => f.type === 'join');
  assert.deepEqual(join, { type: 'join', room: 'r1', lastSeq: 3 });

  ws.emitMessage(JSON.stringify({
    type: 'msg', roomId: 'r1', seq: 4, clientMsgId: 's1',
    from: 'u2', fromName: 'bob', content: 'hi', ts: 1,
  }));
  // 500ms 内不 ACK
  clock.advance(499);
  assert.ok(!ws.sent.some((s) => JSON.parse(s).type === 'ack'));
  clock.advance(1);
  const ack = ws.sent.map(JSON.parse).filter((f) => f.type === 'ack');
  assert.deepEqual(ack, [{ type: 'ack', roomId: 'r1', seq: 4 }]);

  // 重复 seq 被幂等丢弃，不产生新 ACK
  ws.emitMessage(JSON.stringify({
    type: 'msg', roomId: 'r1', seq: 4, clientMsgId: 's1',
    from: 'u2', fromName: 'bob', content: 'hi', ts: 1,
  }));
  clock.advance(600);
  assert.equal(ws.sent.map(JSON.parse).filter((f) => f.type === 'ack').length, 1);
  session.shutdown();
});

test('客户端会话：未 ACK 的自发消息周期重发，收到 ack 后停止', () => {
  const { session, clock } = makeSession();
  session.connect();
  const ws = lastWs();
  ws.emitOpen();
  ws.emitMessage(JSON.stringify({ type: 'joined', roomId: 'r1', name: 'room1', role: 'admin', mutedUntil: 0 }));
  session.setActiveRoom('r1');
  session.postMessage('hello');

  const countMsg = () => ws.sent.map(JSON.parse).filter((f) => f.type === 'msg').length;
  assert.equal(countMsg(), 1);
  clock.advance(2001); // 首次扫描在 1000ms（恰不满足严格大于），2000ms 处应重发
  assert.ok(countMsg() >= 2, '满 1s 未 ACK 应重发');

  const cid = ws.sent.map(JSON.parse).find((f) => f.type === 'msg').clientMsgId;
  ws.emitMessage(JSON.stringify({ type: 'ack', roomId: 'r1', clientMsgId: cid, seq: 5, ts: 2 }));
  const afterAck = countMsg();
  clock.advance(3000);
  assert.equal(countMsg(), afterAck, 'ACK 后不再重发');
  session.shutdown();
});

test('客户端会话：断线重连不重建会话定时器，断线期间重发空转', () => {
  const { session, clock } = makeSession();
  session.connect();
  const ws1 = lastWs();
  ws1.emitOpen();
  session.setActiveRoom('r1'); // saved 中已有 r1

  const intervalsAtStart = clock.intervalCreated;
  assert.equal(intervalsAtStart, 2, '重发扫描 + 成员刷新各一个 interval');

  // 成员刷新定时器确实在工作
  ws1.sent.length = 0;
  clock.advance(5000);
  assert.ok(ws1.sent.map(JSON.parse).some((f) => f.type === 'members'));

  // 断线
  ws1.close();
  assert.equal(clock.activeTimers(), 3, '2 个会话 interval 保留 + 恰好 1 个重连定时器');
  // 断线期间推进时钟（含重发扫描与重连时刻）：不得抛错
  assert.doesNotThrow(() => clock.advance(3000));

  // 重连（重连定时器在 500ms 处已自动建立新 ws，这里模拟握手完成）
  const ws2 = lastWs();
  ws2.emitOpen();
  assert.equal(clock.intervalCreated, intervalsAtStart, '重连不得新建会话定时器');
  assert.equal(ws1.listenerCount('message'), 0, '旧连接监听器已解绑');

  // 恢复后成员刷新继续工作（同一 interval）
  ws2.sent.length = 0;
  clock.advance(5000);
  assert.ok(ws2.sent.map(JSON.parse).some((f) => f.type === 'members'));
  session.shutdown();
});

test('客户端会话：高频重连风暴下定时器/监听器/连接对象不累积', () => {
  const { session, clock } = makeSession();
  session.connect();
  const baseIntervals = clock.intervalCreated;

  for (let i = 0; i < 25; i++) {
    const ws = lastWs();
    ws.emitOpen();
    ws.emitMessage(JSON.stringify({ type: 'joined', roomId: 'r1', name: 'room1', role: 'admin', mutedUntil: 0 }));
    ws.emitMessage(JSON.stringify({
      type: 'msg', roomId: 'r1', seq: 100 + i, clientMsgId: `c${i}`,
      from: 'u2', fromName: 'bob', content: `m${i}`, ts: i,
    })); // 每轮都排 ACK 定时器
    ws.close();
    clock.advance(500); // 到点重连
    // 每轮不变量
    assert.equal(clock.intervalCreated, baseIntervals, '会话定时器数量恒定');
  }
  const ws = lastWs();
  ws.emitOpen();
  // 所有历史 ws 都不应残留监听器
  for (const old of MockWebSocket.instances.slice(0, -1)) {
    assert.equal(old.listenerCount('open') + old.listenerCount('close')
      + old.listenerCount('message') + old.listenerCount('error'), 0);
  }
  session.shutdown();
  assert.equal(clock.activeTimers(), 0, 'shutdown 后不得残留任何定时器');
  assert.equal(ws.listenerCount('open') + ws.listenerCount('close')
    + ws.listenerCount('message') + ws.listenerCount('error'), 0);
});

test('客户端会话：shutdown 幂等并释放 ACK/重连/周期任务/房间状态', () => {
  const { session, clock } = makeSession();
  session.connect();
  const ws = lastWs();
  ws.emitOpen();
  ws.emitMessage(JSON.stringify({
    type: 'msg', roomId: 'r1', seq: 9, clientMsgId: 'x',
    from: 'u2', fromName: 'bob', content: 'hi', ts: 1,
  })); // 排了一个 ACK timeout
  assert.ok(clock.activeTimers() >= 3);

  session.shutdown();
  assert.equal(clock.activeTimers(), 0, 'ACK 定时器、两个 interval、重连定时器全部清空');
  assert.equal(session.rooms.size, 0, '房间状态被释放');
  assert.equal(session.socket.ws, null);
  assert.equal(ws.listenerCount('close'), 0);

  // 迟到事件 / 重复 shutdown 安全
  assert.doesNotThrow(() => {
    ws.emit('close', {});
    ws.emitMessage('{}');
    session.shutdown();
  });
  clock.advance(10_000);
  assert.equal(MockWebSocket.instances.length, 1, 'shutdown 后永不重连');
});
