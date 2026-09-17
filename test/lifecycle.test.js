'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const { createChatServer, TokenBucket } = require('../src/server');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 轮询直到 pred 成立（服务端 close 清理与测试客户端存在事件先后差，用轮询兜底） */
async function waitUntil(pred, { timeout = 2000, interval = 10 } = {}) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (pred()) return;
    await sleep(interval);
  }
  throw new Error('waitUntil: timed out');
}

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

/** 极简测试客户端：自动消费帧（不干扰），暴露 closed promise */
class Client {
  static async connect(port, token, { reject = false } = {}) {
    const c = new Client();
    c.closed = new Promise((res) => (c._onClosed = res));
    c.frames = [];
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => c.frames.push(JSON.parse(raw.toString())));
    c.ws.on('close', () => c._onClosed());
    if (reject) return c;
    await new Promise((res, rej) => {
      c.ws.once('open', res);
      c.ws.once('error', rej);
    });
    await waitUntil(() => c.frames.some((m) => m.type === 'welcome'));
    return c;
  }

  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  async close() {
    this.ws.close();
    await this.closed;
  }
}

async function createRoom(client, name) {
  client.send({ type: 'create_room', name });
  await waitUntil(() => client.frames.some((m) => m.type === 'joined' && m.name === name));
  return client.frames.find((m) => m.type === 'joined' && m.name === name).roomId;
}

async function joinRoom(client, room, lastSeq = 0) {
  client.send({ type: 'join', room, lastSeq });
  await waitUntil(() => client.frames.some((m) => m.type === 'joined'));
}

async function assertHubEmpty(hub) {
  await waitUntil(() => hub.all.size === 0);
  assert.equal(hub.all.size, 0, 'all 索引应清空');
  assert.equal(hub.byUser.size, 0, 'byUser 索引应清空');
  assert.equal(hub.byRoom.size, 0, 'byRoom 索引应清空');
}

// ---------------------------------------------------------------- 正常关闭路径

test('正常关闭：统一清理索引、房间状态、未 ACK 与 ws 业务监听器', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const c = await Client.connect(port, u.token);
    const roomId = await createRoom(c, 'general');

    assert.equal(server.hub.all.size, 1);
    const conn = [...server.hub.all][0];
    const serverWs = conn.ws;
    assert.ok(serverWs, '关闭前 conn 持有 ws');

    await c.close();
    await assertHubEmpty(server.hub);
    assert.equal(server.wss.clients.size, 0, 'wss.clients 应排空（内部 close 监听器未被误删）');

    // Connection 自身状态
    assert.equal(conn.disposed, true);
    assert.equal(conn.ws, null, 'conn -> ws 引用必须释放');
    assert.equal(conn.listeners, null, '监听器引用必须释放');
    assert.equal(conn.rooms.size, 0, '房间集合必须清空');
    assert.equal(conn.unacked.size, 0);
    assert.equal(conn.unackedCount, 0);
    assert.ok(!conn.rooms.has(roomId));

    // 业务监听器精确摘除，ws 库内部监听器保留
    assert.equal(serverWs.listenerCount('pong'), 0);
    assert.equal(serverWs.listenerCount('message'), 0);
    assert.equal(serverWs.listenerCount('error'), 0);
    assert.equal(serverWs.listenerCount('close'), 1, 'ws 库内部 close 监听器应保留');
  } finally {
    await server.stop();
  }
});

test('Hub.remove 幂等：重复清理不抛错、状态保持为空', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const c = await Client.connect(port, u.token);
    const conn = [...server.hub.all][0];

    server.hub.remove(conn);
    assert.doesNotThrow(() => server.hub.remove(conn)); // 第二次应被 disposed 守卫拦截
    server.hub.disposeAll(); // 空集合也应安全
    await c.close();
    await assertHubEmpty(server.hub);
  } finally {
    await server.stop();
  }
});

// ---------------------------------------------------------------- 高频重连

test('高频重连风暴：每条连接关闭后注册中心与 clients 不累积', async () => {
  const { server, port } = await startServer({ maxConnectionsPerUser: 1000 });
  try {
    const u = await login(port, 'storm');
    const ROUNDS = 60;
    for (let i = 0; i < ROUNDS; i++) {
      const c = await Client.connect(port, u.token);
      assert.equal(server.hub.all.size, 1, '存活连接应恰为 1');
      await c.close();
      await waitUntil(() => server.hub.all.size === 0);
    }
    await assertHubEmpty(server.hub);
    assert.equal(server.wss.clients.size, 0);

    // 再来一轮不等待的快速开合，确认 close 事件最终仍全部收敛
    const conns = await Promise.all(Array.from({ length: 20 }, () => Client.connect(port, u.token)));
    assert.equal(server.hub.all.size, 20);
    await Promise.all(conns.map((c) => c.close()));
    await assertHubEmpty(server.hub);
    assert.equal(server.wss.clients.size, 0);
  } finally {
    await server.stop();
  }
});

// ---------------------------------------------------------------- 服务端主动断开路径

test('心跳超时 terminate 走统一清理', async () => {
  const { server, port } = await startServer({ heartbeatTimeoutMs: 50 });
  try {
    const u = await login(port, 'alice');
    const c = await Client.connect(port, u.token);
    const conn = [...server.hub.all][0];

    conn.lastPong = 0; // 模拟长时间无 pong
    server.hub.heartbeatSweep(); // 应 terminate 并经由 close 事件清理

    await c.closed;
    await assertHubEmpty(server.hub);
    assert.equal(server.wss.clients.size, 0);
  } finally {
    await server.stop();
  }
});

test('ACK 重发超限断开走统一清理', async () => {
  const { server, port } = await startServer({
    ackResendIntervalMs: 20,
    ackResendAfterMs: 1,
    ackMaxResend: 1,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    // 双方均不 ACK：广播为接收方 b 登记 unacked，重发超限后 b 应被断开；
    // 发送方 a 收到的是应用层 ACK 帧（不追踪），主动关闭即可
    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.closed;
    await a.close();
    await assertHubEmpty(server.hub);
    assert.equal(server.wss.clients.size, 0);
  } finally {
    await server.stop();
  }
});

test('背压（未 ACK 积压超限）断开走统一清理', async () => {
  const { server, port } = await startServer({ maxUnackedPerConn: 1 });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'one' });
    a.send({ type: 'msg', roomId, clientMsgId: 'm2', content: 'two' }); // 触发 1013 背压断开
    await Promise.all([a.closed, b.closed]);
    await assertHubEmpty(server.hub);
  } finally {
    await server.stop();
  }
});

test('关闭后的连接不再被扫描器触碰（无重复发送）', async () => {
  const { server, port } = await startServer({
    ackResendIntervalMs: 20,
    ackResendAfterMs: 1,
    ackMaxResend: 100,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await waitUntil(() => b.frames.some((m) => m.type === 'msg' && m.seq === 1));

    await b.close();
    await waitUntil(() => server.hub.all.size === 1);
    // 多轮扫描期间对已移除连接执行不应抛错，且存活集合不再变化
    await sleep(150);
    assert.equal(server.hub.all.size, 1);
    await a.close();
    await assertHubEmpty(server.hub);
  } finally {
    await server.stop();
  }
});

// ---------------------------------------------------------------- 升级失败路径

test('协议/鉴权升级失败：socket 被销毁，无连接残留，后续连接正常', async () => {
  const { server, port } = await startServer();
  try {
    // 错误 token —— 401 拒绝
    const bad = await Client.connect(port, 'not-a-valid-token', { reject: true });
    await new Promise((res) => bad.ws.once('error', res));
    bad.ws.terminate();
    await sleep(50);
    assert.equal(server.hub.all.size, 0);
    assert.equal(server.wss.clients.size, 0);

    // 非升级路径的错误 URL 由 HTTP 层返回 404，服务不崩
    const res = await fetch(`http://127.0.0.1:${port}/wrong?token=x`, { headers: { connection: 'close' } });
    assert.equal(res.status, 404);

    // 准入拒绝后服务仍可用
    const u = await login(port, 'alice');
    const c = await Client.connect(port, u.token);
    assert.equal(server.hub.all.size, 1);
    await c.close();
    await assertHubEmpty(server.hub);
  } finally {
    await server.stop();
  }
});

test('单用户连接数超限拒绝后不残留 socket', async () => {
  const { server, port } = await startServer({ maxConnectionsPerUser: 1 });
  try {
    const u = await login(port, 'alice');
    const c1 = await Client.connect(port, u.token);
    const rejected = await Client.connect(port, u.token, { reject: true });
    await new Promise((res) => rejected.ws.once('error', res));
    rejected.ws.terminate();
    await sleep(50);
    assert.equal(server.hub.all.size, 1, '只有一条合法连接');
    assert.equal(server.wss.clients.size, 1);
    await c1.close();
    await assertHubEmpty(server.hub);
  } finally {
    await server.stop();
  }
});

// ---------------------------------------------------------------- 服务停止

test('stop()：通知并清理全部连接，定时器全部停止，资源关闭', async () => {
  let heartbeats = 0;
  const { server, port } = await startServer({ heartbeatIntervalMs: 25 });
  try {
    server.hub.heartbeatSweep = () => { heartbeats++; }; // 定时器回调动态查找方法
    const u = await login(port, 'alice');
    const c = await Client.connect(port, u.token);
    await waitUntil(() => heartbeats > 0);

    let gotShutdown = false;
    c.ws.on('message', (raw) => {
      if (JSON.parse(raw.toString()).type === 'server_shutdown') gotShutdown = true;
    });

    await server.stop();
    await c.closed;
    assert.equal(gotShutdown, true, '停止前应下发 server_shutdown');
    assert.equal(server.httpServer.listening, false, 'HTTP 服务应关闭');
    assert.equal(server.hub.all.size, 0);
    assert.equal(server.wss.clients.size, 0);

    const before = heartbeats;
    await sleep(100); // 4 个心跳周期
    assert.equal(heartbeats, before, '停止后心跳定时器不应再触发');
  } finally {
    await server.stop(); // 幂等：失败路径也保证关停
  }
});

test('stop() 幂等：重复调用返回且不抛错', async () => {
  const { server } = await startServer();
  try {
    const p1 = server.stop();
    const p2 = server.stop();
    assert.equal(p1, p2, '重复 stop 应复用同一 Promise');
    await Promise.all([p1, p2]);
    assert.equal(server.httpServer.listening, false);
  } finally {
    await server.stop();
  }
});

test('停止后可以同 dbPath 立即重启（数据库已释放）', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-restart-'));
  const dbPath = path.join(dir, 'test.db');
  try {
    const s1 = await startServer({ dbPath });
    await s1.server.stop();
    const s2 = await startServer({ dbPath });
    const u = await login(s2.port, 'alice');
    const c = await Client.connect(s2.port, u.token);
    await c.close();
    await s2.server.stop();
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------- 限流映射表

test('TokenBucket.pruneStale：摘除空闲桶，活跃桶保留', () => {
  const b = new TokenBucket(1, 10);
  assert.equal(b.take('active'), true);
  assert.equal(b.take('idle'), true);
  const idle = b.buckets.get('idle');
  idle.updated = Date.now() - 10_000;
  b.pruneStale(5_000);
  assert.ok(b.buckets.has('active'));
  assert.ok(!b.buckets.has('idle'));
});

test('限流器空闲桶被定时清理，服务停止时映射表清空', async () => {
  const { server, port } = await startServer({
    rateLimitPruneIntervalMs: 20,
    rateLimitPruneIdleMs: 3_600_000, // 初始不清理，先稳定观测桶存在
  });
  try {
    const u = await login(port, 'alice');
    const c = await Client.connect(port, u.token);
    const roomId = await createRoom(c, 'general');
    c.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await waitUntil(() => server.limiter.buckets.has(u.userId));

    // 老化该桶，再把空闲阈值调低，下一个清理周期必然摘除
    server.limiter.buckets.get(u.userId).updated = Date.now() - 10_000;
    server.config.rateLimitPruneIdleMs = 1;
    await waitUntil(() => !server.limiter.buckets.has(u.userId));

    await c.close();
    assert.equal(server.limiter.buckets.size, 0);
  } finally {
    await server.stop();
    assert.equal(server.limiter.buckets.size, 0, 'stop 应清空限流映射表');
  }
});
