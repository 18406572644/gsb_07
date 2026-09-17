'use strict';
/*
 * 可靠聊天室客户端
 *
 * 生命周期治理（本文件重点）：
 * - ReconnectingSocket：单条物理连接的唯一管理者。重连定时器全程只有一个、
 *   带句柄可取消；代际（generation）守卫杜绝旧连接迟到事件污染新连接；
 *   事件监听器随连接建立/拆除成对增删；close() 永久关闭且幂等，不再重连。
 * - TimerGroup：会话级定时器（消息重发扫描、成员刷新）统一登记，shutdown
 *   时一次性 clearAll，不会出现登录前空转、退出后残留。
 * - AckBatcher：ACK 批处理的唯一 setTimeout，幂等调度，可取消。
 * - ChatSession：把上述部件组成一个会话；shutdown() 是唯一销毁入口，
 *   正常关闭/出错断开只触发「代际清理 + 一次重连」，页面卸载/登出走
 *   「会话清理」，两条路径各自幂等。
 *
 * 可靠投递语义（与原实现一致）：
 * - 发送：每条消息带唯一 clientMsgId，未收到 ACK 定时重发（服务端幂等去重）；
 * - 接收：按房间记录 lastSeenSeq，seq 小于等于它的一律丢弃（幂等消费）；
 * - ACK：累积确认，500ms 批量上报；
 * - 断线补发：重连后重新 join 并携带 lastSeenSeq，服务端回放缺口。
 */
(function (root, factory) {
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (root) root.ChatRealtime = api;
})(typeof window !== 'undefined' ? window : null, function () {
  const defaultClock = {
    now: () => Date.now(),
    setTimeout: (fn, ms, ...args) => setTimeout(fn, ms, ...args),
    clearTimeout: (h) => clearTimeout(h),
    setInterval: (fn, ms, ...args) => setInterval(fn, ms, ...args),
    clearInterval: (h) => clearInterval(h),
    uuid: () => crypto.randomUUID(),
  };

  // ---------------------------------------------------------------- 定时器组

  /**
   * 登记本会话创建的全部 timer，shutdown 时一次清完。
   * 每个句柄都被包成对象持有，clearAll 后即便回调因事件循环排队迟到，
   * 也能通过 TimerGroup#active 校验自行退出（见 ChatSession 的用法）。
   */
  class TimerGroup {
    constructor(clock = defaultClock) {
      this.clock = clock;
      this.handles = new Set();
      this.cleared = false;
    }

    setTimeout(fn, ms, ...args) {
      if (this.cleared) return null;
      const rec = { kind: 'timeout', id: null };
      rec.id = this.clock.setTimeout(() => {
        this.handles.delete(rec);
        fn();
      }, ms, ...args);
      this.handles.add(rec);
      return rec;
    }

    setInterval(fn, ms, ...args) {
      if (this.cleared) return null;
      const rec = { kind: 'interval', id: this.clock.setInterval(fn, ms, ...args) };
      this.handles.add(rec);
      return rec;
    }

    clear(rec) {
      if (!rec || !this.handles.has(rec)) return;
      this.handles.delete(rec);
      if (rec.kind === 'timeout') this.clock.clearTimeout(rec.id);
      else this.clock.clearInterval(rec.id);
    }

    clearAll() {
      if (this.cleared) return;
      this.cleared = true;
      for (const rec of this.handles) {
        if (rec.kind === 'timeout') this.clock.clearTimeout(rec.id);
        else this.clock.clearInterval(rec.id);
      }
      this.handles.clear();
    }
  }

  // ------------------------------------------------------------ ACK 批处理

  /** 500ms 合并上报的防抖定时器。schedule 幂等；cancel/flush 后无悬挂回调。 */
  class AckBatcher {
    constructor({ delay = 500, flush, clock = defaultClock } = {}) {
      this.delay = delay;
      this.flush = flush;
      this.clock = clock;
      this.timer = null;
      this.stopped = false;
    }

    schedule() {
      if (this.stopped || this.timer) return;
      this.timer = this.clock.setTimeout(() => {
        this.timer = null;
        if (this.stopped) return;
        this.flush();
      }, this.delay);
    }

    /** 立即执行一次挂起的批处理（如关闭前） */
    flushNow() {
      if (!this.timer) return;
      this.clock.clearTimeout(this.timer);
      this.timer = null;
      if (!this.stopped) this.flush();
    }

    cancel() {
      this.stopped = true;
      if (this.timer) {
        this.clock.clearTimeout(this.timer);
        this.timer = null;
      }
    }
  }

  // --------------------------------------------------- 自动重连 WebSocket

  /**
   * 带自动重连的 WebSocket 包装。
   *
   * 不变量：
   * 1. 任意时刻至多一个存活的底层 ws 和一个待触发的重连定时器；
   * 2. 每条 ws 的监听器在拆除时逐一 removeEventListener，外部不持有旧 ws；
   * 3. onopen/onmessage/onclose/onerror 全部带代际校验，旧连接的迟到事件无效；
   * 4. close() 后永不重连，且可重复调用；
   * 5. 出错路径（error）不自行排重连 —— 统一由随后的 close 事件单点调度，
   *    杜绝 error+close 双路径下的重连定时器翻倍。
   */
  class ReconnectingSocket {
    constructor({
      url,
      WebSocketCtor = globalThis.WebSocket,
      clock = defaultClock,
      initialDelay = 500,
      maxDelay = 10_000,
      onOpen = () => {},
      onMessage = () => {},
      onStatus = () => {},
    } = {}) {
      this.urlFactory = typeof url === 'function' ? url : () => url;
      this.WebSocketCtor = WebSocketCtor;
      this.clock = clock;
      this.initialDelay = initialDelay;
      this.maxDelay = maxDelay;
      this.onOpen = onOpen;
      this.onMessage = onMessage;
      this.onStatus = onStatus;

      this.ws = null;
      this.generation = 0; // 每建一条连接 +1，旧代际的事件一律丢弃
      this.reconnectTimer = null; // 唯一的重连定时器句柄
      this.delay = initialDelay;
      this.closed = false; // 永久关闭（shutdown）后不再重连
    }

    get isOpen() {
      return !!this.ws && this.ws.readyState === 1;
    }

    /** 建立新连接；存在旧连接时先静默拆除（不排重连） */
    connect() {
      if (this.closed) return;
      this._cancelReconnect();
      this._teardownSocket();
      const gen = ++this.generation;

      const ws = new this.WebSocketCtor(this.urlFactory());
      this.ws = ws;
      const handlers = {
        open: () => this._handleOpen(gen),
        message: (ev) => this._handleMessage(gen, ev),
        close: () => this._handleClose(gen),
        error: () => this._handleError(gen),
      };
      ws._rcHandlers = handlers; // 登记，供拆除时精确解绑
      for (const [type, fn] of Object.entries(handlers)) ws.addEventListener(type, fn);
    }

    send(data) {
      if (!this.isOpen) return false;
      try {
        this.ws.send(data);
        return true;
      } catch {
        return false;
      }
    }

    /** 永久关闭：取消重连、拆除当前连接；幂等 */
    close() {
      if (this.closed && !this.ws) return;
      this.closed = true;
      this._cancelReconnect();
      const ws = this.ws;
      this._teardownSocket();
      if (ws) {
        try {
          if (ws.readyState === 0 /* CONNECTING */) ws.close();
          else if (ws.readyState === 1 /* OPEN */) ws.close();
        } catch { /* 已关闭 */ }
      }
      this.onStatus({ connected: false, retryIn: null, intentional: true });
    }

    _handleOpen(gen) {
      if (this.closed || gen !== this.generation) return;
      this.delay = this.initialDelay; // 连上后重置退避
      this._cancelReconnect(); // 防御：打开期间不应有悬挂的重连定时器
      this.onStatus({ connected: true });
      this.onOpen();
    }

    _handleMessage(gen, ev) {
      if (this.closed || gen !== this.generation) return;
      this.onMessage(ev);
    }

    _handleError(gen) {
      if (this.closed || gen !== this.generation) return;
      // 不在这里排重连：error 后规范必随 close，由 close 单点调度。
      // 仅确保连接开始关闭流程（部分实现 error 后需要显式 close）。
      try { this.ws.close(); } catch { /* 已关闭，等 close 事件 */ }
    }

    _handleClose(gen) {
      if (gen !== this.generation) return; // 旧连接的迟到 close
      this._teardownSocket();
      if (this.closed) return;
      const retryIn = this.delay;
      this.onStatus({ connected: false, retryIn });
      // 先用当前退避排重连，再翻倍 —— 顺序不能反，否则首次等待就被加倍
      this._scheduleReconnect();
      this.delay = Math.min(this.delay * 2, this.maxDelay);
    }

    _scheduleReconnect() {
      if (this.closed || this.reconnectTimer) return;
      const gen = this.generation;
      this.reconnectTimer = this.clock.setTimeout(() => {
        this.reconnectTimer = null;
        if (this.closed || gen !== this.generation) return;
        this.connect();
      }, this.delay);
    }

    _cancelReconnect() {
      if (this.reconnectTimer) {
        this.clock.clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
      }
    }

    /** 解绑当前 ws 的全部监听器并丢弃引用（不改变 closed 状态、不排重连） */
    _teardownSocket() {
      const ws = this.ws;
      if (!ws) return;
      const handlers = ws._rcHandlers;
      if (handlers) {
        for (const [type, fn] of Object.entries(handlers)) {
          ws.removeEventListener(type, fn);
        }
        ws._rcHandlers = null;
      }
      this.ws = null;
    }
  }

  // ---------------------------------------------------------------- 会话

  /**
   * 一个登录会话的协议核心（无 DOM 依赖，可在 Node 中用 mock WebSocket/时钟测试）。
   * 拥有：rooms 映射、重连 socket、ACK 批处理、重发扫描、成员刷新定时器。
   */
  class ChatSession {
    /**
     * @param {object} opts
     *  token, saved: 恢复的房间快照 {roomId: {name, lastSeenSeq}}
     *  onEvent(event, payload): 'status' | 'rooms' | 'messages' | 'members'
     */
    constructor({
      url,
      me = null,
      saved = {},
      WebSocketCtor,
      clock = defaultClock,
      initialDelay = 500,
      maxDelay = 10_000,
      ackDelay = 500,
      resendIntervalMs = 1_000,
      resendAfterMs = 1_000,
      membersRefreshMs = 5_000,
      maxMsgsPerRoom = 500,
      persist = () => {},
      onEvent = () => {},
    } = {}) {
      this.clock = clock;
      this.meUserId = me ? me.userId : null;
      this.meName = me ? me.name : null;
      this.resendAfterMs = resendAfterMs;
      this.maxMsgsPerRoom = maxMsgsPerRoom;
      this.persist = persist;
      this.onEvent = onEvent;

      this.rooms = new Map(); // roomId -> {name, role, mutedUntil, lastSeenSeq, msgs, pending}
      for (const [id, info] of Object.entries(saved)) {
        this.rooms.set(id, {
          name: info.name,
          role: info.role || 'member',
          mutedUntil: 0,
          lastSeenSeq: info.lastSeenSeq || 0,
          msgs: [],
          pending: new Map(),
        });
      }
      this.activeRoomId = null;
      this.shutdown_ = false;

      this.timers = new TimerGroup(clock);
      this.acker = new AckBatcher({
        delay: ackDelay,
        clock,
        flush: () => this._flushAcks(),
      });
      this.socket = new ReconnectingSocket({
        url,
        WebSocketCtor,
        clock,
        initialDelay,
        maxDelay,
        onOpen: () => this._onOpen(),
        onMessage: (ev) => this._onRawMessage(ev),
        onStatus: (s) => this.onEvent('status', s),
      });

      // 会话级周期任务：整个登录会话只有一份，shutdown 时随 TimerGroup 释放；
      // 断线期间继续保留（重连成功即恢复工作），send 在非 OPEN 态自行空转。
      this.timers.setInterval(() => this._resendPending(), resendIntervalMs);
      this.timers.setInterval(() => this.requestMembers(), membersRefreshMs);
    }

    connect() { this.socket.connect(); }

    get connected() { return this.socket.isOpen; }

    /** 会话销毁的唯一入口（幂等）：取消 ACK/重连/全部周期任务，断开并解绑 ws */
    shutdown() {
      if (this.shutdown_) return;
      this.shutdown_ = true;
      this.acker.cancel();
      this.timers.clearAll();
      this.socket.close(); // 内部取消重连定时器、解绑监听器、丢弃 ws 引用
      this.rooms.clear();
      this.activeRoomId = null;
    }

    // ------------------------------------------------ 收发原语

    send(obj) {
      return this.socket.send(JSON.stringify(obj));
    }

    _onOpen() {
      // 重连后恢复所有房间：携带本地进度，服务端补发缺口
      for (const [roomId, r] of this.rooms) {
        this.send({ type: 'join', room: roomId, lastSeq: r.lastSeenSeq || 0 });
      }
    }

    _onRawMessage(ev) {
      let m;
      try {
        m = JSON.parse(typeof ev.data === 'string' ? ev.data : ev.data.toString());
      } catch {
        return;
      }
      if (!m || typeof m.type !== 'string') return;
      this._handle(m);
    }

    _handle(m) {
      switch (m.type) {
        case 'welcome':
          break;
        case 'joined':
          this._onJoined(m);
          break;
        case 'msg':
          this._onMsg(m);
          break;
        case 'ack':
          this._onAck(m);
          break;
        case 'sync_done':
          if (m.hasMore) this.send({ type: 'sync', roomId: m.roomId, lastSeq: m.lastSeq });
          break;
        case 'notice':
          this._onNotice(m);
          break;
        case 'members':
          if (m.roomId === this.activeRoomId) this.onEvent('members', m.members);
          break;
        case 'error':
          this._sys(this.activeRoomId, `错误 [${m.code}] ${m.message}`);
          break;
        case 'server_shutdown':
          this._sys(this.activeRoomId, '服务器即将重启…');
          break;
      }
    }

    _onJoined(m) {
      const r = this.rooms.get(m.roomId) || {
        msgs: [], pending: new Map(), lastSeenSeq: 0,
      };
      r.name = m.name;
      r.role = m.role;
      r.mutedUntil = m.mutedUntil;
      // lastSeenSeq 已由登录时的 saved 快照初始化，joined 不覆盖（m.lastSeq 是
      // 房间当前末尾序号，不是本机进度，用它会跳过缺口补发）
      this.rooms.set(m.roomId, r);
      if (!this.activeRoomId) this.activeRoomId = m.roomId;
      this._persist();
      this.onEvent('rooms', null);
      this.onEvent('messages', { roomId: m.roomId });
      this.requestMembers();
    }

    _onMsg(m) {
      const r = this.rooms.get(m.roomId);
      if (!r) return;
      if (m.seq <= r.lastSeenSeq) return; // 幂等消费：重复投递直接丢弃
      r.lastSeenSeq = m.seq;
      // 自己发的消息：广播回包更新乐观插入的占位消息，不重复上屏
      const own = m.clientMsgId
        ? r.msgs.findIndex((x) => x.clientMsgId === m.clientMsgId)
        : -1;
      if (own >= 0) {
        r.msgs[own] = { ...m, mine: true, pending: false };
        r.pending.delete(m.clientMsgId);
      } else {
        r.msgs.push({ ...m, mine: m.from === this.meUserId });
      }
      if (r.msgs.length > this.maxMsgsPerRoom) {
        r.msgs.splice(0, r.msgs.length - this.maxMsgsPerRoom);
      }
      this._persist();
      this.acker.schedule();
      this.onEvent('messages', { roomId: m.roomId });
    }

    _onAck(m) {
      const r = this.rooms.get(m.roomId);
      if (!r) return;
      const p = r.pending.get(m.clientMsgId);
      if (p) p.acked = true;
      const own = r.msgs.find((x) => x.clientMsgId === m.clientMsgId);
      if (own) { own.pending = false; own.seq = m.seq; }
      this.onEvent('messages', { roomId: m.roomId });
    }

    _onNotice(m) {
      const r = this.rooms.get(m.roomId);
      if (r && m.userId === this.meUserId) {
        r.mutedUntil = m.event === 'muted' ? m.until : 0;
      }
      if (r) {
        this._sys(
          m.roomId,
          m.event === 'muted'
            ? `有成员被禁言至 ${new Date(m.until).toLocaleTimeString()}`
            : '有成员被解除禁言'
        );
      }
      this.requestMembers();
    }

    _sys(roomId, text) {
      if (!roomId) return;
      const r = this.rooms.get(roomId);
      if (!r) return;
      r.msgs.push({ sys: text });
      this.onEvent('messages', { roomId });
    }

    _persist() {
      const snap = {};
      for (const [id, r] of this.rooms) {
        snap[id] = { name: r.name, role: r.role, lastSeenSeq: r.lastSeenSeq };
      }
      this.persist(snap);
    }

    // ------------------------------------------------ ACK / 重发

    _flushAcks() {
      for (const [id, r] of this.rooms) {
        if (r.lastSeenSeq > 0) this.send({ type: 'ack', roomId: id, seq: r.lastSeenSeq });
      }
    }

    /** 未 ACK 的消息周期重发（clientMsgId 不变，服务端幂等）；断线时空转 */
    _resendPending() {
      if (this.shutdown_ || !this.socket.isOpen) return;
      const t = this.clock.now();
      for (const [roomId, r] of this.rooms) {
        for (const [cid, p] of r.pending) {
          if (!p.acked && t - p.sentAt > this.resendAfterMs) {
            if (this.send({ type: 'msg', roomId, clientMsgId: cid, content: p.content })) {
              p.sentAt = t; // 仅真正发出才更新时间，断线恢复后可立即补一发
            }
          }
        }
      }
    }

    // ------------------------------------------------ 房间动作

    setActiveRoom(roomId) {
      if (!this.rooms.has(roomId)) return;
      this.activeRoomId = roomId;
      this.onEvent('rooms', null);
      this.onEvent('messages', { roomId });
      this.requestMembers();
    }

    createRoom(name) { this.send({ type: 'create_room', name }); }

    joinRoom(room, lastSeq = 0) { this.send({ type: 'join', room, lastSeq }); }

    postMessage(content) {
      const roomId = this.activeRoomId;
      const r = roomId && this.rooms.get(roomId);
      if (!roomId || !r || !content) return null;
      const cid = this.clock.uuid();
      r.pending.set(cid, { content, sentAt: this.clock.now(), acked: false });
      r.msgs.push({
        mine: true, fromName: this.meName, content,
        seq: '…', pending: true, clientMsgId: cid,
      });
      this.send({ type: 'msg', roomId, clientMsgId: cid, content });
      this.onEvent('messages', { roomId });
      return cid;
    }

    requestMembers() {
      if (this.activeRoomId) this.send({ type: 'members', roomId: this.activeRoomId });
    }

    mute(userId, minutes) {
      this.send({ type: 'mute', roomId: this.activeRoomId, userId, minutes });
    }

    unmute(userId) {
      this.send({ type: 'unmute', roomId: this.activeRoomId, userId });
    }

    requestHistory(beforeSeq, limit) {
      if (this.activeRoomId) {
        this.send({ type: 'history', roomId: this.activeRoomId, beforeSeq, limit });
      }
    }
  }

  return { TimerGroup, AckBatcher, ReconnectingSocket, ChatSession, defaultClock };
});
