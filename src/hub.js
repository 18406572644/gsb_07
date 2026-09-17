'use strict';

const { now } = require('./util');

let nextConnId = 1;

/**
 * 单条连接的运行时状态。
 * unacked: Map<roomId, Map<seq, {frame, lastSent, tries}>> —— 已推送但未被客户端
 * 累积 ACK 确认的消息，超时重发；这是「至少一次投递」的服务端正，配合客户端
 * 按 seq 去重（幂等消费）达到效果上的恰好一次。
 *
 * 生命周期：连接的全部清理只允许走 {@link Hub#remove} 一次（destroyed 守卫）。
 * 连接在 upgrade 成功回调里注册监听器；无论正常 close、error 后随附的 close、
 * 心跳 terminate、stop() 批量断开，最终都汇聚到同一个清理入口。
 */
class Connection {
  constructor(ws, user) {
    this.id = nextConnId++;
    this.ws = ws;
    this.userId = user.id;
    this.name = user.name;
    this.connectedAt = now();
    this.lastPong = now(); // 最近一次收到 pong 的时间，心跳判活依据
    this.rooms = new Set(); // 本连接已加入的房间
    this.unacked = new Map();
    this.unackedCount = 0;
    this.destroyed = false; // 清理守卫：保证全生命周期只清理一次
    // 与本连接绑定的事件监听器，关闭时逐一解绑（避免闭包长期持有 conn/ws）
    this._listeners = null;
  }

  /**
   * 绑定并记录 ws 事件监听器。必须在连接加入 Hub 后、welcome 之前调用一次。
   * 显式登记而非匿名内联，是为了关闭时能用同一函数引用 removeListener 精确解绑。
   */
  bindListeners(handlers) {
    this._listeners = Object.entries(handlers).map(([event, fn]) => {
      const listener = (...args) => {
        // 已清理的连接上迟到的事件（close 之后的 error/pong 等）一律忽略
        if (this.destroyed) return;
        fn(...args);
      };
      this.ws.on(event, listener);
      return { event, listener };
    });
  }

  /** 解绑全部监听器，清空房间/未确认映射，释放底层 ws 引用，并置位销毁守卫 */
  release() {
    if (this._listeners) {
      for (const { event, listener } of this._listeners) {
        this.ws.removeListener(event, listener);
      }
      this._listeners = null;
    }
    this.rooms.clear();
    this.unacked.clear();
    this.unackedCount = 0;
    this.lastPong = this.connectedAt;
    this.ws = null; // 断开对 WebSocket 对象的最后引用，使其可被 GC
    this.destroyed = true;
  }

  trackUnacked(roomId, seq, frame) {
    if (this.destroyed) return;
    let room = this.unacked.get(roomId);
    if (!room) {
      room = new Map();
      this.unacked.set(roomId, room);
    }
    room.set(seq, { frame, lastSent: now(), tries: 0 });
    this.unackedCount++;
  }

  /** 累积 ACK：清除 roomId 下所有 seq <= ackSeq 的未确认项，返回新确认的数量 */
  ack(roomId, ackSeq) {
    if (this.destroyed) return 0;
    const room = this.unacked.get(roomId);
    if (!room) return 0;
    let cleared = 0;
    for (const seq of room.keys()) {
      if (seq <= ackSeq) {
        room.delete(seq);
        cleared++;
      }
    }
    if (room.size === 0) this.unacked.delete(roomId);
    this.unackedCount -= cleared;
    return cleared;
  }

  /** 摘出所有超时未确认、需要重发的条目 */
  *pendingResends(staleMs) {
    if (this.destroyed) return;
    const t = now();
    for (const room of this.unacked.values()) {
      for (const entry of room.values()) {
        if (t - entry.lastSent >= staleMs) yield entry;
      }
    }
  }
}

/**
 * 连接注册中心：全局/按用户/按房间的连接索引，广播，心跳与重发扫描。
 */
class Hub {
  constructor(config) {
    this.config = config;
    this.all = new Set(); // 全部连接
    this.byUser = new Map(); // userId -> Set<Connection>
    this.byRoom = new Map(); // roomId -> Set<Connection>
  }

  /** 准入控制：全局上限 + 单用户上限。返回 null 表示可接入，否则返回拒绝原因码。 */
  checkAdmission(userId) {
    if (this.all.size >= this.config.maxConnections) return 'SERVER_FULL';
    const mine = this.byUser.get(userId);
    if (mine && mine.size >= this.config.maxConnectionsPerUser) return 'TOO_MANY_DEVICES';
    return null;
  }

  add(conn) {
    this.all.add(conn);
    let set = this.byUser.get(conn.userId);
    if (!set) {
      set = new Set();
      this.byUser.set(conn.userId, set);
    }
    set.add(conn);
  }

  /**
   * 连接的唯一清理入口（幂等）。
   * 正常 close、ws error 后随附的 close、心跳/重发超时 terminate、stop() 批量
   * 断开都汇聚于此；destroyed 守卫保证任何路径下清理只执行一次。
   */
  remove(conn) {
    if (!conn || conn.destroyed) return;
    if (!this.all.has(conn)) {
      // 未登记（upgrade 回调中 add 之前抛错才可能出现）：仅释放连接自身资源
      conn.release();
      return;
    }
    this.all.delete(conn);
    const mine = this.byUser.get(conn.userId);
    if (mine) {
      mine.delete(conn);
      if (mine.size === 0) this.byUser.delete(conn.userId);
    }
    for (const roomId of conn.rooms) this._leaveRoomSet(roomId, conn);
    conn.release();
  }

  joinRoom(conn, roomId) {
    if (conn.destroyed) return;
    let set = this.byRoom.get(roomId);
    if (!set) {
      set = new Set();
      this.byRoom.set(roomId, set);
    }
    set.add(conn);
    conn.rooms.add(roomId);
  }

  leaveRoom(conn, roomId) {
    if (conn.destroyed) return;
    this._leaveRoomSet(roomId, conn);
    conn.rooms.delete(roomId);
    const room = conn.unacked.get(roomId);
    if (room) {
      conn.unackedCount -= room.size;
      conn.unacked.delete(roomId);
    }
  }

  _leaveRoomSet(roomId, conn) {
    const set = this.byRoom.get(roomId);
    if (set) {
      set.delete(conn);
      if (set.size === 0) this.byRoom.delete(roomId);
    }
  }

  /** 房间内在线用户 ID 列表（去重） */
  onlineUserIds(roomId) {
    const set = this.byRoom.get(roomId);
    if (!set) return [];
    return [...new Set([...set].map((c) => c.userId))];
  }

  /**
   * 发送单帧到指定连接。track=true 时登记未 ACK 追踪（用于 msg 类帧）。
   * 背压：未确认积压超过上限时断开连接（客户端重连后走 sync 补发）。
   */
  send(conn, frame, { track = false, roomId = null, seq = null } = {}) {
    if (conn.destroyed || !conn.ws || conn.ws.readyState !== 1 /* OPEN */) return false;
    if (track && conn.unackedCount >= this.config.maxUnackedPerConn) {
      conn.ws.close(1013, 'backpressure: too many unacked messages');
      return false;
    }
    const str = typeof frame === 'string' ? frame : JSON.stringify(frame);
    try {
      conn.ws.send(str);
    } catch {
      return false;
    }
    if (track && roomId != null && seq != null) conn.trackUnacked(roomId, seq, str);
    return true;
  }

  /** 广播到房间所有连接（含发送者的其他设备）。frame 只序列化一次。 */
  broadcast(roomId, frame, { track = false, seq = null } = {}) {
    const set = this.byRoom.get(roomId);
    if (!set) return 0;
    const str = JSON.stringify(frame);
    let delivered = 0;
    for (const conn of [...set]) {
      // 拷贝遍历：背压 close 可能异步改动 set，且跳过已清理连接
      if (this.send(conn, str, { track, roomId, seq })) delivered++;
    }
    return delivered;
  }

  /** 心跳扫描：超时未 pong 的连接直接 terminate（触发 close 走统一清理） */
  heartbeatSweep() {
    const t = now();
    for (const conn of [...this.all]) {
      if (conn.destroyed || !conn.ws) continue;
      if (t - conn.lastPong > this.config.heartbeatTimeoutMs) {
        try { conn.ws.terminate(); } catch { /* 已关闭，等待 close 回调 */ }
        continue;
      }
      try {
        conn.ws.ping();
      } catch { /* 连接已损坏，等待 close 事件清理 */ }
    }
  }

  /** 重发扫描：超时未 ACK 的消息重发；超过最大重发次数判定连接不可用，断开让客户端重连补发 */
  resendSweep() {
    const { ackResendAfterMs, ackMaxResend } = this.config;
    for (const conn of [...this.all]) {
      if (conn.destroyed || !conn.ws) continue;
      for (const entry of conn.pendingResends(ackResendAfterMs)) {
        entry.tries++;
        if (entry.tries > ackMaxResend) {
          try { conn.ws.close(1011, 'ack timeout'); } catch { /* 已关闭 */ }
          break;
        }
        if (conn.ws.readyState === 1) {
          try {
            conn.ws.send(entry.frame);
            entry.lastSent = now();
          } catch { /* 下一轮再处理 */ }
        }
      }
    }
  }

  stats() {
    return {
      connections: this.all.size,
      users: this.byUser.size,
      rooms: this.byRoom.size,
    };
  }
}

module.exports = { Hub, Connection };
