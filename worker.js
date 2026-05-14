/**
 * Cuven Chat · Cloudflare Worker 后端
 * ============================================
 * 架构：
 *   - 每个聊天房间是一个 Durable Object 实例（按 roomId 路由）
 *   - 用 WebSocket Hibernation API（DO 可休眠，连接成本几乎为 0）
 *   - 房间内最近 200 条消息持久化到 DO storage，新加入者会拉到历史
 *
 * 路由：
 *   GET  /ws?room=ROOMID&name=...&avatar=...&email=...&clientId=...
 *        Upgrade 到 WebSocket，加入房间
 *   GET  /api/history?room=ROOMID
 *        拉取历史消息（200 条上限），用于 HTTP 兜底
 *   GET  /api/health
 *        健康检查
 *   OPTIONS *
 *        CORS 预检
 *
 * 消息协议（客户端 → 服务器）：
 *   { type:'msg', mode:'text'|'html'|'location', content:string|{lng,lat} }
 *
 * 消息协议（服务器 → 客户端）：
 *   { type:'history',  messages:[...]  }   // 加入后第一帧
 *   { type:'msg',      id, clientId, name, avatar, mode, content, ts }
 *   { type:'presence', count, joined?, left? }
 *
 * 部署：
 *   1. wrangler deploy
 *   2. 把 opop_1.html 顶部的 CHAT_API 改成你的 Worker 域名（如 https://chat.cuven.us）
 */

// ─────────────────────────────────────────────────────────────
// Durable Object：聊天房间
// ─────────────────────────────────────────────────────────────
export class ChatRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // sessions: WebSocket → { id, clientId, name, avatar, email }
    // 注意：Hibernation 唤醒后此 Map 是空的，需从 getWebSockets() 恢复
    this.sessions = new Map();
    for (const ws of this.state.getWebSockets()) {
      try {
        const meta = ws.deserializeAttachment();
        if (meta) this.sessions.set(ws, meta);
      } catch (_) { /* 忽略已死的 ws */ }
    }
  }

  // ─── 路由 ────────────────────────────────────────────────
  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === '/ws') {
      return this.handleWebSocketUpgrade(request, url);
    }

    if (url.pathname === '/history') {
      const messages = (await this.state.storage.get('messages')) || [];
      return Response.json({ messages });
    }

    return new Response('Not found', { status: 404 });
  }

  // ─── WebSocket 升级 ──────────────────────────────────────
  async handleWebSocketUpgrade(request, url) {
    const upgrade = request.headers.get('Upgrade');
    if (upgrade !== 'websocket') {
      return new Response('Expected websocket', { status: 426 });
    }

    const name     = (url.searchParams.get('name')   || '').slice(0, 64) || '匿名';
    const avatar   = (url.searchParams.get('avatar') || '').slice(0, 500);
    const email    = (url.searchParams.get('email')  || '').slice(0, 200);
    const clientId = (url.searchParams.get('clientId') || crypto.randomUUID()).slice(0, 64);

    const meta = {
      id: crypto.randomUUID(),  // 服务端 session id
      clientId,                 // 客户端自己的标识（用于回声去重）
      name, avatar, email,
      joinedAt: Date.now(),
    };

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // 关键：用 acceptWebSocket() 而不是 server.accept()，
    // 这样 DO 在没有事件时可以休眠，按访问量计费而不是按连接时长。
    this.state.acceptWebSocket(server);
    server.serializeAttachment(meta);
    this.sessions.set(server, meta);

    // 给新加入者发历史消息
    const history = (await this.state.storage.get('messages')) || [];
    try {
      server.send(JSON.stringify({ type: 'history', messages: history }));
      server.send(JSON.stringify({
        type: 'presence',
        count: this.sessions.size,
        joined: { name, avatar },
      }));
    } catch (_) { /* 客户端可能瞬间断开 */ }

    // 广播 join 通知（不含自己）
    this.broadcast({
      type: 'presence',
      count: this.sessions.size,
      joined: { name, avatar },
    }, server);

    return new Response(null, { status: 101, webSocket: client });
  }

  // ─── Hibernation 回调：收到消息 ──────────────────────────
  async webSocketMessage(ws, raw) {
    let meta = this.sessions.get(ws);
    if (!meta) {
      try { meta = ws.deserializeAttachment(); } catch (_) { meta = null; }
      if (meta) this.sessions.set(ws, meta);
    }
    if (!meta) {
      try { ws.close(1011, 'no session'); } catch (_) {}
      return;
    }

    let data;
    try { data = JSON.parse(typeof raw === 'string' ? raw : new TextDecoder().decode(raw)); }
    catch { return; }

    if (data?.type === 'msg') {
      const mode = (data.mode === 'html' || data.mode === 'location') ? data.mode : 'text';

      // 内容校验 & 截断
      let content;
      if (mode === 'location') {
        const lng = Number(data.content?.lng);
        const lat = Number(data.content?.lat);
        if (!Number.isFinite(lng) || !Number.isFinite(lat)) return;
        if (lng < -180 || lng > 180 || lat < -90 || lat > 90) return;
        content = { lng, lat };
      } else {
        content = String(data.content ?? '').slice(0, 4000);
        if (!content.trim()) return;
      }

      const msg = {
        type: 'msg',
        id: crypto.randomUUID(),
        clientId: meta.clientId,    // 用于客户端识别"自己发的"做回声去重
        name: meta.name,
        avatar: meta.avatar,
        mode,
        content,
        ts: Date.now(),
      };

      // 写入历史（FIFO，保留最近 200 条）
      const history = (await this.state.storage.get('messages')) || [];
      history.push(msg);
      while (history.length > 200) history.shift();
      await this.state.storage.put('messages', history);

      // 广播给房间内所有连接（包括发送者，发送者用 clientId 去重）
      this.broadcast(msg, null);
    } else if (data?.type === 'ping') {
      try { ws.send(JSON.stringify({ type: 'pong', ts: Date.now() })); } catch (_) {}
    }
  }

  // ─── Hibernation 回调：连接关闭 ──────────────────────────
  async webSocketClose(ws /*, code, reason, wasClean */) {
    const meta = this.sessions.get(ws);
    this.sessions.delete(ws);
    try { ws.close(1000, 'closed'); } catch (_) {}
    if (meta) {
      this.broadcast({
        type: 'presence',
        count: this.sessions.size,
        left: { name: meta.name },
      }, null);
    }
  }

  async webSocketError(ws /*, err */) {
    this.sessions.delete(ws);
  }

  // ─── 广播工具 ────────────────────────────────────────────
  broadcast(payload, except) {
    const text = JSON.stringify(payload);
    const dead = [];
    for (const ws of this.sessions.keys()) {
      if (ws === except) continue;
      try { ws.send(text); }
      catch (_) { dead.push(ws); }
    }
    for (const ws of dead) this.sessions.delete(ws);
  }
}


// ─────────────────────────────────────────────────────────────
// Worker 入口
// ─────────────────────────────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age':       '86400',
};

function withCors(res) {
  const headers = new Headers(res.headers);
  for (const [k, v] of Object.entries(CORS)) headers.set(k, v);
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
}

function normalizeRoomId(raw) {
  // 只允许字母数字 _- ，最多 64 字符；空值退到 lobby
  const cleaned = String(raw || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64);
  return cleaned || 'lobby';
}

export default {
  async fetch(request, env /*, ctx */) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS });
    }

    // 健康检查
    if (url.pathname === '/api/health' || url.pathname === '/health') {
      return withCors(Response.json({ ok: true, ts: Date.now() }));
    }

    // WebSocket 升级 → 路由到 Durable Object
    if (url.pathname === '/ws') {
      const roomId = normalizeRoomId(url.searchParams.get('room'));
      const id = env.CHATROOM.idFromName(roomId);
      const stub = env.CHATROOM.get(id);
      // WebSocket 升级响应不能加 CORS 头（WS 不走 CORS）
      return stub.fetch(request);
    }

    // 拉历史消息
    if (url.pathname === '/api/history' || url.pathname === '/history') {
      const roomId = normalizeRoomId(url.searchParams.get('room'));
      const id = env.CHATROOM.idFromName(roomId);
      const stub = env.CHATROOM.get(id);
      const fwd = new URL(request.url);
      fwd.pathname = '/history';
      const res = await stub.fetch(new Request(fwd, request));
      return withCors(res);
    }

    return withCors(new Response('Not found', { status: 404 }));
  },
};
