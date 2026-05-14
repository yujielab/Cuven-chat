/**
 * Cuven Chat · Cloudflare Worker 后端
 * ============================================
 * 架构：
 *   - 每个聊天房间是一个 Durable Object 实例（按 roomId 路由）
 *   - 用 WebSocket Hibernation API（DO 可休眠，连接成本几乎为 0）
 *   - 房间内最近 200 条消息持久化到 DO storage，新加入者会拉到历史
 *   - 图片/文件走 R2 桶 MEDIA，由 Worker 边缘缓存（immutable, 1 年）
 *
 * 路由：
 *   GET  /ws?room=ROOMID&name=...&avatar=...&email=...&clientId=...
 *        Upgrade 到 WebSocket，加入房间
 *   GET  /api/history?room=ROOMID
 *        拉取历史消息（200 条上限），HTTP 兜底
 *   POST /api/upload?room=ROOMID&filename=...
 *        上传二进制到 R2（限 10MB；image/* video/* audio/* application/pdf）
 *        Body 直接是文件流，Content-Type 必填
 *        返回 { url, key, contentType, size }
 *   GET  /r2/<key>
 *        从 R2 取媒体，命中边缘缓存；Cache-Control: immutable
 *   GET  /api/health
 *        健康检查
 *   OPTIONS *
 *        CORS 预检
 *
 * 消息协议（客户端 → 服务器）：
 *   { type:'msg', mode:'text'|'html'|'location'|'image'|'file',
 *     content: string|{lng,lat}|{url,name,contentType,size,width?,height?},
 *     tempId?: string }                // 用于服务端把 echo 关联回客户端乐观渲染的 DOM
 *   { type:'edit',   id:string, content:string }
 *   { type:'delete', id:string }
 *   { type:'ping' }
 *
 * 消息协议（服务器 → 客户端）：
 *   { type:'history',  messages:[...]  }   // 加入后第一帧（已过滤掉被删除的内容）
 *   { type:'msg',      id, tempId?, clientId, name, avatar, mode, content, ts }
 *   { type:'edit',     id, content, editedAt }
 *   { type:'delete',   id, deletedAt }
 *   { type:'presence', count, joined?, left? }
 *
 * 部署：
 *   1. wrangler r2 bucket create cuven-chat-media   （首次）
 *   2. wrangler deploy
 *   3. 把 opop_1.html 顶部的 CHAT_API 改成你的 Worker 域名
 */

// 历史保留上限
const HISTORY_LIMIT = 200;
// 单文件大小上限：10MB
const MAX_UPLOAD = 10 * 1024 * 1024;
// 允许的 MIME 前缀
const ALLOWED_MIME = /^(image|video|audio)\/|^application\/pdf$/i;

// ─────────────────────────────────────────────────────────────
// Durable Object：聊天房间
// ─────────────────────────────────────────────────────────────
export class ChatRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // sessions: WebSocket → { id, clientId, name, avatar, email }
    // Hibernation 唤醒后此 Map 是空的，从 getWebSockets() 恢复
    this.sessions = new Map();
    for (const ws of this.state.getWebSockets()) {
      try {
        const meta = ws.deserializeAttachment();
        if (meta) this.sessions.set(ws, meta);
      } catch (_) { /* ignore */ }
    }
  }

  // ─── 路由 ────────────────────────────────────────────────
  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === '/ws') {
      return this.handleWebSocketUpgrade(request, url);
    }

    if (url.pathname === '/history') {
      const messages = await this.getVisibleHistory();
      return Response.json({ messages });
    }

    return new Response('Not found', { status: 404 });
  }

  // 取出"对加入者可见"的历史：被 delete 的消息保留占位（mode=text + deleted: true）
  async getVisibleHistory() {
    return (await this.state.storage.get('messages')) || [];
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
      clientId,                 // 客户端自己的标识
      name, avatar, email,
      joinedAt: Date.now(),
    };

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // 用 acceptWebSocket() 而不是 server.accept()，DO 在没事件时可休眠
    this.state.acceptWebSocket(server);
    server.serializeAttachment(meta);
    this.sessions.set(server, meta);

    // 给新加入者发历史消息
    const history = await this.getVisibleHistory();
    try {
      server.send(JSON.stringify({ type: 'history', messages: history }));
      server.send(JSON.stringify({
        type: 'presence',
        count: this.sessions.size,
        joined: { name, avatar },
      }));
    } catch (_) {}

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

    // ── 新消息 ──
    if (data?.type === 'msg') {
      const mode = ['html','location','image','file'].includes(data.mode) ? data.mode : 'text';

      let content;
      if (mode === 'location') {
        const lng = Number(data.content?.lng);
        const lat = Number(data.content?.lat);
        if (!Number.isFinite(lng) || !Number.isFinite(lat)) return;
        if (lng < -180 || lng > 180 || lat < -90 || lat > 90) return;
        content = { lng, lat };
      } else if (mode === 'image' || mode === 'file') {
        // 媒体消息：必须带 url，且 url 必须是字符串
        const c = data.content || {};
        const u = String(c.url || '');
        if (!u || u.length > 500) return;
        content = {
          url: u,
          name: String(c.name || '').slice(0, 200),
          contentType: String(c.contentType || '').slice(0, 100),
          size: Number.isFinite(+c.size) ? +c.size : 0,
          width:  Number.isFinite(+c.width)  ? +c.width  : 0,
          height: Number.isFinite(+c.height) ? +c.height : 0,
        };
      } else {
        content = String(data.content ?? '').slice(0, 4000);
        if (!content.trim()) return;
      }

      const msg = {
        type: 'msg',
        id: crypto.randomUUID(),
        tempId: typeof data.tempId === 'string' ? data.tempId.slice(0, 64) : undefined,
        clientId: meta.clientId,
        name: meta.name,
        avatar: meta.avatar,
        mode,
        content,
        ts: Date.now(),
      };

      const history = (await this.state.storage.get('messages')) || [];
      history.push(msg);
      while (history.length > HISTORY_LIMIT) history.shift();
      await this.state.storage.put('messages', history);

      this.broadcast(msg, null);
      return;
    }

    // ── 编辑消息（允许任意人编辑任意 text/html 消息）──
    if (data?.type === 'edit') {
      const id = String(data.id || '');
      const newContent = String(data.content ?? '').slice(0, 4000);
      if (!id || !newContent.trim()) return;

      const history = (await this.state.storage.get('messages')) || [];
      const idx = history.findIndex(m => m.id === id);
      if (idx === -1) return;
      const old = history[idx];
      if (old.deleted) return;
      // 仅 text / html 可编辑
      if (old.mode !== 'text' && old.mode !== 'html') return;

      old.content = newContent;
      old.edited = true;
      old.editedAt = Date.now();
      old.editedBy = meta.clientId;
      await this.state.storage.put('messages', history);

      this.broadcast({
        type: 'edit',
        id,
        content: newContent,
        editedAt: old.editedAt,
      }, null);
      return;
    }

    // ── 删除消息（允许任意人删除任意消息；保留占位）──
    if (data?.type === 'delete') {
      const id = String(data.id || '');
      if (!id) return;

      const history = (await this.state.storage.get('messages')) || [];
      const idx = history.findIndex(m => m.id === id);
      if (idx === -1) return;
      const old = history[idx];
      if (old.deleted) return;

      // 如果是媒体消息，顺便从 R2 删（避免占用配额）
      if ((old.mode === 'image' || old.mode === 'file') && old.content?.url) {
        const key = extractR2Key(old.content.url);
        if (key) {
          try { await this.env.MEDIA.delete(key); } catch (_) {}
        }
      }

      old.deleted = true;
      old.deletedAt = Date.now();
      old.deletedBy = meta.clientId;
      // 清空内容以节省 storage 体积，但保留 id/ts/mode 用作占位
      old.content = old.mode === 'location'
        ? { lng: 0, lat: 0 }
        : (old.mode === 'image' || old.mode === 'file') ? { url: '' } : '';
      await this.state.storage.put('messages', history);

      this.broadcast({
        type: 'delete',
        id,
        deletedAt: old.deletedAt,
      }, null);
      return;
    }

    // ── ping ──
    if (data?.type === 'ping') {
      try { ws.send(JSON.stringify({ type: 'pong', ts: Date.now() })); } catch (_) {}
    }
  }

  async webSocketClose(ws) {
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

  async webSocketError(ws) {
    this.sessions.delete(ws);
  }

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
  'Access-Control-Allow-Headers': 'Content-Type, X-Filename',
  'Access-Control-Max-Age':       '86400',
};

function withCors(res) {
  const headers = new Headers(res.headers);
  for (const [k, v] of Object.entries(CORS)) headers.set(k, v);
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
}

function normalizeRoomId(raw) {
  const cleaned = String(raw || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64);
  return cleaned || 'lobby';
}

// 从我们的 /r2/<key> URL 里把 key 抽出来（用于删除时清理 R2）
function extractR2Key(url) {
  try {
    const u = new URL(url);
    if (!u.pathname.startsWith('/r2/')) return null;
    return decodeURIComponent(u.pathname.slice(4));
  } catch (_) { return null; }
}

// 根据 contentType 决定一个安全的扩展名
function extFromMime(mime) {
  const m = String(mime || '').toLowerCase();
  if (m === 'image/jpeg' || m === 'image/jpg') return 'jpg';
  if (m === 'image/png')  return 'png';
  if (m === 'image/gif')  return 'gif';
  if (m === 'image/webp') return 'webp';
  if (m === 'image/heic') return 'heic';
  if (m === 'image/heif') return 'heif';
  if (m === 'image/svg+xml') return 'svg';
  if (m === 'video/mp4')  return 'mp4';
  if (m === 'video/quicktime') return 'mov';
  if (m === 'video/webm') return 'webm';
  if (m === 'audio/mpeg') return 'mp3';
  if (m === 'audio/mp4')  return 'm4a';
  if (m === 'audio/webm') return 'webm';
  if (m === 'audio/wav')  return 'wav';
  if (m === 'application/pdf') return 'pdf';
  return 'bin';
}

function safeFilename(raw) {
  return String(raw || '')
    .replace(/[^\w.\-]+/g, '_')  // 只允许字母数字下划线点横杠
    .replace(/_+/g, '_')
    .slice(0, 80);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS });
    }

    // 健康检查
    if (url.pathname === '/api/health' || url.pathname === '/health') {
      return withCors(Response.json({ ok: true, ts: Date.now() }));
    }

    // ── 上传媒体到 R2 ─────────────────────────────────────
    if (url.pathname === '/api/upload' && request.method === 'POST') {
      const contentType = (request.headers.get('Content-Type') || '').split(';')[0].trim().toLowerCase();
      if (!ALLOWED_MIME.test(contentType)) {
        return withCors(new Response(JSON.stringify({ error: 'Unsupported media type' }), {
          status: 415, headers: { 'Content-Type': 'application/json' }
        }));
      }
      const lenHdr = request.headers.get('Content-Length');
      if (lenHdr && Number(lenHdr) > MAX_UPLOAD) {
        return withCors(new Response(JSON.stringify({ error: 'File too large (>10MB)' }), {
          status: 413, headers: { 'Content-Type': 'application/json' }
        }));
      }

      // 读出 body（Cloudflare 已经会按 Content-Length 截断；额外做一次大小检查）
      const buf = await request.arrayBuffer();
      if (buf.byteLength > MAX_UPLOAD) {
        return withCors(new Response(JSON.stringify({ error: 'File too large (>10MB)' }), {
          status: 413, headers: { 'Content-Type': 'application/json' }
        }));
      }

      const roomId  = normalizeRoomId(url.searchParams.get('room'));
      const rawName = url.searchParams.get('filename') || request.headers.get('X-Filename') || '';
      const ext     = (rawName.match(/\.([A-Za-z0-9]{1,8})$/)?.[1] || extFromMime(contentType)).toLowerCase();
      const id      = crypto.randomUUID();
      const baseName = safeFilename(rawName.replace(/\.[A-Za-z0-9]{1,8}$/, '')) || 'file';
      const key      = `${roomId}/${id}-${baseName}.${ext}`;

      try {
        await env.MEDIA.put(key, buf, {
          httpMetadata: {
            contentType,
            cacheControl: 'public, max-age=31536000, immutable',
          },
          customMetadata: {
            originalName: rawName.slice(0, 200),
            room: roomId,
            uploadedAt: String(Date.now()),
          },
        });
      } catch (e) {
        return withCors(new Response(JSON.stringify({ error: 'Upload failed: ' + (e.message || e) }), {
          status: 500, headers: { 'Content-Type': 'application/json' }
        }));
      }

      // 拼出公开 URL（同 origin 的 /r2/<key>）
      const fileURL = new URL(request.url);
      fileURL.pathname = '/r2/' + key;
      fileURL.search   = '';

      return withCors(Response.json({
        url:         fileURL.toString(),
        key,
        contentType,
        size:        buf.byteLength,
      }));
    }

    // ── 从 R2 取媒体（带边缘缓存）─────────────────────────
    if (url.pathname.startsWith('/r2/')) {
      const key = decodeURIComponent(url.pathname.slice(4));
      if (!key) return withCors(new Response('Bad key', { status: 400 }));

      // Cloudflare 边缘缓存（按 URL 缓存）
      const cache    = caches.default;
      const cacheKey = new Request(url.toString(), { method: 'GET' });
      let cached     = await cache.match(cacheKey);
      if (cached) return cached;

      // Range / ETag 支持（视频流播放友好）
      const range = request.headers.get('Range');
      const ifNoneMatch = request.headers.get('If-None-Match');

      const obj = await env.MEDIA.get(key, {
        range: range ? parseRange(range) : undefined,
        onlyIf: ifNoneMatch ? { etagDoesNotMatch: ifNoneMatch } : undefined,
      });

      if (!obj) {
        // 304 走 onlyIf 时也会进这条
        if (ifNoneMatch) {
          const head = await env.MEDIA.head(key);
          if (head && head.httpEtag === ifNoneMatch) {
            return new Response(null, { status: 304, headers: { 'ETag': head.httpEtag } });
          }
        }
        return withCors(new Response('Not found', { status: 404 }));
      }

      const headers = new Headers();
      obj.writeHttpMetadata(headers);
      headers.set('etag', obj.httpEtag);
      headers.set('Cache-Control', 'public, max-age=31536000, immutable');
      headers.set('Access-Control-Allow-Origin', '*');
      headers.set('Accept-Ranges', 'bytes');
      if (range && obj.range) {
        const total = obj.size;
        const start = obj.range.offset;
        const end   = start + obj.range.length - 1;
        headers.set('Content-Range', `bytes ${start}-${end}/${total}`);
        const res206 = new Response(obj.body, { status: 206, headers });
        // 不缓存 206 partial
        return res206;
      }

      const res200 = new Response(obj.body, { status: 200, headers });
      // 在边缘异步写入缓存
      ctx.waitUntil(cache.put(cacheKey, res200.clone()));
      return res200;
    }

    // ── WebSocket 升级 → 路由到 Durable Object ──────────
    if (url.pathname === '/ws') {
      const roomId = normalizeRoomId(url.searchParams.get('room'));
      const id = env.CHATROOM.idFromName(roomId);
      const stub = env.CHATROOM.get(id);
      return stub.fetch(request);
    }

    // ── 拉历史消息 ─────────────────────────────────────
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

// 极简 Range 解析：只支持单段 "bytes=start-end"
function parseRange(h) {
  const m = /^bytes=(\d+)-(\d*)$/.exec(h || '');
  if (!m) return undefined;
  const start = parseInt(m[1], 10);
  const end   = m[2] ? parseInt(m[2], 10) : undefined;
  if (end !== undefined && end >= start) return { offset: start, length: end - start + 1 };
  return { offset: start };
}
