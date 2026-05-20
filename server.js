/**
 * RainScreenShare – 시그널링 + 스트림 릴레이 서버
 *
 * 프로토콜:
 *   텍스트 프레임 : JSON (join, pointer, peer-joined, peer-left, invalid-room)
 *   바이너리 프레임: H.264 NAL 유닛 패킷 (host → viewer 단방향)
 *
 * 바이너리 패킷 포맷 (host가 전송):
 *   [1 byte flags] [4 bytes BE uint32 timestamp_ms] [N bytes NAL data]
 *   flags bit0 = 1 : SPS/PPS 포함(키프레임 헤더)
 *   flags bit1 = 1 : 키프레임 슬라이스
 */

const WebSocket = require('ws');
const http = require('http');

// ── 헬퍼 ──────────────────────────────────────────────────────────────────────
function writeJson(res, statusCode, obj, extraHeaders = {}) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    ...extraHeaders,
  });
  res.end(JSON.stringify(obj));
}

// ── HTTP 서버 ─────────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200);
    res.end('OK');
    return;
  }
  res.writeHead(200);
  res.end('RainScreenShare Relay Server');
});

// ── WebSocket 서버 ────────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server, maxPayload: 4 * 1024 * 1024 }); // 4 MB

// Render 로드밸런서(~60s 유휴 컷오프) 대응 ping
const PING_INTERVAL_MS = 30_000;
const keepAlive = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, PING_INTERVAL_MS);
wss.on('close', () => clearInterval(keepAlive));

// rooms: roomId → { host: WebSocket|null, viewers: Set<WebSocket> }
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, { host: null, viewers: new Set() });
  return rooms.get(roomId);
}

function cleanRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (!room.host && room.viewers.size === 0) rooms.delete(roomId);
}

function notifyViewers(room, msg) {
  const data = typeof msg === 'string' ? msg : JSON.stringify(msg);
  room.viewers.forEach((v) => {
    if (v.readyState === WebSocket.OPEN) v.send(data);
  });
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  let roomId = null;
  let role = null; // 'host' | 'viewer'

  // ── 메시지 핸들러 ───────────────────────────────────────────────────────────
  ws.on('message', (data, isBinary) => {

    // ─ 바이너리 : host가 보내는 H.264 NAL 패킷 → viewers에게 relay ─
    if (isBinary) {
      if (role !== 'host' || !roomId) return;
      const room = rooms.get(roomId);
      if (!room) return;
      room.viewers.forEach((v) => {
        if (v.readyState === WebSocket.OPEN) v.send(data, { binary: true });
      });
      return;
    }

    // ─ 텍스트 JSON ─────────────────────────────────────────────────────────
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    // join
    if (msg.type === 'join') {
      roomId = (msg.roomId || '').toString().toUpperCase().trim();
      role = msg.role;
      if (!roomId || !role) return;

      const room = getOrCreateRoom(roomId);

      if (role === 'host') {
        // 기존 host 종료
        if (room.host && room.host !== ws && room.host.readyState === WebSocket.OPEN) {
          room.host.close(1000, 'replaced');
        }
        room.host = ws;
        console.log(`[${roomId}] host joined`);
        // 이미 대기 중인 viewer가 있으면 알림
        if (room.viewers.size > 0) {
          ws.send(JSON.stringify({ type: 'viewer-ready' }));
        }

      } else if (role === 'viewer') {
        if (!room.host || room.host.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'invalid-room' }));
          return;
        }
        room.viewers.add(ws);
        console.log(`[${roomId}] viewer joined (total: ${room.viewers.size})`);
        // host에게 viewer 접속 알림
        if (room.host.readyState === WebSocket.OPEN) {
          room.host.send(JSON.stringify({ type: 'viewer-ready' }));
        }
        ws.send(JSON.stringify({ type: 'stream-ready' }));
      }
      return;
    }

    // pointer (viewer → host)
    if (msg.type === 'pointer' && role === 'viewer' && roomId) {
      const room = rooms.get(roomId);
      if (room?.host?.readyState === WebSocket.OPEN) {
        room.host.send(data.toString());
      }
      return;
    }

    // 그 외 텍스트는 상대방에게 relay (확장성)
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;
    if (role === 'host') {
      notifyViewers(room, data.toString());
    } else if (role === 'viewer' && room.host?.readyState === WebSocket.OPEN) {
      room.host.send(data.toString());
    }
  });

  // ── 연결 종료 ───────────────────────────────────────────────────────────────
  ws.on('close', () => {
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;

    if (role === 'host' && room.host === ws) {
      room.host = null;
      console.log(`[${roomId}] host disconnected`);
      notifyViewers(room, { type: 'host-left' });
    } else if (role === 'viewer') {
      room.viewers.delete(ws);
      console.log(`[${roomId}] viewer disconnected (remaining: ${room.viewers.size})`);
    }
    cleanRoom(roomId);
  });

  ws.on('error', (err) => console.error('WS error:', err.message));
});

// ── 시작 ──────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log(`✅ RainScreenShare relay server on port ${PORT}`));
