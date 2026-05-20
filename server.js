const WebSocket = require('ws');
const http = require('http');
const twilio = require('twilio');

const accountSid = process.env.TWILIO_ACCOUNT_SID || '';
const authToken = process.env.TWILIO_AUTH_TOKEN || '';
const twilioClient = (accountSid && authToken) ? twilio(accountSid, authToken) : null;

let cachedIceServers = null;
let cachedIceAtMs = 0;
const ICE_CACHE_MS = 30 * 60 * 1000;

async function getTwilioIceServers() {
  if (!twilioClient) {
    const err = new Error('Twilio env missing: TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN');
    err.code = 'TWILIO_ENV_MISSING';
    throw err;
  }

  const now = Date.now();
  if (cachedIceServers && (now - cachedIceAtMs) < ICE_CACHE_MS) return cachedIceServers;

  const token = await twilioClient.tokens.create();
  const ice = token && token.ice_servers;
  if (!Array.isArray(ice) || ice.length === 0) {
    const err = new Error('Twilio token returned empty ice_servers');
    err.code = 'TWILIO_ICE_EMPTY';
    throw err;
  }
  cachedIceServers = ice;
  cachedIceAtMs = now;
  return ice;
}

function writeJson(res, statusCode, obj, extraHeaders = {}) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    ...extraHeaders,
  });
  res.end(JSON.stringify(obj));
}

const server = http.createServer(async (req, res) => {
  // Basic CORS for /ice (viewer may be served from a different origin).
  if (req.url === '/ice') {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      });
      res.end();
      return;
    }
    if (req.method !== 'GET') {
      writeJson(res, 405, { error: 'method_not_allowed' }, { 'Access-Control-Allow-Origin': '*' });
      return;
    }
    try {
      const iceServers = await getTwilioIceServers();
      writeJson(res, 200, { iceServers }, { 'Access-Control-Allow-Origin': '*' });
    } catch (e) {
      writeJson(res, 500, { error: 'failed_to_get_ice', message: String(e && e.message ? e.message : e) }, { 'Access-Control-Allow-Origin': '*' });
    }
    return;
  }

  res.writeHead(200);
  res.end('Screen Share Signaling Server Running');
});

// Render 로드밸런서(~60s 유휴 컷오프) 대응: 30초마다 ping → 응답 없으면 종료
const wss = new WebSocket.Server({ server });
const PING_INTERVAL_MS = 30_000;

const keepAlive = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, PING_INTERVAL_MS);
wss.on('close', () => clearInterval(keepAlive));

const rooms = new Map();

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let roomId = null;
  let role = null;

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }

    if (msg.type === 'join') {
      roomId = msg.roomId?.toUpperCase();
      role = msg.role;
      if (!roomId || !role) return;

      if (role === 'viewer') {
        const existing = rooms.get(roomId);
        if (!existing || !existing.host || existing.host.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'invalid-room' }));
          return;
        }
      }

      if (!rooms.has(roomId)) rooms.set(roomId, {});
      rooms.get(roomId)[role] = ws;
      console.log(`[${roomId}] ${role} joined`);

      const otherRole = role === 'host' ? 'viewer' : 'host';
      const otherWs = rooms.get(roomId)?.[otherRole];
      if (otherWs && otherWs.readyState === WebSocket.OPEN) {
        otherWs.send(JSON.stringify({ type: 'peer-joined', role }));
        ws.send(JSON.stringify({ type: 'peer-joined', role: otherRole }));
      }
      return;
    }

    // pointer 포함 모든 메시지 릴레이
    if (!roomId || !rooms.has(roomId)) return;
    const targetRole = role === 'host' ? 'viewer' : 'host';
    const targetWs = rooms.get(roomId)?.[targetRole];
    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
      targetWs.send(data.toString());
    }
  });

  ws.on('close', () => {
    if (!roomId || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    delete room[role];
    console.log(`[${roomId}] ${role} disconnected`);
    const otherRole = role === 'host' ? 'viewer' : 'host';
    const otherWs = room[otherRole];
    if (otherWs && otherWs.readyState === WebSocket.OPEN) {
      otherWs.send(JSON.stringify({ type: 'peer-left' }));
    }
    if (!room.host && !room.viewer) rooms.delete(roomId);
  });

  ws.on('error', (err) => console.error('WS error:', err.message));
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log(`✅ Signaling server running on port ${PORT}`));
