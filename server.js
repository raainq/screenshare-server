const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('Screen Share Signaling Server Running');
});

const wss = new WebSocket.Server({ server });

// roomId -> { host: ws, viewer: ws }
const rooms = new Map();

wss.on('connection', (ws) => {
  let roomId = null;
  let role = null;

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }

    if (msg.type === 'join') {
      roomId = msg.roomId?.toUpperCase();
      role = msg.role; // 'host' | 'viewer'
      if (!roomId || !role) return;

      if (!rooms.has(roomId)) rooms.set(roomId, {});
      rooms.get(roomId)[role] = ws;

      console.log(`[${roomId}] ${role} joined`);

      // 상대방이 이미 있으면 알림
      const otherRole = role === 'host' ? 'viewer' : 'host';
      const otherWs = rooms.get(roomId)[otherRole];
      if (otherWs && otherWs.readyState === WebSocket.OPEN) {
        otherWs.send(JSON.stringify({ type: 'peer-joined', role }));
        // viewer가 나중에 들어왔으면 host에게 offer 시작 신호
        if (role === 'viewer') {
          ws.send(JSON.stringify({ type: 'peer-joined', role: 'host' }));
        }
      }
      return;
    }

    // offer / answer / candidate 릴레이
    if (!roomId || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const targetRole = role === 'host' ? 'viewer' : 'host';
    const targetWs = room[targetRole];

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
server.listen(PORT, () => {
  console.log(`✅ Signaling server running on port ${PORT}`);
});
