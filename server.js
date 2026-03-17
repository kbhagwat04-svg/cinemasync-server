// server.js — CinemaSync WebSocket Server (Production v1.1 — All bugs fixed)
// Fixes applied:
//   BUG-01: ROOM_STATE not sent to new joiners → now sent after ROOM_JOINED
//   BUG-02: XSS not sanitized in chat messages → sanitize() now strips HTML tags
//   BUG-03: Long messages not truncated → sanitize() enforces 500 char limit
//   BUG-04: Playback state lost after SYNC_PLAY then SYNC_PAUSE → state persisted correctly

const { WebSocketServer, WebSocket } = require("ws");
const { v4: uuidv4 } = require("uuid");
const http = require("http");

// ── Config ───────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const ROOM_TTL_MS = 6 * 60 * 60 * 1000;
const PING_INTERVAL_MS = 30_000;
const MAX_MEMBERS = 10;
const MAX_MESSAGE_LENGTH = 500;

// ── In-memory store ──────────────────────────────────────────────────────────
const rooms = new Map();

// ── HTTP server ───────────────────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", rooms: rooms.size, uptime: Math.floor(process.uptime()) }));
    return;
  }
  if (req.url === "/rooms") {
    const list = [...rooms.values()].map(r => ({
      id: r.id, members: r.members.size,
      playing: r.playback.playing, currentTime: r.playback.currentTime
    }));
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(list));
    return;
  }
  res.writeHead(404); res.end();
});

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

wss.on("connection", (ws, req) => {
  ws._userId = null;
  ws._roomId = null;
  ws.isAlive  = true;

  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); }
    catch { return send(ws, { type: "ERROR", message: "Invalid JSON" }); }
    handleMessage(ws, msg);
  });
  ws.on("close", () => handleDisconnect(ws));
  ws.on("error", (err) => console.error("[ws error]", err.message));
});

// ── Heartbeat ─────────────────────────────────────────────────────────────────
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
    send(ws, { type: "PING" });
  });
}, PING_INTERVAL_MS);

wss.on("close", () => clearInterval(pingInterval));

// ── Room cleanup ──────────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [id, room] of rooms) {
    if (now - room.lastActivity > ROOM_TTL_MS || room.members.size === 0) {
      rooms.delete(id);
      console.log(`[~] Room ${id} expired`);
    }
  }
}, 60 * 60 * 1000);

// ── Message handler ───────────────────────────────────────────────────────────
function handleMessage(ws, msg) {
  touch(ws._roomId);

  switch (msg.type) {

    case "CREATE_ROOM": {
      const { userId, username } = msg;
      if (!userId || !username) return send(ws, { type: "ROOM_ERROR", message: "Missing userId or username" });

      const roomId = generateRoomCode();
      const room = {
        id: roomId,
        hostId: userId,
        members: new Map(),
        // BUG-04 FIX: Properly structured playback state object always maintained
        playback: { playing: false, currentTime: 0, updatedAt: Date.now() },
        createdAt: Date.now(),
        lastActivity: Date.now(),
      };

      room.members.set(userId, { userId, username, ws, isHost: true, joinedAt: Date.now() });
      rooms.set(roomId, room);
      ws._userId = userId;
      ws._roomId = roomId;

      send(ws, {
        type: "ROOM_JOINED",
        roomId,
        isHost: true,
        members: serializeMembers(room),
        playback: room.playback,
      });

      console.log(`[+] Room ${roomId} created by ${username}`);
      break;
    }

    case "JOIN_ROOM": {
      const { userId, username, roomId } = msg;
      if (!userId || !username || !roomId) return send(ws, { type: "ROOM_ERROR", message: "Missing fields" });

      const room = rooms.get(roomId);
      if (!room) return send(ws, { type: "ROOM_ERROR", message: `Room ${roomId} not found` });
      if (room.members.size >= MAX_MEMBERS) return send(ws, { type: "ROOM_ERROR", message: `Room is full (max ${MAX_MEMBERS})` });

      room.members.set(userId, { userId, username, ws, isHost: false, joinedAt: Date.now() });
      ws._userId = userId;
      ws._roomId = roomId;

      send(ws, {
        type: "ROOM_JOINED",
        roomId,
        isHost: false,
        members: serializeMembers(room),
        playback: room.playback,
      });

      broadcastToRoom(room, {
        type: "USER_JOINED",
        userId,
        username,
        members: serializeMembers(room),
      }, userId);

      // BUG-01 FIX: Always send ROOM_STATE after JOIN so new joiner syncs to
      // current playback position. Previously this was only sent sometimes.
      send(ws, {
        type: "ROOM_STATE",
        playing: room.playback.playing,
        currentTime: room.playback.currentTime,
        members: serializeMembers(room),
      });

      console.log(`[+] ${username} joined room ${roomId} (${room.members.size} members)`);
      break;
    }

    case "LEAVE_ROOM":
      handleDisconnect(ws);
      break;

    case "SYNC_PLAY": {
      const room = getRoom(ws);
      if (!room || !isHost(ws, room)) return;
      // BUG-04 FIX: Always update full playback state object
      room.playback = { playing: true, currentTime: msg.currentTime, updatedAt: Date.now() };
      broadcastToRoom(room, {
        type: "SYNC_PLAY",
        currentTime: msg.currentTime,
        username: getMember(ws, room)?.username,
      }, ws._userId);
      break;
    }

    case "SYNC_PAUSE": {
      const room = getRoom(ws);
      if (!room || !isHost(ws, room)) return;
      // BUG-04 FIX: Always update full playback state object
      room.playback = { playing: false, currentTime: msg.currentTime, updatedAt: Date.now() };
      broadcastToRoom(room, {
        type: "SYNC_PAUSE",
        currentTime: msg.currentTime,
        username: getMember(ws, room)?.username,
      }, ws._userId);
      break;
    }

    case "SYNC_SEEK": {
      const room = getRoom(ws);
      if (!room || !isHost(ws, room)) return;
      room.playback.currentTime = msg.currentTime;
      room.playback.updatedAt   = Date.now();
      broadcastToRoom(room, {
        type: "SYNC_SEEK",
        currentTime: msg.currentTime,
        playing: room.playback.playing,
      }, ws._userId);
      break;
    }

    case "CHAT_MESSAGE": {
      const room = getRoom(ws);
      if (!room) return;
      // BUG-02 + BUG-03 FIX: Sanitize XSS and enforce length limit
      const safeText = sanitize(msg.text);
      broadcastToRoom(room, {
        type:      "CHAT_MESSAGE",
        userId:    ws._userId,
        username:  sanitizeUsername(msg.username),
        text:      safeText,
        timestamp: Date.now(),
      });
      break;
    }

    case "PONG":
      ws.isAlive = true;
      break;

    default:
      send(ws, { type: "ERROR", message: `Unknown message type: ${msg.type}` });
  }
}

// ── Disconnect ────────────────────────────────────────────────────────────────
function handleDisconnect(ws) {
  const { _userId: userId, _roomId: roomId } = ws;
  if (!userId || !roomId) return;

  const room = rooms.get(roomId);
  if (!room) return;

  const member = room.members.get(userId);
  const username = member?.username || "Unknown";
  room.members.delete(userId);

  console.log(`[-] ${username} left room ${roomId} (${room.members.size} remaining)`);

  if (room.members.size === 0) {
    rooms.delete(roomId);
    console.log(`[~] Room ${roomId} deleted (empty)`);
    return;
  }

  if (room.hostId === userId) {
    const [newHostId, newHost] = room.members.entries().next().value;
    room.hostId    = newHostId;
    newHost.isHost = true;
    console.log(`[~] ${newHost.username} is now host of ${roomId}`);
    broadcastToRoom(room, {
      type: "HOST_CHANGED",
      newHostId,
      newHostUsername: newHost.username,
      members: serializeMembers(room),
    });
    send(newHost.ws, { type: "PROMOTED_TO_HOST" });
  }

  broadcastToRoom(room, {
    type: "USER_LEFT",
    userId,
    username,
    members: serializeMembers(room),
  });
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function broadcastToRoom(room, msg, excludeUserId = null) {
  for (const [uid, member] of room.members) {
    if (uid === excludeUserId) continue;
    send(member.ws, msg);
  }
}

function getRoom(ws)         { return ws._roomId ? rooms.get(ws._roomId) : null; }
function getMember(ws, room) { return room?.members.get(ws._userId); }
function isHost(ws, room)    { return room?.hostId === ws._userId; }

function serializeMembers(room) {
  return [...room.members.values()].map(m => ({
    userId:   m.userId,
    username: m.username,
    isHost:   m.userId === room.hostId,
  }));
}

function touch(roomId) {
  if (roomId) { const r = rooms.get(roomId); if (r) r.lastActivity = Date.now(); }
}

// BUG-02 + BUG-03 FIX: Strip HTML/script tags AND enforce max length
function sanitize(str = "") {
  return String(str)
    .slice(0, MAX_MESSAGE_LENGTH)           // BUG-03: truncate to 500 chars
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")                  // BUG-02: prevent XSS
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#x27;");
}

function sanitizeUsername(str = "") {
  return String(str).slice(0, 30).replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function generateRoomCode() {
  const words = ["NITE","REEL","SYNC","FILM","CINE","PLAY","SHOW","SCENE"];
  return words[Math.floor(Math.random() * words.length)] + "-" + Math.floor(1000 + Math.random() * 9000);
}

// ── Boot ──────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════╗
║         🎬  CinemaSync Server  v1.1    ║
╠════════════════════════════════════════╣
║  WebSocket : ws://localhost:${PORT}       ║
║  Health    : http://localhost:${PORT}/health ║
║  Rooms API : http://localhost:${PORT}/rooms  ║
╚════════════════════════════════════════╝
  `);
});
