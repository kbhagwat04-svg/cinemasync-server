// server.js — CinemaSync v2.0 — Production hardened
// Improvements: input validation, rate limiting, graceful shutdown,
// structured logging, removed unused uuid, better room codes

const { WebSocketServer, WebSocket } = require("ws");
const http = require("http");

// ── Config ────────────────────────────────────────────────────────────────────
const PORT             = process.env.PORT || 3000;
const MAX_MEMBERS      = 10;
const MAX_MSG_LENGTH   = 500;
const ROOM_TTL_MS      = 6 * 60 * 60 * 1000;   // 6 hours
const PING_INTERVAL_MS = 30_000;
const RATE_LIMIT_MAX   = 20;   // max messages per window
const RATE_LIMIT_MS    = 1000; // per 1 second window

// ── Structured logger (lightweight, no deps) ──────────────────────────────────
const log = {
  info:  (data, msg) => console.log(JSON.stringify({ level:"INFO",  msg, ...data, ts: new Date().toISOString() })),
  warn:  (data, msg) => console.log(JSON.stringify({ level:"WARN",  msg, ...data, ts: new Date().toISOString() })),
  error: (data, msg) => console.log(JSON.stringify({ level:"ERROR", msg, ...data, ts: new Date().toISOString() })),
};

// ── In-memory store ───────────────────────────────────────────────────────────
const rooms = new Map();

// ── Input validation ──────────────────────────────────────────────────────────
function isValidTime(t) {
  return Number.isFinite(t) && t >= 0 && t < 86400; // max 24h
}

function isValidString(s, maxLen = 50) {
  return typeof s === "string" && s.trim().length > 0 && s.length <= maxLen;
}

// ── Rate limiting per connection ──────────────────────────────────────────────
function isRateLimited(ws) {
  const now = Date.now();
  ws._msgTimestamps = (ws._msgTimestamps || []).filter(t => now - t < RATE_LIMIT_MS);
  if (ws._msgTimestamps.length >= RATE_LIMIT_MAX) {
    log.warn({ userId: ws._userId }, "Rate limit exceeded");
    return true;
  }
  ws._msgTimestamps.push(now);
  return false;
}

// ── HTTP server ───────────────────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status:"ok", rooms:rooms.size, uptime:Math.floor(process.uptime()), ts: Date.now() }));
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
  ws._userId         = null;
  ws._roomId         = null;
  ws.isAlive         = true;
  ws._msgTimestamps  = [];
  ws._ip             = req.headers["x-forwarded-for"] || req.socket.remoteAddress;

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    // Rate limit
    if (isRateLimited(ws)) {
      send(ws, { type:"ERROR", message:"Rate limit exceeded — slow down" });
      return;
    }

    let msg;
    try { msg = JSON.parse(raw); }
    catch { send(ws, { type:"ERROR", message:"Invalid JSON" }); return; }

    handleMessage(ws, msg);
  });

  ws.on("close", () => handleDisconnect(ws));
  ws.on("error", (err) => log.error({ err: err.message }, "WebSocket error"));
});

// ── Heartbeat ─────────────────────────────────────────────────────────────────
const pingInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
    send(ws, { type:"PING" });
  });
}, PING_INTERVAL_MS);

wss.on("close", () => clearInterval(pingInterval));

// ── Room cleanup (with jitter to avoid thundering herd) ───────────────────────
const CLEANUP_INTERVAL = 60 * 60 * 1000 + Math.floor(Math.random() * 60000);
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;
  for (const [id, room] of rooms) {
    if (now - room.lastActivity > ROOM_TTL_MS || room.members.size === 0) {
      rooms.delete(id);
      cleaned++;
    }
  }
  if (cleaned > 0) log.info({ cleaned }, "Room cleanup complete");
}, CLEANUP_INTERVAL);

// ── Message handler ───────────────────────────────────────────────────────────
function handleMessage(ws, msg) {
  touch(ws._roomId);

  switch (msg.type) {

    case "CREATE_ROOM": {
      if (!isValidString(msg.userId, 64) || !isValidString(msg.username, 30)) {
        return send(ws, { type:"ROOM_ERROR", message:"Invalid userId or username" });
      }
      const roomId = generateRoomCode();
      const room = {
        id: roomId, hostId: msg.userId,
        members: new Map(),
        playback: { playing:false, currentTime:0, updatedAt:Date.now() },
        createdAt: Date.now(), lastActivity: Date.now(),
      };
      room.members.set(msg.userId, { userId:msg.userId, username:msg.username, ws });
      rooms.set(roomId, room);
      ws._userId = msg.userId;
      ws._roomId = roomId;
      send(ws, { type:"ROOM_JOINED", roomId, isHost:true, members:serializeMembers(room), playback:room.playback });
      log.info({ roomId, host:msg.username }, "Room created");
      break;
    }

    case "JOIN_ROOM": {
      if (!isValidString(msg.userId, 64) || !isValidString(msg.username, 30) || !isValidString(msg.roomId, 20)) {
        return send(ws, { type:"ROOM_ERROR", message:"Missing or invalid fields" });
      }
      const room = rooms.get(msg.roomId);
      if (!room) return send(ws, { type:"ROOM_ERROR", message:`Room ${msg.roomId} not found` });
      if (room.members.size >= MAX_MEMBERS) return send(ws, { type:"ROOM_ERROR", message:`Room is full (max ${MAX_MEMBERS})` });

      room.members.set(msg.userId, { userId:msg.userId, username:msg.username, ws });
      ws._userId = msg.userId;
      ws._roomId = msg.roomId;

      send(ws, { type:"ROOM_JOINED", roomId:msg.roomId, isHost:false, members:serializeMembers(room), playback:room.playback });
      broadcastToRoom(room, { type:"USER_JOINED", userId:msg.userId, username:msg.username, members:serializeMembers(room) }, msg.userId);
      send(ws, { type:"ROOM_STATE", playing:room.playback.playing, currentTime:room.playback.currentTime, members:serializeMembers(room) });
      log.info({ roomId:msg.roomId, username:msg.username, members:room.members.size }, "User joined");
      break;
    }

    case "LEAVE_ROOM":
      handleDisconnect(ws);
      break;

    case "SYNC_PLAY": {
      const room = getRoom(ws);
      if (!room || !isHost(ws, room)) return;
      // ── Input validation ──────────────────────────────────────────────────
      if (!isValidTime(msg.currentTime)) {
        log.warn({ userId:ws._userId, currentTime:msg.currentTime }, "Invalid currentTime in SYNC_PLAY");
        return;
      }
      room.playback = { playing:true, currentTime:msg.currentTime, updatedAt:Date.now() };
      broadcastToRoom(room, { type:"SYNC_PLAY", currentTime:msg.currentTime }, ws._userId);
      break;
    }

    case "SYNC_PAUSE": {
      const room = getRoom(ws);
      if (!room || !isHost(ws, room)) return;
      if (!isValidTime(msg.currentTime)) return;
      room.playback = { playing:false, currentTime:msg.currentTime, updatedAt:Date.now() };
      broadcastToRoom(room, { type:"SYNC_PAUSE", currentTime:msg.currentTime }, ws._userId);
      break;
    }

    case "SYNC_SEEK": {
      const room = getRoom(ws);
      if (!room || !isHost(ws, room)) return;
      if (!isValidTime(msg.currentTime)) return;
      room.playback.currentTime = msg.currentTime;
      room.playback.updatedAt   = Date.now();
      broadcastToRoom(room, { type:"SYNC_SEEK", currentTime:msg.currentTime, playing:room.playback.playing }, ws._userId);
      break;
    }

    case "CHAT_MESSAGE": {
      const room = getRoom(ws);
      if (!room) return;
      const safeText = sanitize(msg.text);
      if (!safeText) return; // ignore empty messages
      broadcastToRoom(room, { type:"CHAT_MESSAGE", userId:ws._userId, username:sanitize(msg.username, 30), text:safeText, timestamp:Date.now() });
      break;
    }

    // ── WebRTC signalling for voice calls ─────────────────────────────────────
    case "VOICE_OFFER":
    case "VOICE_ANSWER":
    case "VOICE_ICE": {
      const room = getRoom(ws);
      if (!room || !isValidString(msg.targetUserId, 64)) return;
      const target = room.members.get(msg.targetUserId);
      if (target) send(target.ws, { ...msg, fromUserId: ws._userId });
      break;
    }

    case "VOICE_JOIN": {
      const room = getRoom(ws);
      if (!room) return;
      broadcastToRoom(room, { type:"VOICE_JOIN", userId:ws._userId, username:getMember(ws,room)?.username }, ws._userId);
      break;
    }

    case "VOICE_LEAVE": {
      const room = getRoom(ws);
      if (!room) return;
      broadcastToRoom(room, { type:"VOICE_LEAVE", userId:ws._userId }, ws._userId);
      break;
    }

    case "PONG":
      ws.isAlive = true;
      break;

    default:
      send(ws, { type:"ERROR", message:`Unknown message type: ${msg.type}` });
  }
}

// ── Disconnect handler ────────────────────────────────────────────────────────
function handleDisconnect(ws) {
  const { _userId:userId, _roomId:roomId } = ws;
  if (!userId || !roomId) return;
  const room = rooms.get(roomId);
  if (!room) return;

  const member = room.members.get(userId);
  const username = member?.username || "Unknown";
  room.members.delete(userId);

  log.info({ roomId, username, remaining:room.members.size }, "User left");

  if (room.members.size === 0) { rooms.delete(roomId); return; }

  if (room.hostId === userId) {
    const [newHostId, newHost] = room.members.entries().next().value;
    room.hostId = newHostId;
    broadcastToRoom(room, { type:"HOST_CHANGED", newHostId, newHostUsername:newHost.username, members:serializeMembers(room) });
    send(newHost.ws, { type:"PROMOTED_TO_HOST" });
    log.info({ roomId, newHost:newHost.username }, "Host promoted");
  }

  broadcastToRoom(room, { type:"USER_LEFT", userId, username, members:serializeMembers(room) });
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function broadcastToRoom(room, msg, excludeId = null) {
  for (const [uid, m] of room.members) {
    if (uid !== excludeId) send(m.ws, msg);
  }
}

function getRoom(ws)         { return ws._roomId ? rooms.get(ws._roomId) : null; }
function getMember(ws, room) { return room?.members.get(ws._userId); }
function isHost(ws, room)    { return room?.hostId === ws._userId; }
function touch(roomId)       { const r = rooms.get(roomId); if (r) r.lastActivity = Date.now(); }

function serializeMembers(room) {
  return [...room.members.values()].map(m => ({ userId:m.userId, username:m.username, isHost:m.userId === room.hostId }));
}

function sanitize(str = "", maxLen = MAX_MSG_LENGTH) {
  return String(str).slice(0, maxLen)
    .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;").replace(/'/g,"&#x27;");
}

// Improved room code — more words = fewer collisions
const ROOM_WORDS = ["NITE","REEL","SYNC","FILM","CINE","PLAY","SHOW","SCENE","BEAM","REEL","LENS","TAKE","SHOT","FADE","CAST","CLIP"];
function generateRoomCode() {
  const word = ROOM_WORDS[Math.floor(Math.random() * ROOM_WORDS.length)];
  const num  = Math.floor(1000 + Math.random() * 9000);
  const code = `${word}-${num}`;
  // Retry on collision (extremely rare but handled)
  return rooms.has(code) ? generateRoomCode() : code;
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
function gracefulShutdown(signal) {
  log.info({ signal }, "Graceful shutdown initiated");
  clearInterval(pingInterval);
  wss.clients.forEach(ws => {
    try { ws.close(1001, "Server shutting down"); } catch(e) {}
  });
  httpServer.close(() => {
    log.info({}, "Server closed cleanly");
    process.exit(0);
  });
  setTimeout(() => { log.warn({}, "Forced shutdown after timeout"); process.exit(1); }, 5000);
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT",  () => gracefulShutdown("SIGINT"));

// Catch unhandled errors so server doesn't crash on one bad message
process.on("uncaughtException",  (err) => log.error({ err:err.message, stack:err.stack }, "Uncaught exception"));
process.on("unhandledRejection", (err) => log.error({ err:String(err) }, "Unhandled rejection"));

// ── Boot ──────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  log.info({ port:PORT, env:process.env.NODE_ENV||"development" }, "CinemaSync server started");
  console.log(`
╔════════════════════════════════════════╗
║       🎬  CinemaSync Server v2.0       ║
╠════════════════════════════════════════╣
║  WebSocket : ws://localhost:${PORT}       ║
║  Health    : http://localhost:${PORT}/health ║
║  Rooms     : http://localhost:${PORT}/rooms  ║
╚════════════════════════════════════════╝`);
});
