// server.js — CinemaSync WebSocket Server
// Node.js + ws (native WebSocket library)
// Handles: room creation, joining, playback sync, chat, presence

const { WebSocketServer, WebSocket } = require("ws");
const { v4: uuidv4 } = require("uuid");
const http = require("http");

// ── Config ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const ROOM_TTL_MS = 6 * 60 * 60 * 1000;   // 6 hours inactivity cleanup
const PING_INTERVAL_MS = 30_000;            // heartbeat every 30s

// ── In-memory store ─────────────────────────────────────────────────────────
// rooms: Map<roomId, Room>
// Room = { id, hostId, members: Map<userId, Member>, playback, createdAt, lastActivity }
// Member = { userId, username, ws, isHost, joinedAt }
const rooms = new Map();

// ── HTTP server (health check) ──────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      status: "ok",
      rooms: rooms.size,
      uptime: Math.floor(process.uptime()),
    }));
    return;
  }
  if (req.url === "/rooms" && req.method === "GET") {
    const list = [...rooms.values()].map(r => ({
      id: r.id,
      members: r.members.size,
      playing: r.playback.playing,
      currentTime: r.playback.currentTime,
    }));
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(list));
    return;
  }
  res.writeHead(404);
  res.end();
});

// ── WebSocket server ────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

wss.on("connection", (ws, req) => {
  const clientIp = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
  console.log(`[+] Client connected from ${clientIp}`);

  ws._userId   = null;
  ws._roomId   = null;
  ws.isAlive   = true;

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

// ── Heartbeat ───────────────────────────────────────────────────────────────
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
    send(ws, { type: "PING" });
  });
}, PING_INTERVAL_MS);

wss.on("close", () => clearInterval(pingInterval));

// ── Room cleanup ────────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [id, room] of rooms) {
    if (now - room.lastActivity > ROOM_TTL_MS || room.members.size === 0) {
      rooms.delete(id);
      console.log(`[~] Room ${id} expired and removed`);
    }
  }
}, 60 * 60 * 1000); // check hourly

// ── Message handler ─────────────────────────────────────────────────────────
function handleMessage(ws, msg) {
  touch(ws._roomId);

  switch (msg.type) {

    // ── CREATE_ROOM ──────────────────────────────────────────────────────────
    case "CREATE_ROOM": {
      const { userId, username } = msg;
      if (!userId || !username) return send(ws, { type: "ROOM_ERROR", message: "Missing userId or username" });

      const roomId = generateRoomCode();
      const room = {
        id: roomId,
        hostId: userId,
        members: new Map(),
        playback: { playing: false, currentTime: 0, updatedAt: Date.now() },
        createdAt: Date.now(),
        lastActivity: Date.now(),
      };

      const member = { userId, username, ws, isHost: true, joinedAt: Date.now() };
      room.members.set(userId, member);
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

    // ── JOIN_ROOM ────────────────────────────────────────────────────────────
    case "JOIN_ROOM": {
      const { userId, username, roomId } = msg;
      if (!userId || !username || !roomId) return send(ws, { type: "ROOM_ERROR", message: "Missing fields" });

      const room = rooms.get(roomId);
      if (!room) return send(ws, { type: "ROOM_ERROR", message: `Room ${roomId} not found` });
      if (room.members.size >= 10) return send(ws, { type: "ROOM_ERROR", message: "Room is full (max 10)" });

      const member = { userId, username, ws, isHost: false, joinedAt: Date.now() };
      room.members.set(userId, member);

      ws._userId = userId;
      ws._roomId = roomId;

      // Tell joiner about room state
      send(ws, {
        type: "ROOM_JOINED",
        roomId,
        isHost: false,
        members: serializeMembers(room),
        playback: room.playback,
      });

      // Tell everyone else
      broadcastToRoom(room, {
        type: "USER_JOINED",
        userId,
        username,
        members: serializeMembers(room),
      }, userId);

      // Send current playback position to new joiner
      send(ws, {
        type: "ROOM_STATE",
        ...room.playback,
        members: serializeMembers(room),
      });

      console.log(`[+] ${username} joined room ${roomId} (${room.members.size} members)`);
      break;
    }

    // ── LEAVE_ROOM ───────────────────────────────────────────────────────────
    case "LEAVE_ROOM": {
      handleDisconnect(ws);
      break;
    }

    // ── SYNC_PLAY ────────────────────────────────────────────────────────────
    case "SYNC_PLAY": {
      const room = getRoom(ws);
      if (!room) return;
      if (!isHost(ws, room)) return send(ws, { type: "ERROR", message: "Only host can control playback" });

      room.playback = { playing: true, currentTime: msg.currentTime, updatedAt: Date.now() };

      broadcastToRoom(room, {
        type: "SYNC_PLAY",
        currentTime: msg.currentTime,
        username: getMember(ws, room)?.username,
      }, ws._userId);

      break;
    }

    // ── SYNC_PAUSE ───────────────────────────────────────────────────────────
    case "SYNC_PAUSE": {
      const room = getRoom(ws);
      if (!room) return;
      if (!isHost(ws, room)) return send(ws, { type: "ERROR", message: "Only host can control playback" });

      room.playback = { playing: false, currentTime: msg.currentTime, updatedAt: Date.now() };

      broadcastToRoom(room, {
        type: "SYNC_PAUSE",
        currentTime: msg.currentTime,
        username: getMember(ws, room)?.username,
      }, ws._userId);

      break;
    }

    // ── SYNC_SEEK ────────────────────────────────────────────────────────────
    case "SYNC_SEEK": {
      const room = getRoom(ws);
      if (!room) return;
      if (!isHost(ws, room)) return send(ws, { type: "ERROR", message: "Only host can control playback" });

      room.playback.currentTime = msg.currentTime;
      room.playback.updatedAt   = Date.now();

      broadcastToRoom(room, {
        type: "SYNC_SEEK",
        currentTime: msg.currentTime,
        playing: room.playback.playing,
      }, ws._userId);

      break;
    }

    // ── CHAT_MESSAGE ─────────────────────────────────────────────────────────
    case "CHAT_MESSAGE": {
      const room = getRoom(ws);
      if (!room) return;

      const payload = {
        type: "CHAT_MESSAGE",
        userId:    ws._userId,
        username:  msg.username,
        text:      sanitize(msg.text),
        timestamp: Date.now(),
      };

      // Broadcast to everyone in the room including sender (for consistency)
      broadcastToRoom(room, payload);
      break;
    }

    // ── PONG ─────────────────────────────────────────────────────────────────
    case "PONG":
      ws.isAlive = true;
      break;

    default:
      send(ws, { type: "ERROR", message: `Unknown message type: ${msg.type}` });
  }
}

// ── Disconnect handler ──────────────────────────────────────────────────────
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

  // If host left, promote first remaining member
  if (room.hostId === userId) {
    const [newHostId, newHost] = room.members.entries().next().value;
    room.hostId     = newHostId;
    newHost.isHost  = true;
    console.log(`[~] Host left — ${newHost.username} is now host of ${roomId}`);

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

// ── Utility helpers ─────────────────────────────────────────────────────────
function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function broadcastToRoom(room, msg, excludeUserId = null) {
  for (const [uid, member] of room.members) {
    if (uid === excludeUserId) continue;
    send(member.ws, msg);
  }
}

function getRoom(ws) {
  return ws._roomId ? rooms.get(ws._roomId) : null;
}

function getMember(ws, room) {
  return room?.members.get(ws._userId);
}

function isHost(ws, room) {
  return room?.hostId === ws._userId;
}

function serializeMembers(room) {
  return [...room.members.values()].map(m => ({
    userId:   m.userId,
    username: m.username,
    isHost:   m.userId === room.hostId,
  }));
}

function touch(roomId) {
  if (roomId) {
    const r = rooms.get(roomId);
    if (r) r.lastActivity = Date.now();
  }
}

function generateRoomCode() {
  const words = ["NITE","REEL","SYNC","FILM","CINE","PLAY","SHOW","SCENE"];
  const word   = words[Math.floor(Math.random() * words.length)];
  const num    = Math.floor(1000 + Math.random() * 9000);
  return `${word}-${num}`;
}

function sanitize(str = "") {
  return String(str).slice(0, 500).replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ── Boot ─────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════╗
║         🎬  CinemaSync Server          ║
╠════════════════════════════════════════╣
║  WebSocket : ws://localhost:${PORT}       ║
║  Health    : http://localhost:${PORT}/health ║
║  Rooms API : http://localhost:${PORT}/rooms  ║
╚════════════════════════════════════════╝
  `);
});
