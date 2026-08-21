const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');
const { TikTokLiveConnection, WebcastEvent, SignConfig } = require('tiktok-live-connector');
const { LiveChat } = require('youtube-chat');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// Euler Stream API key (free, no credit card) — daftar di https://www.eulerstream.com/register
// Set sebagai environment variable EULER_API_KEY di Render/Koyeb (jangan hardcode langsung di sini)
if (process.env.EULER_API_KEY) {
  SignConfig.apiKey = process.env.EULER_API_KEY;
} else {
  console.warn('[TikTok] EULER_API_KEY belum di-set — koneksi TikTok mungkin gagal karena rate limit sign server publik.');
}

app.use(express.static(path.join(__dirname, 'public')));

const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_DELAY = 3000; // ms, doubles each retry (backoff)

// --- Simple in-memory spam guard ---
// Filters repeated identical messages from the same user within a short window.
function createSpamFilter({ windowMs = 4000, maxRepeats = 2 } = {}) {
  const history = new Map(); // key -> { lastMessage, count, lastTime }

  return function isSpam(key, message) {
    const now = Date.now();
    const entry = history.get(key);

    if (!entry || now - entry.lastTime > windowMs) {
      history.set(key, { lastMessage: message, count: 1, lastTime: now });
      return false;
    }

    if (entry.lastMessage === message) {
      entry.count += 1;
      entry.lastTime = now;
      return entry.count > maxRepeats;
    }

    entry.lastMessage = message;
    entry.count = 1;
    entry.lastTime = now;
    return false;
  };
}

io.on('connection', (socket) => {
  const { tiktok, youtube } = socket.handshake.query;
  let tiktokConn = null;
  let liveChat = null;
  let tiktokReconnectAttempts = 0;
  let youtubeReconnectTimer = null;
  let closed = false;

  const spamFilter = createSpamFilter();

  const emitChat = (platform, username, message) => {
    if (!message) return;
    const key = `${platform}:${username}`;
    if (spamFilter(key, message)) {
      console.log(`[Spam Filtered] ${platform} - ${username}: ${message}`);
      return;
    }
    socket.emit('chat-message', { platform, username, message, ts: Date.now() });
  };

  // ---------- TikTok ----------
  function connectTikTok() {
    if (closed || !tiktok) return;

    tiktokConn = new TikTokLiveConnection(tiktok, {});

    tiktokConn.connect()
      .then(() => {
        tiktokReconnectAttempts = 0;
        console.log(`[TikTok] Connected to @${tiktok}`);
      })
      .catch((err) => {
        console.error('[TikTok Error] connect failed:', err.message);
        scheduleTikTokReconnect();
      });

    tiktokConn.on(WebcastEvent.CHAT, (data) => {
      emitChat('tiktok', data.user?.nickname || data.user?.uniqueId, data.comment);
    });

    tiktokConn.on(WebcastEvent.DISCONNECTED, () => {
      console.warn('[TikTok] Disconnected');
      scheduleTikTokReconnect();
    });

    tiktokConn.on(WebcastEvent.STREAM_END, () => {
      console.warn('[TikTok] Stream ended by host');
    });
  }

  function scheduleTikTokReconnect() {
    if (closed) return;
    if (tiktokReconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
      console.error('[TikTok] Max reconnect attempts reached, giving up.');
      return;
    }
    const delay = RECONNECT_BASE_DELAY * 2 ** tiktokReconnectAttempts;
    tiktokReconnectAttempts += 1;
    console.log(`[TikTok] Reconnecting in ${delay}ms (attempt ${tiktokReconnectAttempts})`);
    setTimeout(() => {
      if (!closed) connectTikTok();
    }, delay);
  }

  // ---------- YouTube ----------
  function connectYouTube() {
    if (closed || !youtube) return;

    liveChat = new LiveChat({ liveId: youtube });

    liveChat.on('chat', (chatItem) => {
      const textMsg = chatItem.message.map((m) => m.text || m.emojiText || '').join('');
      emitChat('youtube', chatItem.author.name, textMsg);
    });

    liveChat.on('error', (err) => {
      console.error('[YouTube Error]', err.message || err);
      scheduleYouTubeReconnect();
    });

    liveChat.on('end', () => {
      console.warn('[YouTube] Chat ended');
      scheduleYouTubeReconnect();
    });

    liveChat.start()
      .then((ok) => {
        if (!ok) {
          console.error('[YouTube] Failed to start chat listener');
          scheduleYouTubeReconnect();
        } else {
          console.log(`[YouTube] Connected to live ${youtube}`);
        }
      })
      .catch((err) => {
        console.error('[YouTube Error] start failed:', err.message);
        scheduleYouTubeReconnect();
      });
  }

  function scheduleYouTubeReconnect() {
    if (closed || youtubeReconnectTimer) return;
    youtubeReconnectTimer = setTimeout(() => {
      youtubeReconnectTimer = null;
      if (!closed) connectYouTube();
    }, RECONNECT_BASE_DELAY);
  }

  if (tiktok) connectTikTok();
  if (youtube) connectYouTube();

  socket.on('disconnect', () => {
    closed = true;
    if (tiktokConn) {
      try { tiktokConn.disconnect(); } catch (e) { /* ignore */ }
    }
    if (liveChat) {
      try { liveChat.stop(); } catch (e) { /* ignore */ }
    }
    if (youtubeReconnectTimer) clearTimeout(youtubeReconnectTimer);
    console.log('[Socket] Client disconnected, listeners cleaned up.');
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
