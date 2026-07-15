import { randomBytes, timingSafeEqual } from 'node:crypto';
import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { WebSocket, WebSocketServer } from 'ws';
import { createClaudeWatcher } from './watchers/claude.js';
import { createCodexWatcher } from './watchers/codex.js';

const HOST = '127.0.0.1';
const DEFAULT_PORT = 41414;
const ACCESS_TOKEN_BYTES = 32;
const DEFAULT_VOICEVOX_URL = 'http://127.0.0.1:50021';
const MAX_JSON_BODY_BYTES = 4096;
const MAX_VOICE_TEXT_LENGTH = 180;
const VOICEVOX_TIMEOUT_MS = 10_000;
const FALLBACK_TEXT = 'UI ファイルが見つかりません';
const PRIVATE_RESPONSE_HEADERS = {
  'Cache-Control': 'no-store',
  'Referrer-Policy': 'no-referrer',
  'X-Content-Type-Options': 'nosniff',
};
const CONTENT_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

const sourceDir = path.dirname(fileURLToPath(import.meta.url));
const defaultUiRoot = path.resolve(sourceDir, '..', 'ui');
const voicevoxSpeakers = new Map([
  ['zundamon', 3],
  ['metan', 2],
]);

function requestedPort(value) {
  const port = Number(value);
  return Number.isInteger(port) && port >= 0 && port <= 65535 ? port : DEFAULT_PORT;
}

function isInside(root, candidate) {
  return candidate === root || candidate.startsWith(`${root}${path.sep}`);
}

function isAllowedHostname(hostname) {
  return hostname === HOST || hostname === 'localhost';
}

function configuredVoicevoxUrl(value = DEFAULT_VOICEVOX_URL) {
  try {
    const url = new URL(value);
    const isHttp = url.protocol === 'http:';
    const isLoopback = isAllowedHostname(url.hostname);
    const hasCredentials = url.username !== '' || url.password !== '';
    if (!isHttp || !isLoopback || hasCredentials) return null;
    url.pathname = '/';
    url.search = '';
    url.hash = '';
    return url;
  } catch {
    return null;
  }
}

function isAllowedHttpHost(host) {
  if (typeof host !== 'string') return false;
  try {
    const parsed = new URL(`http://${host}`);
    return isAllowedHostname(parsed.hostname)
      && parsed.username === ''
      && parsed.password === ''
      && parsed.pathname === '/'
      && parsed.search === ''
      && parsed.hash === '';
  } catch {
    return false;
  }
}

function isAllowedWsOrigin(origin, actualPort) {
  if (origin === undefined) return true;
  if (typeof origin !== 'string') return false;
  return origin === `http://${HOST}:${actualPort}`
    || origin === `http://localhost:${actualPort}`;
}

function accessPath(pathname, accessToken) {
  const parts = pathname.split('/');
  const candidate = parts[1] ?? '';
  const candidateBuffer = Buffer.from(candidate);
  const tokenBuffer = Buffer.from(accessToken);
  if (candidateBuffer.length !== tokenBuffer.length
    || !timingSafeEqual(candidateBuffer, tokenBuffer)) return null;

  return {
    rootWithoutSlash: parts.length === 2,
    relative: parts.slice(2).join('/'),
  };
}

function requestPathname(request) {
  try {
    return decodeURIComponent(new URL(request.url, `http://${HOST}`).pathname);
  } catch {
    return null;
  }
}

function writeResponse(response, status, body, headers = {}) {
  response.writeHead(status, {
    ...PRIVATE_RESPONSE_HEADERS,
    'Content-Type': 'text/plain; charset=utf-8',
    ...headers,
  });
  response.end(body);
}

function readJsonBody(request) {
  return new Promise((resolve, reject) => {
    let totalBytes = 0;
    const chunks = [];
    request.on('data', (chunk) => {
      totalBytes += chunk.length;
      const isTooLarge = totalBytes > MAX_JSON_BODY_BYTES;
      if (isTooLarge) {
        reject(new Error('request body too large'));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on('end', () => {
      try {
        const body = Buffer.concat(chunks).toString('utf8');
        resolve(body ? JSON.parse(body) : {});
      } catch (error) {
        reject(error);
      }
    });
    request.on('error', reject);
  });
}

function normalizeVoiceText(value) {
  const isString = typeof value === 'string';
  if (!isString) return '';
  const compact = value.replace(/\s+/g, ' ').trim();
  const characters = Array.from(compact);
  const isTooLong = characters.length > MAX_VOICE_TEXT_LENGTH;
  if (isTooLong) return `${characters.slice(0, MAX_VOICE_TEXT_LENGTH).join('')}…`;
  return compact;
}

function voicevoxSpeakerId(value) {
  const namedSpeaker = typeof value === 'string' ? voicevoxSpeakers.get(value) : undefined;
  if (namedSpeaker !== undefined) return namedSpeaker;
  const numericSpeaker = Number(value);
  const isValidSpeaker = Number.isInteger(numericSpeaker) && numericSpeaker >= 0 && numericSpeaker <= 10_000;
  return isValidSpeaker ? numericSpeaker : null;
}

async function fetchWithTimeout(url, options) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), VOICEVOX_TIMEOUT_MS);
  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }
}

async function synthesizeVoicevox({ voicevoxUrl, text, speaker }) {
  const queryUrl = new URL('/audio_query', voicevoxUrl);
  queryUrl.searchParams.set('text', text);
  queryUrl.searchParams.set('speaker', String(speaker));
  const queryResponse = await fetchWithTimeout(queryUrl, {
    method: 'POST',
    headers: { Accept: 'application/json' },
  });
  const isQueryOk = queryResponse.ok;
  if (!isQueryOk) throw new Error(`audio_query failed: ${queryResponse.status}`);

  const audioQuery = await queryResponse.json();
  const synthesisUrl = new URL('/synthesis', voicevoxUrl);
  synthesisUrl.searchParams.set('speaker', String(speaker));
  const synthesisResponse = await fetchWithTimeout(synthesisUrl, {
    method: 'POST',
    headers: {
      Accept: 'audio/wav',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(audioQuery),
  });
  const isSynthesisOk = synthesisResponse.ok;
  if (!isSynthesisOk) throw new Error(`synthesis failed: ${synthesisResponse.status}`);

  return Buffer.from(await synthesisResponse.arrayBuffer());
}

async function handleVoicevoxRequest(request, response, voicevoxUrl) {
  const isPost = request.method === 'POST';
  if (!isPost) {
    writeResponse(response, 405, 'Method Not Allowed');
    return;
  }
  const hasEngine = voicevoxUrl !== null;
  if (!hasEngine) {
    writeResponse(response, 503, 'VOICEVOX endpoint is not allowed');
    return;
  }

  try {
    const payload = await readJsonBody(request);
    const text = normalizeVoiceText(payload?.text);
    const speaker = voicevoxSpeakerId(payload?.speaker);
    const hasText = text.length > 0;
    const hasSpeaker = speaker !== null;
    if (!hasText || !hasSpeaker) {
      writeResponse(response, 400, 'Bad Request');
      return;
    }

    const audio = await synthesizeVoicevox({ voicevoxUrl, text, speaker });
    response.writeHead(200, {
      ...PRIVATE_RESPONSE_HEADERS,
      'Content-Type': 'audio/wav',
      'Content-Length': audio.length,
    });
    response.end(audio);
  } catch (error) {
    if (process.env.AGENTARIUM_DEBUG) console.error('[server] VOICEVOX error', error);
    const message = error?.message === 'request body too large' ? 'Payload Too Large' : 'VOICEVOX unavailable';
    const status = error?.message === 'request body too large' ? 413 : 503;
    writeResponse(response, status, message);
  }
}

function rejectUpgrade(socket, status = 403, message = 'Forbidden') {
  const body = `${message}\n`;
  socket.write([
    `HTTP/1.1 ${status} ${message}`,
    'Connection: close',
    'Content-Type: text/plain; charset=utf-8',
    `Content-Length: ${Buffer.byteLength(body)}`,
    '',
    body,
  ].join('\r\n'));
  socket.destroy();
}

function createHttpHandler(uiRoot, accessToken, voicevoxUrl) {
  const normalizedRoot = path.resolve(uiRoot);
  return async (request, response) => {
    if (!isAllowedHttpHost(request.headers.host)) {
      writeResponse(response, 403, 'Forbidden');
      return;
    }

    const pathname = requestPathname(request);
    if (pathname === null) {
      writeResponse(response, 400, 'Bad Request');
      return;
    }

    const access = accessPath(pathname, accessToken);
    if (!access) {
      writeResponse(response, 403, 'Forbidden');
      return;
    }
    if (access.rootWithoutSlash) {
      writeResponse(response, 308, '', { Location: `${pathname}/` });
      return;
    }

    const isVoicevoxRequest = access.relative === 'voicevox/synthesis';
    if (isVoicevoxRequest) {
      await handleVoicevoxRequest(request, response, voicevoxUrl);
      return;
    }

    if (request.method !== 'GET' && request.method !== 'HEAD') {
      writeResponse(response, 405, 'Method Not Allowed');
      return;
    }

    const relative = access.relative || 'index.html';
    const candidate = path.resolve(normalizedRoot, relative);
    if (!isInside(normalizedRoot, candidate)) {
      writeResponse(response, 403, 'Forbidden');
      return;
    }

    try {
      const info = await stat(candidate);
      if (!info.isFile()) throw new Error('not a file');
      const content = await readFile(candidate);
      response.writeHead(200, {
        ...PRIVATE_RESPONSE_HEADERS,
        'Content-Type': CONTENT_TYPES[path.extname(candidate).toLowerCase()] ?? 'application/octet-stream',
      });
      response.end(request.method === 'HEAD' ? undefined : content);
    } catch {
      if (relative === 'index.html') {
        response.writeHead(200, {
          ...PRIVATE_RESPONSE_HEADERS,
          'Content-Type': 'text/plain; charset=utf-8',
        });
        response.end(request.method === 'HEAD' ? undefined : FALLBACK_TEXT);
      } else {
        writeResponse(response, 404, 'Not Found');
      }
    }
  };
}

export async function startServer({
  port = requestedPort(process.env.AGENTARIUM_PORT ?? DEFAULT_PORT),
  uiRoot = defaultUiRoot,
  claudeRoot,
  codexRoot,
  voicevoxUrl = process.env.AGENTARIUM_VOICEVOX_URL ?? DEFAULT_VOICEVOX_URL,
} = {}) {
  let debounceTimer = null;
  let heartbeatTimer = null;
  let closed = false;
  const accessToken = randomBytes(ACCESS_TOKEN_BYTES).toString('base64url');
  const websocketPath = `/${accessToken}/ws`;
  const server = createServer(createHttpHandler(uiRoot, accessToken, configuredVoicevoxUrl(voicevoxUrl)));
  const wss = new WebSocketServer({ noServer: true });

  server.on('upgrade', (request, socket, head) => {
    const pathname = requestPathname(request);
    if (request.method !== 'GET'
      || !isAllowedHttpHost(request.headers.host)
      || !isAllowedWsOrigin(request.headers.origin, request.socket.localPort)
      || pathname !== websocketPath) {
      rejectUpgrade(socket);
      return;
    }

    wss.handleUpgrade(request, socket, head, (websocket) => {
      wss.emit('connection', websocket, request);
    });
  });

  function sessions(now = Date.now()) {
    return claude.getSessions(now)
      .concat(codex.getSessions(now))
      .sort((left, right) => right.lastActivity - left.lastActivity);
  }

  function snapshot() {
    const at = Date.now();
    return { type: 'snapshot', at, sessions: sessions(at) };
  }

  function broadcast() {
    if (closed) return;
    const payload = JSON.stringify(snapshot());
    for (const client of wss.clients) {
      if (client.readyState === WebSocket.OPEN) client.send(payload);
    }
  }

  function scheduleBroadcast() {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(broadcast, 1000);
  }

  const claude = createClaudeWatcher({
    onUpdate: scheduleBroadcast,
    ...(claudeRoot === undefined ? {} : { root: claudeRoot }),
  });
  const codex = createCodexWatcher({
    onUpdate: scheduleBroadcast,
    ...(codexRoot === undefined ? {} : { root: codexRoot }),
  });

  wss.on('connection', (socket) => {
    socket.send(JSON.stringify(snapshot()));
  });
  wss.on('error', (error) => {
    if (process.env.AGENTARIUM_DEBUG) console.error('[server] WebSocket error', error);
  });

  try {
    await Promise.all([claude.start(), codex.start()]);
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, HOST, () => {
        server.off('error', reject);
        resolve();
      });
    });
  } catch (error) {
    await Promise.allSettled([claude.close(), codex.close()]);
    wss.close();
    throw error;
  }

  heartbeatTimer = setInterval(broadcast, 15_000);
  const address = server.address();
  const actualPort = typeof address === 'object' && address ? address.port : port;

  async function close() {
    if (closed) return;
    closed = true;
    clearTimeout(debounceTimer);
    clearInterval(heartbeatTimer);
    await Promise.allSettled([claude.close(), codex.close()]);
    for (const client of wss.clients) client.terminate();
    await new Promise((resolve) => wss.close(resolve));
    await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }

  return {
    server,
    wss,
    host: HOST,
    port: actualPort,
    url: `http://${HOST}:${actualPort}/${accessToken}/`,
    getSnapshot: snapshot,
    close,
  };
}

const isDirectRun = process.argv[1]
  && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url;

if (isDirectRun) {
  startServer()
    .then(({ url }) => console.log(`Agentarium Space listening on ${url}`))
    .catch((error) => {
      console.error('Agentarium Space failed to start:', error);
      process.exitCode = 1;
    });
}
