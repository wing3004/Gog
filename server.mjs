// Hexa 웹 서버: 게임 페이지(설치형 웹앱 파일 포함)를 내주고, 계정·랭크를 저장하고, 멀티플레이 방을 중계한다.
// 게임 판은 방장 브라우저가 돌리고, 이 서버는 메시지만 나른다 (claude.ai의 room과 같은 모양).
//   node server.mjs            (PORT 기본 8080, DATABASE_URL이 있으면 Postgres에 저장, 없으면 data/hexa-db.json)
import { createServer } from 'node:http';
import { readFileSync, existsSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
import { WebSocketServer } from 'ws';
import { accountsApi, openStore } from './accounts.mjs';

const here = dirname(fileURLToPath(import.meta.url));
// 게임 페이지 위치: 배포용 묶음(server.mjs 옆 index.html 또는 public/index.html), 저장소 안 빌드 결과(apps/client/dist)
const CANDIDATES = ['index.html', 'public/index.html', '../client/dist/index.html', 'apps/client/dist/index.html'].map((p) => join(here, p));
const findPage = () => CANDIDATES.find((p) => existsSync(p));
const ls = (d) => { try { return readdirSync(d).join(', '); } catch { return '(없음)'; } };
const PORT = Number(process.env.PORT || 8080);
const MAX_MSG = 64 * 1024;
const MAX_PRESENCE = 8 * 1024;
const CH_RE = /^(lobby|hw-[a-z0-9]{4,10})$/;
const TOPIC_RE = /^[a-z][a-z0-9_.-]{0,47}$/;

const store = await openStore(here);
const api = accountsApi(store);
if (store.kind === 'file' && process.env.RENDER) console.warn('경고: DATABASE_URL이 없어 계정을 파일에 저장한다. Render 무료 서버는 다시 시작할 때 파일이 지워지니 Postgres(DATABASE_URL)를 연결하자.');

// 설치형 웹앱(안드로이드 홈 화면·앱 포장)에 필요한 파일: index.html과 같은 폴더에서 찾는다
const STATIC = {
  '/manifest.webmanifest': 'application/manifest+json',
  '/sw.js': 'text/javascript; charset=utf-8',
  '/icon-192.png': 'image/png',
  '/icon-512.png': 'image/png',
  '/icon-maskable-512.png': 'image/png',
  '/apple-touch-icon.png': 'image/png',
};

const server = createServer(async (req, res) => {
  const url = new URL(req.url || '/', 'http://x');
  if (url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    return res.end('ok');
  }
  if (await api(req, res, url)) return;
  if (url.pathname === '/.well-known/assetlinks.json') {
    // 안드로이드 앱(TWA)이 이 사이트를 주소창 없이 열 수 있게 하는 인증 파일
    const page = findPage();
    const file = page ? join(dirname(page), 'assetlinks.json') : '';
    const body = process.env.ASSETLINKS_JSON || (file && existsSync(file) ? readFileSync(file, 'utf8') : null);
    if (!body) {
      res.writeHead(404);
      return res.end('not found');
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(body);
  }
  if (STATIC[url.pathname]) {
    const page = findPage();
    const file = page ? join(dirname(page), url.pathname.slice(1)) : '';
    if (!file || !existsSync(file)) {
      res.writeHead(404);
      return res.end('not found');
    }
    res.writeHead(200, { 'content-type': STATIC[url.pathname], 'cache-control': url.pathname === '/sw.js' ? 'no-cache' : 'public, max-age=86400' });
    return res.end(readFileSync(file));
  }
  if (url.pathname !== '/' && url.pathname !== '/index.html') {
    res.writeHead(404);
    return res.end('not found');
  }
  const page = findPage();
  if (!page) {
    res.writeHead(500, { 'content-type': 'text/plain; charset=utf-8' });
    return res.end(
      '게임 파일(index.html)을 찾지 못했습니다. server.mjs와 같은 폴더에 index.html을 올려 주세요.\n\n' +
        `server.mjs 위치: ${here}\n그 폴더의 파일: ${ls(here)}\npublic 폴더의 파일: ${ls(join(here, 'public'))}\n`,
    );
  }
  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
  res.end(readFileSync(page));
});

/** 채널 이름 → (peer → { ws, presence }) */
const chans = new Map();
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MSG });

function peersOf(ch) {
  return [...(chans.get(ch)?.entries() ?? [])].map(([peer, v]) => ({ peer, presence: v.presence }));
}
function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcastPeers(ch, change) {
  const c = chans.get(ch);
  if (!c) return;
  const peers = peersOf(ch);
  for (const v of c.values()) send(v.ws, { kind: 'peers', ch, peers, change });
}
function leave(ch, peer) {
  const c = chans.get(ch);
  if (!c?.has(peer)) return;
  c.delete(peer);
  if (c.size === 0) chans.delete(ch);
  else broadcastPeers(ch, { joined: [], left: [peer], updated: [] });
}

wss.on('connection', (ws) => {
  const peer = randomBytes(8).toString('hex');
  const joined = new Set();
  let budget = 80;
  const refill = setInterval(() => (budget = Math.min(80, budget + 40)), 1000);
  send(ws, { kind: 'hello', peer });
  ws.on('message', (raw) => {
    if (--budget < 0) return;
    let m;
    try {
      m = JSON.parse(String(raw));
    } catch {
      return;
    }
    if (!m || typeof m.ch !== 'string' || !CH_RE.test(m.ch)) return;
    const ch = m.ch;
    if (m.op === 'join') {
      if (!chans.has(ch)) chans.set(ch, new Map());
      if (joined.size >= 17) return;
      chans.get(ch).set(peer, { ws, presence: {} });
      joined.add(ch);
      broadcastPeers(ch, { joined: [peer], left: [], updated: [] });
    } else if (m.op === 'leave') {
      joined.delete(ch);
      leave(ch, peer);
    } else if (m.op === 'presence') {
      const me = chans.get(ch)?.get(peer);
      if (!me || !m.patch || typeof m.patch !== 'object') return;
      const next = { ...me.presence };
      for (const [k, v] of Object.entries(m.patch)) {
        if (v === null) delete next[k];
        else next[k] = v;
      }
      if (JSON.stringify(next).length > MAX_PRESENCE) return;
      me.presence = next;
      broadcastPeers(ch, { joined: [], left: [], updated: [peer] });
    } else if (m.op === 'emit') {
      const c = chans.get(ch);
      if (!c?.has(peer) || typeof m.topic !== 'string' || !TOPIC_RE.test(m.topic)) return;
      const out = JSON.stringify({ kind: 'msg', ch, topic: m.topic, data: m.data, peer });
      for (const v of c.values()) if (v.ws.readyState === 1) v.ws.send(out);
    }
  });
  ws.on('close', () => {
    clearInterval(refill);
    for (const ch of joined) leave(ch, peer);
  });
});

// 끊긴 연결 정리
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
});

server.listen(PORT, () => console.log(`Hexa: http://localhost:${PORT} (계정 저장: ${store.kind === 'postgres' ? 'Postgres' : process.env.DATA_FILE || 'data/hexa-db.json'})`));
const bye = async () => {
  await store.flush?.();
  process.exit(0);
};
process.on('SIGTERM', bye);
process.on('SIGINT', bye);
