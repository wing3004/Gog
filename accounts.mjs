// Hexa 계정과 랭크: 회원가입·로그인(세션 토큰), 판 결과로 RP 반영, 순위표
// 저장소는 둘 중 하나
//   - Postgres: DATABASE_URL이 있으면 (Render 무료 서버처럼 디스크가 지워지는 곳에서는 이걸 써야 한다)
//   - JSON 파일: 없으면 DATA_FILE(기본 ./data/hexa-db.json) 한 파일에 저장 (내 컴퓨터·자체 서버용)
import { scrypt as scryptCb, randomBytes, timingSafeEqual, createHash } from 'node:crypto';
import { readFile, writeFile, rename, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { promisify } from 'node:util';

const scrypt = promisify(scryptCb);

// ───────────── RP 계산 (클라이언트 apps/client/src/rank.ts 와 같은 식) ─────────────
const TIER_MIN = [0, 300, 600, 900, 1200, 1500];
const BASE = [32, 20, 10, -4, -12, -20];
const GAIN = [0.5, 0.8, 1, 1.25];
const LOSS = [1.25, 1, 0.85, 0.7];

export function rpDelta(place, teams, level, minutes, rp, forfeit = false) {
  const idx = Math.round(((place - 1) / Math.max(1, teams - 1)) * (BASE.length - 1));
  const base = forfeit ? -24 : BASE[Math.max(0, Math.min(BASE.length - 1, idx))];
  const len = Math.min(1, 0.6 + (minutes / 60) * 0.4);
  if (base >= 0) return Math.round(base * (GAIN[level] ?? 1) * len);
  let loss = base * (LOSS[level] ?? 1);
  if (rp < TIER_MIN[1]) loss *= 0.5;
  else if (rp >= TIER_MIN[4]) loss *= 1.2;
  return Math.min(-1, Math.round(loss));
}

export const freshRank = () => ({ rp: 0, best: 0, played: 0, wins: 0, top3: 0, history: [], done: [] });

/** 판 결과를 반영한 새 랭크 (같은 판이면 null) */
export function applyMatch(rank, id, r, now = Date.now()) {
  if (rank.done.includes(id)) return null;
  const before = rank.rp;
  const delta = rpDelta(r.place, r.teams, r.level, r.minutes, before, r.forfeit);
  const after = Math.max(0, before + delta);
  const record = { mode: r.mode, place: r.place, teams: r.teams, level: r.level, minutes: r.minutes, ...(r.forfeit ? { forfeit: true } : {}), at: now, delta: after - before, rp: after };
  const next = {
    rp: after,
    best: Math.max(rank.best, after),
    played: rank.played + 1,
    wins: rank.wins + (r.place === 1 && !r.forfeit ? 1 : 0),
    top3: rank.top3 + (r.place <= 3 && !r.forfeit ? 1 : 0),
    history: [record, ...rank.history].slice(0, 30),
    done: [id, ...rank.done].slice(0, 60),
  };
  return { next, result: { before, after, delta: after - before, record } };
}

// ───────────── 저장소 ─────────────

/** JSON 파일 저장소: 쓰기는 모았다가 임시 파일 → 이름 바꾸기로 */
export async function fileStore(path) {
  let db = { nextId: 1, users: [], sessions: {} };
  try {
    db = JSON.parse(await readFile(path, 'utf8'));
  } catch {
    /* 처음이면 빈 저장소 */
  }
  let timer = null;
  let writing = Promise.resolve();
  const flush = () => {
    timer = null;
    const text = JSON.stringify(db);
    writing = writing.then(async () => {
      await mkdir(dirname(path), { recursive: true });
      await writeFile(`${path}.tmp`, text);
      await rename(`${path}.tmp`, path);
    });
    return writing;
  };
  const dirty = () => {
    if (!timer) timer = setTimeout(flush, 200);
  };
  return {
    kind: 'file',
    persistent: !process.env.RENDER,
    async userByKey(key) {
      return db.users.find((u) => u.key === key) ?? null;
    },
    async userById(id) {
      return db.users.find((u) => u.id === id) ?? null;
    },
    async createUser(u) {
      const user = { ...u, id: db.nextId++ };
      db.users.push(user);
      dirty();
      return user;
    },
    async saveRank(id, rank) {
      const u = db.users.find((x) => x.id === id);
      if (u) u.rank = rank;
      dirty();
    },
    async addSession(hash, userId) {
      db.sessions[hash] = { userId, at: Date.now() };
      const mine = Object.entries(db.sessions).filter(([, s]) => s.userId === userId).sort((a, b) => b[1].at - a[1].at);
      for (const [h] of mine.slice(10)) delete db.sessions[h];
      dirty();
    },
    async session(hash) {
      return db.sessions[hash]?.userId ?? null;
    },
    async dropSession(hash) {
      delete db.sessions[hash];
      dirty();
    },
    async top(n) {
      return [...db.users].sort((a, b) => b.rank.rp - a.rank.rp || a.id - b.id).slice(0, n);
    },
    async count() {
      return db.users.length;
    },
    flush,
  };
}

/** Postgres 저장소 (pg Pool 또는 같은 query 모양의 객체) */
export async function pgStore(pool) {
  await pool.query(`create table if not exists hexa_users (
    id serial primary key,
    name text not null,
    name_key text not null unique,
    salt text not null,
    hash text not null,
    created bigint not null,
    rp integer not null default 0,
    rank jsonb not null
  )`);
  await pool.query(`create table if not exists hexa_sessions (
    token text primary key,
    user_id integer not null,
    created bigint not null
  )`);
  const row = (r) => (r ? { id: r.id, name: r.name, key: r.name_key, salt: r.salt, hash: r.hash, created: Number(r.created), rank: typeof r.rank === 'string' ? JSON.parse(r.rank) : r.rank } : null);
  return {
    kind: 'postgres',
    persistent: true,
    async userByKey(key) {
      const r = await pool.query('select * from hexa_users where name_key = $1', [key]);
      return row(r.rows[0]);
    },
    async userById(id) {
      const r = await pool.query('select * from hexa_users where id = $1', [id]);
      return row(r.rows[0]);
    },
    async createUser(u) {
      const r = await pool.query('insert into hexa_users (name, name_key, salt, hash, created, rp, rank) values ($1, $2, $3, $4, $5, $6, $7) returning *', [u.name, u.key, u.salt, u.hash, u.created, u.rank.rp, JSON.stringify(u.rank)]);
      return row(r.rows[0]);
    },
    async saveRank(id, rank) {
      await pool.query('update hexa_users set rank = $1, rp = $2 where id = $3', [JSON.stringify(rank), rank.rp, id]);
    },
    async addSession(hash, userId) {
      await pool.query('insert into hexa_sessions (token, user_id, created) values ($1, $2, $3)', [hash, userId, Date.now()]);
      const r = await pool.query('select token from hexa_sessions where user_id = $1 order by created desc', [userId]);
      for (const old of r.rows.slice(10)) await pool.query('delete from hexa_sessions where token = $1', [old.token]);
    },
    async session(hash) {
      const r = await pool.query('select user_id from hexa_sessions where token = $1', [hash]);
      return r.rows[0]?.user_id ?? null;
    },
    async dropSession(hash) {
      await pool.query('delete from hexa_sessions where token = $1', [hash]);
    },
    async top(n) {
      const r = await pool.query('select * from hexa_users order by rp desc, id asc limit $1', [n]);
      return r.rows.map(row);
    },
    async count() {
      const r = await pool.query('select count(*)::int as n from hexa_users');
      return Number(r.rows[0].n);
    },
    async flush() {},
  };
}

/** 환경에 맞는 저장소 */
export async function openStore(here) {
  if (process.env.DATABASE_URL) {
    const { default: pg } = await import('pg');
    const ssl = /localhost|127\.0\.0\.1/.test(process.env.DATABASE_URL) ? false : { rejectUnauthorized: false };
    const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, ssl, max: 5 });
    return pgStore(pool);
  }
  return fileStore(process.env.DATA_FILE || `${here}/data/hexa-db.json`);
}

// ───────────── HTTP API ─────────────

const NAME_RE = /^[0-9A-Za-z가-힣_]{2,12}$/;
const ID_RE = /^[\w.:-]{4,80}$/;
const sha = (t) => createHash('sha256').update(t).digest('hex');

async function hashPassword(password, salt) {
  return (await scrypt(password, salt, 32)).toString('hex');
}

function publicProfile(u) {
  return { name: u.name, rank: u.rank };
}

export function accountsApi(store) {
  const hits = new Map();
  const limited = (ip, key, max) => {
    const k = `${ip}:${key}`;
    const now = Date.now();
    const h = hits.get(k);
    if (!h || now > h.reset) {
      hits.set(k, { n: 1, reset: now + 60000 });
      return false;
    }
    h.n++;
    return h.n > max;
  };
  setInterval(() => {
    const now = Date.now();
    for (const [k, h] of hits) if (now > h.reset) hits.delete(k);
  }, 60000).unref?.();

  const send = (res, status, body) => {
    res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', ...cors });
    res.end(JSON.stringify(body));
  };
  const cors = { 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type, authorization', 'access-control-allow-methods': 'GET, POST, OPTIONS' };
  const readBody = (req) =>
    new Promise((resolve) => {
      let size = 0;
      const chunks = [];
      req.on('data', (c) => {
        size += c.length;
        if (size > 4096) req.destroy();
        else chunks.push(c);
      });
      req.on('end', () => {
        try {
          resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'));
        } catch {
          resolve(null);
        }
      });
      req.on('error', () => resolve(null));
    });
  const authUser = async (req) => {
    const m = /^Bearer ([0-9a-f]{64})$/.exec(req.headers.authorization || '');
    if (!m) return null;
    const id = await store.session(sha(m[1]));
    return id === null ? null : await store.userById(id);
  };
  const newSession = async (u) => {
    const token = randomBytes(32).toString('hex');
    await store.addSession(sha(token), u.id);
    return token;
  };

  /** 처리했으면 true */
  return async function handle(req, res, url) {
    if (!url.pathname.startsWith('/api/')) return false;
    const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    try {
      if (req.method === 'OPTIONS') {
        res.writeHead(204, cors);
        res.end();
        return true;
      }
      const path = url.pathname;
      if (limited(ip, 'all', 240)) return send(res, 429, { error: '요청이 너무 많다. 잠시 뒤에 다시' }), true;
      if (path === '/api/status' && req.method === 'GET') {
        send(res, 200, { ok: true, storage: store.kind, persistent: store.persistent, users: await store.count() });
        return true;
      }
      if ((path === '/api/register' || path === '/api/login') && req.method === 'POST') {
        if (limited(ip, 'auth', 12)) return send(res, 429, { error: '시도가 너무 많다. 1분 뒤에 다시' }), true;
        const b = await readBody(req);
        const name = typeof b?.name === 'string' ? b.name.trim() : '';
        const password = typeof b?.password === 'string' ? b.password : '';
        if (!NAME_RE.test(name)) return send(res, 400, { error: '이름은 2~12자, 한글·영문·숫자·_만' }), true;
        if (password.length < 4 || password.length > 72) return send(res, 400, { error: '비밀번호는 4~72자' }), true;
        const key = name.toLowerCase();
        const found = await store.userByKey(key);
        if (path === '/api/register') {
          if (found) return send(res, 409, { error: '이미 있는 이름이다' }), true;
          const salt = randomBytes(16).toString('hex');
          const u = await store.createUser({ name, key, salt, hash: await hashPassword(password, salt), created: Date.now(), rank: freshRank() });
          send(res, 200, { token: await newSession(u), profile: publicProfile(u) });
          return true;
        }
        const ok = found && timingSafeEqual(Buffer.from(await hashPassword(password, found.salt), 'hex'), Buffer.from(found.hash, 'hex'));
        if (!ok) return send(res, 401, { error: '이름 또는 비밀번호가 맞지 않는다' }), true;
        send(res, 200, { token: await newSession(found), profile: publicProfile(found) });
        return true;
      }
      if (path === '/api/leaderboard' && req.method === 'GET') {
        const top = await store.top(30);
        send(res, 200, { top: top.map((u) => ({ name: u.name, rp: u.rank.rp, played: u.rank.played, wins: u.rank.wins })) });
        return true;
      }
      const u = await authUser(req);
      if (!u) return send(res, 401, { error: '다시 로그인하자' }), true;
      if (path === '/api/me' && req.method === 'GET') {
        send(res, 200, { profile: publicProfile(u) });
        return true;
      }
      if (path === '/api/logout' && req.method === 'POST') {
        const m = /^Bearer ([0-9a-f]{64})$/.exec(req.headers.authorization || '');
        if (m) await store.dropSession(sha(m[1]));
        send(res, 200, { ok: true });
        return true;
      }
      if (path === '/api/match' && req.method === 'POST') {
        const b = await readBody(req);
        const int = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
        if (!b || typeof b.matchId !== 'string' || !ID_RE.test(b.matchId) || !['solo', 'multi'].includes(b.mode) || !int(b.teams, 2, 6) || !int(b.place, 1, b.teams) || !int(b.level, 0, 3) || !int(b.minutes, 20, 120)) {
          return send(res, 400, { error: '판 결과 형식이 맞지 않다' }), true;
        }
        const r = { mode: b.mode, place: b.place, teams: b.teams, level: b.level, minutes: b.minutes, forfeit: b.forfeit === true };
        const out = applyMatch(u.rank, b.matchId, r);
        if (out) {
          await store.saveRank(u.id, out.next);
          u.rank = out.next;
        }
        send(res, 200, { result: out?.result ?? null, profile: publicProfile(u) });
        return true;
      }
      send(res, 404, { error: '없는 주소' });
      return true;
    } catch (e) {
      console.error('api error', e);
      send(res, 500, { error: '서버 오류' });
      return true;
    }
  };
}
