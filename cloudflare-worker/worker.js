// Lokalise CORS Proxy + scheduled Figma sync queue.
// /api2/* continues to proxy Lokalise; sync routes use SYNC_JOBS KV.

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH, OPTIONS',
  'Access-Control-Allow-Headers': '*',
  'Access-Control-Max-Age': '86400',
};
const LEASE_MS = 20 * 60 * 1000;
const HISTORY_TTL_SECONDS = 30 * 24 * 60 * 60;

function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json', ...CORS } });
}
function getSyncJobsBinding(env) {
  return env.SYNC_JOBS || null;
}
function jobKey(target) {
  const pageRef = target.pageId || ('name:' + target.pageName);
  return 'sync:' + encodeURIComponent(target.fileKey) + ':' + encodeURIComponent(pageRef) + ':' + encodeURIComponent(target.mode);
}
function validTarget(value) {
  return value && typeof value.fileKey === 'string' &&
    (typeof value.pageId === 'string' || typeof value.pageName === 'string') &&
    (value.mode === 'main' || value.mode === 'branch');
}
async function readBody(req) {
  try { return await req.json(); } catch { return null; }
}
async function getJob(kv, target) {
  const raw = await kv.get(jobKey(target), 'json');
  if (!raw) return null;
  if (raw.state === 'running' && Number(raw.leaseUntil || 0) <= Date.now()) {
    raw.state = 'pending';
    delete raw.claimedAt;
    delete raw.leaseUntil;
    await kv.put(jobKey(target), JSON.stringify(raw));
  }
  return raw;
}
function requireSchedulerToken(req, env) {
  return !!env.SYNC_API_TOKEN && req.headers.get('Authorization') === 'Bearer ' + env.SYNC_API_TOKEN;
}

async function handleAutomation(req, url, env) {
  const kv = getSyncJobsBinding(env);
  if (!kv) return json({ error: 'SYNC_JOBS KV binding is not configured.' }, 503);

  if (url.pathname === '/sync-status' && req.method === 'GET') {
    const target = {
      fileKey: url.searchParams.get('fileKey') || '',
      pageId: url.searchParams.get('pageId') || '',
      pageName: url.searchParams.get('pageName') || '',
      mode: url.searchParams.get('mode') || '',
    };
    if (!validTarget(target)) return json({ error: 'fileKey, pageId or pageName, and mode are required.' }, 400);
    let job = target.pageId ? await getJob(kv, target) : null;
    if (!job && target.pageName) job = await getJob(kv, { ...target, pageId: '' });
    return json({ pending: !!job && job.state === 'pending', job: job || null });
  }

  const body = await readBody(req);
  if (!validTarget(body)) return json({ error: 'fileKey, pageId or pageName, and mode are required.' }, 400);
  const target = { fileKey: body.fileKey, pageId: body.pageId || '', pageName: body.pageName || '', mode: body.mode };
  const key = jobKey(target);

  if (url.pathname === '/request-sync' && req.method === 'POST') {
    if (!requireSchedulerToken(req, env)) return json({ error: 'Unauthorized scheduler request.' }, 401);
    const existing = await getJob(kv, target);
    const job = {
      id: existing?.id || crypto.randomUUID(), ...target, state: 'pending',
      requestedAt: new Date().toISOString(),
      requestedBy: typeof body.requestedBy === 'string' ? body.requestedBy : 'scheduler',
    };
    await kv.put(key, JSON.stringify(job));
    return json({ queued: true, job });
  }

  if (url.pathname === '/claim-sync' && req.method === 'POST') {
    const job = await getJob(kv, target);
    if (!job || job.state !== 'pending' || (body.id && body.id !== job.id)) return json({ claimed: false });
    job.state = 'running';
    job.claimedAt = new Date().toISOString();
    job.leaseUntil = Date.now() + LEASE_MS;
    await kv.put(key, JSON.stringify(job));
    return json({ claimed: true, job });
  }

  if (url.pathname === '/ack-sync' && req.method === 'POST') {
    const job = await getJob(kv, target);
    if (!job || (body.id && body.id !== job.id)) return json({ acknowledged: false }, 404);
    if (!['success', 'failed', 'blocked'].includes(body.outcome)) return json({ error: 'Invalid outcome.' }, 400);
    job.state = body.outcome === 'success' ? 'completed' : 'pending';
    job.lastOutcome = body.outcome;
    job.lastMessage = typeof body.message === 'string' ? body.message.slice(0, 1000) : '';
    job.lastUpdatedAt = new Date().toISOString();
    delete job.claimedAt;
    delete job.leaseUntil;
    await kv.put(key, JSON.stringify(job), body.outcome === 'success' ? { expirationTtl: HISTORY_TTL_SECONDS } : undefined);
    return json({ acknowledged: true, pending: job.state === 'pending' });
  }
  return json({ error: 'Automation endpoint not found.' }, 404);
}

async function handle(req, env) {
  if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
  const url = new URL(req.url);
  if (url.pathname.startsWith('/sync-') || url.pathname === '/request-sync' || url.pathname === '/claim-sync' || url.pathname === '/ack-sync') return handleAutomation(req, url, env);

  const target = 'https://api.lokalise.com' + url.pathname + url.search;
  const headers = {};
  for (const [key, value] of req.headers.entries()) if (key.toLowerCase() !== 'host') headers[key] = value;
  const body = (req.method === 'GET' || req.method === 'HEAD') ? undefined : await req.arrayBuffer();
  const res = await fetch(target, { method: req.method, headers, body });
  return new Response(await res.arrayBuffer(), { status: res.status, headers: { 'Content-Type': 'application/json', ...CORS } });
}

export default {
  fetch(request, env) {
    return handle(request, env);
  },
};
