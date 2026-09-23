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
const MERGE_TARGET_INDEX_KEY = 'merge-target-index:v1';
const MERGE_TARGET_PREFIX = 'merge-target:v1:';
const MARKER_KEY = 'lokMainAllUiSyncRevision';

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
function validFileKey(value) {
  return typeof value === 'string' && /^[A-Za-z0-9_-]{6,128}$/.test(value);
}
function mergeTargetKey(branchFileKey) {
  return MERGE_TARGET_PREFIX + branchFileKey;
}
async function getMergeTargetIndex(kv) {
  const index = await kv.get(MERGE_TARGET_INDEX_KEY, 'json');
  return Array.isArray(index) ? index.filter(validFileKey) : [];
}
async function saveMergeTarget(kv, target) {
  await kv.put(mergeTargetKey(target.branchFileKey), JSON.stringify(target));
}

async function fetchFigmaRootSharedData(fileKey, env) {
  if (!env.FIGMA_API_TOKEN) return { error: 'FIGMA_API_TOKEN is not configured on this Worker.' };
  const response = await fetch(
    'https://api.figma.com/v1/files/' + encodeURIComponent(fileKey) + '?plugin_data=shared&depth=1',
    { headers: { 'X-Figma-Token': env.FIGMA_API_TOKEN } },
  );
  if (!response.ok) return { error: 'Figma API returned ' + response.status + ' for this file.' };
  const payload = await response.json();
  return { shared: payload?.document?.sharedPluginData?.lok || {} };
}

async function handleBranchMarkerValidation(req, url, env) {
  if (req.method !== 'GET') return json({ error: 'Method not allowed.' }, 405);
  if (!requireSchedulerToken(req, env)) return json({ error: 'Unauthorized scheduler request.' }, 401);
  const mainFileKey = url.searchParams.get('mainFileKey') || '';
  const branchFileKey = url.searchParams.get('branchFileKey') || '';
  if (!mainFileKey || !branchFileKey) return json({ error: 'mainFileKey and branchFileKey are required.' }, 400);

  const [main, branch] = await Promise.all([
    fetchFigmaRootSharedData(mainFileKey, env),
    fetchFigmaRootSharedData(branchFileKey, env),
  ]);
  if (main.error || branch.error) return json({ error: main.error || branch.error }, 502);

  const mainRevision = main.shared[MARKER_KEY] || null;
  const branchRevision = branch.shared[MARKER_KEY] || null;
  const matches = !!mainRevision && mainRevision === branchRevision;
  return json({
    mainRevision,
    branchRevision,
    matches,
    result: !mainRevision ? 'main-marker-missing' : matches ? 'propagated' : 'awaiting-update',
  });
}

async function queueMergeSync(kv, target, revision) {
  const syncTarget = {
    fileKey: target.branchFileKey,
    pageId: target.pageId || '',
    pageName: target.pageName || 'All UI',
    mode: 'branch',
  };
  const existing = await getJob(kv, syncTarget);
  // Never replace an active Widget lease. The next scan can queue a new job.
  if (existing && (existing.state === 'pending' || existing.state === 'running')) return false;
  await kv.put(jobKey(syncTarget), JSON.stringify({
    id: crypto.randomUUID(), ...syncTarget, state: 'pending',
    requestedAt: new Date().toISOString(), requestedBy: 'merge-scan',
    mergeRevision: revision,
  }));
  return true;
}

async function scanMergeTarget(kv, target, env) {
  const [main, branch] = await Promise.all([
    fetchFigmaRootSharedData(target.mainFileKey, env),
    fetchFigmaRootSharedData(target.branchFileKey, env),
  ]);
  const now = new Date().toISOString();
  const previousStatus = target.status || 'not-scanned';
  target.lastScannedAt = now;
  if (main.error || branch.error) {
    target.status = 'unavailable';
    target.error = main.error || branch.error;
  } else {
    target.mainRevision = main.shared[MARKER_KEY] || null;
    target.branchRevision = branch.shared[MARKER_KEY] || null;
    delete target.error;
    target.status = !target.mainRevision
      ? 'main-sync-required'
      : target.mainRevision === target.branchRevision ? 'up-to-date' : 'update-required';
    if (target.status === 'up-to-date' && target.autoSync === true && target.lastQueuedRevision !== target.branchRevision) {
      if (await queueMergeSync(kv, target, target.branchRevision)) target.lastQueuedRevision = target.branchRevision;
    }
  }
  if (target.status !== previousStatus) target.lastStatusChangedAt = now;
  await saveMergeTarget(kv, target);
  return target;
}

async function handleMergeTargets(req, url, env) {
  const kv = getSyncJobsBinding(env);
  if (!kv) return json({ error: 'SYNC_JOBS KV binding is not configured.' }, 503);
  // Merge metadata is private. Only the scheduler/admin caller holding the
  // secret can register or inspect Figma file pairs.
  if (!requireSchedulerToken(req, env)) return json({ error: 'Unauthorized scheduler request.' }, 401);
  if (url.pathname === '/merge-targets' && req.method === 'GET') {
    const targets = await Promise.all((await getMergeTargetIndex(kv)).map((key) => kv.get(mergeTargetKey(key), 'json')));
    return json({ targets: targets.filter(Boolean) });
  }
  if (url.pathname === '/register-merge-target' && req.method === 'POST') {
    const body = await readBody(req);
    if (!body || !validFileKey(body.mainFileKey) || !validFileKey(body.branchFileKey)) {
      return json({ error: 'Valid mainFileKey and branchFileKey are required.' }, 400);
    }
    if (body.pageId !== undefined && typeof body.pageId !== 'string') return json({ error: 'pageId must be a string.' }, 400);
    if (body.pageName !== undefined && typeof body.pageName !== 'string') return json({ error: 'pageName must be a string.' }, 400);
    const existing = await kv.get(mergeTargetKey(body.branchFileKey), 'json');
    const target = {
      ...existing,
      mainFileKey: body.mainFileKey,
      branchFileKey: body.branchFileKey,
      pageId: body.pageId || existing?.pageId || '',
      pageName: body.pageName || existing?.pageName || 'All UI',
      label: typeof body.label === 'string' ? body.label.slice(0, 160) : (existing?.label || ''),
      autoSync: body.autoSync !== false,
      createdAt: existing?.createdAt || new Date().toISOString(),
      status: existing?.status || 'not-scanned',
    };
    const index = await getMergeTargetIndex(kv);
    if (!index.includes(target.branchFileKey)) {
      index.push(target.branchFileKey);
      await kv.put(MERGE_TARGET_INDEX_KEY, JSON.stringify(index));
    }
    await saveMergeTarget(kv, target);
    await scanMergeTarget(kv, target, env);
    return json({ registered: true, target });
  }
  return json({ error: 'Merge-target endpoint not found.' }, 404);
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
  if (url.pathname === '/validate-branch-marker') return handleBranchMarkerValidation(req, url, env);
  if (url.pathname === '/merge-targets' || url.pathname === '/register-merge-target') return handleMergeTargets(req, url, env);
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
  async scheduled(_event, env, ctx) {
    const kv = getSyncJobsBinding(env);
    if (!kv) return;
    ctx.waitUntil((async () => {
      const keys = await getMergeTargetIndex(kv);
      await Promise.all(keys.map(async (branchFileKey) => {
        const target = await kv.get(mergeTargetKey(branchFileKey), 'json');
        if (target) await scanMergeTarget(kv, target, env);
      }));
    })());
  },
};
