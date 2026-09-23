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
const WEEK_MS = 7 * 24 * 60 * 60 * 1000;
const SCAN_BATCH_SIZE = 4;

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
function validFolderId(value) {
  return typeof value === 'string' && /^\d{6,32}$/.test(value);
}
function mergeTargetKey(branchFileKey) {
  return MERGE_TARGET_PREFIX + branchFileKey;
}
function mergeStatusLabel(status) {
  return {
    'main-sync-required': 'Main sync is required',
    'update-required': 'Update branch from main is required',
    'up-to-date': 'Branch is up to date; background sync is queued',
    'unavailable': 'Scan is unavailable',
  }[status] || status;
}
async function getMergeTargetIndex(kv) {
  const index = await kv.get(MERGE_TARGET_INDEX_KEY, 'json');
  return Array.isArray(index) ? index.filter(validFileKey) : [];
}
async function saveMergeTarget(kv, target) {
  await kv.put(mergeTargetKey(target.branchFileKey), JSON.stringify(target));
}
async function notifySlackOfMergeChange(target, env, isTest = false) {
  const channel = target.notificationTarget || env.SLACK_NOTIFY_TARGET;
  if (!channel || !env.SLACK_BOT_TOKEN) return;
  const label = target.label || target.branchFileKey;
  const text = [
    isTest ? '*Figma UI sync notification test*' : '*Figma UI sync status changed*',
    `*${label}*`,
    `Status: ${mergeStatusLabel(target.status)}`,
    `Checked: ${target.lastScannedAt}`,
    `<https://www.figma.com/design/${target.branchFileKey}|Open Branch file in Figma>`,
  ].join('\n');
  try {
    const response = await fetch('https://slack.com/api/chat.postMessage', {
      method: 'POST',
      headers: { Authorization: `Bearer ${env.SLACK_BOT_TOKEN}`, 'Content-Type': 'application/json; charset=utf-8' },
      body: JSON.stringify({ channel, text, unfurl_links: false, unfurl_media: false }),
    });
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error || `Slack returned ${response.status}`);
    target.lastNotificationAt = new Date().toISOString();
    target.lastNotifiedStatus = target.status;
    delete target.lastNotificationError;
  } catch (error) {
    target.lastNotificationError = String(error?.message || error).slice(0, 500);
  }
}

async function fetchFigmaRootSharedData(fileKey, env) {
  if (!env.FIGMA_API_TOKEN) return { error: 'FIGMA_API_TOKEN is not configured on this Worker.' };
  const response = await fetch(
    'https://api.figma.com/v1/files/' + encodeURIComponent(fileKey) + '?plugin_data=shared&depth=1',
    { headers: { 'X-Figma-Token': env.FIGMA_API_TOKEN } },
  );
  if (!response.ok) return { error: 'Figma API returned ' + response.status + ' for this file.' };
  const payload = await response.json();
  return {
    shared: payload?.document?.sharedPluginData?.lok || {},
    pages: Array.isArray(payload?.document?.children) ? payload.document.children.map((page) => page?.name).filter(Boolean) : [],
  };
}

async function fetchFigmaFolderFiles(folderId, env) {
  if (!env.FIGMA_API_TOKEN) return { error: 'FIGMA_API_TOKEN is not configured on this Worker.' };
  const headers = { 'X-Figma-Token': env.FIGMA_API_TOKEN };
  // v2 is the current Figma folders API. Keep v1 as a compatibility fallback
  // for tokens created before folders:read became available.
  let response = await fetch(`https://api.figma.com/v2/folders/${encodeURIComponent(folderId)}/files?branch_data=true`, { headers });
  if (response.status === 404) response = await fetch(`https://api.figma.com/v1/projects/${encodeURIComponent(folderId)}/files?branch_data=true`, { headers });
  if (!response.ok) return { error: `Figma API returned ${response.status} while listing this folder.` };
  const payload = await response.json();
  return { files: Array.isArray(payload?.files) ? payload.files : [] };
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
    if (!branch.pages.includes(target.pageName || 'All UI')) {
      target.status = 'unavailable';
      target.error = `Branch file does not contain the ${target.pageName || 'All UI'} Page.`;
      target.mainRevision = main.shared[MARKER_KEY] || null;
      target.branchRevision = branch.shared[MARKER_KEY] || null;
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
  }
  if (target.status !== previousStatus) {
    target.lastStatusChangedAt = now;
    // A scan may continue even when Slack is temporarily unavailable; the
    // persisted status remains the source of truth for a later dashboard.
    if (!(previousStatus === 'not-scanned' && target.suppressInitialNotification === true && target.status !== 'unavailable')) {
      await notifySlackOfMergeChange(target, env);
    }
  }
  target.suppressInitialNotification = false;
  target.nextScanAt = new Date(Date.now() + (target.status === 'unavailable' ? 60 * 60 * 1000 : WEEK_MS)).toISOString();
  await saveMergeTarget(kv, target);
  return target;
}

async function upsertMergeTarget(kv, body, options = {}) {
  const existing = await kv.get(mergeTargetKey(body.branchFileKey), 'json');
  const target = {
    ...existing,
    mainFileKey: body.mainFileKey,
    branchFileKey: body.branchFileKey,
    pageId: body.pageId || existing?.pageId || '',
    pageName: body.pageName || existing?.pageName || 'All UI',
    label: typeof body.label === 'string' ? body.label.slice(0, 160) : (existing?.label || ''),
    notificationTarget: typeof body.notificationTarget === 'string' ? body.notificationTarget.slice(0, 160) : (existing?.notificationTarget || ''),
    autoSync: body.autoSync !== false,
    createdAt: existing?.createdAt || new Date().toISOString(),
    status: existing?.status || 'not-scanned',
    suppressInitialNotification: options.suppressInitialNotification === true && !existing,
    nextScanAt: existing?.nextScanAt || new Date().toISOString(),
  };
  const index = await getMergeTargetIndex(kv);
  if (!index.includes(target.branchFileKey)) {
    index.push(target.branchFileKey);
    await kv.put(MERGE_TARGET_INDEX_KEY, JSON.stringify(index));
  }
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
  if (url.pathname === '/test-slack-notification' && req.method === 'POST') {
    const body = await readBody(req);
    const branchFileKey = body?.branchFileKey || '';
    if (!validFileKey(branchFileKey)) return json({ error: 'A valid branchFileKey is required.' }, 400);
    const target = await kv.get(mergeTargetKey(branchFileKey), 'json');
    if (!target) return json({ error: 'This branch file is not registered.' }, 404);
    target.lastScannedAt = new Date().toISOString();
    await notifySlackOfMergeChange(target, env, true);
    await saveMergeTarget(kv, target);
    if (target.lastNotificationError) return json({ notified: false, error: target.lastNotificationError }, 502);
    return json({ notified: true, target: target.label || target.branchFileKey });
  }
  if (url.pathname === '/register-merge-target' && req.method === 'POST') {
    const body = await readBody(req);
    if (!body || !validFileKey(body.mainFileKey) || !validFileKey(body.branchFileKey)) {
      return json({ error: 'Valid mainFileKey and branchFileKey are required.' }, 400);
    }
    if (body.pageId !== undefined && typeof body.pageId !== 'string') return json({ error: 'pageId must be a string.' }, 400);
    if (body.pageName !== undefined && typeof body.pageName !== 'string') return json({ error: 'pageName must be a string.' }, 400);
    if (body.notificationTarget !== undefined && typeof body.notificationTarget !== 'string') return json({ error: 'notificationTarget must be a string.' }, 400);
    const target = await upsertMergeTarget(kv, body);
    await scanMergeTarget(kv, target, env);
    return json({ registered: true, target });
  }
  if (url.pathname === '/import-folder-targets' && req.method === 'POST') {
    const body = await readBody(req);
    const folderId = body?.folderId || '';
    if (!validFolderId(folderId)) return json({ error: 'A valid folderId is required.' }, 400);
    const listing = await fetchFigmaFolderFiles(folderId, env);
    if (listing.error) return json({ error: listing.error }, 502);
    const pageName = typeof body.pageName === 'string' && body.pageName ? body.pageName : 'All UI';
    const notificationTarget = typeof body.notificationTarget === 'string' ? body.notificationTarget : '';
    const platform = typeof body.platform === 'string' ? body.platform.slice(0, 40) : 'Figma';
    const results = { folderId, discovered: 0, registered: [], noMktBranch: [], multipleMktBranches: [] };
    for (const file of listing.files.filter((item) => /^\d/.test(item?.name || ''))) {
      results.discovered += 1;
      const branches = (Array.isArray(file?.branches) ? file.branches : []).filter((branch) => /MKT/i.test(branch?.name || ''));
      if (branches.length === 0) { results.noMktBranch.push({ name: file.name, fileKey: file.key }); continue; }
      if (branches.length > 1) { results.multipleMktBranches.push({ name: file.name, fileKey: file.key, branches: branches.map((branch) => branch.name) }); continue; }
      const branch = branches[0];
      if (!validFileKey(file.key) || !validFileKey(branch.key)) continue;
      const target = await upsertMergeTarget(kv, {
        mainFileKey: file.key,
        branchFileKey: branch.key,
        pageName,
        label: `${platform} ${file.name} — ${branch.name}`,
        notificationTarget,
        autoSync: true,
      }, { suppressInitialNotification: true });
      results.registered.push({ name: file.name, mainFileKey: file.key, branchFileKey: branch.key, status: target.status });
    }
    return json(results);
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
  if (url.pathname === '/merge-targets' || url.pathname === '/register-merge-target' || url.pathname === '/import-folder-targets' || url.pathname === '/test-slack-notification') return handleMergeTargets(req, url, env);
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
      const now = Date.now();
      const targets = (await Promise.all((await getMergeTargetIndex(kv)).map((key) => kv.get(mergeTargetKey(key), 'json'))))
        .filter(Boolean)
        .filter((target) => !target.nextScanAt || Date.parse(target.nextScanAt) <= now)
        .sort((a, b) => String(a.nextScanAt || '').localeCompare(String(b.nextScanAt || '')))
        .slice(0, SCAN_BATCH_SIZE);
      for (const target of targets) await scanMergeTarget(kv, target, env);
    })());
  },
};
