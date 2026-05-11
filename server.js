// ═══════════════════════════════════════════════════════
// apm-pipedrive-proxy-v2 — server.js
// v3 — fix OOM: carga en serie + slim deals (solo campos necesarios)
// Cache en disco (cache.json) → snapshot persistente entre reinicios.
// ═══════════════════════════════════════════════════════
const express = require('express');
const fetch   = require('node-fetch');
const cors    = require('cors');
const fs      = require('fs');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── CONFIG ───────────────────────────────────────────────
const PIPEDRIVE_BASE = 'https://api.pipedrive.com/v1';
const API_TOKEN      = process.env.PIPEDRIVE_TOKEN || '';
const CACHE_FILE     = path.join(__dirname, 'cache.json');
const REFRESH_MS     = 60 * 60 * 1000; // 1 hora

const APM_IDS = [2, 3, 4, 9, 13, 17];
const PIPELINE_STAGES = {
  2:  [116, 62, 46, 6, 33, 154, 70, 32, 9],
  3:  [171, 76, 117, 47, 77, 13],
  4:  [15, 71, 96, 49, 16],
  9:  [104, 66, 64, 105, 67, 68, 153],
  13: [137, 128, 127, 129, 175, 130],
  17: [155, 157, 158, 159, 160, 172, 161, 162, 163, 164, 173, 165, 166, 168, 169, 170],
};

// Campos que usa el dashboard — descarta el resto para ahorrar RAM
const KEEP_FIELDS = new Set([
  'id', 'status', 'pipeline_id', 'stage_id', 'org_name',
  '9b3b3995f1f726feaee7fdabbe6c7695ef7d7f09', // svDone
  '67225c24d04109f7fa56373160b7efeb1ba7b857', // svReq
  '5751376cbd3ce91828479ecd16f6ab3913b16f9e', // svSch
  '4eeb6232a77052b8f0ad39c199ecf8f2ad0eaa50', // svProv
  '7ac852e7f10486b93bbdf4a2a16dacc225eed886', // rdySv
  '625c899d638cf47d9435ed048ab7383264b67771', // egDone
  'ad7bee71b88f2813747efc61746be52aff2bac8b', // egReq
  'f4b5f5248662fce649f697db0cd21dce984b93b4', // egSch
  'b23e2d471253e5271d79b15c736b54d6fb769dd9', // egFail
  '040eb4600ed2df829da452a308a2fdf27b76ddaa', // egProv
  '8d746b4699d7c04a646436b0f1ae4d038b048ebd', // inDep
  'aacf967fc363fbc73db37cc912b31a2fe343931a', // inSch
  'fa25efa2dd60a8f4abe4af567d9d3cf5fbf6b978', // inRmv
  'e6c9301c6c4c7751d212727024a1cfa507d13992', // inProv
  '5dad53f6ffced67539def909f4329c14f37783b1', // apPart
  '0cc4dfd4e72d45a5f0be9b48743a936cfe6c6a2b', // land
  '2ddcd354da2bdf692cdbb531c10d410b248cc852', // apmSize
  'b49c4d80d008ba209738f3bee0ed1fe548c5a4b4', // indoor
  '4a4e30eb9d47fc061dea264544ccd5b6b86c08dd', // exDone
]);

function slimDeal(d) {
  const slim = {};
  for (const k of KEEP_FIELDS) {
    if (k in d) slim[k] = d[k];
  }
  return slim;
}

// ── MIDDLEWARE ───────────────────────────────────────────
app.use(cors());
app.use(express.json());

// ── CACHE STATE ───────────────────────────────────────────
let cache = {
  deals:     [],
  options:   {},
  cachedAt:  null,
  snapshots: [],
};
let cacheInProgress = false;

// ── SNAPSHOT HISTORY ─────────────────────────────────────
function addSnapshot(deals, ts) {
  cache.snapshots.push({
    ts,
    totalDeals: deals.length,
    openDeals:  deals.filter(d => d.status === 'open').length,
    lostDeals:  deals.filter(d => d.status === 'lost').length,
  });
  if (cache.snapshots.length > 24) cache.snapshots.shift();
}

// ── DISCO ─────────────────────────────────────────────────
function loadCacheFromDisk() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const data = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
      cache.deals     = data.deals     || [];
      cache.options   = data.options   || {};
      cache.cachedAt  = data.cachedAt  || null;
      cache.snapshots = data.snapshots || [];
      console.log(`[cache] ✓ Disco: ${cache.deals.length} deals · ${cache.cachedAt}`);
      return true;
    }
  } catch (err) {
    console.warn('[cache] No se pudo leer cache.json:', err.message);
  }
  return false;
}

function saveCacheToDisk() {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify({
      deals:     cache.deals,
      options:   cache.options,
      cachedAt:  cache.cachedAt,
      snapshots: cache.snapshots,
    }), 'utf8');
    console.log(`[cache] ✓ Guardado en disco · ${cache.deals.length} deals`);
  } catch (err) {
    console.error('[cache] Error guardando en disco:', err.message);
  }
}

// ── PIPEDRIVE HELPERS ─────────────────────────────────────
async function pdGet(urlPath, token) {
  const sep = urlPath.includes('?') ? '&' : '?';
  const url = `${PIPEDRIVE_BASE}${urlPath}${sep}api_token=${token}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Pipedrive ${res.status}: ${urlPath}`);
  return res.json();
}

async function fetchByStatus(id, status, token) {
  let all = [], start = 0, page = 0;
  while (true) {
    page++;
    const d = await pdGet(`/pipelines/${id}/deals?limit=500&start=${start}&status=${status}`, token);
    const b = d.data || [];
    all = all.concat(b.map(slimDeal)); // slim inmediatamente al recibir
    if (!d.additional_data?.pagination?.more_items_in_collection || b.length === 0) break;
    start += b.length;
    if (page > 50) break;
  }
  return all;
}

async function fetchByStages(stages, status, token) {
  let all = [];
  for (const sid of stages) {
    let start = 0, page = 0;
    while (true) {
      page++;
      const d = await pdGet(`/deals?limit=500&start=${start}&stage_id=${sid}&status=${status}`, token);
      const b = d.data || [];
      all = all.concat(b.map(slimDeal)); // slim inmediatamente
      if (!d.additional_data?.pagination?.more_items_in_collection || b.length === 0) break;
      start += b.length;
      if (page > 50) break;
    }
  }
  return all;
}

async function fetchPipe(id, token) {
  const stages = PIPELINE_STAGES[id] || [];
  // SERIE en vez de Promise.all — evita pico de RAM simultáneo
  const open = await fetchByStatus(id, 'open', token);
  const lost = await fetchByStages(stages, 'lost', token);
  const seen = new Set();
  return [...open, ...lost].filter(d => {
    if (seen.has(d.id)) return false;
    seen.add(d.id);
    return true;
  });
}

// ── REFRESH PRINCIPAL ─────────────────────────────────────
async function refreshCache(token) {
  if (cacheInProgress) {
    console.log('[cache] Carga ya en curso, skip.');
    return;
  }
  const t = token || API_TOKEN;
  if (!t) {
    console.warn('[cache] Sin token. Configura PIPEDRIVE_TOKEN en Render → Environment.');
    return;
  }

  cacheInProgress = true;
  console.log('[cache] Iniciando refresh…', new Date().toISOString());

  try {
    // 1. Deal fields
    const fieldsResp = await pdGet('/dealFields?limit=500', t);
    const options = {};
    (fieldsResp.data || []).forEach(f =>
      (f.options || []).forEach(o => {
        options[o.id]         = o.label;
        options[String(o.id)] = o.label;
      })
    );

    // 2. Pipelines en SERIE — un pipeline a la vez para controlar RAM
    const allDeals = [];
    const seen = new Set();
    for (const id of APM_IDS) {
      console.log(`[cache] Pipeline ${id}…`);
      const deals = await fetchPipe(id, t);
      for (const d of deals) {
        if (!seen.has(d.id)) { seen.add(d.id); allDeals.push(d); }
      }
      console.log(`[cache] Pipeline ${id} OK · ${deals.length} · total: ${allDeals.length}`);
    }

    const ts = new Date().toISOString();
    cache.deals    = allDeals;
    cache.options  = options;
    cache.cachedAt = ts;

    addSnapshot(allDeals, ts);
    saveCacheToDisk();

    console.log(`[cache] ✓ Completado: ${allDeals.length} deals · ${ts}`);
  } catch (err) {
    console.error('[cache] Error en refresh:', err.message);
    // Cache anterior sigue siendo válido
  } finally {
    cacheInProgress = false;
  }
}

// ── ARRANQUE ──────────────────────────────────────────────
loadCacheFromDisk(); // instantáneo desde disco si existe

if (API_TOKEN) {
  refreshCache();                              // primera carga en background
  setInterval(() => refreshCache(), REFRESH_MS); // cada hora
} else {
  console.warn('[cache] PIPEDRIVE_TOKEN no configurado.');
}

// ══════════════════════════════════════════════════════════
// ENDPOINTS
// ══════════════════════════════════════════════════════════

app.get('/cache', (req, res) => {
  if (!cache.cachedAt) {
    return res.status(503).json({
      error: 'Cache no disponible aún. Espera ~2 minutos al primer arranque.',
      cachedAt: null,
    });
  }
  res.json({
    deals:     cache.deals,
    options:   cache.options,
    cachedAt:  cache.cachedAt,
    snapshots: cache.snapshots,
  });
});

app.get('/cache/meta', (req, res) => {
  res.json({
    ready:         !!cache.cachedAt,
    cachedAt:      cache.cachedAt,
    dealCount:     cache.deals.length,
    inProgress:    cacheInProgress,
    snapshots:     cache.snapshots,
    snapshotCount: cache.snapshots.length,
  });
});

app.post('/cache/refresh', async (req, res) => {
  const token = req.headers['x-pipedrive-token'] || API_TOKEN;
  if (!token) return res.status(400).json({ error: 'Sin token.' });
  refreshCache(token);
  res.json({ ok: true, message: 'Refresh iniciado (~2 min).', previousCachedAt: cache.cachedAt });
});

app.get('/health', (req, res) => {
  res.json({
    ok:            true,
    cachedAt:      cache.cachedAt,
    dealCount:     cache.deals.length,
    inProgress:    cacheInProgress,
    snapshotCount: cache.snapshots.length,
    diskSnapshot:  fs.existsSync(CACHE_FILE),
  });
});

// Proxy legacy
app.use('/pipedrive', async (req, res) => {
  try {
    const token = req.headers['x-pipedrive-token'] || API_TOKEN;
    if (!token) return res.status(401).json({ error: 'Sin token' });
    const sep = req.url.includes('?') ? '&' : '?';
    const url = `${PIPEDRIVE_BASE}${req.url}${sep}api_token=${token}`;
    const upstream = await fetch(url, { method: req.method });
    const data = await upstream.json();
    res.status(upstream.status).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`[server] Puerto ${PORT} · Render free tier optimizado`);
  console.log(`[server] Disco: ${CACHE_FILE} · Refresh cada ${REFRESH_MS / 60000} min`);
});
