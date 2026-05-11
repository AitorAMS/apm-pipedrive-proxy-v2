// ═══════════════════════════════════════════════════════
// apm-pipedrive-proxy-v2 — server.js
// Cache en disco (cache.json) → snapshot persistente.
// Al arrancar: carga desde disco inmediatamente, luego
// refresca desde Pipedrive y sobreescribe. Cada hora repite.
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

// ── MIDDLEWARE ───────────────────────────────────────────
app.use(cors());
app.use(express.json());

// ── CACHE STATE (en memoria) ──────────────────────────────
let cache = {
  deals:     [],
  options:   {},
  cachedAt:  null,
  snapshots: [],   // historial ligero de las últimas 24 cargas
};
let cacheInProgress = false;

// ── SNAPSHOT HISTORY ─────────────────────────────────────
// Cada carga guarda un snapshot ligero (conteos + timestamp)
// para que el dashboard pueda mostrar evolución horaria.
function addSnapshot(deals, ts) {
  const snap = {
    ts,
    totalDeals: deals.length,
    openDeals:  deals.filter(d => d.status === 'open').length,
    lostDeals:  deals.filter(d => d.status === 'lost').length,
  };
  cache.snapshots.push(snap);
  // Mantiene solo las últimas 24 (= 24h a 1 carga/hora)
  if (cache.snapshots.length > 24) cache.snapshots.shift();
}

// ── DISCO: leer / escribir cache.json ────────────────────
function loadCacheFromDisk() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const raw  = fs.readFileSync(CACHE_FILE, 'utf8');
      const data = JSON.parse(raw);
      cache.deals     = data.deals     || [];
      cache.options   = data.options   || {};
      cache.cachedAt  = data.cachedAt  || null;
      cache.snapshots = data.snapshots || [];
      console.log(`[cache] ✓ Snapshot cargado desde disco: ${cache.deals.length} deals · ${cache.cachedAt}`);
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
    console.log(`[cache] ✓ Snapshot guardado en disco · ${cache.deals.length} deals`);
  } catch (err) {
    console.error('[cache] Error al guardar cache.json:', err.message);
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
    all = all.concat(b);
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
      all = all.concat(b);
      if (!d.additional_data?.pagination?.more_items_in_collection || b.length === 0) break;
      start += b.length;
      if (page > 50) break;
    }
  }
  return all;
}

async function fetchPipe(id, token) {
  const stages = PIPELINE_STAGES[id] || [];
  const [open, lost] = await Promise.all([
    fetchByStatus(id, 'open', token),
    fetchByStages(stages, 'lost', token),
  ]);
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
    console.log('[cache] Ya hay una carga en curso, skip.');
    return;
  }

  const t = token || API_TOKEN;
  if (!t) {
    console.warn('[cache] Sin token. Configura PIPEDRIVE_TOKEN en Render → Environment.');
    return;
  }

  cacheInProgress = true;
  console.log('[cache] Iniciando refresh desde Pipedrive…', new Date().toISOString());

  try {
    // 1. Deal fields (enums / opciones)
    const fieldsResp = await pdGet('/dealFields?limit=500', t);
    const options = {};
    (fieldsResp.data || []).forEach(f =>
      (f.options || []).forEach(o => {
        options[o.id]         = o.label;
        options[String(o.id)] = o.label;
      })
    );

    // 2. Todos los pipelines en paralelo
    const results = await Promise.all(APM_IDS.map(id => fetchPipe(id, t)));

    // 3. Deduplicar
    const seen = new Set();
    const allDeals = results.flat().filter(d => {
      if (seen.has(d.id)) return false;
      seen.add(d.id);
      return true;
    });

    const ts = new Date().toISOString();

    // 4. Actualizar memoria
    cache.deals    = allDeals;
    cache.options  = options;
    cache.cachedAt = ts;

    // 5. Guardar snapshot horario en el historial
    addSnapshot(allDeals, ts);

    // 6. Persistir todo en disco (sobrevive a reinicios de Render)
    saveCacheToDisk();

    console.log(`[cache] ✓ Refresh completado: ${allDeals.length} deals · ${ts}`);
  } catch (err) {
    console.error('[cache] Error en refresh:', err.message);
    // Mantiene el cache anterior — el dashboard sigue funcionando
  } finally {
    cacheInProgress = false;
  }
}

// ── ARRANQUE ──────────────────────────────────────────────
// Paso 1: carga instantánea desde disco si existe snapshot previo
loadCacheFromDisk();

// Paso 2: refresca desde Pipedrive en background (no bloquea el servidor)
if (API_TOKEN) {
  refreshCache();
  setInterval(() => refreshCache(), REFRESH_MS);
} else {
  console.warn('[cache] PIPEDRIVE_TOKEN no configurado en variables de entorno.');
  console.warn('[cache] El cache se actualizará al recibir POST /cache/refresh con el token en el header.');
}

// ══════════════════════════════════════════════════════════
// ENDPOINTS
// ══════════════════════════════════════════════════════════

// GET /cache — devuelve el JSON completo al dashboard (<100ms)
app.get('/cache', (req, res) => {
  if (!cache.cachedAt) {
    return res.status(503).json({
      error: 'Cache todavía no disponible. El servidor acaba de arrancar, espera ~30s.',
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

// GET /cache/meta — info rápida sin enviar los deals
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

// POST /cache/refresh — fuerza recarga manual
// Úsalo también como Cron Job en Render: schedule 0 * * * *
// Command: curl -X POST https://<tu-proxy>.onrender.com/cache/refresh
app.post('/cache/refresh', async (req, res) => {
  const token = req.headers['x-pipedrive-token'] || API_TOKEN;
  if (!token) {
    return res.status(400).json({ error: 'Sin token. Envía x-pipedrive-token en el header o configura PIPEDRIVE_TOKEN.' });
  }
  refreshCache(token); // background
  res.json({
    ok:               true,
    message:          'Refresh iniciado en background (~30s).',
    previousCachedAt: cache.cachedAt,
  });
});

// GET /health
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

// Proxy legacy /pipedrive/* — mantiene compatibilidad
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

// ── START ─────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`[server] Puerto ${PORT}`);
  console.log(`[server] Snapshot en disco: ${CACHE_FILE}`);
  console.log(`[server] Refresh automático cada ${REFRESH_MS / 60000} minutos`);
});
