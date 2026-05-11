const express = require('express');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  credentials: true
}));

app.use(express.json());
app.use(express.static('public'));

const PIPEDRIVE_API_KEY = process.env.PIPEDRIVE_API_KEY;
const PIPEDRIVE_BASE_URL = 'https://api.pipedrive.com/v1';
const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;
const HUBSPOT_BASE_URL = 'https://api.hubapi.com';

if (!PIPEDRIVE_API_KEY) console.warn('⚠️ PIPEDRIVE_API_KEY no está configurada');
if (!HUBSPOT_API_KEY) console.warn('⚠️ HUBSPOT_API_KEY no está configurada');

async function pipedriveRequest(endpoint, options = {}) {
  const url = `${PIPEDRIVE_BASE_URL}${endpoint}${endpoint.includes('?') ? '&' : '?'}api_token=${PIPEDRIVE_API_KEY}`;
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json', ...options.headers }
    });
    if (!response.ok) throw new Error(`Pipedrive HTTP ${response.status}`);
    return await response.json();
  } catch (error) {
    console.error(`❌ Pipedrive request failed: ${endpoint}`, error.message);
    throw error;
  }
}

// ✅ Fetch deals by individual STAGE ID — bypasses pipeline filter issues
async function hubspotFetchByStage(stageId, properties) {
  const deals = [];
  let after = undefined;
  let page = 0;

  while (page < 100) {
    page++;
    const body = {
      filterGroups: [{
        filters: [{
          propertyName: 'dealstage',
          operator: 'EQ',
          value: stageId
        }]
      }],
      properties,
      limit: 100,
      ...(after ? { after } : {})
    };

    const response = await fetch(`${HUBSPOT_BASE_URL}/crm/v3/objects/deals/search`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${HUBSPOT_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });

    if (!response.ok) throw new Error(`HubSpot HTTP ${response.status}`);
    const data = await response.json();

    if (data.results) deals.push(...data.results);
    if (!data.paging?.next?.after) break;
    after = data.paging.next.after;
  }

  return deals;
}

app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    server: 'APM Pipedrive Proxy',
    timestamp: new Date().toISOString(),
    apis: {
      pipedrive: PIPEDRIVE_API_KEY ? '🟢 configured' : '🔴 missing key',
      hubspot: HUBSPOT_API_KEY ? '🟢 configured' : '🔴 missing key'
    }
  });
});

app.get('/api/pipedrive', async (req, res) => {
  try {
    if (!PIPEDRIVE_API_KEY) {
      return res.status(401).json({ error: 'PIPEDRIVE_API_KEY not configured' });
    }

    const apmPipelines = [1, 2, 3, 4, 8, 9, 13, 17];
    const allDeals = [];

    for (const pipelineId of apmPipelines) {
      try {
        const data = await pipedriveRequest(`/pipelines/${pipelineId}/deals?limit=500&status=open`);
        if (data.success && data.data) allDeals.push(...data.data);
      } catch (e) {
        console.warn(`⚠️ Could not fetch pipeline ${pipelineId}:`, e.message);
      }
    }

    const fieldsData = await pipedriveRequest('/dealFields?limit=500');
    const fieldMap = {};
    if (fieldsData.success && fieldsData.data) {
      fieldsData.data.forEach(field => {
        if (field.options) {
          field.options.forEach(opt => { fieldMap[opt.id] = opt.label; });
        }
      });
    }

    res.json({
      success: true,
      summary: { totalDeals: allDeals.length, pipelines: apmPipelines.length, timestamp: new Date().toISOString() },
      deals: allDeals,
      fieldMap
    });

  } catch (error) {
    console.error('❌ /api/pipedrive error:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// ═══════════════════════════════════════════════════════
// 📊 /api/hubspot — filtra por stage ID (más fiable)
// ═══════════════════════════════════════════════════════
app.get('/api/hubspot', async (req, res) => {
  try {
    if (!HUBSPOT_API_KEY) {
      return res.status(401).json({ error: 'HUBSPOT_API_KEY not configured' });
    }

    const IBERIA_PIPELINES = [
      {
        id: '821946550',
        name: 'IBERIA: ENTERPRISE',
        stages: [
          '1217112747','1217112748','1288966436','1217112749',
          '1217112750','1217112751','1217112752',
          '1217112753','1217112754'
        ]
      },
      {
        id: '822050313',
        name: 'IBERIA: FIELD TEAM',
        stages: [
          '1217117009','1217117010','1217117011','1217117012',
          '1217117013','1217117014','1217117015','1217117016'
        ]
      },
      {
        id: '807042859',
        name: 'IBERIA: LAS APMs',
        stages: [
          '1188103598','1188103599','1188103600',
          '1188103601','1188103602','1188019859','1188103604'
        ]
      },
    ];

    const PROPERTIES = [
      'dealname', 'dealstage', 'pipeline',
      'createdate', 'closedate', 'hs_lastmodifieddate',
      'amount', 'closed_lost_reason', 'num_associated_contacts',
      'hubspot_owner_id', 'hs_deal_stage_probability',
      'ib_net__no_locations',
      // Aging — ENTERPRISE
      'hs_date_entered_1288966436','hs_date_entered_1217112747',
      'hs_date_entered_1217112748','hs_date_entered_1217112749',
      'hs_date_entered_1217112750','hs_date_entered_1217112751',
      'hs_date_entered_1217112752','hs_date_entered_1217112753',
      'hs_date_entered_1217112754',
      // Aging — FIELD TEAM
      'hs_date_entered_1217117009','hs_date_entered_1217117010',
      'hs_date_entered_1217117011','hs_date_entered_1217117012',
      'hs_date_entered_1217117013','hs_date_entered_1217117014',
      'hs_date_entered_1217117015','hs_date_entered_1217117016',
      // Aging — LAS APMs
      'hs_date_entered_1188103598','hs_date_entered_1188103599',
      'hs_date_entered_1188103600','hs_date_entered_1188103601',
      'hs_date_entered_1188103602','hs_date_entered_1188019859',
      'hs_date_entered_1188103604',
    ];

    const allDeals = [];
    const seen = new Set(); // evitar duplicados entre stages

    for (const pipe of IBERIA_PIPELINES) {
      let pipeCount = 0;

      for (const stageId of pipe.stages) {
        try {
          const stageDeals = await hubspotFetchByStage(stageId, PROPERTIES);
          for (const deal of stageDeals) {
            if (!seen.has(deal.id)) {
              seen.add(deal.id);
              deal._pipelineName = pipe.name;
              allDeals.push(deal);
              pipeCount++;
            }
          }
          console.log(`  stage ${stageId}: ${stageDeals.length} deals`);
        } catch (e) {
          console.warn(`⚠️ Error stage ${stageId}:`, e.message);
        }
      }

      console.log(`✅ ${pipe.name}: ${pipeCount} deals total`);
    }

    res.json({
      success: true,
      summary: {
        totalDeals: allDeals.length,
        pipelines: IBERIA_PIPELINES.map(p => p.name),
        timestamp: new Date().toISOString()
      },
      deals: allDeals
    });

  } catch (error) {
    console.error('❌ /api/hubspot error:', error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/pipedrive/*', async (req, res) => {
  try {
    if (!PIPEDRIVE_API_KEY) return res.status(401).json({ error: 'PIPEDRIVE_API_KEY not configured' });
    const path = req.params[0];
    const queryString = Object.keys(req.query).length > 0 ? '&' + new URLSearchParams(req.query).toString() : '';
    const url = `${PIPEDRIVE_BASE_URL}/${path}?api_token=${PIPEDRIVE_API_KEY}${queryString}`;
    const response = await fetch(url);
    res.json(await response.json());
  } catch (error) {
    console.error('❌ /pipedrive proxy error:', error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/hubspot/*', async (req, res) => {
  try {
    if (!HUBSPOT_API_KEY) return res.status(401).json({ error: 'HUBSPOT_API_KEY not configured' });
    const path = req.params[0];
    const queryString = Object.keys(req.query).length > 0 ? '?' + new URLSearchParams(req.query).toString() : '';
    const url = `${HUBSPOT_BASE_URL}/${path}${queryString}`;
    const response = await fetch(url, {
      headers: { 'Authorization': `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' }
    });
    res.json(await response.json());
  } catch (error) {
    console.error('❌ /hubspot proxy error:', error.message);
    res.status(500).json({ error: error.message });
  }
});

app.use((req, res) => {
  res.status(404).json({
    error: 'Endpoint not found',
    path: req.path,
    availableEndpoints: ['GET /api/health','GET /api/pipedrive','GET /api/hubspot','GET /pipedrive/*','GET /hubspot/*']
  });
});

app.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════════════════════╗
║  APM Pipedrive Proxy - Dashboard API Server                ║
╠════════════════════════════════════════════════════════════╣
║  🚀 Running on port ${PORT}
║  📡 Pipedrive: ${PIPEDRIVE_API_KEY ? '✅ Configured' : '❌ Missing key'}
║  📊 HubSpot:   ${HUBSPOT_API_KEY ? '✅ Configured' : '❌ Missing key'}
║  🔵 Iberia: Enterprise · Field Team · LAS APMs            ║
╠════════════════════════════════════════════════════════════╣
║  Filtra por stage ID — más fiable que pipeline filter     ║
╚════════════════════════════════════════════════════════════╝
  `);
});

module.exports = app;
