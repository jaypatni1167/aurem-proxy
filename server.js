const express = require('express');
const { WebSocketServer } = require('ws');
const fetch = require('node-fetch');
const { initializeApp } = require('firebase/app');
const { getAuth, signInWithCustomToken } = require('firebase/auth');
const { getDatabase, ref, onValue } = require('firebase/database');
const path = require('path');
const fs = require('fs');
const os = require('os');

const app = express();
app.use(require('cors')());
app.use(express.json());

// Find HTML dashboards — check same dir first (Render deploy), then parent dir (local dev)
function findHtml(name) {
  const candidates = [path.join(__dirname, name), path.join(__dirname, '..', name)];
  for (const c of candidates) if (fs.existsSync(c)) return c;
  return candidates[0];
}
function serveNoCache(res, file) {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
  res.set('Pragma', 'no-cache');
  res.sendFile(file);
}
app.get('/', (req, res) => serveNoCache(res, findHtml('aurem_rate_dashboard.html')));
app.get('/arb', (req, res) => serveNoCache(res, findHtml('aurem_arb_dashboard.html')));
app.get('/main', (req, res) => serveNoCache(res, findHtml('aurem_main_dashboard.html')));

// ── MT5 price feed via file watching ──────────────────────────
// MT5 Files folder on Mac (Wine) — search for it
function findMT5FilesFolder() {
  const home = os.homedir();
  const candidates = [
    path.join(home, 'Library/Application Support/net.metaquotes.wine.metatrader5/drive_c/Program Files/MetaTrader 5/MQL5/Files'),
    path.join(home, 'Library/Application Support/net.metaquotes.wine.metatrader5/drive_c/users/Wineskin/AppData/Roaming/MetaQuotes/Terminal'),
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  // Fallback: try to find recursively
  return null;
}

let mt5LastLog = 0;
function readMT5File() {
  const folder = findMT5FilesFolder();
  if (!folder) return;
  const file = path.join(folder, 'aurem_prices.txt');
  if (!fs.existsSync(file)) return;

  try {
    const content = fs.readFileSync(file, 'utf8').trim();
    // Format: xauBid,xauAsk,xagBid,xagAsk
    const parts = content.split(',').map(parseFloat);
    if (parts.length < 4 || parts.some(isNaN)) return;
    const [xauBid, xauAsk, xagBid, xagAsk] = parts;

    const rates = {
      source: 'mt5',
      timestamp: Date.now(),
      prices: {
        XAUUSD: { name: 'XAUUSD', sell: xauAsk, buy: xauBid, high: 0, low: 0 },
        XAGUSD: { name: 'XAGUSD', sell: xagAsk, buy: xagBid, high: 0, low: 0 },
      }
    };
    latestRates.mt5 = rates;
    broadcast({ type: 'rates', ...rates });

    if (Date.now() - mt5LastLog > 30000) {
      console.log(`[MT5] XAU ${xauBid}/${xauAsk}  XAG ${xagBid}/${xagAsk}`);
      mt5LastLog = Date.now();
    }
  } catch (e) {
    // file being written — ignore
  }
}

// Poll the file every second
const mt5Folder = findMT5FilesFolder();
if (mt5Folder) {
  console.log('[MT5] Watching:', path.join(mt5Folder, 'aurem_prices.txt'));
  setInterval(readMT5File, 250);
} else {
  console.log('[MT5] MT5 Files folder not found — file feed disabled');
}

const firebaseConfig = {
  apiKey: "AIzaSyD5C_xlP9tcbl4c7norSC6ohi8RVtoU7lY",
  authDomain: "rsbl-spot-gold-silver-prices.firebaseapp.com",
  databaseURL: "https://rsbl-spot-gold-silver-prices.firebaseio.com",
  projectId: "rsbl-spot-gold-silver-prices"
};

const firebaseApp = initializeApp(firebaseConfig);
const auth = getAuth(firebaseApp);
const db = getDatabase(firebaseApp);

// Latest rates snapshot broadcast to all WS clients
let latestRates = {};
const clients = new Set();

function broadcast(data) {
  const msg = JSON.stringify(data);
  for (const client of clients) {
    if (client.readyState === 1) client.send(msg);
  }
}

function httpsGet(url, headers) {
  return new Promise((resolve, reject) => {
    const https = require('https');
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: 'GET',
      headers: headers
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: data }));
    });
    req.on('error', reject);
    req.end();
  });
}

async function connectAugmont() {
  console.log('[Augmont] Fetching auth token...');
  try {
    // First hit the liverates page to get session cookie + CSRF token
    const sessionRes = await httpsGet('https://spot.augmont.com/liverates', {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
    });

    // Extract session cookie
    const rawCookie = sessionRes.headers['set-cookie'];
    const cookieStr = Array.isArray(rawCookie)
      ? rawCookie.map(c => c.split(';')[0]).join('; ')
      : (rawCookie || '').split(';')[0];

    // Extract CSRF token from meta tag
    const csrfMatch = sessionRes.body.match(/name="_token"\s+content="([^"]+)"/);
    const csrf = csrfMatch ? csrfMatch[1] : '';
    console.log('[Augmont] Cookie:', cookieStr ? 'yes' : 'no', '| CSRF:', csrf ? csrf.substring(0, 10) + '...' : 'not found');

    const tokenRes = await httpsGet('https://spot.augmont.com/token/100', {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Accept': 'application/json, text/plain, */*',
      'Accept-Language': 'en-US,en;q=0.9',
      'Referer': 'https://spot.augmont.com/liverates',
      'Origin': 'https://spot.augmont.com',
      'X-Requested-With': 'XMLHttpRequest',
      'X-CSRF-TOKEN': csrf,
      'Cookie': cookieStr,
    });

    console.log('[Augmont] Token status:', tokenRes.status, '| body:', tokenRes.body.substring(0, 100));
    const token = tokenRes.body;
    console.log('[Augmont] Token received, signing in to Firebase...');

    await signInWithCustomToken(auth, token.trim().replace(/^"|"$/g, ''));
    console.log('[Augmont] Firebase auth OK — subscribing to liverates...');

    const liveRef = ref(db, 'liverates');
    onValue(liveRef, (snapshot) => {
      const data = snapshot.val();
      if (!data) return;

      const rates = { source: 'augmont', timestamp: Date.now(), prices: {} };

      Object.values(data).forEach(item => {
        if (!item.Name) return;
        rates.prices[item.Name] = {
          name: item.Name,
          sell: parseFloat(item.Sell) || 0,
          buy: parseFloat(item.Buy) || 0,
          high: parseFloat(item.High) || 0,
          low: parseFloat(item.Low) || 0,
          symbol: item.Symbol,
          time: item.Time,
          date: item.Date
        };
      });

      // Debug: print all keys with their sell values on first receive
      if (!latestRates.augmont) {
        console.log('[Augmont] Firebase keys received:');
        Object.entries(rates.prices).forEach(([k, v]) => {
          console.log(`  ${k}: sell=${v.sell}, buy=${v.buy}`);
        });
      }

      latestRates.augmont = rates;
      broadcast({ type: 'rates', ...rates });
    });

  } catch (err) {
    console.error('[Augmont] Error:', err.message);
    console.log('[Augmont] Retrying in 30s...');
    setTimeout(connectAugmont, 30000);
  }
}

// ── Arihant (Chirayu VOTS REST API) ──────────────────────────
function parseArihantData(raw) {
  const prices = {};
  raw.trim().split('\n').forEach(line => {
    const parts = line.trim().split('\t').map(s => s.trim());
    if (parts.length < 4) return;
    const [id, name, sell, buy, high, low] = parts;
    if (!name) return;
    const key = name.toUpperCase().replace(/\s+/g, '_');
    prices[key] = {
      name: name.trim(),
      sell: parseFloat(sell) || 0,
      buy: parseFloat(buy) || 0,
      high: parseFloat(high) || 0,
      low: parseFloat(low) || 0,
    };
  });
  return prices;
}

async function fetchArihant() {
  try {
    const [goldRes, silverRes] = await Promise.all([
      httpsGet('https://bcast.arihantspot.in/VOTSBroadcastStreaming/Services/xml/GetLiveRateByTemplateID/arihant', {
        'User-Agent': 'Mozilla/5.0',
        'Accept': '*/*',
        'Referer': 'https://www.arihantspot.in/',
      }),
      httpsGet('https://bcast.arihantspot.in/VOTSBroadcastStreaming/Services/xml/GetLiveRateByTemplateID/arihantsilver', {
        'User-Agent': 'Mozilla/5.0',
        'Accept': '*/*',
        'Referer': 'https://www.arihantspot.in/',
      }),
    ]);

    const goldPrices = parseArihantData(goldRes.body);
    const silverPrices = parseArihantData(silverRes.body);
    const allPrices = { ...goldPrices, ...silverPrices };

    const rates = { source: 'arihant', timestamp: Date.now(), prices: allPrices };
    latestRates.arihant = rates;
    broadcast({ type: 'rates', ...rates });
    // Log Arihant updates once per minute to keep terminal clean
    if (!global._ariLastLog || Date.now() - global._ariLastLog > 60000) {
      console.log(`[Arihant] Updated — ${Object.keys(allPrices).length} symbols`);
      global._ariLastLog = Date.now();
    }
  } catch (err) {
    console.error('[Arihant] Fetch error:', err.message);
  }
}

// Arihant is a poll-based REST API — refresh every 5 seconds
fetchArihant();
setInterval(fetchArihant, 5000);

// ── HTTP fallback for spot prices (works even where WS is blocked) ─────
async function fetchTvSpotHttp() {
  try {
    const body = JSON.stringify({
      symbols: { tickers: ['OANDA:XAUUSD', 'TVC:SILVER', 'FX_IDC:USDINR', 'NYMEX:CL1!', 'ICEEUR:BRN1!'] },
      columns: ['close', 'bid', 'ask', 'change']
    });
    const res = await new Promise((resolve, reject) => {
      const https = require('https');
      const req = https.request({
        hostname: 'scanner.tradingview.com', path: '/global/scan', method: 'POST',
        headers: { 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0' }
      }, (r) => { let s = ''; r.on('data', c => s += c); r.on('end', () => resolve(s)); });
      req.on('error', reject); req.write(body); req.end();
    });
    const parsed = JSON.parse(res);
    const mapping = { 'OANDA:XAUUSD':'XAUUSD', 'TVC:SILVER':'XAGUSD', 'FX_IDC:USDINR':'USDINR', 'NYMEX:CL1!':'WTI', 'ICEEUR:BRN1!':'BRENT' };
    (parsed.data || []).forEach(row => {
      const [close, bid, ask, change] = row.d;
      const key = mapping[row.s];
      if (!key) return;
      // Only update if WS hasn't fed this symbol in the last 10 seconds
      const existing = spotState[key];
      const wsIsFresh = existing && existing._wsFresh && existing._wsTs && (Date.now() - existing._wsTs < 10000);
      if (!wsIsFresh) {
        spotState[key] = {
          symbol: key,
          close: parseFloat(close),
          bid: bid != null ? parseFloat(bid) : parseFloat(close),
          ask: ask != null ? parseFloat(ask) : parseFloat(close),
          change: change != null ? parseFloat(change) : 0,
        };
      }
    });
    // Broadcast with augmont USDINR override
    const prices = { ...spotState };
    const augUsd = latestRates.augmont?.prices?.USDINR;
    if (augUsd) {
      prices.USDINR = { symbol: 'USDINR', close: augUsd.buy, bid: augUsd.buy, ask: augUsd.sell, change: 0 };
    }
    latestRates.tvspot = { source: 'tvspot', timestamp: Date.now(), prices };
    broadcast({ type: 'rates', source: 'tvspot', timestamp: Date.now(), prices });
  } catch (e) {
    console.error('[TvSpot-HTTP]', e.message);
  }
}
fetchTvSpotHttp();
setInterval(fetchTvSpotHttp, 2000);

// ── goldprice.org spot poller — refreshes every 1s, faster tick cadence for XAU/XAG ──
async function fetchGoldPrice() {
  try {
    const data = await new Promise((resolve, reject) => {
      const https = require('https');
      const req = https.request({
        hostname: 'data-asg.goldprice.org', path: '/dbXRates/USD?t=' + Date.now(), method: 'GET',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
          'Referer': 'https://goldprice.org/', 'Origin': 'https://goldprice.org',
          'Accept': 'application/json',
        }
      }, (r) => { let s=''; r.on('data', c=>s+=c); r.on('end', ()=>resolve(s)); });
      req.on('error', reject); req.end();
    });
    const parsed = JSON.parse(data);
    const item = (parsed.items || [])[0];
    if (!item) return;

    // Only override if WS hasn't fed these symbols in the last 10s
    const now = Date.now();
    const xauExisting = spotState['XAUUSD'];
    const xagExisting = spotState['XAGUSD'];
    const xauWsFresh = xauExisting && xauExisting._wsFresh && (now - (xauExisting._wsTs || 0) < 10000);
    const xagWsFresh = xagExisting && xagExisting._wsFresh && (now - (xagExisting._wsTs || 0) < 10000);

    if (!xauWsFresh) {
      spotState['XAUUSD'] = { symbol: 'XAUUSD', close: item.xauPrice, bid: item.xauPrice, ask: item.xauPrice, change: item.chgXau };
    }
    if (!xagWsFresh) {
      spotState['XAGUSD'] = { symbol: 'XAGUSD', close: item.xagPrice, bid: item.xagPrice, ask: item.xagPrice, change: item.chgXag };
    }

    // Broadcast
    const prices = { ...spotState };
    const augUsd = latestRates.augmont?.prices?.USDINR;
    if (augUsd) prices.USDINR = { symbol: 'USDINR', close: augUsd.buy, bid: augUsd.buy, ask: augUsd.sell, change: 0 };
    latestRates.tvspot = { source: 'tvspot', timestamp: Date.now(), prices };
    broadcast({ type: 'rates', source: 'tvspot', timestamp: Date.now(), prices });
  } catch (e) {
    // Silent - goldprice.org can rate-limit
  }
}
fetchGoldPrice();
setInterval(fetchGoldPrice, 1000);

// ── TradingView WebSocket — TRUE real-time XAU/XAG/USDINR ─────
// Reverse-engineered from tradingview.com's own live chart feed.
const WebSocketClient = require('ws');

// Spot symbols
const spotSymbols = {
  'OANDA:XAUUSD':   'XAUUSD',
  'TVC:SILVER':     'XAGUSD',
  'FX_IDC:USDINR':  'USDINR',
  'NYMEX:CL1!':     'WTI',
  'ICEEUR:BRN1!':   'BRENT',
};
// Key futures contracts (front + near months) for real-time subscription
const futuresSymbols = [
  // MCX Gold, COMEX Gold
  'MCX:GOLD1!', 'MCX:GOLDM1!',
  'COMEX:GC1!',
  // MCX Silver, COMEX Silver
  'MCX:SILVER1!', 'MCX:SILVERM1!',
  'COMEX:SI1!',
  // WTI + Brent front months
  'NYMEX:CL1!', 'ICEEUR:BRN1!',
];
const spotState = {}; // keyed by short symbol
const futuresLiveState = {}; // keyed by full TV symbol e.g. 'MCX:GOLD1!'
let tvWs = null;
let tvReconnectTimer = null;
let tvLastBroadcast = 0;

function packTv(msg) {
  const s = JSON.stringify(msg);
  return `~m~${s.length}~m~${s}`;
}
function packRaw(str) { return `~m~${str.length}~m~${str}`; }

function connectTvWs() {
  clearTimeout(tvReconnectTimer);
  const session = 'qs_' + Math.random().toString(36).slice(2, 14);
  tvWs = new WebSocketClient('wss://data.tradingview.com/socket.io/websocket', {
    headers: {
      'Origin': 'https://data.tradingview.com',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    }
  });

  tvWs.on('open', () => {
    console.log('[TV-WS] Connected');
    tvWs.send(packTv({ m: 'set_auth_token', p: ['unauthorized_user_token'] }));
    tvWs.send(packTv({ m: 'quote_create_session', p: [session] }));
    tvWs.send(packTv({ m: 'quote_set_fields', p: [session, 'lp', 'bid', 'ask', 'ch', 'chp'] }));
    tvWs.send(packTv({ m: 'quote_add_symbols', p: [session, ...Object.keys(spotSymbols), ...futuresSymbols] }));
  });

  tvWs.on('message', (raw) => {
    const str = raw.toString();
    const chunks = str.split(/~m~\d+~m~/).filter(Boolean);
    for (const c of chunks) {
      if (c.startsWith('~h~')) {
        // Heartbeat — echo back so TV keeps the session alive
        tvWs.send(packRaw(c));
        continue;
      }
      try {
        const msg = JSON.parse(c);
        if (msg.m === 'qsd' && msg.p && msg.p[1]) {
          const data = msg.p[1];
          if (!data.v) continue;
          const shortKey = spotSymbols[data.n];
          if (shortKey) {
            // Spot
            const cur = spotState[shortKey] || { symbol: shortKey, bid: null, ask: null, close: null, change: 0 };
            if (data.v.bid  != null) cur.bid   = data.v.bid;
            if (data.v.ask  != null) cur.ask   = data.v.ask;
            if (data.v.lp   != null) cur.close = data.v.lp;
            if (data.v.ch   != null) cur.change = data.v.ch;
            cur._wsFresh = true;    // mark as freshly updated by WS
            cur._wsTs = Date.now();
            spotState[shortKey] = cur;
          } else if (futuresSymbols.includes(data.n)) {
            // Futures — real-time updates
            const cur = futuresLiveState[data.n] || { symbol: data.n, bid: null, ask: null, close: null, change: 0 };
            if (data.v.bid  != null) cur.bid   = data.v.bid;
            if (data.v.ask  != null) cur.ask   = data.v.ask;
            if (data.v.lp   != null) cur.close = data.v.lp;
            if (data.v.ch   != null) cur.change = data.v.ch;
            futuresLiveState[data.n] = cur;
          }
        }
      } catch(e) {}
    }
    // Throttle broadcasts to max 4/sec (still visibly real-time, avoids WS spam)
    if (Date.now() - tvLastBroadcast > 250) {
      tvLastBroadcast = Date.now();
      const prices = { ...spotState };
      const augUsd = latestRates.augmont?.prices?.USDINR;
      if (augUsd) {
        prices.USDINR = { symbol: 'USDINR', close: augUsd.buy, bid: augUsd.buy, ask: augUsd.sell, change: 0 };
      }
      latestRates.tvspot = { source: 'tvspot', timestamp: Date.now(), prices };
      broadcast({ type: 'rates', source: 'tvspot', timestamp: Date.now(), prices });

      // Also update tradingview snapshot with live futures values, then rebroadcast futures
      if (latestRates.tradingview && Object.keys(futuresLiveState).length) {
        Object.entries(futuresLiveState).forEach(([sym, live]) => {
          if (latestRates.tradingview.prices[sym]) {
            const p = latestRates.tradingview.prices[sym];
            if (live.close != null) p.close = live.close;
            if (live.bid   != null) p.bid   = live.bid;
            if (live.ask   != null) p.ask   = live.ask;
            if (live.change != null) p.change = live.change;
            p.live = true;
          }
        });
        latestRates.tradingview.timestamp = Date.now();
        broadcast({ type: 'rates', source: 'tradingview', timestamp: Date.now(), prices: latestRates.tradingview.prices });
      }
    }
  });

  tvWs.on('error', (e) => console.error('[TV-WS] Error:', e.message));
  tvWs.on('close', () => {
    console.log('[TV-WS] Disconnected, reconnecting in 5s...');
    tvReconnectTimer = setTimeout(connectTvWs, 5000);
  });
}
connectTvWs();

// ── TradingView futures poller (MCX + COMEX gold + silver) ────
let tvLastLog = 0;

function tvScan(matchTerm, exchanges = ['MCX', 'COMEX'], field = 'name,description', op = 'match') {
  const body = JSON.stringify({
    filter: [
      { left: field, operation: op, right: matchTerm },
      { left: 'exchange', operation: 'in_range', right: exchanges }
    ],
    columns: ['name', 'description', 'close', 'change', 'expiration', 'bid', 'ask'],
    range: [0, 80]
  });
  return new Promise((resolve, reject) => {
    const https = require('https');
    const req = https.request({
      hostname: 'scanner.tradingview.com',
      path: '/futures/scan',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0',
      }
    }, (r) => {
      let data = '';
      r.on('data', c => data += c);
      r.on('end', () => resolve(data));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function fetchTradingView() {
  try {
    const [goldRaw, silverRaw, oilRaw] = await Promise.all([
      tvScan('GOLD'),
      tvScan('SILVER'),
      tvScan('^(Crude Oil Futures|Brent Crude Futures)', ['NYMEX', 'ICEEUR'], 'description'),
    ]);
    const gold   = JSON.parse(goldRaw);
    const silver = JSON.parse(silverRaw);
    const oil    = JSON.parse(oilRaw);

    const prices = {};
    const addRow = (row, metal) => {
      const [name, description, close, change, expiration, bid, ask] = row.d;
      // Skip variants we don't need for metals
      if (metal !== 'oil' && /PETAL|GUINEA|4GC|SGC|SGU|1OZ|SHANGHAI/.test(row.s)) return;
      const live = futuresLiveState[row.s];
      prices[row.s] = {
        symbol: row.s,
        name,
        description,
        metal,               // 'gold' | 'silver' | 'oil'
        close: live?.close ?? parseFloat(close),
        change: live?.change ?? parseFloat(change),
        expiration,
        bid: live?.bid ?? (bid ? parseFloat(bid) : null),
        ask: live?.ask ?? (ask ? parseFloat(ask) : null),
        live: !!live,
      };
    };
    (gold.data   || []).forEach(r => addRow(r, 'gold'));
    (silver.data || []).forEach(r => addRow(r, 'silver'));
    (oil.data    || []).forEach(r => addRow(r, 'oil'));

    const rates = { source: 'tradingview', timestamp: Date.now(), prices };
    latestRates.tradingview = rates;
    broadcast({ type: 'rates', ...rates });

    if (Date.now() - tvLastLog > 60000) {
      const g = Object.values(prices).filter(p => p.metal === 'gold').length;
      const s = Object.values(prices).filter(p => p.metal === 'silver').length;
      const o = Object.values(prices).filter(p => p.metal === 'oil').length;
      console.log(`[TradingView] Updated — ${g} gold + ${s} silver + ${o} oil contracts`);
      tvLastLog = Date.now();
    }
  } catch (e) {
    console.error('[TradingView] Error:', e.message);
  }
}
fetchTradingView();
setInterval(fetchTradingView, 5000);

// Health check endpoint
app.get('/health', (req, res) => res.json({ status: 'ok', augmont: !!latestRates.augmont, arihant: !!latestRates.arihant }));
app.get('/rates', (req, res) => res.json(latestRates));

const PORT = process.env.PORT || 3001;
const server = app.listen(PORT, () => {
  console.log(`AUREM Proxy running on http://localhost:${PORT}`);
  console.log('WebSocket on ws://localhost:3001');
  connectAugmont();
});

const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`[WS] Client connected (${clients.size} total)`);

  // Send current snapshot immediately on connect
  if (latestRates.augmont) {
    ws.send(JSON.stringify({ type: 'rates', ...latestRates.augmont }));
  }

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[WS] Client disconnected (${clients.size} total)`);
  });
});
