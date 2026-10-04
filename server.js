// XAUUSD Deriv bot — EMA(50) crossover on 15m candles, Multipliers (MULTUP/MULTDOWN).
// Deploy on Render as a Web Service. Set env var DERIV_TOKEN (demo token first!).
import http from "node:http";
import fs from "node:fs";
import WebSocket from "ws";

const PORT = process.env.PORT || 10000;
const APP_ID = process.env.DERIV_APP_ID || "1089";
const WS_URL = const WS_URL = const WS_URL = `wss://ws.derivws.com/websockets/v3?app_id=${APP_ID}`;
const SYMBOL = process.env.DERIV_SYMBOL || "frxXAUUSD"; // Deriv's gold/USD symbol
const GRANULARITY = 900; // 15 minutes
const EMA_PERIOD = 50;
const LOG_FILE = process.env.TRADE_LOG || "./trades.jsonl";

// ---------- state ----------
const settings = {
  stake: 1,          // USD stake per trade ("lot size" equivalent on Deriv)
  multiplier: 100,
  stopLoss: 2,       // USD
  takeProfit: 4,     // USD
  dailyMaxLoss: 10,  // USD — bot stops itself when reached
  maxTrades: 1,      // max simultaneous open contracts
};
const state = {
  status: "STOPPED",
  connected: false,
  authorized: false,
  loginid: null,
  isVirtual: true,
  currency: "USD",
  balance: null,
  candles: [],       // {epoch, close}
  lastSignal: null,
  open: new Map(),   // contract_id -> contract
  dailyPnl: 0,
  day: new Date().toISOString().slice(0, 10),
  lastError: null,
};
const history = loadHistory();

function loadHistory() {
  try {
    return fs.readFileSync(LOG_FILE, "utf8").trim().split("\n").filter(Boolean).map((l) => JSON.parse(l));
  } catch { return []; }
}
function logTrade(entry) {
  const row = { at: new Date().toISOString(), ...entry };
  history.unshift(row);
  if (history.length > 500) history.length = 500;
  fs.appendFile(LOG_FILE, JSON.stringify(row) + "\n", () => {});
  console.log("[TRADE]", JSON.stringify(row));
}

// ---------- Deriv websocket ----------
let ws;
let reqId = 1;
const pending = new Map();

function send(msg) {
  return new Promise((resolve, reject) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return reject(new Error("Not connected to Deriv"));
    const id = reqId++;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ ...msg, req_id: id }));
  });
}

function connect() {
  const token = process.env.DERIV_TOKEN;
  if (!token) { state.lastError = "DERIV_TOKEN env var is missing"; console.error(state.lastError); return; }
  ws = new WebSocket(WS_URL);

  ws.on("open", async () => {
    state.connected = true;
    try {
      const auth = await send({ authorize: token });
      state.authorized = true;
      state.loginid = auth.authorize.loginid;
      state.isVirtual = !!auth.authorize.is_virtual;
      state.currency = auth.authorize.currency || "USD";
      state.balance = auth.authorize.balance;
      console.log(`Authorized ${state.loginid} (${state.isVirtual ? "DEMO" : "REAL"})`);
      ws.send(JSON.stringify({ balance: 1, subscribe: 1 }));
      ws.send(JSON.stringify({ proposal_open_contract: 1, subscribe: 1 }));
      ws.send(JSON.stringify({ ticks_history: SYMBOL, style: "candles", granularity: GRANULARITY, count: 200, end: "latest", subscribe: 1 }));
    } catch (e) {
      state.lastError = `Authorize failed: ${e.message}`;
      console.error(state.lastError);
    }
  });

  ws.on("message", (raw) => {
    const msg = JSON.parse(raw.toString());
    if (msg.req_id && pending.has(msg.req_id)) {
      const p = pending.get(msg.req_id); pending.delete(msg.req_id);
      msg.error ? p.reject(new Error(msg.error.message)) : p.resolve(msg);
      return;
    }
    if (msg.error) { state.lastError = msg.error.message; console.error("Deriv:", msg.error.message); return; }
    switch (msg.msg_type) {
      case "balance": state.balance = msg.balance.balance; break;
      case "candles": state.candles = msg.candles.map((c) => ({ epoch: c.epoch, close: +c.close })); break;
      case "ohlc": onOhlc(msg.ohlc); break;
      case "proposal_open_contract": onContract(msg.proposal_open_contract); break;
    }
  });

  ws.on("close", () => {
    state.connected = false; state.authorized = false;
    console.log("Deriv socket closed — reconnecting in 5s");
    setTimeout(connect, 5000);
  });
  ws.on("error", (e) => { state.lastError = e.message; });
}
setInterval(() => { if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: 1 })); }, 30000);

// ---------- strategy ----------
function ema(values, period) {
  if (values.length < period) return null;
  const k = 2 / (period + 1);
  let e = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < values.length; i++) e = values[i] * k + e * (1 - k);
  return e;
}

function onOhlc(o) {
  const epoch = +o.open_time;
  const close = +o.close;
  const last = state.candles[state.candles.length - 1];
  if (last && last.epoch === epoch) { last.close = close; return; }
  // A new candle opened → previous candle is closed. Evaluate crossover on closed candles.
  state.candles.push({ epoch, close });
  if (state.candles.length > 300) state.candles.shift();
  evaluate();
}

function evaluate() {
  const closed = state.candles.slice(0, -1).map((c) => c.close);
  if (closed.length < EMA_PERIOD + 2) return;
  const emaNow = ema(closed, EMA_PERIOD);
  const emaPrev = ema(closed.slice(0, -1), EMA_PERIOD);
  const pNow = closed[closed.length - 1];
  const pPrev = closed[closed.length - 2];
  let signal = null;
  if (pPrev <= emaPrev && pNow > emaNow) signal = "BUY";
  else if (pPrev >= emaPrev && pNow < emaNow) signal = "SELL";
  if (!signal) return;
  state.lastSignal = { signal, price: pNow, ema: +emaNow.toFixed(2), at: new Date().toISOString() };
  console.log("Signal", state.lastSignal);
  if (state.status === "RUNNING") placeTrade(signal).catch((e) => { state.lastError = e.message; });
}

function resetDayIfNeeded() {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== state.day) { state.day = today; state.dailyPnl = 0; }
}

async function placeTrade(side) {
  resetDayIfNeeded();
  if (state.dailyPnl <= -Math.abs(settings.dailyMaxLoss)) { state.status = "STOPPED"; throw new Error("Daily max loss reached — bot stopped"); }
  if (state.open.size >= settings.maxTrades) return;
  const parameters = {
    amount: settings.stake,
    basis: "stake",
    contract_type: side === "BUY" ? "MULTUP" : "MULTDOWN",
    currency: state.currency,
    symbol: SYMBOL,
    multiplier: settings.multiplier,
    limit_order: { stop_loss: settings.stopLoss, take_profit: settings.takeProfit },
  };
  const res = await send({ buy: 1, price: settings.stake, parameters });
  logTrade({ event: "OPEN", side, contract_id: res.buy.contract_id, stake: settings.stake, multiplier: settings.multiplier, sl: settings.stopLoss, tp: settings.takeProfit, price: res.buy.buy_price });
}

function onContract(c) {
  if (!c || !c.contract_id) return;
  if (c.is_sold) {
    if (state.open.has(c.contract_id)) {
      state.open.delete(c.contract_id);
      resetDayIfNeeded();
      state.dailyPnl += +c.profit;
      logTrade({ event: "CLOSE", side: c.contract_type === "MULTUP" ? "BUY" : "SELL", contract_id: c.contract_id, profit: +c.profit, entry: +c.entry_spot, exit: +c.exit_tick });
      if (state.dailyPnl <= -Math.abs(settings.dailyMaxLoss)) { state.status = "STOPPED"; state.lastError = "Daily max loss reached — bot stopped"; }
    }
    return;
  }
  state.open.set(c.contract_id, c);
}

async function closeAll() {
  for (const id of [...state.open.keys()]) {
    try { await send({ sell: id, price: 0 }); } catch (e) { state.lastError = e.message; }
  }
}

// ---------- HTTP API ----------
function json(res, code, body) {
  res.writeHead(code, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  });
  res.end(JSON.stringify(body));
}

function statusBody() {
  const openTrades = [...state.open.values()].map((c) => ({
    ticket: c.contract_id,
    time: new Date(c.date_start * 1000).toISOString().slice(11, 16),
    type: c.contract_type === "MULTUP" ? "BUY" : "SELL",
    lot: +c.buy_price,
    price: +c.entry_spot || 0,
    sl: c.limit_order?.stop_loss?.order_amount ?? 0,
    tp: c.limit_order?.take_profit?.order_amount ?? 0,
    profit: +c.profit || 0,
  }));
  const floating = openTrades.reduce((s, t) => s + t.profit, 0);
  return {
    status: state.status,
    mode: state.isVirtual ? "DEMO" : "REAL",
    connected: state.connected && state.authorized,
    account: state.loginid,
    balance: state.balance,
    equity: state.balance == null ? null : +(state.balance + floating).toFixed(2),
    dailyPnl: +state.dailyPnl.toFixed(2),
    openTrades,
    lastSignal: state.lastSignal,
    settings,
    lastError: state.lastError,
  };
}

const server = http.createServer((req, res) => {
  if (req.method === "OPTIONS") return json(res, 204, {});
  const url = new URL(req.url, "http://x");
  if (req.method === "GET" && url.pathname === "/") return json(res, 200, { ok: true, bot: "xauusd-deriv" });
  if (req.method === "GET" && url.pathname === "/api/status") return json(res, 200, statusBody());
  if (req.method === "GET" && url.pathname === "/api/trades") return json(res, 200, { trades: history.filter((t) => t.event === "CLOSE") });
  if (req.method === "POST" && url.pathname === "/api/command") {
    let body = "";
    req.on("data", (c) => { body += c; if (body.length > 10000) req.destroy(); });
    req.on("end", async () => {
      try {
        const cmd = JSON.parse(body || "{}");
        switch (cmd.action) {
          case "start":
            if (!state.authorized) return json(res, 503, { ok: false, error: "Not connected to Deriv" });
            state.status = "RUNNING"; state.lastError = null; break;
          case "stop": state.status = "STOPPED"; break;
          case "emergency_stop": state.status = "STOPPED"; await closeAll(); break;
          case "close_trade": await send({ sell: Number(cmd.ticket), price: 0 }); break;
          case "update_settings": {
            const s = cmd.settings || {};
            for (const k of Object.keys(settings)) {
              const v = Number(s[k]);
              if (s[k] != null && Number.isFinite(v) && v > 0) settings[k] = v;
            }
            break;
          }
          default: return json(res, 400, { ok: false, error: "Unknown action" });
        }
        logTrade({ event: "COMMAND", action: cmd.action });
        json(res, 200, { ok: true, ...statusBody() });
      } catch (e) {
        json(res, 400, { ok: false, error: e.message });
      }
    });
    return;
  }
  json(res, 404, { error: "Not found" });
});

server.listen(PORT, () => { console.log(`Bot API on :${PORT}`); connect(); });
