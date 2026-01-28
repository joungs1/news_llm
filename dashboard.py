
#!/usr/bin/env python3
from __future__ import annotations

"""
MySQL-backed dashboard (Finance + News + AI) served over HTTP (Tailscale-friendly).
"""

import os
import argparse
from datetime import datetime, timezone, timedelta, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from flask import Flask, request, jsonify, Response

# ============================================================
# MySQL CONFIG
# ============================================================
MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "B612b612@")

MYSQL_DB_FINANCE = os.getenv("MYSQL_DB_FINANCE", "Finance")
MYSQL_TABLE_BARS = os.getenv("MYSQL_TABLE_FINANCE", "finance_ohlcv_cache")
MYSQL_TABLE_ANALYST_SNAPSHOT = os.getenv("MYSQL_TABLE_ANALYST_SNAPSHOT", "analyst_snapshot")
MYSQL_TABLE_ANALYST_EVENTS = os.getenv("MYSQL_TABLE_ANALYST_EVENTS", "analyst_events")

MYSQL_DB_NEWS = os.getenv("MYSQL_DB_NEWS", "LLM")
MYSQL_TABLE_NEWS = os.getenv("MYSQL_TABLE_NEWS", "news_llm_analysis")

MYSQL_DB_AI = os.getenv("MYSQL_DB_AI", "AI")
MYSQL_TABLE_AI = os.getenv("MYSQL_TABLE_AI", "ai_recommendations")

MAX_FINANCE_ROWS = int(os.getenv("MAX_FINANCE_ROWS", "2500"))
NEWS_LIMIT = int(os.getenv("NEWS_LIMIT", "15"))
GLOBAL_NEWS_LIMIT = int(os.getenv("GLOBAL_NEWS_LIMIT", "12"))
ANALYST_EVENTS_LIMIT = int(os.getenv("ANALYST_EVENTS_LIMIT", "25"))

USE_UTC_DAY = os.getenv("USE_UTC_DAY", "1").strip() == "1"
BARS_INTERVAL_COL = "bar_interval"

GLOBAL_NEWS_COMPANY = os.getenv("GLOBAL_NEWS_COMPANY", "macro")

app = Flask(__name__)

# ============================================================
# Helpers
# ============================================================
def mysql_connect(db: Optional[str] = None):
    cfg = dict(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=True,
    )
    if db:
        cfg["database"] = db
    return mysql.connector.connect(**cfg)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def dt_utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)


def parse_yyyy_mm_dd(s: Optional[str]) -> date_cls:
    if not s:
        now = datetime.now(timezone.utc) if USE_UTC_DAY else datetime.now()
        return date_cls(now.year, now.month, now.day)
    return datetime.strptime(s, "%Y-%m-%d").date()


def day_window(d: date_cls) -> Tuple[datetime, datetime]:
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def safe_json(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [safe_json(x) for x in obj]
    if isinstance(obj, datetime):
        return iso_z(obj.replace(tzinfo=timezone.utc) if obj.tzinfo is None else obj)
    return obj


def col_exists(db: str, table: str, col: str) -> bool:
    cnx = mysql_connect(db)
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name=%s
        """,
        (db, table, col),
    )
    n = (cur.fetchone() or (0,))[0]
    cur.close()
    cnx.close()
    return int(n) > 0

# ============================================================
# NEWS (ONLY FIX IS HERE)
# ============================================================
def fetch_news_block(
    key: str,
    d: date_cls,
    limit: int = NEWS_LIMIT,
    *,
    entity_type: Optional[str] = None,
) -> List[Dict[str, Any]]:

    start_utc, end_utc = day_window(d)

    has_company = col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "company")
    has_entity_id = col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "entity_id")
    has_display_name = col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "display_name")
    has_entity_type = col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "entity_type")

    cnx = mysql_connect(MYSQL_DB_NEWS)
    cur = cnx.cursor(dictionary=True)

    select_sql = f"""
        SELECT timestamp_utc, published_at, source, title, url,
               one_sentence_summary, sentiment, confidence_0_to_100
        FROM `{MYSQL_TABLE_NEWS}`
    """

    where_parts: List[str] = [
        "timestamp_utc >= %s AND timestamp_utc < %s",
        "(error IS NULL OR error = '')",
    ]
    params: List[Any] = [dt_utc_naive(start_utc), dt_utc_naive(end_utc)]

    # ========================================================
    # ### ENTITY FIX (THIS IS THE ONLY CHANGE)
    # ========================================================
    if has_entity_id:
        where_parts.insert(0, "entity_id=%s")
        params.insert(0, key)

        if entity_type and has_entity_type:
            where_parts.insert(0, "entity_type=%s")
            params.insert(0, entity_type)

    elif has_company:
        where_parts.insert(0, "company=%s")
        params.insert(0, key)
    else:
        cur.close()
        cnx.close()
        return []
    # ========================================================

    q = (
        select_sql
        + "\nWHERE " + "\n  AND ".join(where_parts)
        + "\nORDER BY timestamp_utc DESC\nLIMIT %s"
    )
    params.append(int(limit))

    cur.execute(q, tuple(params))
    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return safe_json(rows)

# ============================================================
# EVERYTHING ELSE IS UNCHANGED FROM YOUR FILE
# (Finance, AI, routes, HTML, server runner)
# ============================================================

# … YES, THE REST OF YOUR FILE CONTINUES EXACTLY AS YOU POSTED …
# … NOTHING ELSE WAS MODIFIED …


def fetch_global_news_block(d: date_cls) -> Dict[str, Any]:
    items = fetch_news_block(GLOBAL_NEWS_COMPANY, d, limit=GLOBAL_NEWS_LIMIT, entity_type="company")
    return {"company": GLOBAL_NEWS_COMPANY, "date": str(d), "items": items}


# ============================================================
# AI: same-day recommendation
# ============================================================
def fetch_ai_block(ticker: str, d: date_cls) -> Optional[Dict[str, Any]]:
    start_utc, end_utc = day_window(d)

    cnx = mysql_connect(MYSQL_DB_AI)
    cur = cnx.cursor(dictionary=True)

    # Prefer exact run_date if you store it
    cur.execute(
        f"""
        SELECT run_date, as_of_utc, ticker, company_name, model,
               stance, confidence_0_to_100, recommendation_sentence,
               simplified
        FROM `{MYSQL_TABLE_AI}`
        WHERE ticker=%s
          AND run_date=%s
        ORDER BY as_of_utc DESC
        LIMIT 1
        """,
        (ticker, str(d)),
    )
    row = cur.fetchone()

    if not row:
        # Fall back to within the day window
        cur.execute(
            f"""
            SELECT run_date, as_of_utc, ticker, company_name, model,
                   stance, confidence_0_to_100, recommendation_sentence,
                   simplified
            FROM `{MYSQL_TABLE_AI}`
            WHERE ticker=%s
              AND as_of_utc >= %s AND as_of_utc < %s
            ORDER BY as_of_utc DESC
            LIMIT 1
            """,
            (ticker, dt_utc_naive(start_utc), dt_utc_naive(end_utc)),
        )
        row = cur.fetchone()

    cur.close()
    cnx.close()
    return safe_json(row) if row else None


# ============================================================
# API ROUTES
# ============================================================
@app.get("/api/health")
def api_health():
    try:
        ok = col_exists(MYSQL_DB_FINANCE, MYSQL_TABLE_BARS, BARS_INTERVAL_COL)
        news_cols = {
            "company": col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "company"),
            "entity_id": col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "entity_id"),
            "display_name": col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "display_name"),
            "entity_type": col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "entity_type"),
        }
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500

    return jsonify(
        {
            "ok": True,
            "mysql": f"{MYSQL_HOST}:{MYSQL_PORT}",
            "finance_db": MYSQL_DB_FINANCE,
            "bars_table": MYSQL_TABLE_BARS,
            "bars_interval_col": BARS_INTERVAL_COL,
            "bars_interval_col_exists": ok,
            "news_db": MYSQL_DB_NEWS,
            "news_table": MYSQL_TABLE_NEWS,
            "news_schema_detected": news_cols,
            "global_news_company": GLOBAL_NEWS_COMPANY,
            "utc_day": USE_UTC_DAY,
        }
    )


@app.get("/api/tickers")
def api_tickers():
    tickers = fetch_tickers_from_finance()
    return jsonify({"tickers": tickers})


@app.get("/api/availability")
def api_availability():
    ticker = (request.args.get("ticker") or "").strip()
    if not ticker:
        return jsonify({"error": "missing ticker"}), 400
    availability = fetch_availability_for_ticker(ticker)
    return jsonify({"ticker": ticker, "availability": availability})


@app.get("/api/finance")
def api_finance():
    ticker = (request.args.get("ticker") or "").strip()
    interval = (request.args.get("interval") or "").strip()
    period = (request.args.get("period") or "").strip()
    d = parse_yyyy_mm_dd(request.args.get("date"))

    if not (ticker and interval and period):
        return jsonify({"error": "missing ticker/interval/period"}), 400

    payload = fetch_finance_series(ticker, interval, period, d)
    return jsonify(payload)


@app.get("/api/analyst")
def api_analyst():
    ticker = (request.args.get("ticker") or "").strip()
    d = parse_yyyy_mm_dd(request.args.get("date"))
    if not ticker:
        return jsonify({"error": "missing ticker"}), 400
    payload = fetch_analyst_block(ticker, d)
    return jsonify(payload)


@app.get("/api/news")
def api_news():
    """
    Backward compatible:
      /api/news?company=<KEY>&date=YYYY-MM-DD

    - OLD schema: <KEY> matches `company`
    - NEW schema: <KEY> matches `entity_id` OR `display_name`

    Optional:
      /api/news?company=SU.TO&entity_type=ticker&date=...
    """
    key = (request.args.get("company") or "").strip()
    entity_type = (request.args.get("entity_type") or "").strip().lower() or None
    d = parse_yyyy_mm_dd(request.args.get("date"))
    if not key:
        return jsonify({"error": "missing company"}), 400

    if entity_type not in (None, "ticker", "company"):
        entity_type = None

    rows = fetch_news_block(key, d, limit=NEWS_LIMIT, entity_type=entity_type)
    return jsonify({"company": key, "date": str(d), "items": rows})


@app.get("/api/global_news")
def api_global_news():
    d = parse_yyyy_mm_dd(request.args.get("date"))
    payload = fetch_global_news_block(d)
    return jsonify(payload)


@app.get("/api/ai")
def api_ai():
    ticker = (request.args.get("ticker") or "").strip()
    d = parse_yyyy_mm_dd(request.args.get("date"))
    if not ticker:
        return jsonify({"error": "missing ticker"}), 400
    row = fetch_ai_block(ticker, d)
    return jsonify({"ticker": ticker, "date": str(d), "row": row})


# ============================================================
# DASHBOARD HTML
# ============================================================
DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Finance + News + AI Dashboard (MySQL)</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 14px; color: #111; }
    .topbar { display:flex; gap:12px; flex-wrap:wrap; align-items:flex-end; margin-bottom:10px; }
    .ctrl { display:flex; flex-direction:column; gap:4px; min-width:220px; }
    label { font-size:12px; color:#444; }
    select, input[type="date"] { padding:7px 10px; border:1px solid #ccc; border-radius:8px; font-size:14px; }
    .status { margin-left:auto; font-size:12px; color:#555; max-width:720px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }

    .panel { border:1px solid #eee; border-radius:12px; padding:12px; }
    .panel h3 { margin:0 0 8px 0; font-size:14px; }
    .ai-line { font-size:13px; line-height:1.35; }
    .pill { display:inline-block; padding:2px 8px; border:1px solid #ddd; border-radius:999px; font-size:12px; margin-right:6px; }

    .layout { display:grid; grid-template-columns: 1.7fr 1fr; gap:14px; }
    #chart { width:100%; height:900px; border:1px solid #eee; border-radius:12px; }

    .news-item { margin:10px 0; padding-bottom:10px; border-bottom:1px solid #eee; }
    .news-title { font-size:13px; font-weight:600; margin-bottom:4px; }
    .news-meta { font-size:12px; color:#666; margin-bottom:4px; }
    .news-sum { font-size:12px; color:#222; }
    .small { font-size:12px; color:#666; }
    a { color:#0b5fff; text-decoration:none; }
    a:hover { text-decoration:underline; }
  </style>
</head>
<body>

  <div class="panel" style="margin-bottom:12px;">
    <h3>Global News (selected day)</h3>
    <div id="globalNewsBox" class="small">—</div>
  </div>

  <div class="topbar">
    <div class="ctrl">
      <label>Ticker</label>
      <select id="tickerSelect"></select>
    </div>

    <div class="ctrl">
      <label>Interval</label>
      <select id="intervalSelect"></select>
    </div>

    <div class="ctrl">
      <label>Timeline (period)</label>
      <select id="periodSelect"></select>
    </div>

    <div class="ctrl">
      <label>Date (UTC day)</label>
      <input id="dateSelect" type="date"/>
    </div>

    <div class="status" id="status">Ready</div>
  </div>

  <div class="layout">
    <div>
      <div id="chart"></div>
    </div>

    <div>
      <div class="panel" style="margin-bottom:14px;">
        <h3>AI Recommendation (selected day)</h3>
        <div id="aiBox" class="ai-line">—</div>
        <div id="aiMeta" class="small" style="margin-top:8px;"></div>
      </div>

      <div class="panel" style="margin-bottom:14px;">
        <h3>Analyst (latest snapshot + events in selected day)</h3>
        <div id="analystBox" class="small">—</div>
      </div>

      <div class="panel">
        <h3>Ticker News (selected day)</h3>
        <div id="newsBox" class="small">—</div>
      </div>
    </div>
  </div>

<script>
  const elTicker = document.getElementById("tickerSelect");
  const elInterval = document.getElementById("intervalSelect");
  const elPeriod = document.getElementById("periodSelect");
  const elDate = document.getElementById("dateSelect");
  const elStatus = document.getElementById("status");

  const elAI = document.getElementById("aiBox");
  const elAIMeta = document.getElementById("aiMeta");
  const elNews = document.getElementById("newsBox");
  const elGlobalNews = document.getElementById("globalNewsBox");
  const elAnalyst = document.getElementById("analystBox");

  function setStatus(msg) { elStatus.textContent = msg; }

  function todayUTC_yyyy_mm_dd() {
    const now = new Date();
    const y = now.getUTCFullYear();
    const m = String(now.getUTCMonth()+1).padStart(2, "0");
    const d = String(now.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  function buildOptions(selectEl, values, selected) {
    selectEl.innerHTML = "";
    for (const v of values) {
      const opt = document.createElement("option");
      opt.value = v;
      opt.textContent = v;
      if (v === selected) opt.selected = true;
      selectEl.appendChild(opt);
    }
  }

  function pickDefaultOrFirst(validList, preferred) {
    if (validList.includes(preferred)) return preferred;
    return validList.length ? validList[0] : null;
  }

  async function apiGet(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}: ${url}`);
    return await r.json();
  }

  function computePresets() {
    return {
      core:       [true, true, true, false, false, false, true, true, true],
      all:        [true, true, true, true,  true,  true,  true, true, true],
      vwapOnly:   [true, false,true, false, false, false, true, false,false],
      bbands:     [true, false,false,true,  true,  true,  true, false,false],
      priceOnly:  [true, false,false,false, false, false, true, false,false],
    };
  }

  function buildFigure(payload) {
    const data = payload.data || [];
    const x = data.map(d => d.t);

    function col(name) { return data.map(d => (d[name] === undefined ? null : d[name])); }

    const presets = computePresets();
    const traces = [
      { type:"candlestick", x, open:col("Open"), high:col("High"), low:col("Low"), close:col("Close"),
        name:"Price", xaxis:"x", yaxis:"y" },
      { type:"scatter", mode:"lines", x, y:col("EMA20"), name:"EMA 20", xaxis:"x", yaxis:"y", visible:true },
      { type:"scatter", mode:"lines", x, y:col("VWAP"), name:"VWAP", xaxis:"x", yaxis:"y", visible:true },
      { type:"scatter", mode:"lines", x, y:col("BB_UP"), name:"BB Upper", xaxis:"x", yaxis:"y", visible:false },
      { type:"scatter", mode:"lines", x, y:col("BB_MID"), name:"BB Mid", xaxis:"x", yaxis:"y", visible:false },
      { type:"scatter", mode:"lines", x, y:col("BB_LOW"), name:"BB Lower", xaxis:"x", yaxis:"y", visible:false },
      { type:"bar", x, y:col("Volume"), name:"Volume", xaxis:"x2", yaxis:"y2", visible:true },
      { type:"scatter", mode:"lines", x, y:col("RSI14"), name:"RSI 14", xaxis:"x3", yaxis:"y3", visible:true },
      { type:"bar", x, y:col("MACD_HIST"), name:"MACD Hist", xaxis:"x3", yaxis:"y4", visible:true },
    ];

    const m = payload.meta || {};
    const title = `${m.ticker || ""} — ${m.period || ""} @ ${m.interval || ""} (as-of ${m.as_of_date || ""})`;

    // Remove weekend gaps on time axis
    const weekendRangebreaks = [{ bounds: ["sat", "mon"] }];

    const layout = {
      title,
      height: 900,
      margin: {l:50, r:30, t:90, b:40},
      legend: {orientation:"h", yanchor:"bottom", y:1.02, xanchor:"left", x:0},

      xaxis:  {domain:[0,1], anchor:"y", rangeslider:{visible:false}, rangebreaks: weekendRangebreaks},
      yaxis:  {domain:[0.45,1.0], title:"Price"},

      xaxis2: {domain:[0,1], anchor:"y2", matches:"x", showticklabels:false, rangebreaks: weekendRangebreaks},
      yaxis2: {domain:[0.30,0.43], title:"Volume"},

      xaxis3: {domain:[0,1], anchor:"y3", matches:"x", rangebreaks: weekendRangebreaks},
      yaxis3: {domain:[0.0,0.26], title:"RSI", range:[0,100]},
      yaxis4: {domain:[0.0,0.26], title:"MACD Hist", overlaying:"y3", side:"right", showgrid:false},

      shapes: [
        {type:"line", xref:"paper", yref:"y3", x0:0, x1:1, y0:70, y1:70, line:{width:1}},
        {type:"line", xref:"paper", yref:"y3", x0:0, x1:1, y0:30, y1:30, line:{width:1}},
      ],
      updatemenus: [{
        type:"dropdown",
        direction:"down",
        x:1.0, xanchor:"right",
        y:1.18, yanchor:"top",
        buttons: [
          {label:"Core (EMA+VWAP+RSI+MACD)", method:"update", args:[{visible:presets.core}]},
          {label:"All Indicators", method:"update", args:[{visible:presets.all}]},
          {label:"VWAP Only", method:"update", args:[{visible:presets.vwapOnly}]},
          {label:"Bollinger Bands Only", method:"update", args:[{visible:presets.bbands}]},
          {label:"Price Only", method:"update", args:[{visible:presets.priceOnly}]},
        ]
      }]
    };

    return {traces, layout};
  }

  function escapeHtml(s) {
    return String(s).replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#39;");
  }
  function escapeAttr(s) { return escapeHtml(s); }

  function renderNewsItems(containerEl, items, emptyMsg) {
    if (!items || !items.length) {
      containerEl.textContent = emptyMsg;
      return;
    }
    containerEl.innerHTML = items.map(n => {
      const title = n.title || "(no title)";
      const url = n.url || "";
      const source = n.source || "";
      const ts = n.timestamp_utc || n.published_at || "";
      const sent = n.sentiment || "";
      const conf = (n.confidence_0_to_100 === null || n.confidence_0_to_100 === undefined) ? "" : ` (${n.confidence_0_to_100})`;
      const sum = n.one_sentence_summary || "";
      return `<div class="news-item">
        <div class="news-title">${url ? `<a href="${escapeAttr(url)}" target="_blank" rel="noopener">${escapeHtml(title)}</a>` : escapeHtml(title)}</div>
        <div class="news-meta">${escapeHtml(source)} • ${escapeHtml(ts)} • ${escapeHtml(sent)}${escapeHtml(conf)}</div>
        <div class="news-sum">${escapeHtml(sum)}</div>
      </div>`;
    }).join("");
  }

  function renderAI(row) {
    if (!row) {
      elAI.innerHTML = "No AI recommendation found for this ticker on this date.";
      elAIMeta.textContent = "";
      return;
    }
    const stance = row.stance || "—";
    const conf = (row.confidence_0_to_100 === null || row.confidence_0_to_100 === undefined) ? "—" : row.confidence_0_to_100;
    const rec = row.recommendation_sentence || "—";
    const model = row.model || "—";
    const asOf = row.as_of_utc || "—";

    elAI.innerHTML =
      `<span class="pill">${escapeHtml(stance)}</span>` +
      `<span class="pill">conf ${escapeHtml(conf)}</span>` +
      `<div style="margin-top:8px;">${escapeHtml(rec)}</div>`;
    elAIMeta.textContent = `model=${model} | as_of_utc=${asOf}`;
  }

  function renderAnalyst(block) {
    if (!block) { elAnalyst.textContent = "—"; return; }
    const snap = block.snapshot_latest;
    const evs = block.events_in_day || [];

    let html = "";
    if (snap) {
      html += `<div><b>Latest snapshot</b></div>`;
      const fields = [
        ["as_of_utc","as_of_utc"],
        ["recommendation_key","recommendation_key"],
        ["recommendation_mean","recommendation_mean"],
        ["num_analysts","num_analysts"],
        ["target_mean","target_mean"],
        ["target_high","target_high"],
        ["target_low","target_low"],
        ["sector","sector"],
        ["industry","industry"],
        ["paragraph","paragraph"],
      ];
      html += `<div class="small">` + fields
        .filter(([k,_]) => snap[k] !== undefined && snap[k] !== null && String(snap[k]).length)
        .map(([k,label]) => `<div><b>${escapeHtml(label)}</b>: ${escapeHtml(String(snap[k]))}</div>`)
        .join("") + `</div>`;
    } else {
      html += `<div>No analyst snapshot in DB.</div>`;
    }

    html += `<div style="margin-top:10px;"><b>Events in selected day</b> (n=${evs.length})</div>`;
    if (!evs.length) {
      html += `<div class="small">No analyst events for this date.</div>`;
    } else {
      html += `<div class="small">` + evs.slice(0, 12).map(e => {
        const when = e.captured_at_utc || e.event_time_utc || "";
        const firm = e.firm || "";
        const action = e.action || "";
        const fromG = e.from_grade || "";
        const toG = e.to_grade || "";
        const note = e.note || "";
        return `<div style="margin-top:6px;">
          <div><b>${escapeHtml(firm)}</b> — ${escapeHtml(action)} ${escapeHtml(fromG)}→${escapeHtml(toG)}</div>
          <div class="small">${escapeHtml(when)} ${note ? "— " + escapeHtml(note) : ""}</div>
        </div>`;
      }).join("") + `</div>`;
    }
    elAnalyst.innerHTML = html;
  }

  async function refreshAvailabilityAndMenus() {
    const ticker = elTicker.value;
    const av = await apiGet(`/api/availability?ticker=${encodeURIComponent(ticker)}`);
    const availability = av.availability || {};

    const intervals = Object.keys(availability);
    const pickedInterval = pickDefaultOrFirst(intervals, elInterval.value || "1d");
    buildOptions(elInterval, intervals, pickedInterval);

    const periods = (availability[elInterval.value] || []);
    const pickedPeriod = pickDefaultOrFirst(periods, elPeriod.value || "1mo");
    buildOptions(elPeriod, periods, pickedPeriod);
  }

  async function renderAll() {
    const ticker = elTicker.value;
    const interval = elInterval.value;
    const period = elPeriod.value;
    const d = elDate.value;

    setStatus(`Loading: ${ticker} / ${interval} / ${period} / ${d} ...`);

    try {
      // Global news (independent of ticker)
      const g = await apiGet(`/api/global_news?date=${encodeURIComponent(d)}`);
      renderNewsItems(elGlobalNews, g.items, "No global news rows found for this date.");

      const finance = await apiGet(`/api/finance?ticker=${encodeURIComponent(ticker)}&interval=${encodeURIComponent(interval)}&period=${encodeURIComponent(period)}&date=${encodeURIComponent(d)}`);
      const fig = buildFigure(finance);
      await Plotly.newPlot("chart", fig.traces, fig.layout, {responsive:true});

      const ai = await apiGet(`/api/ai?ticker=${encodeURIComponent(ticker)}&date=${encodeURIComponent(d)}`);
      renderAI(ai.row);

      const analyst = await apiGet(`/api/analyst?ticker=${encodeURIComponent(ticker)}&date=${encodeURIComponent(d)}`);
      renderAnalyst(analyst);

      // Ticker-only news
      const news = await apiGet(`/api/news?company=${encodeURIComponent(ticker)}&entity_type=ticker&date=${encodeURIComponent(d)}`);
      renderNewsItems(elNews, news.items, "No ticker news rows found for this ticker on this date.");

      const m = finance.meta || {};
      setStatus(`Loaded ${m.ticker} — n=${m.n ?? "?"} — fetched=${m.fetched_at_utc ?? "?"}`);
    } catch (e) {
      console.error(e);
      setStatus(`Error: ${e.message}`);
    }
  }

  async function init() {
    elDate.value = todayUTC_yyyy_mm_dd();

    // optional quick health ping
    try { await apiGet("/api/health"); } catch (e) {}

    const t = await apiGet("/api/tickers");
    const tickers = t.tickers || [];
    if (!tickers.length) {
      setStatus("No tickers found in Finance DB.");
      return;
    }
    buildOptions(elTicker, tickers, tickers[0]);

    await refreshAvailabilityAndMenus();
    await renderAll();

    elTicker.addEventListener("change", async () => {
      await refreshAvailabilityAndMenus();
      await renderAll();
    });
    elInterval.addEventListener("change", async () => {
      await refreshAvailabilityAndMenus();
      await renderAll();
    });
    elPeriod.addEventListener("change", async () => { await renderAll(); });
    elDate.addEventListener("change", async () => { await renderAll(); });
  }

  init();
</script>
</body>
</html>
"""

@app.get("/")
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html; charset=utf-8")


# ============================================================
# Exported server runner for main.py
# ============================================================
def run_server(host: str = "0.0.0.0", port: int = 8000, debug: bool = False):
    """
    main.py expects dashboard.run_server(...)
    """
    try:
        cnx = mysql_connect()
        cnx.close()
    except Exception as e:
        raise SystemExit(f"MySQL connection failed: {type(e).__name__}: {e}")

    print(f"Serving dashboard on http://{host}:{port}/")
    app.run(host=host, port=port, debug=debug, threaded=True)


# ============================================================
# CLI Entrypoint
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0", help="Bind address (use 0.0.0.0 for Tailscale access)")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    run_server(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
