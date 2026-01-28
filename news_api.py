#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import json
import time
import hashlib
import threading
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

import requests
import mysql.connector
from openai import OpenAI

# ============================================================
# CONFIG (use env vars; do NOT hardcode secrets)
# ============================================================

THENEWSAPI_TOKEN = os.getenv("THENEWSAPI_TOKEN") or os.getenv("NEWS_API_TOKEN") or "GLW7gjLDEnhMk0iA2bOLz5ZrFwANg1ZXlunXaR2e"

MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "B612b612@")
MYSQL_DB = os.getenv("MYSQL_DB", "LLM")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "news_llm_analysis")

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_CANDIDATES = [
    Path(os.getenv("NEWS_JSON", "")) if os.getenv("NEWS_JSON") else None,
    ROOT / "json" / "news.json",
    ROOT / "news_llm" / "json" / "news.json",
]
DEFAULT_CONFIG_CANDIDATES = [p for p in DEFAULT_CONFIG_CANDIDATES if p and str(p) != ""]

# Dashboard
DASH_DIR = Path(os.getenv("NEWS_DASH_DIR", str(ROOT / "dashboard_data"))).resolve()
DASH_PORT = int(os.getenv("NEWS_DASH_PORT", "8011"))
DASH_RECENT_N = int(os.getenv("NEWS_DASH_RECENT_N", "250"))
DASH_ENABLE = os.getenv("NEWS_DASH_ENABLE", "1").strip().lower() not in {"0", "false", "no"}
SAVE_LLM_RAW_TEXT = os.getenv("NEWS_SAVE_LLM_RAW_TEXT", "1").strip().lower() not in {"0", "false", "no"}

# Live MySQL inserts:
#  - If 1: insert each article immediately after LLM returns (what you asked)
MYSQL_STREAMING_INSERTS = os.getenv("NEWS_MYSQL_STREAMING", "1").strip().lower() not in {"0", "false", "no"}

# ============================================================
# GLOBAL STATS (proves both APIs are used)
# ============================================================

_STATS = {
    "thenewsapi_requests": 0,
    "thenewsapi_articles": 0,
    "llm_calls": 0,
    "llm_success": 0,
    "llm_fail": 0,
    "mysql_rows_attempted": 0,     # how many rows we tried writing
    "mysql_rows_written": 0,       # how many INSERT statements executed (duplicates still count as written)
    "last_cycle_written": 0,
    "last_cycle_start_utc": None,
    "last_cycle_end_utc": None,
    "cycle": 0,
}

# In-memory ring buffer for dashboard recent events
_RECENT: List[Dict[str, Any]] = []
_RECENT_LOCK = threading.Lock()

# ============================================================
# TIME HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def thenewsapi_dt(dt: datetime) -> str:
    dtu = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dtu.strftime("%Y-%m-%dT%H:%M:%S")

# ============================================================
# UTIL
# ============================================================

def strip_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def normalize_ticker(raw: Any) -> str:
    if raw is None:
        return ""
    s = str(raw).strip().replace(" ", "")
    if not s or s.lower() in {"none", "nan"}:
        return ""
    s = s.replace("/", "-")
    if s.upper().endswith("-TO"):
        s = s[:-3] + ".TO"
    return s

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def clamp_str(v: Any, max_len: int) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    if not s.strip():
        return None
    return s[:max_len]

def _clean_keywords(kws: Any) -> List[str]:
    if not kws or not isinstance(kws, list):
        return []
    out: List[str] = []
    seen = set()
    for k in kws:
        kk = str(k).strip()
        if not kk:
            continue
        lk = kk.lower()
        if lk in seen:
            continue
        seen.add(lk)
        out.append(kk)
    return out

def build_company_search(company_obj: dict) -> str:
    kws = _clean_keywords(company_obj.get("keywords") or [])
    if not kws:
        name = (company_obj.get("display_name") or company_obj.get("id") or "").strip()
        kws = [name] if name else []
    parts = [f"\"{k}\"" if " " in k else k for k in kws]
    return " | ".join(parts)

def build_ticker_search(ticker: str, name: Optional[str], keywords: Optional[List[str]] = None) -> str:
    t = normalize_ticker(ticker)
    if not t:
        return ""
    base = t.split(".")[0] if "." in t else t
    terms: List[str] = []

    if name:
        nm = name.strip()
        if nm:
            terms.append(f"\"{nm}\"")

    terms.append(f"\"{t}\"")
    if base != t:
        terms.append(f"\"{base}\"")

    kws = _clean_keywords(keywords or [])
    for kk in kws[:20]:
        terms.append(f"\"{kk}\"" if " " in kk else kk)

    return " OR ".join(terms)

def safe_parse_published_at(published_at_val: Any) -> Optional[datetime]:
    if not published_at_val:
        return None
    s = str(published_at_val).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)
    except Exception:
        return None

def article_brief(a: dict) -> dict:
    return {
        "uuid": a.get("uuid"),
        "source": a.get("source"),
        "published_at": a.get("published_at"),
        "title": a.get("title"),
        "url": a.get("url"),
        "description": a.get("description"),
        "snippet": a.get("snippet"),
        "relevance_score": a.get("relevance_score"),
        "language": a.get("language"),
        "categories": a.get("categories"),
        "locale": a.get("locale"),
    }

def add_recent(entry: Dict[str, Any]) -> None:
    with _RECENT_LOCK:
        _RECENT.append(entry)
        if len(_RECENT) > DASH_RECENT_N:
            del _RECENT[: len(_RECENT) - DASH_RECENT_N]

# ============================================================
# CONFIG LOAD
# ============================================================

def load_config() -> dict:
    for p in DEFAULT_CONFIG_CANDIDATES:
        try:
            if p and p.exists():
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                cfg["_loaded_from"] = str(p)
                return cfg
        except Exception:
            continue
    raise FileNotFoundError("Could not find news.json. Tried: " + ", ".join(str(p) for p in DEFAULT_CONFIG_CANDIDATES))

# ============================================================
# MySQL
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

def _index_exists(cur, table: str, index_name: str) -> bool:
    cur.execute(f"SHOW INDEX FROM `{table}` WHERE Key_name=%s", (index_name,))
    return len(cur.fetchall()) > 0

def _column_exists(cur, table: str, col: str) -> bool:
    cur.execute(f"SHOW COLUMNS FROM `{table}` LIKE %s", (col,))
    return len(cur.fetchall()) > 0

def ensure_table():
    # Create DB
    cnx = mysql_connect()
    cur = cnx.cursor(buffered=True)
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

    # Create/Migrate table
    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor(buffered=True)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      cycle INT NULL,
      timestamp_utc DATETIME NULL,
      company VARCHAR(255) NULL,

      uuid VARCHAR(255) NULL,
      source VARCHAR(255) NULL,
      published_at DATETIME NULL,
      title TEXT NULL,
      url TEXT NULL,
      url_hash VARCHAR(40) NULL,
      description TEXT NULL,
      snippet TEXT NULL,
      image_url TEXT NULL,

      language VARCHAR(16) NULL,
      categories JSON NULL,
      locale VARCHAR(64) NULL,
      relevance_score DOUBLE NULL,

      sentiment VARCHAR(16) NULL,
      confidence_0_to_100 INT NULL,
      one_sentence_summary TEXT NULL,

      market_direction VARCHAR(16) NULL,
      market_time_horizon VARCHAR(32) NULL,
      market_rationale TEXT NULL,

      elapsed_s DOUBLE NULL,
      prompt_tokens INT NULL,
      completion_tokens INT NULL,
      total_tokens INT NULL,

      analysis_json JSON NULL,
      error TEXT NULL,

      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_company_time (company, timestamp_utc),
      KEY idx_time (timestamp_utc)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # Ensure url_hash exists
    if not _column_exists(cur, MYSQL_TABLE, "url_hash"):
        cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN url_hash VARCHAR(40) NULL AFTER url")

    # Drop old uniq_url if present
    if _index_exists(cur, MYSQL_TABLE, "uniq_url"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` DROP INDEX uniq_url")
        except Exception:
            pass

    # Add unique index on (company, url_hash)
    if not _index_exists(cur, MYSQL_TABLE, "uniq_company_urlhash"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD UNIQUE KEY uniq_company_urlhash (company(191), url_hash)")
        except Exception:
            pass

    # Fix MySQL 1406: widen market_time_horizon
    try:
        cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` MODIFY COLUMN market_time_horizon TEXT NULL")
    except Exception:
        pass

    cur.close()
    cnx.close()

def _insert_rows(rows: List[Dict[str, Any]]) -> None:
    """Writes rows (insert or update)."""
    if not rows:
        return

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor(buffered=True)

    sql = f"""
    INSERT INTO `{MYSQL_TABLE}` (
      cycle, timestamp_utc, company,
      uuid, source, published_at, title, url, url_hash, description, snippet, image_url,
      language, categories, locale, relevance_score,
      sentiment, confidence_0_to_100, one_sentence_summary,
      market_direction, market_time_horizon, market_rationale,
      elapsed_s, prompt_tokens, completion_tokens, total_tokens,
      analysis_json, error
    ) VALUES (
      %s,%s,%s,
      %s,%s,%s,%s,%s,%s,%s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,%s,
      %s,%s
    )
    ON DUPLICATE KEY UPDATE
      cycle=VALUES(cycle),
      timestamp_utc=VALUES(timestamp_utc),
      company=VALUES(company),
      uuid=VALUES(uuid),
      source=VALUES(source),
      published_at=VALUES(published_at),
      title=VALUES(title),
      description=VALUES(description),
      snippet=VALUES(snippet),
      image_url=VALUES(image_url),
      language=VALUES(language),
      categories=VALUES(categories),
      locale=VALUES(locale),
      relevance_score=VALUES(relevance_score),
      sentiment=VALUES(sentiment),
      confidence_0_to_100=VALUES(confidence_0_to_100),
      one_sentence_summary=VALUES(one_sentence_summary),
      market_direction=VALUES(market_direction),
      market_time_horizon=VALUES(market_time_horizon),
      market_rationale=VALUES(market_rationale),
      elapsed_s=VALUES(elapsed_s),
      prompt_tokens=VALUES(prompt_tokens),
      completion_tokens=VALUES(completion_tokens),
      total_tokens=VALUES(total_tokens),
      analysis_json=VALUES(analysis_json),
      error=VALUES(error);
    """

    vals = []
    for r in rows:
        vals.append((
            r.get("cycle"),
            r.get("timestamp_utc"),
            r.get("company"),
            r.get("uuid"),
            r.get("source"),
            r.get("published_at"),
            r.get("title"),
            r.get("url"),
            r.get("url_hash"),
            r.get("description"),
            r.get("snippet"),
            r.get("image_url"),
            r.get("language"),
            r.get("categories"),
            r.get("locale"),
            r.get("relevance_score"),
            r.get("sentiment"),
            r.get("confidence_0_to_100"),
            r.get("one_sentence_summary"),
            r.get("market_direction"),
            r.get("market_time_horizon"),
            r.get("market_rationale"),
            r.get("elapsed_s"),
            r.get("prompt_tokens"),
            r.get("completion_tokens"),
            r.get("total_tokens"),
            r.get("analysis_json"),
            r.get("error"),
        ))

    cur.executemany(sql, vals)
    cur.close()
    cnx.close()

def insert_row_live(row: Dict[str, Any], state: dict) -> None:
    """
    Inserts *immediately* for live progress.
    Updates counters + dashboard files right away.
    """
    _STATS["mysql_rows_attempted"] += 1
    _STATS["mysql_rows_written"] += 1
    _STATS["last_cycle_written"] += 1

    _insert_rows([row])

    if DASH_ENABLE:
        dash_write_files(state)

# ============================================================
# TheNewsAPI
# ============================================================

def _resolve_thenewsapi_endpoint(cfg: dict) -> str:
    api_cfg = (cfg.get("thenewsapi") or {})
    endpoint = (api_cfg.get("endpoint") or "").strip().lower()
    if endpoint in ("top", "all"):
        return endpoint

    qmode = ((cfg.get("query") or {}).get("mode") or "").strip().lower()
    if qmode in ("everything", "all"):
        return "all"
    return "top"

def _resolve_domains(cfg: dict) -> Tuple[List[str], List[str]]:
    api_cfg = (cfg.get("thenewsapi") or {})
    dom = api_cfg.get("domains") or {}
    inc = dom.get("include") or []
    exc = dom.get("exclude") or []
    return list(inc), list(exc)

def fetch_thenewsapi(cfg: dict, search: str, after: datetime, before: datetime) -> List[Dict[str, Any]]:
    endpoint = _resolve_thenewsapi_endpoint(cfg)
    base = f"https://api.thenewsapi.com/v1/news/{endpoint}"

    polling = (cfg.get("polling") or {})
    language = polling.get("language", "en")
    limit = int(polling.get("page_size", polling.get("limit", 50)))

    params: Dict[str, Any] = {
        "api_token": THENEWSAPI_TOKEN,
        "search": search,
        "language": language,
        "published_after": thenewsapi_dt(after),
        "published_before": thenewsapi_dt(before),
        "limit": limit,
        "page": 1,
    }

    api_cfg = (cfg.get("thenewsapi") or {})
    if api_cfg.get("search_fields"):
        params["search_fields"] = api_cfg["search_fields"]

    if api_cfg.get("locale"):
        params["locale"] = ",".join(api_cfg["locale"])

    include_domains, exclude_domains = _resolve_domains(cfg)
    if include_domains:
        params["domains"] = ",".join(include_domains)
    if exclude_domains:
        params["exclude_domains"] = ",".join(exclude_domains)

    cats = api_cfg.get("categories") or {}
    if cats.get("include"):
        params["categories"] = ",".join(cats["include"])
    if cats.get("exclude"):
        params["exclude_categories"] = ",".join(cats["exclude"])

    out: List[Dict[str, Any]] = []
    while True:
        _STATS["thenewsapi_requests"] += 1
        r = requests.get(base, params=params, timeout=30)
        if r.status_code >= 400:
            raise requests.HTTPError(f"{r.status_code} {r.reason}: {r.text[:800]}", response=r)

        data = r.json() or {}
        items = data.get("data") or []
        if isinstance(items, list):
            _STATS["thenewsapi_articles"] += len(items)
            out.extend(items)

        meta = data.get("meta") or {}
        next_page = meta.get("next_page")
        if not next_page:
            break
        params["page"] = int(next_page)
        if len(out) >= 5000:
            break

    return out

# ============================================================
# LLM
# ============================================================

def extract_json_object(text: str) -> Dict[str, Any]:
    if not text or not text.strip():
        raise ValueError("Empty model response")

    raw = text.strip()
    try:
        return json.loads(raw)
    except Exception:
        pass

    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", raw)
        raw = re.sub(r"\s*```\s*$", "", raw).strip()

    start = raw.find("{")
    if start == -1:
        raise ValueError("Could not locate JSON in model output")

    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(raw)):
        ch = raw[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return json.loads(raw[start:i + 1])

    raise ValueError("Found '{' but could not extract a complete JSON object")

def build_prompt(article: dict) -> str:
    schema = {
        "sentiment": "positive|neutral|negative",
        "confidence_0_to_100": 0,
        "one_sentence_summary": "string",
        "market_impact": {
            "direction": "up|down|mixed|unclear",
            "time_horizon": "intraday|days|weeks|months|unclear",
            "rationale": "string"
        }
    }
    return (
        "Return ONLY valid JSON. No markdown.\n\n"
        "Use ONLY the provided article fields. Do not invent facts.\n\n"
        f"Schema:\n{json.dumps(schema, indent=2)}\n\n"
        f"Article:\n{json.dumps(article, ensure_ascii=False)}"
    )

def analyze_article(client: OpenAI, model: str, article: dict) -> Tuple[Dict[str, Any], Dict[str, Any], str]:
    _STATS["llm_calls"] += 1
    t0 = time.time()

    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": build_prompt(article)}],
        temperature=0.2,
        max_tokens=650,
        stream=False,
    )

    elapsed = time.time() - t0
    raw_text = (resp.choices[0].message.content or "").strip()

    parsed = extract_json_object(raw_text)
    _STATS["llm_success"] += 1

    usage = getattr(resp, "usage", None)
    perf = {
        "elapsed_s": round(elapsed, 3),
        "prompt_tokens": getattr(usage, "prompt_tokens", None) if usage else None,
        "completion_tokens": getattr(usage, "completion_tokens", None) if usage else None,
        "total_tokens": getattr(usage, "total_tokens", None) if usage else None,
    }
    return parsed, perf, raw_text

# ============================================================
# DASHBOARD (static files + HTTP server)
# ============================================================

DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>News LLM Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; color: #111; }
    .row { display:flex; gap:12px; flex-wrap: wrap; }
    .card { border:1px solid #ddd; border-radius:10px; padding:12px; background:#fff; }
    .kpi { min-width: 240px; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; }
    .small { font-size: 12px; color: #444; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid #eee; padding: 8px; vertical-align: top; }
    th { text-align:left; background:#fafafa; position: sticky; top: 0; }
    .pill { display:inline-block; padding:2px 8px; border:1px solid #ddd; border-radius:999px; font-size:12px; margin-right:6px; }
    details { border:1px solid #eee; border-radius:10px; padding:8px; background:#fcfcfc; }
    summary { cursor:pointer; }
    a { color: #0b57d0; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .muted { color:#666; }
  </style>
</head>
<body>
  <h2>News → LLM → MySQL (Live Inserts)</h2>
  <div class="row">
    <div class="card kpi">
      <div><b>Cycle</b>: <span id="cycle">-</span></div>
      <div class="small">Last window: <span id="window">-</span></div>
    </div>
    <div class="card kpi">
      <div><b>TheNewsAPI</b></div>
      <div>Requests: <span id="t_req">-</span></div>
      <div>Articles: <span id="t_art">-</span></div>
    </div>
    <div class="card kpi">
      <div><b>LLM</b></div>
      <div>Calls: <span id="l_calls">-</span></div>
      <div>Success: <span id="l_ok">-</span></div>
      <div>Fail: <span id="l_fail">-</span></div>
    </div>
    <div class="card kpi">
      <div><b>MySQL</b></div>
      <div>Attempted rows: <span id="m_try">-</span></div>
      <div>Written ops: <span id="m_written">-</span></div>
      <div>Last cycle written: <span id="m_last">-</span></div>
    </div>
  </div>

  <h3 style="margin-top:18px;">Recent: sent to LLM + response</h3>
  <div class="small muted">Auto-refresh every 3 seconds. Newest first.</div>
  <div id="recent"></div>

<script>
async function loadJSON(path){
  const r = await fetch(path + "?_=" + Date.now());
  if(!r.ok) throw new Error("HTTP " + r.status);
  return await r.json();
}
function esc(s){
  if(s === null || s === undefined) return "";
  return String(s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));
}
function renderRecent(items){
  const root = document.getElementById("recent");
  if(!items || items.length === 0){
    root.innerHTML = "<div class='card'>No recent items yet.</div>";
    return;
  }
  let html = "<table><thead><tr>" +
    "<th>When</th><th>Company/Ticker</th><th>Article</th><th>LLM Output</th></tr></thead><tbody>";

  for(const it of items){
    const when = esc(it.ts_utc || "");
    const company = esc(it.company || "");
    const title = esc((it.article||{}).title || "");
    const url = esc((it.article||{}).url || "");
    const source = esc((it.article||{}).source || "");
    const pub = esc((it.article||{}).published_at || "");
    const snippet = esc((it.article||{}).snippet || (it.article||{}).description || "");

    const parsed = it.parsed || {};
    const sentiment = esc(parsed.sentiment || "");
    const conf = esc(parsed.confidence_0_to_100 || "");
    const one = esc(parsed.one_sentence_summary || "");
    const mi = parsed.market_impact || {};
    const dir = esc(mi.direction || "");
    const hor = esc(mi.time_horizon || "");
    const rat = esc(mi.rationale || "");

    html += "<tr>";
    html += "<td class='small'>" + when + "</td>";
    html += "<td><span class='pill'>" + company + "</span><div class='small muted'>stage=" + esc(it.stage||"") + "</div></td>";
    html += "<td>" +
      "<div><b>" + (url ? "<a target='_blank' href='" + url + "'>" + title + "</a>" : title) + "</b></div>" +
      "<div class='small muted'>source=" + source + " • published=" + pub + "</div>" +
      (snippet ? "<div class='small'>" + snippet + "</div>" : "") +
      "</td>";

    html += "<td>" +
      "<div class='small'><span class='pill'>sentiment=" + sentiment + "</span>" +
      "<span class='pill'>conf=" + conf + "</span>" +
      "<span class='pill'>dir=" + dir + "</span>" +
      "<span class='pill'>horizon=" + hor + "</span></div>" +
      (one ? "<div><b>Summary:</b> " + one + "</div>" : "") +
      (rat ? "<div class='small'><b>Rationale:</b> " + rat + "</div>" : "") +
      (it.error ? "<div class='small' style='color:#b00020'><b>Error:</b> " + esc(it.error) + "</div>" : "") +
      "<details style='margin-top:6px;'><summary class='mono'>Raw JSON (parsed)</summary>" +
      "<pre class='mono'>" + esc(JSON.stringify(parsed || {}, null, 2)) + "</pre></details>" +
      (it.raw_text ? "<details><summary class='mono'>Raw LLM text</summary><pre class='mono'>" + esc(it.raw_text) + "</pre></details>" : "") +
      "</td>";

    html += "</tr>";
  }
  html += "</tbody></table>";
  root.innerHTML = html;
}

async function refresh(){
  try{
    const s = await loadJSON("stats.json");
    document.getElementById("cycle").textContent = s.cycle ?? "-";
    document.getElementById("window").textContent = (s.last_cycle_start_utc || "-") + " → " + (s.last_cycle_end_utc || "-");
    document.getElementById("t_req").textContent = s.thenewsapi_requests ?? "-";
    document.getElementById("t_art").textContent = s.thenewsapi_articles ?? "-";
    document.getElementById("l_calls").textContent = s.llm_calls ?? "-";
    document.getElementById("l_ok").textContent = s.llm_success ?? "-";
    document.getElementById("l_fail").textContent = s.llm_fail ?? "-";
    document.getElementById("m_try").textContent = s.mysql_rows_attempted ?? "-";
    document.getElementById("m_written").textContent = s.mysql_rows_written ?? "-";
    document.getElementById("m_last").textContent = s.last_cycle_written ?? "-";
  }catch(e){}

  try{
    const r = await loadJSON("recent.json");
    renderRecent(r.items || []);
  }catch(e){}
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""

def dash_write_files(state: dict) -> None:
    if not DASH_ENABLE:
        return
    DASH_DIR.mkdir(parents=True, exist_ok=True)

    stats_payload = dict(_STATS)
    with open(DASH_DIR / "stats.json", "w", encoding="utf-8") as f:
        json.dump(stats_payload, f, ensure_ascii=False, indent=2)

    with _RECENT_LOCK:
        items = list(reversed(_RECENT))
    with open(DASH_DIR / "recent.json", "w", encoding="utf-8") as f:
        json.dump({"items": items}, f, ensure_ascii=False)

    html_path = DASH_DIR / "dashboard.html"
    if not html_path.exists():
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(DASHBOARD_HTML)

def dash_serve_forever() -> None:
    if not DASH_ENABLE:
        return
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    html_path = DASH_DIR / "dashboard.html"
    if not html_path.exists():
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(DASHBOARD_HTML)

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(DASH_DIR), **kwargs)

    httpd = ThreadingHTTPServer(("0.0.0.0", DASH_PORT), Handler)
    print(f"[dashboard] serving {DASH_DIR} on http://0.0.0.0:{DASH_PORT}/dashboard.html")
    httpd.serve_forever()

# ============================================================
# RUNTIME
# ============================================================

def list_ollama_models(ollama_host: str) -> List[str]:
    r = requests.get(f"{ollama_host}/api/tags", timeout=20)
    r.raise_for_status()
    j = r.json() or {}
    models = j.get("models") or []
    return [m.get("name") for m in models if m.get("name")]

def pick_model(models: List[str], preferred: str, fallback_index: int) -> str:
    if preferred and preferred in models:
        return preferred
    if not models:
        raise RuntimeError("No models found on the Ollama host.")
    if 0 <= fallback_index < len(models):
        return models[fallback_index]
    return models[0]

def _print_loaded_keywords(companies: List[dict], tickers: List[Dict[str, Any]]) -> None:
    print("\n[keywords_loaded] companies:")
    for c in companies:
        name = (c.get("display_name") or c.get("id") or "UNKNOWN").strip()
        kws = _clean_keywords(c.get("keywords") or [])
        preview = ", ".join(kws[:12])
        suffix = " ..." if len(kws) > 12 else ""
        print(f"  - {name}: {len(kws)} keywords -> {preview}{suffix}")

    print("\n[keywords_loaded] tickers:")
    for t in tickers:
        sym = t.get("ticker") or ""
        nm = t.get("name") or sym
        kws = _clean_keywords(t.get("keywords") or [])
        preview = ", ".join(kws[:10])
        suffix = " ..." if len(kws) > 10 else ""
        print(f"  - {sym} ({nm}): {len(kws)} keywords -> {preview}{suffix}")
    print("")

def build_runtime(cfg: dict) -> dict:
    polling = cfg.get("polling", {}) or {}
    lookback_minutes = int(polling.get("lookback_minutes", 30))

    model_cfg = cfg.get("model", {}) or {}
    ollama_host = model_cfg.get("host")
    if not ollama_host:
        raise RuntimeError("news.json missing model.host")

    preferred_model = model_cfg.get("preferred_model", "")
    fallback_idx = int(model_cfg.get("fallback_model_index", 0))

    companies = (cfg.get("query") or {}).get("companies") or []

    tickers_cfg = cfg.get("tickers") or {}
    tickers_mode = (tickers_cfg.get("mode") or "").strip().lower()
    tickers_track = tickers_cfg.get("track") or []
    tickers: List[Dict[str, Any]] = []
    if tickers_mode == "explicit" and isinstance(tickers_track, list):
        for t in tickers_track:
            if not isinstance(t, dict):
                continue
            sym = normalize_ticker(t.get("ticker"))
            if not sym:
                continue
            nm = strip_or_none(t.get("name")) or sym
            kws = _clean_keywords(t.get("keywords") or [])
            tickers.append({"ticker": sym, "name": nm, "keywords": kws})

    if not companies and not tickers:
        raise RuntimeError("news.json must include query.companies and/or tickers.track (mode=explicit).")

    ensure_table()

    models = list_ollama_models(ollama_host)
    chosen_model = pick_model(models, preferred_model, fallback_idx)
    client = OpenAI(base_url=f"{ollama_host}/v1", api_key="ollama")

    _print_loaded_keywords(companies, tickers)

    state = {
        "cfg": cfg,
        "client": client,
        "model": chosen_model,
        "companies": companies,
        "tickers": tickers,
        "seen": set(),  # (company, url_hash) to dedupe within runtime too
        "last_poll": utc_now() - timedelta(minutes=lookback_minutes),
        "cycle": 0,
    }

    if DASH_ENABLE:
        t = threading.Thread(target=dash_serve_forever, daemon=True)
        t.start()
        dash_write_files(state)

    return state

_STATE: Optional[dict] = None

def run_once() -> Tuple[int, datetime]:
    global _STATE

    if not THENEWSAPI_TOKEN:
        raise RuntimeError("Missing THENEWSAPI_TOKEN env var.")
    if not MYSQL_PASSWORD:
        raise RuntimeError("Missing MYSQL_PASSWORD env var.")

    if _STATE is None:
        cfg = load_config()
        _STATE = build_runtime(cfg)
        print(f"[news_api] loaded_config={_STATE['cfg'].get('_loaded_from')}")
        print(f"[news_api] tickers_loaded={len(_STATE['tickers'])} companies_loaded={len(_STATE['companies'])}")
        print(f"[news_api] model={_STATE['model']}")
        if DASH_ENABLE:
            print(f"[news_api] dashboard_url=http://localhost:{DASH_PORT}/dashboard.html")

    # cycle bookkeeping
    _STATE["cycle"] += 1
    _STATS["cycle"] = _STATE["cycle"]
    _STATS["last_cycle_written"] = 0  # reset per cycle

    cfg = _STATE["cfg"]
    client: OpenAI = _STATE["client"]
    model: str = _STATE["model"]
    companies = _STATE["companies"]
    tickers = _STATE["tickers"]
    seen = _STATE["seen"]
    last_poll = _STATE["last_poll"]

    poll_end = utc_now()
    _STATS["last_cycle_start_utc"] = iso_z(last_poll)
    _STATS["last_cycle_end_utc"] = iso_z(poll_end)

    inserted_like = 0  # how many rows we attempted to write this cycle (live)

    def process_article(company_label: str, a: dict) -> None:
        nonlocal inserted_like

        url = (a.get("url") or "").strip()
        if not url:
            return

        url_hash = sha1_hex(url)
        seen_key = (company_label, url_hash)
        if seen_key in seen:
            return
        seen.add(seen_key)

        # Dashboard: show what is being sent to LLM
        add_recent({
            "ts_utc": iso_z(utc_now()),
            "company": company_label,
            "stage": "sent_to_llm",
            "article": article_brief(a),
        })
        if DASH_ENABLE:
            dash_write_files(_STATE)

        try:
            analysis, perf, raw_text = analyze_article(client, model, a)
            mi = (analysis.get("market_impact") or {})

            add_recent({
                "ts_utc": iso_z(utc_now()),
                "company": company_label,
                "stage": "llm_response",
                "article": article_brief(a),
                "parsed": analysis,
                "raw_text": raw_text if SAVE_LLM_RAW_TEXT else None,
            })

            row = {
                "cycle": _STATE["cycle"],
                "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),
                "company": company_label,
                "uuid": a.get("uuid"),
                "source": a.get("source"),
                "published_at": safe_parse_published_at(a.get("published_at")),
                "title": a.get("title"),
                "url": url,
                "url_hash": url_hash,
                "description": a.get("description"),
                "snippet": a.get("snippet"),
                "image_url": a.get("image_url"),
                "language": a.get("language"),
                "categories": json.dumps(a.get("categories"), ensure_ascii=False) if a.get("categories") is not None else None,
                "locale": a.get("locale"),
                "relevance_score": a.get("relevance_score"),
                "sentiment": clamp_str(analysis.get("sentiment"), 16),
                "confidence_0_to_100": analysis.get("confidence_0_to_100"),
                "one_sentence_summary": analysis.get("one_sentence_summary"),
                "market_direction": clamp_str(mi.get("direction"), 16),
                "market_time_horizon": clamp_str(mi.get("time_horizon"), 500),  # safe even if long (col is TEXT)
                "market_rationale": mi.get("rationale"),
                "elapsed_s": perf.get("elapsed_s"),
                "prompt_tokens": perf.get("prompt_tokens"),
                "completion_tokens": perf.get("completion_tokens"),
                "total_tokens": perf.get("total_tokens"),
                "analysis_json": json.dumps(analysis, ensure_ascii=False),
                "error": None,
            }

            if MYSQL_STREAMING_INSERTS:
                insert_row_live(row, _STATE)
                inserted_like += 1
            else:
                # fallback: still write immediately, but same path
                insert_row_live(row, _STATE)
                inserted_like += 1

        except Exception as e:
            _STATS["llm_fail"] += 1
            add_recent({
                "ts_utc": iso_z(utc_now()),
                "company": company_label,
                "stage": "llm_error",
                "article": article_brief(a),
                "error": f"{type(e).__name__}: {e}",
            })

            err_row = {
                "cycle": _STATE["cycle"],
                "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),
                "company": company_label,
                "uuid": a.get("uuid"),
                "source": a.get("source"),
                "published_at": safe_parse_published_at(a.get("published_at")),
                "title": a.get("title"),
                "url": url,
                "url_hash": url_hash,
                "error": f"analysis_error: {type(e).__name__}: {e}",
            }

            insert_row_live(err_row, _STATE)
            inserted_like += 1

    # A) Companies (macro)
    for c in companies:
        company_name = (c.get("display_name") or c.get("id") or "UNKNOWN").strip()
        search = build_company_search(c)
        if not search:
            continue
        articles = fetch_thenewsapi(cfg, search, last_poll, poll_end)
        for a in articles:
            process_article(company_name, a)

    # B) Tickers (company label = ticker)
    for t in tickers:
        ticker = normalize_ticker(t.get("ticker"))
        name = strip_or_none(t.get("name")) or ticker
        keywords = t.get("keywords") or []
        if not ticker:
            continue
        search = build_ticker_search(ticker, name, keywords)
        if not search:
            continue
        articles = fetch_thenewsapi(cfg, search, last_poll, poll_end)
        for a in articles:
            process_article(ticker, a)

    _STATE["last_poll"] = poll_end
    if DASH_ENABLE:
        dash_write_files(_STATE)

    print(
        f"[stats] thenewsapi_requests={_STATS['thenewsapi_requests']} "
        f"thenewsapi_articles={_STATS['thenewsapi_articles']} "
        f"llm_calls={_STATS['llm_calls']} llm_success={_STATS['llm_success']} llm_fail={_STATS['llm_fail']} "
        f"mysql_rows_attempted={_STATS['mysql_rows_attempted']} mysql_rows_written={_STATS['mysql_rows_written']} "
        f"last_cycle_written={_STATS['last_cycle_written']}"
    )

    return inserted_like, poll_end

def run_forever():
    cfg = load_config()
    polling = cfg.get("polling", {}) or {}
    interval_seconds = int(polling.get("interval_seconds", 60))

    global _STATE
    _STATE = build_runtime(cfg)

    print(f"[news_api] loaded_config={_STATE['cfg'].get('_loaded_from')}")
    print(f"[news_api] tickers_loaded={len(_STATE['tickers'])} companies_loaded={len(_STATE['companies'])}")
    print(f"[news_api] model={_STATE['model']}")
    print(f"[news_api] mysql_streaming_inserts={MYSQL_STREAMING_INSERTS}")
    if DASH_ENABLE:
        print(f"[news_api] dashboard_url=http://localhost:{DASH_PORT}/dashboard.html")

    while True:
        written, poll_end = run_once()
        print(f"[{iso_z(poll_end)}] wrote_rows={written} -> {MYSQL_DB}.{MYSQL_TABLE}")
        time.sleep(interval_seconds)

def main():
    # one-shot run
    written, poll_end = run_once()
    print(f"[{iso_z(poll_end)}] wrote_rows={written} -> {MYSQL_DB}.{MYSQL_TABLE}")

if __name__ == "__main__":
    main()
