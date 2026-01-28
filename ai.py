#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import json
import time
from datetime import datetime, timezone, timedelta, date as date_cls
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from openai import OpenAI

# ============================================================
# CONFIG
# ============================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
WATCHLIST_JSON = Path(os.getenv("WATCHLIST_JSON", str(REPO_ROOT / "news_llm" / "json" / "tickers.json")))

# --- Ollama (OpenAI-compatible) ---
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://100.88.217.85:11434")
OPENAI_BASE = f"{OLLAMA_HOST}/v1"
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b")
FALLBACK_MODEL = os.getenv("FALLBACK_MODEL", "phi3:latest")  # optional
RETRY_ON_BAD_JSON = os.getenv("RETRY_ON_BAD_JSON", "1").strip() == "1"

# --- MySQL ---
MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "B612b612@")

DB_FIN = os.getenv("MYSQL_DB_FINANCE", "Finance")
T_BARS = os.getenv("MYSQL_TABLE_FINANCE", "finance_ohlcv_cache")

DB_NEWS = os.getenv("MYSQL_DB_NEWS", "LLM")
T_NEWS = os.getenv("MYSQL_TABLE_NEWS", "news_llm_analysis")

DB_AI = os.getenv("MYSQL_DB_AI", "AI")
T_AI = os.getenv("MYSQL_TABLE_AI", "ai_recommendations")

# --- Windows & limits ---
USE_UTC_DAY = os.getenv("USE_UTC_DAY", "1").strip() == "1"
NEWS_LIMIT = int(os.getenv("NEWS_LIMIT", "10"))

# **Requested**: past day 15m, include ALL finance columns
BARS_INTERVAL_15M = os.getenv("BARS_INTERVAL_15M", "15m")
BARS_PERIOD_15M = os.getenv("BARS_PERIOD_15M", "1d")
BARS_LIMIT_15M = int(os.getenv("BARS_LIMIT_15M", "200"))  # 1d of 15m ~ 26–30 bars

# LLM tuning (max_tokens = OUTPUT tokens only)
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.1"))
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "1000"))
LLM_TIMEOUT_S = float(os.getenv("LLM_TIMEOUT_S", "200"))
FORCE_JSON_MODE = os.getenv("FORCE_JSON_MODE", "1").strip() == "1"

# Prompt / row size guardrails
PROMPT_CHAR_BUDGET = int(os.getenv("PROMPT_CHAR_BUDGET", "12000"))
STORE_15M_FULL_MAX_ROWS = int(os.getenv("STORE_15M_FULL_MAX_ROWS", "80"))  # avoid huge AI rows
SLEEP_BETWEEN_TICKERS_S = float(os.getenv("SLEEP_BETWEEN_TICKERS_S", "0.2"))

# Finance staleness policy
MAX_STALE_DAYS = int(os.getenv("MAX_STALE_DAYS", "7"))  # treat older than N days as missing

# If true, normalize ALL datetime fields in finance rows to ISO strings (in addition to DB-write sanitization)
NORMALIZE_ALL_DATETIME_FIELDS = os.getenv("NORMALIZE_ALL_DATETIME_FIELDS", "1").strip() == "1"

# ============================================================
# JSON SANITIZATION (FIXES: "datetime is not JSON serializable")
# ============================================================

def json_sanitize(obj: Any) -> Any:
    """
    Recursively convert non-JSON-native types (datetime/date/bytes/etc.) to JSON-safe types.
    Use this BEFORE json.dumps when storing JSON blobs to MySQL.
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            # treat as UTC-naive
            return obj.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return obj.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(obj, date_cls):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, list):
        return [json_sanitize(x) for x in obj]
    if isinstance(obj, tuple):
        return [json_sanitize(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): json_sanitize(v) for k, v in obj.items()}
    return str(obj)

# ============================================================
# TIME HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def dt_utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)

def utc_day_window(d: Optional[date_cls] = None) -> Tuple[datetime, datetime]:
    if USE_UTC_DAY:
        now = utc_now()
        d = d or date_cls(now.year, now.month, now.day)
        start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        return start, end

    now_local = datetime.now()
    d = d or date_cls(now_local.year, now_local.month, now_local.day)
    start_local = datetime(d.year, d.month, d.day)
    end_local = start_local + timedelta(days=1)
    return start_local.replace(tzinfo=timezone.utc), end_local.replace(tzinfo=timezone.utc)

# ============================================================
# TICKER NORMALIZATION + ALIASES
# ============================================================

def normalize_ticker(raw: Any) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    s = s.replace(" ", "").replace("/", "-")
    if s.upper().endswith("-TO"):
        s = s[:-3] + ".TO"
    return s

def ticker_variants(t: str) -> List[str]:
    t = normalize_ticker(t)
    out = [t]
    if t and "." not in t:
        out.append(t + ".TO")
    if t.endswith(".TO"):
        out.append(t[:-3])
    seen = set()
    dedup = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            dedup.append(x)
    return dedup

# ============================================================
# WATCHLIST
# ============================================================

def load_watchlist(path: Path) -> List[Dict[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("tickers.json must be a JSON array")

    out: List[Dict[str, str]] = []
    seen = set()
    for r in payload:
        if not isinstance(r, dict):
            continue
        t = normalize_ticker(r.get("Ticker") or r.get("ticker"))
        if not t or t in seen:
            continue
        seen.add(t)
        name = (r.get("Potential") or r.get("display_name") or r.get("name") or t).strip()
        out.append({"ticker": t, "name": name})
    return out

def load_known_tickers() -> set[str]:
    try:
        wl = load_watchlist(WATCHLIST_JSON)
        return {w["ticker"].upper() for w in wl if w.get("ticker")}
    except Exception:
        return set()

_KNOWN_TICKERS = load_known_tickers()

# ============================================================
# MYSQL
# ============================================================

def mysql_connect(db: str):
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        autocommit=True,
    )

def ensure_ai_db_and_table() -> None:
    cnx = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, autocommit=True
    )
    cur = cnx.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_AI}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

    cnx = mysql_connect(DB_AI)
    cur = cnx.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{T_AI}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      run_date DATE NOT NULL,
      as_of_utc DATETIME NOT NULL,

      ticker VARCHAR(32) NOT NULL,
      company_name VARCHAR(255) NULL,

      model VARCHAR(255) NOT NULL,

      stance VARCHAR(16) NULL,
      confidence_0_to_100 INT NULL,

      recommendation_sentence TEXT NULL,
      simplified JSON NULL,
      input_digest JSON NULL,

      error TEXT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

      PRIMARY KEY (id),
      UNIQUE KEY uniq_run (run_date, ticker, model),
      KEY idx_ticker (ticker),
      KEY idx_asof (as_of_utc)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    cur.close()
    cnx.close()

def upsert_ai_row(row: Dict[str, Any]) -> None:
    cnx = mysql_connect(DB_AI)
    cur = cnx.cursor()
    sql = f"""
    INSERT INTO `{T_AI}` (
      run_date, as_of_utc,
      ticker, company_name,
      model,
      stance, confidence_0_to_100,
      recommendation_sentence,
      simplified, input_digest,
      error
    ) VALUES (
      %s, %s,
      %s, %s,
      %s,
      %s, %s,
      %s,
      %s, %s,
      %s
    )
    ON DUPLICATE KEY UPDATE
      as_of_utc=VALUES(as_of_utc),
      company_name=VALUES(company_name),
      stance=VALUES(stance),
      confidence_0_to_100=VALUES(confidence_0_to_100),
      recommendation_sentence=VALUES(recommendation_sentence),
      simplified=VALUES(simplified),
      input_digest=VALUES(input_digest),
      error=VALUES(error);
    """

    simplified = row.get("simplified")
    input_digest = row.get("input_digest")

    vals = (
        row["run_date"],
        row["as_of_utc"],
        row["ticker"],
        row.get("company_name"),
        row["model"],
        row.get("stance"),
        row.get("confidence_0_to_100"),
        row.get("recommendation_sentence"),
        json.dumps(json_sanitize(simplified), ensure_ascii=False) if simplified is not None else None,
        json.dumps(json_sanitize(input_digest), ensure_ascii=False) if input_digest is not None else None,
        row.get("error"),
    )
    cur.execute(sql, vals)
    cur.close()
    cnx.close()

# ============================================================
# FINANCE TABLE INTROSPECTION + FETCH (ALL COLUMNS)
# ============================================================

_FIN_SCHEMA: Optional[Dict[str, Any]] = None

def get_fin_schema() -> Dict[str, Any]:
    global _FIN_SCHEMA
    if _FIN_SCHEMA is not None:
        return _FIN_SCHEMA

    cnx = mysql_connect(DB_FIN)
    cur = cnx.cursor()
    cur.execute(f"SHOW COLUMNS FROM `{T_BARS}`")
    cols = cur.fetchall()  # (Field, Type, Null, Key, Default, Extra)
    cur.close()
    cnx.close()

    colnames = [c[0] for c in cols]
    lc = {c[0].lower() for c in cols}

    interval_candidates = ["bar_interval", "interval", "timeframe", "tf"]
    period_candidates = ["period", "lookback_period", "window", "range_name"]
    interval_col = next((c for c in interval_candidates if c in lc), None)
    period_col = next((c for c in period_candidates if c in lc), None)

    interval_col_real = None
    period_col_real = None
    if interval_col:
        interval_col_real = next((x for x in colnames if x.lower() == interval_col), None)
    if period_col:
        period_col_real = next((x for x in colnames if x.lower() == period_col), None)

    _FIN_SCHEMA = {
        "columns": colnames,
        "interval_col": interval_col_real,
        "period_col": period_col_real,
    }
    return _FIN_SCHEMA

def _normalize_row_datetimes_in_place(row: Dict[str, Any]) -> None:
    if not NORMALIZE_ALL_DATETIME_FIELDS:
        return
    for k, v in list(row.items()):
        if isinstance(v, datetime):
            # store as ISO string
            if v.tzinfo is None:
                row[k] = v.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            else:
                row[k] = v.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def fetch_finance_bars_filtered(
    ticker: str,
    *,
    interval_val: Optional[str],
    period_val: Optional[str],
    limit: int,
    select_columns: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    schema = get_fin_schema()
    interval_col = schema["interval_col"]
    period_col = schema["period_col"]

    # default if caller didn't pass select_columns
    cols = select_columns or ["ts_utc", "open", "high", "low", "close", "volume", "ema20", "vwap", "rsi14", "macd_hist", "bb_up", "bb_mid", "bb_low"]

    allowed = set(schema["columns"])
    cols = [c for c in cols if c in allowed]
    if "ticker" in allowed and "ticker" not in cols:
        cols = ["ticker"] + cols

    select_list = ", ".join([f"`{c}`" for c in cols])

    cnx = mysql_connect(DB_FIN)
    cur = cnx.cursor(dictionary=True)

    if interval_col and period_col and interval_val and period_val:
        q = f"""
          SELECT {select_list}
          FROM `{T_BARS}`
          WHERE ticker=%s AND `{interval_col}`=%s AND `{period_col}`=%s
          ORDER BY ts_utc DESC
          LIMIT %s
        """
        cur.execute(q, (ticker, interval_val, period_val, limit))
    else:
        q = f"""
          SELECT {select_list}
          FROM `{T_BARS}`
          WHERE ticker=%s
          ORDER BY ts_utc DESC
          LIMIT %s
        """
        cur.execute(q, (ticker, limit))

    rows = cur.fetchall()
    cur.close()
    cnx.close()

    # chronological
    rows = list(reversed(rows))

    # Always normalize ts_utc if present; optionally normalize ALL datetime fields
    for r in rows:
        if isinstance(r.get("ts_utc"), datetime):
            r["ts_utc"] = r["ts_utc"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        _normalize_row_datetimes_in_place(r)

    return rows

def fetch_finance_any_variant(
    ticker: str,
    *,
    interval_val: Optional[str],
    period_val: Optional[str],
    limit: int,
    select_columns: Optional[List[str]] = None,
) -> Tuple[str, List[Dict[str, Any]]]:
    for cand in ticker_variants(ticker):
        rows = fetch_finance_bars_filtered(
            cand,
            interval_val=interval_val,
            period_val=period_val,
            limit=limit,
            select_columns=select_columns,
        )
        if rows:
            return cand, rows
    return ticker, []

def is_stale_ts(ts_iso: Optional[str]) -> bool:
    if not ts_iso:
        return True
    try:
        ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        return (utc_now() - ts) > timedelta(days=MAX_STALE_DAYS)
    except Exception:
        return True

# ============================================================
# NEWS (alias-aware) - USE STORED LLM RESPONSE, DO NOT INCLUDE FULL ARTICLE TEXT
# ============================================================

def _table_has_column(db: str, table: str, col: str) -> bool:
    cnx = mysql_connect(db)
    cur = cnx.cursor()
    cur.execute(f"SHOW COLUMNS FROM `{table}` LIKE %s", (col,))
    ok = cur.fetchone() is not None
    cur.close()
    cnx.close()
    return ok

_NEWS_SCHEMA: Optional[Dict[str, bool]] = None

def get_news_schema() -> Dict[str, bool]:
    global _NEWS_SCHEMA
    if _NEWS_SCHEMA is not None:
        return _NEWS_SCHEMA

    cols = [
        "company",
        "entity_id", "entity_type", "display_name",
        "timestamp_utc", "published_at", "source", "title", "url",
        "one_sentence_summary", "sentiment", "confidence_0_to_100",
        "analysis_json", "llm_raw_text",
        "error",
        "market_direction", "market_time_horizon", "market_rationale",
    ]
    schema: Dict[str, bool] = {}
    for c in cols:
        try:
            schema[c] = _table_has_column(DB_NEWS, T_NEWS, c)
        except Exception:
            schema[c] = False

    _NEWS_SCHEMA = schema
    return schema

def fetch_news_in_window(news_key: str, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
    """
    Query by:
      - entity_id if present (new schema)
      - else company if present (old schema)
    Only pull stored LLM outputs; NO original article full text.
    """
    sch = get_news_schema()

    select_cols: List[str] = []
    for c in [
        "timestamp_utc", "published_at", "source", "title", "url",
        "one_sentence_summary", "sentiment", "confidence_0_to_100",
        "market_direction", "market_time_horizon", "market_rationale",
        "analysis_json", "llm_raw_text",
        "display_name", "entity_type", "entity_id",
        "error",
    ]:
        if sch.get(c):
            select_cols.append(c)

    if not select_cols:
        return []

    where_key_col = "entity_id" if sch.get("entity_id") else ("company" if sch.get("company") else None)
    if not where_key_col:
        return []

    error_filter = "1=1"
    if sch.get("error"):
        error_filter = "(error IS NULL OR error = '')"

    q = f"""
      SELECT {", ".join(select_cols)}
      FROM `{T_NEWS}`
      WHERE `{where_key_col}`=%s
        AND timestamp_utc >= %s AND timestamp_utc < %s
        AND {error_filter}
      ORDER BY timestamp_utc DESC
      LIMIT %s
    """

    cnx = mysql_connect(DB_NEWS)
    cur = cnx.cursor(dictionary=True)
    cur.execute(q, (news_key, dt_utc_naive(start_utc), dt_utc_naive(end_utc), NEWS_LIMIT))
    rows = cur.fetchall()
    cur.close()
    cnx.close()

    for r in rows:
        if isinstance(r.get("timestamp_utc"), datetime):
            r["timestamp_utc"] = r["timestamp_utc"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        if isinstance(r.get("published_at"), datetime):
            r["published_at"] = r["published_at"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        _normalize_row_datetimes_in_place(r)

    return rows

def candidate_news_keys(name: str, ticker: str) -> List[str]:
    keys: List[str] = []
    if name:
        keys.append(name)
    for t in ticker_variants(ticker):
        keys.append(t)
    out: List[str] = []
    for k in keys:
        if k and k not in out:
            out.append(k)
    return out

def fetch_news_any_key(name: str, ticker: str, start_utc: datetime, end_utc: datetime) -> Tuple[str, List[Dict[str, Any]]]:
    for k in candidate_news_keys(name, ticker):
        rows = fetch_news_in_window(k, start_utc, end_utc)
        if rows:
            return k, rows
    return "", []

# ============================================================
# JSON EXTRACTION
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

# ============================================================
# FINANCE SUMMARY (small, deterministic)
# ============================================================

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def summarize_finance(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"available": False}

    last = rows[-1]
    last_ts = last.get("ts_utc")
    if is_stale_ts(last_ts):
        return {"available": False, "reason": "stale_finance_cache", "last_ts_utc": last_ts}

    closes: List[float] = []
    for r in rows:
        c = _safe_float(r.get("close"))
        if c is not None:
            closes.append(c)

    last_close = _safe_float(last.get("close"))
    ema20 = _safe_float(last.get("ema20"))
    rsi = _safe_float(last.get("rsi14"))
    macd_hist = _safe_float(last.get("macd_hist"))
    vwap = _safe_float(last.get("vwap"))

    ret_1 = None
    ret_5 = None
    if len(closes) >= 2 and last_close is not None and closes[-2] not in (None, 0):
        ret_1 = (last_close / closes[-2] - 1.0) * 100.0
    if len(closes) >= 6 and last_close is not None and closes[-6] not in (None, 0):
        ret_5 = (last_close / closes[-6] - 1.0) * 100.0

    trend = None
    if last_close is not None and ema20 is not None:
        trend = "above_ema20" if last_close > ema20 else "below_ema20"

    return {
        "available": True,
        "bars_used": len(rows),
        "last_ts_utc": last_ts,
        "last_close": last_close,
        "ret_1_bar_pct": None if ret_1 is None else round(ret_1, 3),
        "ret_5_bar_pct": None if ret_5 is None else round(ret_5, 3),
        "ema20": ema20,
        "vwap": vwap,
        "rsi14": rsi,
        "macd_hist": macd_hist,
        "trend": trend,
    }

# ============================================================
# PACKET SLIMMING (LLM input) - DO NOT INCLUDE FULL ARTICLE TEXT
# ============================================================

def _json_preview(v: Any, max_chars: int = 1400) -> Any:
    if v is None:
        return None
    # If DB stored JSON as string, keep as string but cap length
    s = v if isinstance(v, str) else json.dumps(json_sanitize(v), ensure_ascii=False)
    if len(s) > max_chars:
        return s[:max_chars] + "…[TRUNCATED]"
    return s

def _text_preview(v: Any, max_chars: int = 900) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    if len(s) > max_chars:
        return s[:max_chars] + "…[TRUNCATED]"
    return s

def slim_packet(packet: Dict[str, Any]) -> Dict[str, Any]:
    """
    Key change you requested:
      - Use stored LLM response fields from news table (analysis_json + llm_raw_text)
      - DO NOT include full_text / original article body (reduces tokens)
    """
    news = packet.get("news_rows") or []
    news_small: List[Dict[str, Any]] = []
    for n in news[:3]:
        if not isinstance(n, dict):
            continue
        news_small.append({
            "source": n.get("source"),
            "title": n.get("title"),
            "published_at": n.get("published_at"),
            "url": n.get("url"),

            # Old summary fields (if present)
            "sentiment": n.get("sentiment"),
            "confidence_0_to_100": n.get("confidence_0_to_100"),
            "one_sentence_summary": n.get("one_sentence_summary"),

            # Market impact (if present)
            "market_direction": n.get("market_direction"),
            "market_time_horizon": n.get("market_time_horizon"),
            "market_rationale": _text_preview(n.get("market_rationale"), 700),

            # ✅ Stored LLM artifacts (bounded)
            "analysis_json": _json_preview(n.get("analysis_json"), 1400),
            "llm_raw_text": _text_preview(n.get("llm_raw_text"), 900),
        })

    return {
        "ticker": packet.get("ticker"),
        "company_name": packet.get("company_name"),
        "window_utc": packet.get("day_window_utc"),

        # Global context
        "global_macro_news_top": packet.get("global_macro_news_top"),

        # Ticker context
        "finance_summary_15m_1d": packet.get("finance_summary_15m_1d"),
        "news_top_3": news_small,
        "analyst_snapshot": packet.get("analyst_snapshot"),
        "analyst_events": packet.get("analyst_events"),
    }

# ============================================================
# VALIDATION (force {ticker}: prefix)
# ============================================================

def validate_llm_output(out: Dict[str, Any], expected_ticker: str) -> Dict[str, Any]:
    if not isinstance(out, dict):
        raise ValueError("LLM output is not a JSON object")

    required = ("ticker", "stance", "confidence_0_to_100", "time_horizon", "recommendation_sentence")
    missing = [k for k in required if k not in out]
    if missing:
        raise ValueError(f"LLM output missing fields: {missing}")

    got_ticker = str(out.get("ticker") or "").strip()
    if got_ticker != expected_ticker:
        raise ValueError(f"Ticker mismatch: expected={expected_ticker} got={got_ticker}")

    stance = str(out.get("stance") or "").strip().lower()
    if stance not in ("bullish", "neutral", "bearish"):
        raise ValueError(f"Invalid stance: {stance}")
    out["stance"] = stance

    try:
        out["confidence_0_to_100"] = max(0, min(100, int(out["confidence_0_to_100"])))
    except Exception:
        raise ValueError("Invalid confidence_0_to_100")

    th = str(out.get("time_horizon") or "").strip().lower()
    if th not in ("intraday", "days", "weeks"):
        th = "days"
    out["time_horizon"] = th

    rec = str(out.get("recommendation_sentence") or "").strip()
    if not rec:
        raise ValueError("Empty recommendation_sentence")

    prefix = f"{expected_ticker}:"
    if not rec.startswith(prefix):
        rec = f"{prefix} {rec}"

    rec = re.sub(r"\s+", " ", rec).strip()
    if len(rec) > 220:
        rec = rec[:220].rstrip()
    out["recommendation_sentence"] = rec

    if _KNOWN_TICKERS:
        upper = rec.upper()
        others = []
        for t in _KNOWN_TICKERS:
            if t == expected_ticker.upper():
                continue
            if re.search(rf"\b{re.escape(t)}\b", upper):
                others.append(t)
                if len(others) >= 3:
                    break
        if others:
            out["_note"] = f"rec_mentions_other_tracked_tickers={others}"

    return out

# ============================================================
# LLM PROMPT
# ============================================================

def build_prompt(packet: Dict[str, Any]) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    expected_ticker = str(packet.get("ticker") or "").strip()
    slim = slim_packet(packet)

    schema = {
        "ticker": "MUST equal input ticker exactly",
        "stance": "bullish|neutral|bearish",
        "confidence_0_to_100": "integer 0..100",
        "time_horizon": "intraday|days|weeks",
        "recommendation_sentence": f"MUST start with '{expected_ticker}: ' ONE sentence, max 220 chars. Do NOT mention other tickers."
    }

    system = (
        "You are a JSON API. Output ONLY valid JSON. No markdown. No extra keys. "
        "Do not include any text outside the JSON object."
    )

    user = (
        "Return ONE JSON object with EXACT keys and allowed values.\n"
        f"Schema:\n{json.dumps(schema, indent=2)}\n\n"
        "Rules:\n"
        f"- ticker MUST equal: {expected_ticker}\n"
        f"- recommendation_sentence MUST start with '{expected_ticker}: '\n"
        "- If news_top_3 is empty, still decide using finance_summary_15m_1d.\n"
        "- Do not mention any indicator unless its value is present (not null).\n\n"
        f"Input:\n{json.dumps(slim, ensure_ascii=False)}"
    )

    if len(user) > PROMPT_CHAR_BUDGET:
        user = user[:PROMPT_CHAR_BUDGET] + "\n\n[TRUNCATED_INPUT]"

    messages = [{"role": "system", "content": system},
                {"role": "user", "content": user}]
    digest = {"packet_slim": slim, "prompt_chars": len(user)}
    return messages, digest

def neutral_fallback(reason: str, expected_ticker: str) -> Dict[str, Any]:
    return {
        "ticker": expected_ticker,
        "stance": "neutral",
        "confidence_0_to_100": 10,
        "time_horizon": "days",
        "recommendation_sentence": f"{expected_ticker}: Neutral due to insufficient/invalid model output: {reason}",
    }

def run_llm_once(client: OpenAI, model: str, packet: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    expected_ticker = str(packet.get("ticker") or "").strip()
    messages, prompt_digest = build_prompt(packet)

    kwargs: Dict[str, Any] = dict(
        model=model,
        messages=messages,
        temperature=LLM_TEMPERATURE,
        max_tokens=LLM_MAX_TOKENS,
        stream=False,
        timeout=LLM_TIMEOUT_S,
    )

    if FORCE_JSON_MODE:
        try:
            resp = client.chat.completions.create(**kwargs, response_format={"type": "json_object"})
            text = (resp.choices[0].message.content or "").strip()
            out = extract_json_object(text)
            return validate_llm_output(out, expected_ticker), prompt_digest
        except Exception:
            pass

    resp = client.chat.completions.create(**kwargs)
    text = (resp.choices[0].message.content or "").strip()
    out = extract_json_object(text)
    return validate_llm_output(out, expected_ticker), prompt_digest

def run_llm_with_fallback(client: OpenAI, packet: Dict[str, Any]) -> Tuple[Dict[str, Any], str, Optional[str], Dict[str, Any]]:
    expected_ticker = str(packet.get("ticker") or "").strip()
    try:
        out, dig = run_llm_once(client, DEFAULT_MODEL, packet)
        return out, DEFAULT_MODEL, None, dig
    except Exception as e1:
        if not RETRY_ON_BAD_JSON or not FALLBACK_MODEL:
            return neutral_fallback(f"{type(e1).__name__}: {e1}", expected_ticker), DEFAULT_MODEL, f"primary_failed: {e1}", {"primary_failed": str(e1)}

        try:
            out2, dig2 = run_llm_once(client, FALLBACK_MODEL, packet)
            return out2, FALLBACK_MODEL, f"primary_failed: {type(e1).__name__}: {e1}", dig2
        except Exception as e2:
            reason = f"primary={type(e1).__name__}: {e1}; fallback={type(e2).__name__}: {e2}"
            return neutral_fallback(reason, expected_ticker), DEFAULT_MODEL, reason, {"primary_failed": str(e1), "fallback_failed": str(e2)}


# ============================================================
# ANALYST DATA (NEW)
# ============================================================

def fetch_analyst_snapshot(ticker: str) -> Optional[Dict[str, Any]]:
    cnx = mysql_connect(DB_FIN)
    cur = cnx.cursor(dictionary=True)
    cur.execute("""
        SELECT *
        FROM analyst_snapshot
        WHERE ticker=%s
        ORDER BY as_of_utc DESC
        LIMIT 1
    """, (ticker,))
    row = cur.fetchone()
    cur.close()
    cnx.close()
    return row


def fetch_analyst_events_day(ticker: str, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
    cnx = mysql_connect(DB_FIN)
    cur = cnx.cursor(dictionary=True)
    cur.execute("""
        SELECT *
        FROM analyst_events
        WHERE ticker=%s
          AND event_time_utc >= %s
          AND event_time_utc < %s
        ORDER BY event_time_utc DESC
        LIMIT 10
    """, (ticker, dt_utc_naive(start_utc), dt_utc_naive(end_utc)))
    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return rows


# ============================================================
# GLOBAL MACRO NEWS (NEW)
# ============================================================

def fetch_global_macro_news(start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
    # entity_id='macro' in news_llm_analysis
    return fetch_news_in_window("macro", start_utc, end_utc)


# ============================================================
# MAIN RUN
# ============================================================

_STATE: Optional[Dict[str, Any]] = None

def build_state() -> Dict[str, Any]:
    ensure_ai_db_and_table()
    client = OpenAI(base_url=OPENAI_BASE, api_key="ollama")
    return {"client": client, "cycle": 0}

def main_run_once(run_date: Optional[date_cls] = None, max_tickers: Optional[int] = None) -> Dict[str, Any]:
    global _STATE
    if _STATE is None:
        _STATE = build_state()

    _STATE["cycle"] += 1
    client: OpenAI = _STATE["client"]

    start_utc, end_utc = utc_day_window(run_date)
    run_date_val = start_utc.date()
    as_of = dt_utc_naive(utc_now())

    watch = load_watchlist(WATCHLIST_JSON)
    if max_tickers is not None:
        watch = watch[: int(max_tickers)]

    saved_ok = 0
    saved_err = 0
    errors: List[str] = []

    schema = get_fin_schema()
    all_fin_cols = schema["columns"]

    for item in watch:
        ticker = item["ticker"]
        name = item["name"]

        try:
            # Finance: past day, 15m, ALL columns (store for inspection)
            matched_15m, bars_15m_full = fetch_finance_any_variant(
                ticker,
                interval_val=BARS_INTERVAL_15M,
                period_val=BARS_PERIOD_15M,
                limit=BARS_LIMIT_15M,
                select_columns=all_fin_cols,  # ALL columns
            )

            # Trim stored full bars to avoid giant JSON rows
            bars_15m_full_store = bars_15m_full[-STORE_15M_FULL_MAX_ROWS:] if bars_15m_full else []

            fin_summary_15m = summarize_finance(bars_15m_full)

            # News: try name + ticker variants (now pulls LLM artifacts, not full article text)
            matched_news_key, news_rows = fetch_news_any_key(name, ticker, start_utc, end_utc)

            # Global macro LLM results (NEW)
            macro_news = fetch_global_macro_news(start_utc, end_utc)

            # Analyst data (NEW)
            analyst_snapshot = fetch_analyst_snapshot(ticker)
            analyst_events = fetch_analyst_events_day(ticker, start_utc, end_utc)

            packet = {
                "ticker": ticker,
                "company_name": name,
                "day_window_utc": {
                    "start": start_utc.isoformat().replace("+00:00", "Z"),
                    "end": end_utc.isoformat().replace("+00:00", "Z"),
                },

                # Global macro context
                "global_macro_news_top": macro_news[:3],

                # Ticker context
                "finance_summary_15m_1d": fin_summary_15m,
                "news_rows": news_rows,

                # Analyst context
                "analyst_snapshot": analyst_snapshot,
                "analyst_events": analyst_events,
            }

            out, used_model, note, prompt_digest = run_llm_with_fallback(client, packet)

            row = {
                "run_date": run_date_val,
                "as_of_utc": as_of,
                "ticker": ticker,
                "company_name": name,
                "model": used_model,
                "stance": out.get("stance"),
                "confidence_0_to_100": out.get("confidence_0_to_100"),
                "recommendation_sentence": out.get("recommendation_sentence"),
                "simplified": out,
                "input_digest": {
                    "finance": {
                        "requested_interval": BARS_INTERVAL_15M,
                        "requested_period": BARS_PERIOD_15M,
                        "matched_ticker_15m": matched_15m,
                        "bars_15m_rows": len(bars_15m_full),
                        "bars_15m_full": bars_15m_full_store,  # ALL columns (trimmed)
                        "summary_15m": fin_summary_15m,
                        "schema": {"interval_col": schema["interval_col"], "period_col": schema["period_col"]},
                    },
                    "news": {
                        "matched_key": matched_news_key,
                        "rows": len(news_rows),
                    },
                    "llm": {
                        "temperature": LLM_TEMPERATURE,
                        "max_tokens": LLM_MAX_TOKENS,
                        "timeout_s": LLM_TIMEOUT_S,
                        "json_mode_requested": FORCE_JSON_MODE,
                        "primary": DEFAULT_MODEL,
                        "fallback": FALLBACK_MODEL if RETRY_ON_BAD_JSON else None,
                        "note": note,
                        "prompt_digest": prompt_digest,
                    },
                },
                "error": None,
            }

            upsert_ai_row(row)
            saved_ok += 1

        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            errors.append(f"{ticker}: {err}")

            fallback = neutral_fallback(err, ticker)
            row = {
                "run_date": run_date_val,
                "as_of_utc": as_of,
                "ticker": ticker,
                "company_name": name,
                "model": DEFAULT_MODEL,
                "stance": fallback["stance"],
                "confidence_0_to_100": fallback["confidence_0_to_100"],
                "recommendation_sentence": fallback["recommendation_sentence"],
                "simplified": fallback,
                "input_digest": {"error": err},
                "error": err,
            }
            try:
                upsert_ai_row(row)
            except Exception as e2:
                errors.append(f"{ticker}: FAILED_TO_SAVE_ERROR_ROW: {type(e2).__name__}: {e2}")

            saved_err += 1

        if SLEEP_BETWEEN_TICKERS_S:
            time.sleep(SLEEP_BETWEEN_TICKERS_S)

    return {
        "cycle": _STATE["cycle"],
        "run_date": str(run_date_val),
        "window_utc": {"start": start_utc.isoformat(), "end": end_utc.isoformat()},
        "tickers": len(watch),
        "saved_ok": saved_ok,
        "saved_err": saved_err,
        "errors": errors[:50],
        "ts_utc": utc_now().isoformat(),
    }

def run_once(run_date: Optional[date_cls] = None, max_tickers: Optional[int] = None) -> Dict[str, Any]:
    """
    Backwards-compatible wrapper.
    Your repo expects ai.run_once(), so keep this stable.
    """
    return main_run_once(run_date=run_date, max_tickers=max_tickers)

def main():
    summary = run_once()
    print("[AI]", json.dumps(summary, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
