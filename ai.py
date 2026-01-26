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
# CONFIG (env overrides supported)
# ============================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
WATCHLIST_JSON = Path(os.getenv("WATCHLIST_JSON", str(REPO_ROOT / "news_llm" / "json" / "tickers.json")))

# --- Ollama (remote) ---
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://100.72.98.127:11434")
OPENAI_BASE = f"{OLLAMA_HOST}/v1"
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b")

# --- MySQL connection ---
MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "B612b612@")

# --- Source DBs/Tables ---
MYSQL_DB_NEWS = os.getenv("MYSQL_DB_NEWS", "LLM")
MYSQL_TABLE_NEWS = os.getenv("MYSQL_TABLE_NEWS", "news_llm_analysis")

MYSQL_DB_FINANCE = os.getenv("MYSQL_DB_FINANCE", "Finance")
MYSQL_TABLE_BARS = os.getenv("MYSQL_TABLE_FINANCE", "finance_ohlcv_cache")

# --- Output DB/table ---
MYSQL_DB_AI = os.getenv("MYSQL_DB_AI", "AI")
MYSQL_TABLE_AI = os.getenv("MYSQL_TABLE_AI", "ai_recommendations")

# --- “Same day” window ---
USE_UTC_DAY = os.getenv("USE_UTC_DAY", "1").strip() == "1"

# --- Pull size limits ---
NEWS_LIMIT = int(os.getenv("NEWS_LIMIT", "10"))
BARS_LIMIT = int(os.getenv("BARS_LIMIT", "120"))
BARS_INTERVAL = os.getenv("BARS_INTERVAL", "1d")     # matches Finance.finance_ohlcv_cache.bar_interval
BARS_PERIOD = os.getenv("BARS_PERIOD", "6mo")        # matches Finance.finance_ohlcv_cache.period

# Pacing
SLEEP_BETWEEN_TICKERS_S = float(os.getenv("SLEEP_BETWEEN_TICKERS_S", "0.2"))

# Retry on bad JSON with fallback
RETRY_ON_BAD_JSON = os.getenv("RETRY_ON_BAD_JSON", "1").strip() == "1"
FALLBACK_MODEL = os.getenv("FALLBACK_MODEL", "deepseek-r1:latest")

# If your Ollama OpenAI-compatible server supports response_format JSON mode.
# If not supported, the code auto-falls back.
FORCE_JSON_MODE = os.getenv("FORCE_JSON_MODE", "1").strip() == "1"


# ============================================================
# TIME HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def dt_utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)

def day_window_for_date(d: Optional[date_cls] = None) -> Tuple[datetime, datetime]:
    """
    Returns (start_utc, end_utc) as timezone-aware UTC datetimes.
    If d is None: uses "today" in UTC (default) or local day window if USE_UTC_DAY=0.
    """
    if USE_UTC_DAY:
        if d is None:
            now = utc_now()
            d = date_cls(now.year, now.month, now.day)
        start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        return start, end

    # local-day fallback
    if d is None:
        now_local = datetime.now()
        d = date_cls(now_local.year, now_local.month, now_local.day)
    start_local = datetime(d.year, d.month, d.day)
    end_local = start_local + timedelta(days=1)
    return start_local.replace(tzinfo=timezone.utc), end_local.replace(tzinfo=timezone.utc)


# ============================================================
# WATCHLIST JSON
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

def load_watchlist(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Watchlist JSON not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Watchlist JSON must be a JSON array (list) of objects.")

    out: List[Dict[str, str]] = []
    seen = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
        t = normalize_ticker(row.get("Ticker"))
        if not t or t in seen:
            continue
        seen.add(t)
        out.append({
            "ticker": t,
            "name": str(row.get("Potential") or row.get("display_name") or t).strip()
        })
    return out


# ============================================================
# MYSQL
# ============================================================

def mysql_connect(db: str | None = None):
    cfg: Dict[str, Any] = dict(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=True,
    )
    if db:
        cfg["database"] = db
    return mysql.connector.connect(**cfg)

def ensure_ai_db_and_table() -> None:
    cnx = mysql_connect()
    cur = cnx.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB_AI}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

    cnx = mysql_connect(MYSQL_DB_AI)
    cur = cnx.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE_AI}` (
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
    cnx = mysql_connect(MYSQL_DB_AI)
    cur = cnx.cursor()

    sql = f"""
    INSERT INTO `{MYSQL_TABLE_AI}` (
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

    vals = (
        row["run_date"],
        row["as_of_utc"],
        row["ticker"],
        row.get("company_name"),
        row["model"],
        row.get("stance"),
        row.get("confidence_0_to_100"),
        row.get("recommendation_sentence"),
        json.dumps(row.get("simplified"), ensure_ascii=False) if row.get("simplified") is not None else None,
        json.dumps(row.get("input_digest"), ensure_ascii=False) if row.get("input_digest") is not None else None,
        row.get("error"),
    )

    cur.execute(sql, vals)
    cur.close()
    cnx.close()


# ============================================================
# FINANCE BARS (FULL SCHEMA: bar_interval + period)
#   Your schema:
#     ticker, bar_interval, period, ts_utc, open, high, low, close, volume,
#     ema20, vwap, rsi14, macd_hist, bb_up, bb_mid, bb_low, ...
# ============================================================

def fetch_finance_bars(ticker: str) -> List[Dict[str, Any]]:
    cnx = mysql_connect(MYSQL_DB_FINANCE)
    cur = cnx.cursor(dictionary=True)

    cur.execute(f"""
      SELECT ts_utc, open, high, low, close, volume,
             ema20, vwap, rsi14, macd_hist, bb_up, bb_mid, bb_low
      FROM `{MYSQL_TABLE_BARS}`
      WHERE ticker=%s
        AND bar_interval=%s
        AND period=%s
      ORDER BY ts_utc DESC
      LIMIT %s
    """, (ticker, BARS_INTERVAL, BARS_PERIOD, BARS_LIMIT))

    rows = cur.fetchall()
    cur.close()
    cnx.close()

    rows = list(reversed(rows))
    for r in rows:
        if isinstance(r.get("ts_utc"), datetime):
            r["ts_utc"] = r["ts_utc"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return rows


# ============================================================
# NEWS (optional; safe if empty)
# ============================================================

def fetch_news_in_window(company_key: str, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
    cnx = mysql_connect(MYSQL_DB_NEWS)
    cur = cnx.cursor(dictionary=True)
    cur.execute(f"""
      SELECT timestamp_utc, published_at, source, title, url,
             one_sentence_summary, sentiment, confidence_0_to_100
      FROM `{MYSQL_TABLE_NEWS}`
      WHERE company=%s
        AND timestamp_utc >= %s AND timestamp_utc < %s
        AND (error IS NULL OR error = '')
      ORDER BY timestamp_utc DESC
      LIMIT %s
    """, (company_key, dt_utc_naive(start_utc), dt_utc_naive(end_utc), NEWS_LIMIT))
    rows = cur.fetchall()
    cur.close()
    cnx.close()

    for r in rows:
        if isinstance(r.get("timestamp_utc"), datetime):
            r["timestamp_utc"] = r["timestamp_utc"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return rows


# ============================================================
# JSON EXTRACTION (ROBUST)
# ============================================================

def extract_json_object(text: str) -> Dict[str, Any]:
    if not text or not text.strip():
        raise ValueError("Empty model response")

    raw = text.strip()

    # If the model returned valid JSON, return immediately
    try:
        return json.loads(raw)
    except Exception:
        pass

    # Remove code fences
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", raw)
        raw = re.sub(r"\s*```\s*$", "", raw).strip()

    # Find first balanced JSON object
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
                    candidate = raw[start:i+1].strip()
                    return json.loads(candidate)

    raise ValueError("Found '{' but could not extract a complete JSON object")


# ============================================================
# LLM (OpenAI-compatible via Ollama)
# ============================================================

def build_prompt(packet: Dict[str, Any]) -> str:
    schema = {
        "stance": "bullish|neutral|bearish",
        "confidence_0_to_100": 0,
        "recommendation_sentence": "ONE sentence recommendation.",
        "key_drivers": ["string", "string", "string"],
        "risks": ["string", "string", "string"],
        "what_to_watch_next": ["string", "string", "string"],
    }
    return (
        "Return ONLY valid JSON. No markdown.\n\n"
        "You are an investment monitoring assistant. Use ONLY the provided input.\n"
        "Do not invent facts. If evidence is insufficient, be neutral and say what is missing.\n\n"
        f"Required JSON schema:\n{json.dumps(schema, indent=2)}\n\n"
        "Input packet:\n"
        f"{json.dumps(packet, ensure_ascii=False)}"
    )

def neutral_fallback(reason: str) -> Dict[str, Any]:
    return {
        "stance": "neutral",
        "confidence_0_to_100": 10,
        "recommendation_sentence": f"Neutral due to insufficient/invalid model output: {reason}",
        "key_drivers": ["Insufficient structured signal from inputs"],
        "risks": ["Model output parsing failure or missing data"],
        "what_to_watch_next": ["Verify bars/news ingestion and enable JSON-mode output if supported"],
    }

def run_llm_once(client: OpenAI, model: str, packet: Dict[str, Any]) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = dict(
        model=model,
        messages=[{"role": "user", "content": build_prompt(packet)}],
        temperature=0.0,
        max_tokens=650,
        stream=False,
    )

    # Try JSON mode if supported by your Ollama OpenAI-compatible endpoint
    if FORCE_JSON_MODE:
        kwargs["response_format"] = {"type": "json_object"}

    resp = client.chat.completions.create(**kwargs)
    text = (resp.choices[0].message.content or "").strip()
    return extract_json_object(text)

def run_llm_with_optional_fallback(client: OpenAI, model: str, packet: Dict[str, Any]) -> Tuple[Dict[str, Any], str, Optional[str]]:
    """
    Returns (output, used_model, note)
    """
    try:
        out = run_llm_once(client, model, packet)
        return out, model, None
    except Exception as e1:
        if not RETRY_ON_BAD_JSON or not FALLBACK_MODEL:
            raise
        try:
            out = run_llm_once(client, FALLBACK_MODEL, packet)
            return out, FALLBACK_MODEL, f"primary_failed: {type(e1).__name__}: {e1}"
        except Exception as e2:
            # Both failed: return neutral fallback so dashboard still has a rec_sentence
            reason = f"primary={type(e1).__name__}: {e1}; fallback={type(e2).__name__}: {e2}"
            return neutral_fallback(reason), model, reason


# ============================================================
# ORCHESTRATION API (what main.py expects)
# ============================================================

_STATE: Optional[Dict[str, Any]] = None

def build_state() -> Dict[str, Any]:
    ensure_ai_db_and_table()
    client = OpenAI(base_url=OPENAI_BASE, api_key="ollama")
    return {
        "client": client,
        "watchlist_path": WATCHLIST_JSON,
        "cycle": 0,
    }

def run_once(
    *,
    run_date: Optional[date_cls] = None,
    tickers: Optional[List[str]] = None,
    max_tickers: Optional[int] = None,
) -> Dict[str, Any]:
    """
    One cycle:
      - load watchlist
      - fetch finance bars + same-day news (optional)
      - run LLM
      - upsert result to AI.ai_recommendations

    Returns a summary dict for logging.
    """
    global _STATE
    if _STATE is None:
        _STATE = build_state()

    _STATE["cycle"] += 1
    client: OpenAI = _STATE["client"]

    start_utc, end_utc = day_window_for_date(run_date)
    run_date_val = start_utc.date()
    as_of = dt_utc_naive(utc_now())

    watch = load_watchlist(Path(_STATE["watchlist_path"]))
    if not watch:
        raise RuntimeError(f"No tickers found in: {_STATE['watchlist_path']}")

    # optional filter
    if tickers is not None:
        wanted = {normalize_ticker(t) for t in tickers}
        watch = [w for w in watch if w["ticker"] in wanted]

    if max_tickers is not None:
        watch = watch[: int(max_tickers)]

    saved_ok = 0
    saved_err = 0
    errors: List[str] = []

    for item in watch:
        ticker = item["ticker"]
        name = item["name"]

        try:
            bars = fetch_finance_bars(ticker)

            news = fetch_news_in_window(name, start_utc, end_utc)
            if not news and name != ticker:
                news = fetch_news_in_window(ticker, start_utc, end_utc)

            packet = {
                "ticker": ticker,
                "company_name": name,
                "day_window_utc": {
                    "start": start_utc.isoformat().replace("+00:00", "Z"),
                    "end": end_utc.isoformat().replace("+00:00", "Z"),
                },
                "finance": {
                    "bars_n": len(bars),
                    "bar_interval": BARS_INTERVAL,
                    "period": BARS_PERIOD,
                    "tail_30": bars[-30:] if len(bars) > 30 else bars,
                },
                "news_in_window": news,
            }

            out, used_model, note = run_llm_with_optional_fallback(client, DEFAULT_MODEL, packet)

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
                    "bars_n": len(bars),
                    "bar_interval": BARS_INTERVAL,
                    "period": BARS_PERIOD,
                    "news_count": len(news or []),
                    "models": {
                        "primary": DEFAULT_MODEL,
                        "fallback": FALLBACK_MODEL if RETRY_ON_BAD_JSON else None,
                        "json_mode_requested": FORCE_JSON_MODE,
                    },
                    "note": note,
                },
                "error": None,
            }

            upsert_ai_row(row)
            saved_ok += 1

        except Exception as e:
            err = f"{type(e).__name__}: {str(e)}"
            errors.append(f"{ticker}: {err}")

            row = {
                "run_date": run_date_val,
                "as_of_utc": as_of,
                "ticker": ticker,
                "company_name": name,
                "model": DEFAULT_MODEL,
                "stance": "neutral",
                "confidence_0_to_100": 10,
                "recommendation_sentence": f"Neutral due to pipeline error: {err}",
                "simplified": neutral_fallback(err),
                "input_digest": {"bar_interval": BARS_INTERVAL, "period": BARS_PERIOD},
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
        "model_primary": DEFAULT_MODEL,
        "saved_ok": saved_ok,
        "saved_err": saved_err,
        "errors": errors[:50],
        "ts_utc": utc_now().isoformat(),
    }


# ============================================================
# Standalone entrypoint (optional)
# ============================================================

def main():
    summary = run_once()
    print(f"[AI] {summary}")

if __name__ == "__main__":
    main()
