#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import json
import time
import argparse
import requests
from datetime import datetime, timezone, timedelta, date as date_cls
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from openai import OpenAI

# ============================================================
# CONFIG (env overrides supported)
# ============================================================

# --- Repo paths ---
REPO_ROOT = Path(__file__).resolve().parents[1]
WATCHLIST_JSON = Path(os.getenv("WATCHLIST_JSON", str(REPO_ROOT / "json" / "tickers.json")))

# --- Ollama (remote) ---
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://100.88.217.85:11434")
OPENAI_BASE = f"{OLLAMA_HOST}/v1"

# Preferred default model for this workflow:
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
MYSQL_TABLE_ANALYST_SNAPSHOT = os.getenv("MYSQL_TABLE_ANALYST_SNAPSHOT", "analyst_snapshot")
MYSQL_TABLE_ANALYST_EVENTS = os.getenv("MYSQL_TABLE_ANALYST_EVENTS", "analyst_events")

# --- Output DB/table ---
MYSQL_DB_AI = os.getenv("MYSQL_DB_AI", "AI")
MYSQL_TABLE_AI = os.getenv("MYSQL_TABLE_AI", "ai_recommendations")

# --- “Same day” window ---
USE_UTC_DAY = os.getenv("USE_UTC_DAY", "1").strip() == "1"

# --- Pull size limits ---
NEWS_LIMIT = int(os.getenv("NEWS_LIMIT", "10"))
ANALYST_EVENTS_LIMIT = int(os.getenv("ANALYST_EVENTS_LIMIT", "20"))

BARS_LIMIT = int(os.getenv("BARS_LIMIT", "120"))
BARS_INTERVAL = os.getenv("BARS_INTERVAL", "1d")
BARS_PERIOD = os.getenv("BARS_PERIOD", "6mo")

# Pacing
SLEEP_BETWEEN_TICKERS_S = float(os.getenv("SLEEP_BETWEEN_TICKERS_S", "0.2"))

# If JSON fails, optionally retry once with a fallback model
RETRY_ON_BAD_JSON = os.getenv("RETRY_ON_BAD_JSON", "1").strip() == "1"
FALLBACK_MODEL = os.getenv("FALLBACK_MODEL", "deepseek-r1:latest")


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
    If d is None: uses "today" in UTC (if USE_UTC_DAY) or local day otherwise.
    """
    if USE_UTC_DAY:
        if d is None:
            now = utc_now()
            d = date_cls(now.year, now.month, now.day)
        start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        return start, end

    # local day window fallback (approx)
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
# DATA FETCH: finance + analyst + news
# ============================================================
def fetch_finance_bars(ticker: str) -> List[Dict[str, Any]]:
    cnx = mysql_connect(MYSQL_DB_FINANCE)
    cur = cnx.cursor(dictionary=True)
    cur.execute(f"""
      SELECT ts_utc, open, high, low, close, volume,
             ema20, vwap, rsi14, macd_hist, bb_up, bb_mid, bb_low
      FROM `{MYSQL_TABLE_BARS}`
      WHERE ticker=%s AND interval=%s AND period=%s
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

def fetch_latest_analyst_snapshot(ticker: str) -> Optional[Dict[str, Any]]:
    cnx = mysql_connect(MYSQL_DB_FINANCE)
    cur = cnx.cursor(dictionary=True)
    cur.execute(f"""
      SELECT *
      FROM `{MYSQL_TABLE_ANALYST_SNAPSHOT}`
      WHERE ticker=%s
      ORDER BY as_of_utc DESC
      LIMIT 1
    """, (ticker,))
    row = cur.fetchone()
    cur.close()
    cnx.close()

    if not row:
        return None

    if isinstance(row.get("as_of_utc"), datetime):
        row["as_of_utc"] = row["as_of_utc"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return row

def fetch_analyst_events_in_window(ticker: str, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
    cnx = mysql_connect(MYSQL_DB_FINANCE)
    cur = cnx.cursor(dictionary=True)
    cur.execute(f"""
      SELECT captured_at_utc, event_time_utc, firm, action, from_grade, to_grade, note
      FROM `{MYSQL_TABLE_ANALYST_EVENTS}`
      WHERE ticker=%s
        AND captured_at_utc >= %s AND captured_at_utc < %s
      ORDER BY captured_at_utc DESC
      LIMIT %s
    """, (ticker, dt_utc_naive(start_utc), dt_utc_naive(end_utc), ANALYST_EVENTS_LIMIT))
    rows = cur.fetchall()
    cur.close()
    cnx.close()

    for r in rows:
        for k in ("captured_at_utc", "event_time_utc"):
            if isinstance(r.get(k), datetime):
                r[k] = r[k].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return rows

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
# PACKET BUILDER
# ============================================================
def summarize_bars_for_packet(bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not bars:
        return {"bars_n": 0, "interval": BARS_INTERVAL, "period": BARS_PERIOD}

    latest = bars[-1]
    tail = bars[-30:]

    def last_non_null(vals: List[Any]) -> Any:
        for v in reversed(vals):
            if v is not None:
                return v
        return None

    last_close = latest.get("close")
    last_ema20 = latest.get("ema20")
    price_vs_ema_pct = None
    if last_close is not None and last_ema20 not in (None, 0):
        try:
            price_vs_ema_pct = (float(last_close) / float(last_ema20) - 1.0) * 100.0
        except Exception:
            price_vs_ema_pct = None

    rsi_vals = [r.get("rsi14") for r in tail]
    macd_vals = [r.get("macd_hist") for r in tail]

    return {
        "bars_n": len(bars),
        "interval": BARS_INTERVAL,
        "period": BARS_PERIOD,
        "latest": latest,
        "tail_30": [
            {
                "t": r.get("ts_utc"),
                "close": r.get("close"),
                "rsi14": r.get("rsi14"),
                "macd_hist": r.get("macd_hist"),
                "ema20": r.get("ema20"),
            }
            for r in tail
        ],
        "derived": {
            "last_close": last_close,
            "last_rsi14": last_non_null(rsi_vals),
            "last_macd_hist": last_non_null(macd_vals),
            "price_vs_ema20_pct": price_vs_ema_pct,
        }
    }

def build_packet(ticker: str, company_name: str, start_utc: datetime, end_utc: datetime) -> Dict[str, Any]:
    bars = fetch_finance_bars(ticker)
    snapshot = fetch_latest_analyst_snapshot(ticker)
    events = fetch_analyst_events_in_window(ticker, start_utc, end_utc)

    news = fetch_news_in_window(company_name, start_utc, end_utc)
    if not news and company_name != ticker:
        news = fetch_news_in_window(ticker, start_utc, end_utc)

    return {
        "ticker": ticker,
        "company_name": company_name,
        "day_window_utc": {
            "start": start_utc.isoformat().replace("+00:00", "Z"),
            "end": end_utc.isoformat().replace("+00:00", "Z"),
        },
        "finance": summarize_bars_for_packet(bars),
        "analyst": {
            "snapshot_latest": snapshot,
            "events_in_window": events,
        },
        "news_in_window": news,
    }


# ============================================================
# OLLAMA MODEL SELECTION
# ============================================================
def list_ollama_models(ollama_host: str) -> List[str]:
    r = requests.get(f"{ollama_host}/api/tags", timeout=15)
    r.raise_for_status()
    data = r.json()
    return [m.get("name") for m in data.get("models", []) if m.get("name")]

def pick_model(models: List[str], preferred: str) -> str:
    if preferred and preferred in models:
        return preferred
    if not models:
        raise RuntimeError("No models found on the Ollama host.")
    return models[0]


# ============================================================
# LLM (OpenAI-compatible via Ollama)
# ============================================================
def extract_json_object(text: str) -> Dict[str, Any]:
    if not text:
        raise ValueError("Empty model response")
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not m:
            raise ValueError("Could not locate JSON in model output")
        return json.loads(m.group(0))

def build_prompt(packet: Dict[str, Any]) -> str:
    schema = {
        "stance": "bullish|neutral|bearish",
        "confidence_0_to_100": 0,
        "recommendation_sentence": "ONE sentence recommendation.",
        "key_drivers": ["string", "string", "string"],
        "risks": ["string", "string", "string"],
        "what_to_watch_next": ["string", "string", "string"]
    }
    return (
        "Return ONLY valid JSON. No markdown.\n\n"
        "You are an investment monitoring assistant. Use ONLY the provided input.\n"
        "Do not invent facts. If evidence is insufficient, be neutral and say what is missing.\n\n"
        f"Required JSON schema:\n{json.dumps(schema, indent=2)}\n\n"
        "Input packet:\n"
        f"{json.dumps(packet, ensure_ascii=False)}"
    )

def run_llm_once(client: OpenAI, model: str, packet: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    prompt = build_prompt(packet)
    start = time.time()

    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=650,
        stream=False,
    )

    elapsed = time.time() - start
    text = resp.choices[0].message.content or ""
    parsed = extract_json_object(text)

    usage = getattr(resp, "usage", None)
    perf = {
        "elapsed_s": round(elapsed, 3),
        "prompt_tokens": getattr(usage, "prompt_tokens", None) if usage else None,
        "completion_tokens": getattr(usage, "completion_tokens", None) if usage else None,
        "total_tokens": getattr(usage, "total_tokens", None) if usage else None,
    }
    return parsed, perf

def run_llm_with_optional_fallback(
    client: OpenAI,
    model: str,
    packet: Dict[str, Any],
    allow_retry: bool,
    fallback_model: str
) -> Tuple[Dict[str, Any], Dict[str, Any], str]:
    try:
        analysis, perf = run_llm_once(client, model, packet)
        return analysis, perf, model
    except Exception as e:
        if not allow_retry or not fallback_model:
            raise
        analysis, perf = run_llm_once(client, fallback_model, packet)
        perf["note"] = f"primary_failed: {type(e).__name__}: {str(e)}"
        return analysis, perf, fallback_model


# ============================================================
# ORCHESTRATION (NEW): load_config / build_state / run_once
# ============================================================
def load_config(watchlist_path: Path = WATCHLIST_JSON) -> Dict[str, Any]:
    """
    Configuration for orchestrator use (main.py).
    """
    return {
        "watchlist_path": str(watchlist_path),
        "model_preferred": DEFAULT_MODEL,
        "fallback_model": FALLBACK_MODEL,
        "retry_on_bad_json": RETRY_ON_BAD_JSON,
        "sleep_s": float(os.getenv("SLEEP_BETWEEN_TICKERS_S", str(SLEEP_BETWEEN_TICKERS_S))),
    }

def build_state(cfg: Optional[Dict[str, Any]] = None, watchlist_path: Path = WATCHLIST_JSON) -> Dict[str, Any]:
    """
    Build reusable state (connection checks + cached model list + OpenAI client).
    """
    if cfg is None:
        cfg = load_config(watchlist_path)

    ensure_ai_db_and_table()

    # Discover models once
    models = list_ollama_models(OLLAMA_HOST)
    chosen = pick_model(models, cfg.get("model_preferred") or DEFAULT_MODEL)

    client = OpenAI(base_url=OPENAI_BASE, api_key="ollama")

    return {
        "cfg": cfg,
        "models": models,
        "model": chosen,
        "client": client,
        "cycle": 0,
        "watchlist_path": Path(cfg.get("watchlist_path") or str(watchlist_path)),
    }

def run_once(
    state: Optional[Dict[str, Any]] = None,
    watchlist_path: Path = WATCHLIST_JSON,
    *,
    run_date: Optional[date_cls] = None,
    tickers: Optional[List[str]] = None,
    max_tickers: Optional[int] = None,
) -> Dict[str, Any]:
    """
    One cycle:
      - builds packet per ticker (finance+analyst+news)
      - runs LLM
      - upserts into AI DB

    Returns summary dict for main.py logging.
    """
    if state is None:
        state = build_state(None, watchlist_path)

    state["cycle"] += 1
    cfg = state["cfg"]
    client: OpenAI = state["client"]
    chosen_model: str = state["model"]

    # Determine day window
    start_utc, end_utc = day_window_for_date(run_date)
    as_of = dt_utc_naive(utc_now())
    run_date_val = start_utc.date()

    # Load watchlist
    watch = load_watchlist(state["watchlist_path"])
    if not watch:
        raise RuntimeError(f"No tickers found in: {state['watchlist_path']}")

    # Filter by tickers if provided
    if tickers is not None:
        wanted = {normalize_ticker(t) for t in tickers}
        watch = [w for w in watch if w["ticker"] in wanted]

    if max_tickers is not None:
        watch = watch[: int(max_tickers)]

    sleep_s = float(cfg.get("sleep_s", 0.2))

    saved_ok = 0
    saved_err = 0
    errors: List[str] = []

    for item in watch:
        ticker = item["ticker"]
        name = item["name"]

        try:
            packet = build_packet(ticker, name, start_utc, end_utc)

            analysis, perf, used_model = run_llm_with_optional_fallback(
                client=client,
                model=chosen_model,
                packet=packet,
                allow_retry=bool(cfg.get("retry_on_bad_json", RETRY_ON_BAD_JSON)),
                fallback_model=str(cfg.get("fallback_model", FALLBACK_MODEL)),
            )

            row = {
                "run_date": run_date_val,
                "as_of_utc": as_of,
                "ticker": ticker,
                "company_name": name,
                "model": used_model,
                "stance": analysis.get("stance"),
                "confidence_0_to_100": analysis.get("confidence_0_to_100"),
                "recommendation_sentence": analysis.get("recommendation_sentence"),
                "simplified": analysis,
                "input_digest": {
                    "window": packet.get("day_window_utc"),
                    "news_count": len(packet.get("news_in_window") or []),
                    "analyst_events_count": len((packet.get("analyst") or {}).get("events_in_window") or []),
                    "bars_n": (packet.get("finance") or {}).get("bars_n"),
                    "perf": perf,
                    "primary_model": chosen_model,
                    "fallback_model": cfg.get("fallback_model") if cfg.get("retry_on_bad_json") else None,
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
                "model": chosen_model,
                "stance": None,
                "confidence_0_to_100": None,
                "recommendation_sentence": None,
                "simplified": None,
                "input_digest": None,
                "error": err,
            }
            try:
                upsert_ai_row(row)
            except Exception as e2:
                errors.append(f"{ticker}: FAILED_TO_SAVE_ERROR_ROW: {type(e2).__name__}: {e2}")

            saved_err += 1

        if sleep_s:
            time.sleep(sleep_s)

    return {
        "cycle": state["cycle"],
        "run_date": str(run_date_val),
        "window_utc": {"start": start_utc.isoformat(), "end": end_utc.isoformat()},
        "tickers": len(watch),
        "model": state["model"],
        "saved_ok": saved_ok,
        "saved_err": saved_err,
        "errors": errors[:50],
        "ts_utc": utc_now().isoformat(),
    }

def run_forever(
    *,
    interval_seconds: int = 900,
    watchlist_path: Path = WATCHLIST_JSON,
):
    """
    Optional: continuous mode if you want ai.py to run as a worker.
    """
    state = build_state(None, watchlist_path)
    while True:
        summary = run_once(state, watchlist_path)
        print(f"[AI] {summary}")
        time.sleep(int(interval_seconds))


# ============================================================
# OPTIONAL: quick model bakeoff on ONE ticker (CLI only)
# ============================================================
def score_schema(obj: Dict[str, Any]) -> Tuple[int, List[str]]:
    required = ["stance", "confidence_0_to_100", "recommendation_sentence", "key_drivers", "risks", "what_to_watch_next"]
    missing = [k for k in required if k not in obj]
    score = 100 - 15 * len(missing)
    return max(score, 0), missing


# ============================================================
# CLI MAIN (unchanged behavior)
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interactive-model", action="store_true", help="Prompt to pick model if preferred is missing.")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="Preferred model name.")
    ap.add_argument("--ticker", default=None, help="Run only this ticker (e.g. RY.TO).")
    ap.add_argument("--bakeoff", action="store_true", help="Run one-ticker bakeoff across all models and print a small report (no DB writes).")
    args = ap.parse_args()

    ensure_ai_db_and_table()

    try:
        models = list_ollama_models(OLLAMA_HOST)
    except Exception as e:
        raise SystemExit(f"Could not reach Ollama at {OLLAMA_HOST}. Error: {e}")

    if not models:
        raise SystemExit(f"No models found on Ollama host: {OLLAMA_HOST}")

    # Interactive selection for CLI only (kept)
    chosen_model = None
    if args.model and args.model in models:
        chosen_model = args.model
    elif args.interactive_model:
        print("\nAvailable models on remote Ollama:\n")
        for i, name in enumerate(models, start=1):
            print(f"{i:>2}. {name}")
        while True:
            choice = input(f"\nSelect model by number (or type exact name). Preferred={args.model}: ").strip()
            if choice.isdigit():
                idx = int(choice)
                if 1 <= idx <= len(models):
                    chosen_model = models[idx - 1]
                    break
                print("Invalid number. Try again.")
            else:
                if choice in models:
                    chosen_model = choice
                    break
                print("Model name not found. Try again.")
    else:
        chosen_model = models[0]

    print(f"Using model: {chosen_model}")
    client = OpenAI(base_url=OPENAI_BASE, api_key="ollama")

    start_utc, end_utc = day_window_for_date()
    run_date = start_utc.date()
    as_of = dt_utc_naive(utc_now())

    watch = load_watchlist(WATCHLIST_JSON)
    if not watch:
        raise SystemExit(f"No tickers found in: {WATCHLIST_JSON}")

    if args.ticker:
        t_norm = normalize_ticker(args.ticker)
        watch = [w for w in watch if w["ticker"] == t_norm]
        if not watch:
            raise SystemExit(f"Ticker not found in watchlist: {t_norm}")

    if args.bakeoff:
        item = watch[0]
        packet = build_packet(item["ticker"], item["name"], start_utc, end_utc)

        print(f"\nBAKEOFF on ticker={item['ticker']} ({item['name']}) window={start_utc.isoformat()} -> {end_utc.isoformat()}\n")
        rows = []
        for m in models:
            try:
                out, perf = run_llm_once(client, m, packet)
                score, missing = score_schema(out)
                rows.append((m, "OK", score, perf.get("elapsed_s"), missing, out.get("stance"), out.get("confidence_0_to_100")))
            except Exception as e:
                rows.append((m, "ERR", 0, None, [str(e)[:120]], None, None))

        rows.sort(key=lambda x: (x[1] != "OK", -(x[2] or 0), (x[3] or 9999)))

        print("Model".ljust(22), "Status".ljust(6), "Score".ljust(6), "Sec".ljust(6), "Stance".ljust(8), "Conf".ljust(6), "Notes")
        print("-" * 100)
        for m, status, score, sec, missing, stance, conf in rows:
            note = ""
            if status == "OK" and missing:
                note = f"missing={missing}"
            elif status != "OK":
                note = f"{missing}"
            print(
                str(m).ljust(22),
                str(status).ljust(6),
                str(score).ljust(6),
                (f"{sec:.2f}" if sec is not None else "-").ljust(6),
                (str(stance) if stance is not None else "-").ljust(8),
                (str(conf) if conf is not None else "-").ljust(6),
                note
            )
        return

    print(f"Tickers loaded: {len(watch)} | Day window: {start_utc.isoformat()} -> {end_utc.isoformat()}")
    print(f"Saving to: {MYSQL_DB_AI}.{MYSQL_TABLE_AI} (unique per run_date,ticker,model)\n")

    for i, item in enumerate(watch, start=1):
        ticker = item["ticker"]
        name = item["name"]

        print(f"[{i}/{len(watch)}] {ticker} ({name})")

        try:
            packet = build_packet(ticker, name, start_utc, end_utc)

            analysis, perf, used_model = run_llm_with_optional_fallback(
                client=client,
                model=chosen_model,
                packet=packet,
                allow_retry=RETRY_ON_BAD_JSON,
                fallback_model=FALLBACK_MODEL
            )

            row = {
                "run_date": run_date,
                "as_of_utc": as_of,
                "ticker": ticker,
                "company_name": name,
                "model": used_model,
                "stance": analysis.get("stance"),
                "confidence_0_to_100": analysis.get("confidence_0_to_100"),
                "recommendation_sentence": analysis.get("recommendation_sentence"),
                "simplified": analysis,
                "input_digest": {
                    "window": packet.get("day_window_utc"),
                    "news_count": len(packet.get("news_in_window") or []),
                    "analyst_events_count": len((packet.get("analyst") or {}).get("events_in_window") or []),
                    "bars_n": (packet.get("finance") or {}).get("bars_n"),
                    "perf": perf,
                    "primary_model": chosen_model,
                    "fallback_model": FALLBACK_MODEL if RETRY_ON_BAD_JSON else None,
                },
                "error": None,
            }

            upsert_ai_row(row)
            print(f"  -> saved | model={used_model} stance={row['stance']} conf={row['confidence_0_to_100']}")
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)}"
            row = {
                "run_date": run_date,
                "as_of_utc": as_of,
                "ticker": ticker,
                "company_name": name,
                "model": chosen_model,
                "stance": None,
                "confidence_0_to_100": None,
                "recommendation_sentence": None,
                "simplified": None,
                "input_digest": None,
                "error": err,
            }
            upsert_ai_row(row)
            print(f"  -> ERROR saved: {err}")

        time.sleep(SLEEP_BETWEEN_TICKERS_S)

    print("\nDone.")


if __name__ == "__main__":
    main()
