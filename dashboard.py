#!/usr/bin/env python3
from __future__ import annotations

"""
MySQL-backed dashboard (Finance + News + AI) served over HTTP (Tailscale-friendly).

BACKWARD COMPATIBLE with:
- OLD news schema (company column)
- NEW news schema (entity_type / entity_id / display_name)

Databases used:
- Finance → prices, indicators, analysts
- LLM     → news
- AI      → recommendations
"""

import os
import argparse
from datetime import datetime, timezone, timedelta, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from flask import Flask, request, jsonify, Response

# ============================================================
# MYSQL CONFIG
# ============================================================

MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "B612b612@")

# ---- Finance DB ----
MYSQL_DB_FINANCE = os.getenv("MYSQL_DB_FINANCE", "Finance")
MYSQL_TABLE_BARS = os.getenv("MYSQL_TABLE_FINANCE", "finance_ohlcv_cache")
MYSQL_TABLE_ANALYST_SNAPSHOT = os.getenv("MYSQL_TABLE_ANALYST_SNAPSHOT", "analyst_snapshot")
MYSQL_TABLE_ANALYST_EVENTS = os.getenv("MYSQL_TABLE_ANALYST_EVENTS", "analyst_events")

# ---- News DB ----
MYSQL_DB_NEWS = os.getenv("MYSQL_DB_NEWS", "LLM")
MYSQL_TABLE_NEWS = os.getenv("MYSQL_TABLE_NEWS", "news_llm_analysis")

# ---- AI DB ----
MYSQL_DB_AI = os.getenv("MYSQL_DB_AI", "AI")
MYSQL_TABLE_AI = os.getenv("MYSQL_TABLE_AI", "ai_recommendations")

# ---- Limits ----
MAX_FINANCE_ROWS = int(os.getenv("MAX_FINANCE_ROWS", "2500"))
NEWS_LIMIT = int(os.getenv("NEWS_LIMIT", "15"))
GLOBAL_NEWS_LIMIT = int(os.getenv("GLOBAL_NEWS_LIMIT", "12"))
ANALYST_EVENTS_LIMIT = int(os.getenv("ANALYST_EVENTS_LIMIT", "25"))

USE_UTC_DAY = os.getenv("USE_UTC_DAY", "1") == "1"

# Finance table column
BARS_INTERVAL_COL = "bar_interval"

# Stable Global Macro entity key
GLOBAL_NEWS_COMPANY = os.getenv("GLOBAL_NEWS_COMPANY", "macro")

# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)

# ============================================================
# HELPERS
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
        now = datetime.now(timezone.utc if USE_UTC_DAY else None)
        return date_cls(now.year, now.month, now.day)
    return datetime.strptime(s, "%Y-%m-%d").date()


def day_window(d: date_cls) -> Tuple[datetime, datetime]:
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def safe_json(v: Any) -> Any:
    if isinstance(v, datetime):
        return iso_z(v if v.tzinfo else v.replace(tzinfo=timezone.utc))
    if isinstance(v, list):
        return [safe_json(x) for x in v]
    if isinstance(v, dict):
        return {k: safe_json(x) for k, x in v.items()}
    return v


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
    ok = (cur.fetchone() or (0,))[0] > 0
    cur.close()
    cnx.close()
    return ok

# ============================================================
# FINANCE
# ============================================================

def fetch_tickers() -> List[str]:
    cnx = mysql_connect(MYSQL_DB_FINANCE)
    cur = cnx.cursor()
    cur.execute(f"SELECT DISTINCT ticker FROM `{MYSQL_TABLE_BARS}` ORDER BY ticker")
    rows = [r[0] for r in cur.fetchall() if r and r[0]]
    cur.close()
    cnx.close()
    return rows


def fetch_availability(ticker: str) -> Dict[str, List[str]]:
    cnx = mysql_connect(MYSQL_DB_FINANCE)
    cur = cnx.cursor()
    cur.execute(
        f"""
        SELECT DISTINCT {BARS_INTERVAL_COL}, period
        FROM `{MYSQL_TABLE_BARS}`
        WHERE ticker=%s
        """,
        (ticker,),
    )
    rows = cur.fetchall()
    cur.close()
    cnx.close()

    out: Dict[str, List[str]] = {}
    for itv, per in rows:
        out.setdefault(itv, []).append(per)
    return out


def fetch_finance_series(ticker: str, interval: str, period: str, d: date_cls):
    _, end_utc = day_window(d)

    cnx = mysql_connect(MYSQL_DB_FINANCE)
    cur = cnx.cursor(dictionary=True)
    cur.execute(
        f"""
        SELECT ts_utc, open, high, low, close, volume,
               ema20, vwap, rsi14, macd_hist, bb_up, bb_mid, bb_low
        FROM `{MYSQL_TABLE_BARS}`
        WHERE ticker=%s AND {BARS_INTERVAL_COL}=%s AND period=%s
          AND ts_utc < %s
        ORDER BY ts_utc DESC
        LIMIT %s
        """,
        (ticker, interval, period, dt_utc_naive(end_utc), MAX_FINANCE_ROWS),
    )
    rows = list(reversed(cur.fetchall()))
    cur.close()
    cnx.close()

    return {
        "meta": {
            "ticker": ticker,
            "interval": interval,
            "period": period,
            "as_of_date": str(d),
            "n": len(rows),
        },
        "data": [
            {
                "t": iso_z(r["ts_utc"]),
                "Open": r["open"],
                "High": r["high"],
                "Low": r["low"],
                "Close": r["close"],
                "Volume": r["volume"],
                "EMA20": r["ema20"],
                "VWAP": r["vwap"],
                "RSI14": r["rsi14"],
                "MACD_HIST": r["macd_hist"],
                "BB_UP": r["bb_up"],
                "BB_MID": r["bb_mid"],
                "BB_LOW": r["bb_low"],
            }
            for r in rows
        ],
    }

# ============================================================
# NEWS (BACKWARD COMPATIBLE)
# ============================================================

def fetch_news(key: str, d: date_cls, limit: int, entity_type: Optional[str] = None):
    start, end = day_window(d)

    has_company = col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "company")
    has_entity_id = col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "entity_id")
    has_display = col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "display_name")
    has_type = col_exists(MYSQL_DB_NEWS, MYSQL_TABLE_NEWS, "entity_type")

    cnx = mysql_connect(MYSQL_DB_NEWS)
    cur = cnx.cursor(dictionary=True)

    where = ["timestamp_utc >= %s", "timestamp_utc < %s", "(error IS NULL OR error='')"]
    params: List[Any] = [dt_utc_naive(start), dt_utc_naive(end)]

    if has_company:
        where.insert(0, "company=%s")
        params.insert(0, key)
    else:
        key_parts = []
        key_params = []
        if has_entity_id:
            key_parts.append("entity_id=%s")
            key_params.append(key)
        if has_display:
            key_parts.append("display_name=%s")
            key_params.append(key)

        where.insert(0, "(" + " OR ".join(key_parts) + ")")
        params = key_params + params

        if entity_type and has_type:
            where.insert(0, "entity_type=%s")
            params.insert(0, entity_type)

    sql = f"""
        SELECT timestamp_utc, published_at, source, title, url,
               one_sentence_summary, sentiment, confidence_0_to_100
        FROM `{MYSQL_TABLE_NEWS}`
        WHERE {" AND ".join(where)}
        ORDER BY timestamp_utc DESC
        LIMIT %s
    """
    params.append(limit)

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return safe_json(rows)

# ============================================================
# AI
# ============================================================

def fetch_ai(ticker: str, d: date_cls):
    start, end = day_window(d)
    cnx = mysql_connect(MYSQL_DB_AI)
    cur = cnx.cursor(dictionary=True)

    cur.execute(
        f"""
        SELECT *
        FROM `{MYSQL_TABLE_AI}`
        WHERE ticker=%s AND run_date=%s
        ORDER BY as_of_utc DESC
        LIMIT 1
        """,
        (ticker, str(d)),
    )
    row = cur.fetchone()

    if not row:
        cur.execute(
            f"""
            SELECT *
            FROM `{MYSQL_TABLE_AI}`
            WHERE ticker=%s AND as_of_utc >= %s AND as_of_utc < %s
            ORDER BY as_of_utc DESC
            LIMIT 1
            """,
            (ticker, dt_utc_naive(start), dt_utc_naive(end)),
        )
        row = cur.fetchone()

    cur.close()
    cnx.close()
    return safe_json(row)

# ============================================================
# API
# ============================================================

@app.get("/api/health")
def api_health():
    return jsonify({
        "finance_db": MYSQL_DB_FINANCE,
        "news_db": MYSQL_DB_NEWS,
        "ai_db": MYSQL_DB_AI,
        "global_news_key": GLOBAL_NEWS_COMPANY,
        "utc_day": USE_UTC_DAY,
    })


@app.get("/api/tickers")
def api_tickers():
    return jsonify({"tickers": fetch_tickers()})


@app.get("/api/availability")
def api_availability():
    t = request.args.get("ticker")
    return jsonify(fetch_availability(t))


@app.get("/api/finance")
def api_finance():
    return jsonify(
        fetch_finance_series(
            request.args["ticker"],
            request.args["interval"],
            request.args["period"],
            parse_yyyy_mm_dd(request.args.get("date")),
        )
    )


@app.get("/api/news")
def api_news():
    key = request.args["company"]
    return jsonify({
        "company": key,
        "items": fetch_news(
            key,
            parse_yyyy_mm_dd(request.args.get("date")),
            NEWS_LIMIT,
            request.args.get("entity_type"),
        ),
    })


@app.get("/api/global_news")
def api_global_news():
    return jsonify({
        "company": GLOBAL_NEWS_COMPANY,
        "items": fetch_news(
            GLOBAL_NEWS_COMPANY,
            parse_yyyy_mm_dd(request.args.get("date")),
            GLOBAL_NEWS_LIMIT,
            "company",
        ),
    })


@app.get("/api/ai")
def api_ai():
    return jsonify(fetch_ai(
        request.args["ticker"],
        parse_yyyy_mm_dd(request.args.get("date")),
    ))

# ============================================================
# RUNNER
# ============================================================

def run_server(host="0.0.0.0", port=8000, debug=False):
    print(f"Serving dashboard on http://{host}:{port}")
    app.run(host=host, port=port, debug=debug, threaded=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    run_server(args.host, args.port, args.debug)


if __name__ == "__main__":
    main()
