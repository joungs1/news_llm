
#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import time
from datetime import datetime, timezone, timedelta, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from openai import OpenAI

# ============================================================
# CONFIG
# ============================================================

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://100.72.98.127:11434")
OPENAI_BASE = f"{OLLAMA_HOST}/v1"
PRIMARY_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b")
FALLBACK_MODEL = os.getenv("FALLBACK_MODEL", "phi3:latest")

MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
DB_FIN = "Finance"
DB_NEWS = "LLM"
DB_AI = "AI"

client = OpenAI(base_url=OPENAI_BASE, api_key="ollama")

# ============================================================
# HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def dt_naive(d: datetime) -> datetime:
    return d.replace(tzinfo=None)

def mysql_connect(db: str):
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        autocommit=True,
    )

# ============================================================
# FINANCE (15m, past 1 month)
# ============================================================

def fetch_finance_15m_month(ticker: str) -> List[Dict[str, Any]]:
    end = utc_now()
    start = end - timedelta(days=30)

    cnx = mysql_connect(DB_FIN)
    cur = cnx.cursor(dictionary=True)
    cur.execute("""
        SELECT *
        FROM finance_ohlcv_cache
        WHERE ticker=%s
          AND bar_interval='15m'
          AND ts_utc >= %s
          AND ts_utc < %s
        ORDER BY ts_utc ASC
    """, (ticker, dt_naive(start), dt_naive(end)))
    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return rows

# ============================================================
# ANALYST DATA
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


def fetch_analyst_events_day(ticker: str, day: date_cls) -> List[Dict[str, Any]]:
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    cnx = mysql_connect(DB_FIN)
    cur = cnx.cursor(dictionary=True)
    cur.execute("""
        SELECT *
        FROM analyst_events
        WHERE ticker=%s
          AND event_time_utc >= %s
          AND event_time_utc < %s
        ORDER BY event_time_utc DESC
    """, (ticker, dt_naive(start), dt_naive(end)))
    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return rows

# ============================================================
# NEWS (stored LLM only)
# ============================================================

def fetch_news(entity_key: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    cnx = mysql_connect(DB_NEWS)
    cur = cnx.cursor(dictionary=True)
    cur.execute("""
        SELECT title, sentiment, confidence_0_to_100,
               market_direction, market_time_horizon
        FROM news_llm_analysis
        WHERE (entity_id=%s OR ticker=%s OR display_name=%s)
          AND timestamp_utc >= %s
          AND timestamp_utc < %s
        ORDER BY timestamp_utc DESC
    """, (entity_key, entity_key, entity_key, dt_naive(start), dt_naive(end)))
    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return rows

# ============================================================
# LLM
# ============================================================

def run_llm(prompt: str) -> str:
    try:
        r = client.chat.completions.create(
            model=PRIMARY_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        return r.choices[0].message.content
    except Exception as e:
        return f"<LLM ERROR: {e}>"

# ============================================================
# MAIN
# ============================================================

def main():
    today = utc_now().date()

    # Example ticker list — replace with your real watchlist loader
    tickers = [
        ("ZUT.TO", "BMO Equal Weight Utilities Index ETF"),
    ]

    for ticker, name in tickers:
        print(f"\n================ TICKER {ticker} =================")

        finance_rows = fetch_finance_15m_month(ticker)
        analyst_snapshot = fetch_analyst_snapshot(ticker)
        analyst_events = fetch_analyst_events_day(ticker, today)

        start = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)
        end = start + timedelta(days=1)

        macro_news = fetch_news("macro", start, end)
        ticker_news = fetch_news(ticker, start, end)

        packet = {
            "ticker": ticker,
            "company_name": name,
            "finance_15m_last_month": finance_rows,
            "analyst_snapshot": analyst_snapshot,
            "analyst_events_today": analyst_events,
            "global_macro_news": macro_news,
            "ticker_news": ticker_news,
        }

        prompt = (
            "You are a market analyst.\n"
            "Use ALL provided data to determine stance and confidence.\n"
            "Return STRICT JSON only.\n\n"
            f"{json.dumps(packet, default=str)}"
        )

        print("\n--- LLM INPUT (truncated) ---")
        print(prompt[:3000])

        llm_out = run_llm(prompt)

        print("\n--- LLM OUTPUT ---")
        print(llm_out)

        time.sleep(0.5)

if __name__ == "__main__":
    main()
