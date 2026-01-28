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
# CONFIG
# ============================================================

THENEWSAPI_TOKEN = os.getenv("THENEWSAPI_TOKEN")
MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB", "LLM")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "news_llm_analysis")

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_CANDIDATES = [
    Path(os.getenv("NEWS_JSON", "")) if os.getenv("NEWS_JSON") else None,
    ROOT / "json" / "news.json",
    ROOT / "news_llm" / "json" / "news.json",
]
DEFAULT_CONFIG_CANDIDATES = [p for p in DEFAULT_CONFIG_CANDIDATES if p]

# Dashboard
DASH_DIR = Path(os.getenv("NEWS_DASH_DIR", str(ROOT / "dashboard_data"))).resolve()
DASH_PORT = int(os.getenv("NEWS_DASH_PORT", "8011"))
DASH_ENABLE = True

# ============================================================
# HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()

def normalize_ticker(v):
    if not v:
        return ""
    return str(v).strip().upper()

def clean_keywords(kws):
    if not kws:
        return []
    seen = set()
    out = []
    for k in kws:
        k = str(k).strip()
        if k and k.lower() not in seen:
            seen.add(k.lower())
            out.append(k)
    return out

# ============================================================
# SEARCH BUILDERS (FIXED)
# ============================================================

def build_company_search(c: dict) -> str:
    kws = clean_keywords(c.get("keywords"))
    if not kws:
        name = c.get("display_name") or c.get("id")
        kws = [name]
    return " OR ".join(f"\"{k}\"" if " " in k else k for k in kws)

def build_ticker_search(ticker: str, name: str, keywords: List[str]) -> str:
    terms = [f"\"{name}\"", f"\"{ticker}\""]
    base = ticker.split(".")[0]
    if base != ticker:
        terms.append(f"\"{base}\"")

    for k in clean_keywords(keywords)[:8]:  # 🔑 CRITICAL
        terms.append(f"\"{k}\"" if " " in k else k)

    return " OR ".join(terms)

# ============================================================
# CONFIG LOAD
# ============================================================

def load_config():
    for p in DEFAULT_CONFIG_CANDIDATES:
        if p and p.exists():
            with open(p, "r") as f:
                cfg = json.load(f)
            cfg["_loaded_from"] = str(p)
            return cfg
    raise FileNotFoundError("news.json not found")

# ============================================================
# MYSQL
# ============================================================

def mysql_connect(db=None):
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        autocommit=True,
    )

def ensure_table():
    cnx = mysql_connect()
    cur = cnx.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}`")
    cur.close()
    cnx.close()

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE}` (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      company VARCHAR(255),
      title TEXT,
      url TEXT,
      url_hash VARCHAR(40),
      published_at DATETIME,
      sentiment VARCHAR(16),
      confidence INT,
      summary TEXT,
      direction VARCHAR(16),
      horizon TEXT,
      rationale TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_company_url (company(191), url_hash)
    )
    """)
    cur.close()
    cnx.close()

# ============================================================
# NEWS API
# ============================================================

def fetch_thenewsapi(cfg, search, after, before):
    url = "https://api.thenewsapi.com/v1/news/all"
    params = {
        "api_token": THENEWSAPI_TOKEN,
        "search": search,
        "language": "en",
        "published_after": after.strftime("%Y-%m-%dT%H:%M:%S"),
        "published_before": before.strftime("%Y-%m-%dT%H:%M:%S"),
        "limit": cfg["polling"]["limit"],
    }

    domains = cfg["thenewsapi"]["domains"].get("include")
    if domains:
        params["domains"] = ",".join(domains)

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    return data

# ============================================================
# LLM
# ============================================================

def analyze_article(client, model, article):
    prompt = f"""
Return ONLY JSON:
{{
  "sentiment": "positive|neutral|negative",
  "confidence": 0-100,
  "summary": "one sentence",
  "market_impact": {{
    "direction": "up|down|mixed|unclear",
    "time_horizon": "intraday|days|weeks|months|unclear",
    "rationale": "string"
  }}
}}

Article:
{json.dumps(article, ensure_ascii=False)}
"""
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=500,
    )
    return json.loads(resp.choices[0].message.content)

# ============================================================
# MAIN LOOP
# ============================================================

def run_once():
    cfg = load_config()
    ensure_table()

    client = OpenAI(base_url=f"{cfg['model']['host']}/v1", api_key="ollama")
    model = cfg["model"]["preferred_model"]

    lookback = timedelta(minutes=cfg["polling"]["lookback_minutes"])
    start = utc_now() - lookback
    end = utc_now()

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()

    # ---------------- MACRO ----------------
    for c in cfg["query"]["companies"]:
        label = c["display_name"]
        search = build_company_search(c)
        print(f"[macro] {label}")
        articles = fetch_thenewsapi(cfg, search, start, end)
        print(f"  fetched={len(articles)}")

        for a in articles:
            url = a.get("url")
            if not url:
                continue
            h = sha1_hex(url)

            out = analyze_article(client, model, a)
            mi = out["market_impact"]

            cur.execute(
                f"""
                INSERT IGNORE INTO `{MYSQL_TABLE}`
                (company, title, url, url_hash, published_at,
                 sentiment, confidence, summary,
                 direction, horizon, rationale)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    label,
                    a.get("title"),
                    url,
                    h,
                    a.get("published_at"),
                    out["sentiment"],
                    out["confidence"],
                    out["summary"],
                    mi["direction"],
                    mi["time_horizon"],
                    mi["rationale"],
                ),
            )

    # ---------------- TICKERS ----------------
    for t in cfg["tickers"]["track"]:
        ticker = normalize_ticker(t["ticker"])
        name = t["name"]
        search = build_ticker_search(ticker, name, t["keywords"])

        print(f"[ticker] {ticker} ({name})")
        articles = fetch_thenewsapi(cfg, search, start, end)
        print(f"  fetched={len(articles)}")

        for a in articles:
            url = a.get("url")
            if not url:
                continue
            h = sha1_hex(url)

            out = analyze_article(client, model, a)
            mi = out["market_impact"]

            cur.execute(
                f"""
                INSERT IGNORE INTO `{MYSQL_TABLE}`
                (company, title, url, url_hash, published_at,
                 sentiment, confidence, summary,
                 direction, horizon, rationale)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    ticker,
                    a.get("title"),
                    url,
                    h,
                    a.get("published_at"),
                    out["sentiment"],
                    out["confidence"],
                    out["summary"],
                    mi["direction"],
                    mi["time_horizon"],
                    mi["rationale"],
                ),
            )

    cur.close()
    cnx.close()
    print("DONE")

# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    run_once()
