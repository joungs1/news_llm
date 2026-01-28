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

THENEWSAPI_TOKEN = os.getenv("THENEWSAPI_TOKEN") or os.getenv("NEWS_API_TOKEN") or "GLW7gjLDEnhMk0iA2bOLz5ZrFwANg1ZXlunXaR2e"
MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD","B612b612@")
MYSQL_DB = os.getenv("MYSQL_DB", "LLM")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "news_llm_analysis")
ROOT = Path(__file__).resolve().parents[0]
DEFAULT_CONFIG_CANDIDATES = [
    Path(os.getenv("NEWS_JSON", "")) if os.getenv("NEWS_JSON") else None,
    ROOT / "news.json",
    ROOT / "json" / "news.json",
    ROOT / "news_llm" / "json" / "news.json",
]
DEFAULT_CONFIG_CANDIDATES = [p for p in DEFAULT_CONFIG_CANDIDATES if p and str(p).strip()]

# TheNewsAPI search guardrails
MAX_TICKER_KWS = int(os.getenv("NEWS_MAX_TICKER_KWS", "8"))      # keep small to avoid query blow-up
MAX_COMPANY_KWS = int(os.getenv("NEWS_MAX_COMPANY_KWS", "20"))
MAX_SEARCH_CHARS = int(os.getenv("NEWS_MAX_SEARCH_CHARS", "900"))  # defensive cap

# LLM guardrails
LLM_MAX_TOKENS = int(os.getenv("NEWS_LLM_MAX_TOKENS", "650"))
LLM_RETRIES = int(os.getenv("NEWS_LLM_RETRIES", "2"))
LLM_TIMEOUT_S = int(os.getenv("NEWS_LLM_TIMEOUT_S", "60"))

# ============================================================
# TIME / HASH
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def thenewsapi_dt(dt: datetime) -> str:
    # TheNewsAPI expects UTC; docs show ISO-like.
    # We send "YYYY-MM-DDTHH:MM:SS"
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

# ============================================================
# TEXT HELPERS
# ============================================================

def normalize_ticker(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip().replace(" ", "")
    if not s:
        return ""
    # keep case as provided; tickers often include ".TO"
    s = s.replace("/", "-")
    if s.upper().endswith("-TO"):
        s = s[:-3] + ".TO"
    return s

def clean_keywords(kws: Any) -> List[str]:
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

def quote_term(t: str) -> str:
    t = t.strip()
    if not t:
        return ""
    # TheNewsAPI supports quoted phrases :contentReference[oaicite:1]{index=1}
    if " " in t or ":" in t:
        return f"\"{t}\""
    return t

def clamp_search(s: str) -> str:
    s = (s or "").strip()
    if len(s) <= MAX_SEARCH_CHARS:
        return s
    # trim safely
    return s[:MAX_SEARCH_CHARS].rstrip(" |+(").strip()

# ============================================================
# SEARCH BUILDERS (IMPORTANT FIX)
# TheNewsAPI uses: + (AND), | (OR), - (NOT) :contentReference[oaicite:2]{index=2}
# ============================================================

def build_company_search(c: Dict[str, Any]) -> str:
    kws = clean_keywords(c.get("keywords"))[:MAX_COMPANY_KWS]
    if not kws:
        name = (c.get("display_name") or c.get("id") or "").strip()
        if name:
            kws = [name]
    # OR them with '|'
    parts = [quote_term(k) for k in kws if quote_term(k)]
    if not parts:
        return ""
    # single group is good practice
    q = "(" + " | ".join(parts) + ")"
    return clamp_search(q)

def build_ticker_search(ticker: str, name: str, keywords: List[str]) -> str:
    t = normalize_ticker(ticker)
    if not t:
        return ""
    base = t.split(".")[0] if "." in t else t

    terms: List[str] = []

    # Always include ticker variants
    terms.append(quote_term(t))
    if base and base != t:
        terms.append(quote_term(base))

    # Include name
    nm = (name or "").strip()
    if nm:
        terms.append(quote_term(nm))

    # Add a few keywords (cap hard)
    for k in clean_keywords(keywords)[:MAX_TICKER_KWS]:
        terms.append(quote_term(k))

    # De-dupe terms while preserving order
    seen = set()
    uniq = []
    for x in terms:
        if not x:
            continue
        if x.lower() in seen:
            continue
        seen.add(x.lower())
        uniq.append(x)

    if not uniq:
        return ""

    q = "(" + " | ".join(uniq) + ")"
    return clamp_search(q)

# ============================================================
# CONFIG LOAD
# ============================================================

def load_config() -> Dict[str, Any]:
    for p in DEFAULT_CONFIG_CANDIDATES:
        try:
            if p and p.exists():
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                cfg["_loaded_from"] = str(p)
                return cfg
        except Exception:
            continue
    raise FileNotFoundError("news.json not found. Tried: " + ", ".join(str(p) for p in DEFAULT_CONFIG_CANDIDATES))

# ============================================================
# MYSQL
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

def ensure_table() -> None:
    cnx = mysql_connect()
    cur = cnx.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

      company VARCHAR(255) NULL,
      title TEXT NULL,
      url TEXT NULL,
      url_hash VARCHAR(40) NULL,
      published_at VARCHAR(64) NULL,
      source VARCHAR(255) NULL,

      sentiment VARCHAR(16) NULL,
      confidence INT NULL,
      summary TEXT NULL,

      direction VARCHAR(16) NULL,
      horizon VARCHAR(32) NULL,
      rationale TEXT NULL,

      keywords_json JSON NULL,

      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_company_url (company(191), url_hash)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    cur.close()
    cnx.close()

# ============================================================
# THENEWSAPI
# ============================================================

def fetch_thenewsapi(cfg: Dict[str, Any], search: str, after: datetime, before: datetime) -> List[Dict[str, Any]]:
    endpoint = "all"
    api_cfg = (cfg.get("thenewsapi") or {})
    if (api_cfg.get("endpoint") or "").strip().lower() in {"top", "all"}:
        endpoint = api_cfg["endpoint"].strip().lower()

    url = f"https://api.thenewsapi.com/v1/news/{endpoint}"

    polling = cfg.get("polling", {}) or {}
    language = polling.get("language", "en")
    limit = int(polling.get("limit", 50))

    params: Dict[str, Any] = {
        "api_token": THENEWSAPI_TOKEN,
        "search": search,
        "language": language,
        "published_after": thenewsapi_dt(after),
        "published_before": thenewsapi_dt(before),
        "limit": limit,
        "page": 1,
    }

    # Domains include/exclude
    dom = (api_cfg.get("domains") or {})
    inc = dom.get("include") or []
    exc = dom.get("exclude") or []
    if inc:
        params["domains"] = ",".join(inc)
    if exc:
        params["exclude_domains"] = ",".join(exc)

    out: List[Dict[str, Any]] = []
    while True:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code >= 400:
            # print enough to debug malformed query
            raise RuntimeError(f"TheNewsAPI {r.status_code}: {r.text[:800]} (search={search!r})")
        j = r.json() or {}
        items = j.get("data") or []
        if isinstance(items, list):
            out.extend(items)

        meta = j.get("meta") or {}
        next_page = meta.get("next_page")
        if not next_page:
            break
        params["page"] = int(next_page)
        if len(out) >= 5000:
            break

    return out

# ============================================================
# LLM JSON EXTRACTION (robust)
# ============================================================

def extract_json_object(text: str) -> Dict[str, Any]:
    if not text or not text.strip():
        raise ValueError("Empty LLM response text")

    raw = text.strip()

    # allow fenced block
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", raw)
        raw = re.sub(r"\s*```\s*$", "", raw).strip()

    # try direct
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # find first full object
    start = raw.find("{")
    if start < 0:
        raise ValueError("No '{' found in LLM output")

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
                    candidate = raw[start:i+1]
                    obj = json.loads(candidate)
                    if not isinstance(obj, dict):
                        raise ValueError("Top-level JSON is not an object")
                    return obj

    raise ValueError("Could not extract complete JSON object from LLM output")

def build_prompt(article: Dict[str, Any]) -> str:
    schema = {
        "sentiment": "positive|neutral|negative",
        "confidence": "0-100 integer",
        "summary": "one sentence",
        "keywords": ["string", "string"],
        "market_impact": {
            "direction": "up|down|mixed|unclear",
            "time_horizon": "intraday|days|weeks|months|unclear",
            "rationale": "string"
        }
    }

    return (
        "Return ONLY valid JSON. No markdown.\n"
        "Use ONLY the provided article fields. Do not invent facts.\n\n"
        f"Schema:\n{json.dumps(schema, indent=2)}\n\n"
        f"Article:\n{json.dumps(article, ensure_ascii=False)}"
    )

def analyze_article(client: OpenAI, model: str, article: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    last_err: Optional[Exception] = None
    for attempt in range(LLM_RETRIES + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": build_prompt(article)}],
                temperature=0.2,
                max_tokens=LLM_MAX_TOKENS,
                stream=False,
                timeout=LLM_TIMEOUT_S,
            )
            raw_text = (resp.choices[0].message.content or "").strip()
            parsed = extract_json_object(raw_text)
            return parsed, raw_text
        except Exception as e:
            last_err = e
            # small backoff for transient 500s
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"LLM failed after retries: {type(last_err).__name__}: {last_err}")

# ============================================================
# PRINT LOADED KEYWORDS
# ============================================================

def print_loaded_keywords(cfg: Dict[str, Any]) -> None:
    companies = (cfg.get("query") or {}).get("companies") or []
    tickers = (cfg.get("tickers") or {}).get("track") or []

    print("\n[keywords_loaded] companies:")
    for c in companies:
        name = (c.get("display_name") or c.get("id") or "UNKNOWN").strip()
        kws = clean_keywords(c.get("keywords"))[:MAX_COMPANY_KWS]
        preview = ", ".join(kws[:12])
        suffix = " ..." if len(kws) > 12 else ""
        print(f"  - {name}: {len(kws)} keywords -> {preview}{suffix}")

    print("\n[keywords_loaded] tickers:")
    for t in tickers:
        sym = normalize_ticker(t.get("ticker"))
        nm = (t.get("name") or sym or "UNKNOWN").strip()
        kws = clean_keywords(t.get("keywords"))[:MAX_TICKER_KWS]
        preview = ", ".join(kws[:10])
        suffix = " ..." if len(kws) > 10 else ""
        print(f"  - {sym} ({nm}): {len(kws)} keywords -> {preview}{suffix}")
    print("")

# ============================================================
# MAIN
# ============================================================

def run_once() -> None:
    if not THENEWSAPI_TOKEN:
        raise RuntimeError("Missing THENEWSAPI_TOKEN / NEWS_API_TOKEN")
    if not MYSQL_PASSWORD:
        raise RuntimeError("Missing MYSQL_PASSWORD")

    cfg = load_config()
    print(f"[news_api] loaded_config={cfg.get('_loaded_from')}")

    ensure_table()

    # Ollama/OpenAI-compatible
    ollama_host = (cfg.get("model") or {}).get("host")
    if not ollama_host:
        raise RuntimeError("news.json missing model.host")
    model = (cfg.get("model") or {}).get("preferred_model") or "llama3"

    client = OpenAI(base_url=f"{ollama_host}/v1", api_key="ollama")

    polling = cfg.get("polling", {}) or {}
    lookback_min = int(polling.get("lookback_minutes", 1440))
    start = utc_now() - timedelta(minutes=lookback_min)
    end = utc_now()

    print_loaded_keywords(cfg)

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()

    # ---------------- MACRO ----------------
    companies = (cfg.get("query") or {}).get("companies") or []
    for c in companies:
        label = (c.get("display_name") or c.get("id") or "UNKNOWN").strip()
        search = build_company_search(c)
        if not search:
            print(f"[macro] {label}: SKIP (no search)")
            continue

        print(f"[macro] {label}")
        print(f"  search={search}")
        articles = fetch_thenewsapi(cfg, search, start, end)
        print(f"  fetched={len(articles)}")

        for a in articles:
            url = (a.get("url") or "").strip()
            if not url:
                continue
            h = sha1_hex(url)

            out, _raw = analyze_article(client, model, a)
            mi = out.get("market_impact") or {}

            cur.execute(
                f"""
                INSERT IGNORE INTO `{MYSQL_TABLE}`
                (company, title, url, url_hash, published_at, source,
                 sentiment, confidence, summary,
                 direction, horizon, rationale,
                 keywords_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    label,
                    a.get("title"),
                    url,
                    h,
                    a.get("published_at"),
                    a.get("source"),
                    out.get("sentiment"),
                    out.get("confidence"),
                    out.get("summary"),
                    mi.get("direction"),
                    mi.get("time_horizon"),
                    mi.get("rationale"),
                    json.dumps(out.get("keywords") or [], ensure_ascii=False),
                ),
            )

    # ---------------- TICKERS ----------------
    tickers = (cfg.get("tickers") or {}).get("track") or []
    for t in tickers:
        ticker = normalize_ticker(t.get("ticker"))
        name = (t.get("name") or ticker).strip()
        keywords = t.get("keywords") or []
        if not ticker:
            continue

        search = build_ticker_search(ticker, name, keywords)
        if not search:
            print(f"[ticker] {ticker} ({name}): SKIP (no search)")
            continue

        print(f"[ticker] {ticker} ({name})")
        print(f"  search={search}")
        articles = fetch_thenewsapi(cfg, search, start, end)
        print(f"  fetched={len(articles)}")

        for a in articles:
            url = (a.get("url") or "").strip()
            if not url:
                continue
            h = sha1_hex(url)

            out, _raw = analyze_article(client, model, a)
            mi = out.get("market_impact") or {}

            cur.execute(
                f"""
                INSERT IGNORE INTO `{MYSQL_TABLE}`
                (company, title, url, url_hash, published_at, source,
                 sentiment, confidence, summary,
                 direction, horizon, rationale,
                 keywords_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    ticker,  # << label by ticker
                    a.get("title"),
                    url,
                    h,
                    a.get("published_at"),
                    a.get("source"),
                    out.get("sentiment"),
                    out.get("confidence"),
                    out.get("summary"),
                    mi.get("direction"),
                    mi.get("time_horizon"),
                    mi.get("rationale"),
                    json.dumps(out.get("keywords") or [], ensure_ascii=False),
                ),
            )

    cur.close()
    cnx.close()
    print("DONE")

if __name__ == "__main__":
    run_once()
