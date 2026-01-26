#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import json
import time
import hashlib
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from openai import OpenAI
import mysql.connector

# ============================================================
# CONFIG
# ============================================================

THENEWSAPI_TOKEN = os.getenv("THENEWSAPI_TOKEN", "")
if not THENEWSAPI_TOKEN:
    # You hardcoded a token in your original snippet; keeping env-first is safer.
    # If you still want fallback, set it here.
    THENEWSAPI_TOKEN = os.getenv("NEWS_API_TOKEN", "")

# MySQL "app login"
MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "B612b612@")
MYSQL_DB = os.getenv("MYSQL_DB", "LLM")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "news_llm_analysis")

# repo root heuristic
ROOT = Path(__file__).resolve().parents[1]

# IMPORTANT: repo has json/news.json (per your git output)
DEFAULT_CONFIG_CANDIDATES = [
    Path(os.getenv("NEWS_JSON", "")) if os.getenv("NEWS_JSON") else None,
    ROOT / "json" / "news.json",
    ROOT / "news_llm" / "json" / "news.json",
]
DEFAULT_CONFIG_CANDIDATES = [p for p in DEFAULT_CONFIG_CANDIDATES if p and str(p) != ""]

# ============================================================
# TIME HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def thenewsapi_dt(dt: datetime) -> str:
    """
    FIX #1 (400 Bad Request):
    TheNewsAPI commonly expects UTC timestamps WITHOUT trailing 'Z' in query params.
    Example: 2026-01-26T19:00:37  (UTC assumed)
    """
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

def build_ticker_search(ticker: str, name: Optional[str]) -> str:
    """
    Note: TheNewsAPI supports boolean-ish search, but syntax can be finicky.
    Keep your original intent: include name + ticker + base ticker.
    Use OR terms; avoid insane length.
    """
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

    # Use OR (your original)
    return " OR ".join(terms)

def build_company_search(company_obj: dict) -> str:
    kws = [k.strip() for k in (company_obj.get("keywords") or []) if k and str(k).strip()]
    if not kws:
        name = (company_obj.get("display_name") or company_obj.get("id") or "").strip()
        kws = [name] if name else []
    parts = [f"\"{k}\"" if " " in k else k for k in kws]
    # Use | (your original)
    return " | ".join(parts)

def safe_parse_published_at(published_at_val: Any) -> Optional[datetime]:
    """
    TheNewsAPI often returns published_at like: 2026-01-26T18:37:13Z or with microseconds.
    We store DATETIME in MySQL (naive UTC).
    """
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
    raise FileNotFoundError(
        "Could not find news.json. Tried: "
        + ", ".join(str(p) for p in DEFAULT_CONFIG_CANDIDATES)
    )

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
    return cur.fetchone() is not None

def _column_exists(cur, table: str, col: str) -> bool:
    cur.execute(f"SHOW COLUMNS FROM `{table}` LIKE %s", (col,))
    return cur.fetchone() is not None

def ensure_table():
    """
    Keeps your original schema, but applies FIX #2:
      - allow same URL to be stored for multiple companies/tickers
      - do this by adding url_hash and unique(company, url_hash)
      - drop uniq_url if it exists (otherwise you can never store per-ticker rows)
    """
    cnx = mysql_connect()
    cur = cnx.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()

    # Create table if missing (original schema + url_hash)
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

    # If table existed before, ensure url_hash exists
    if not _column_exists(cur, MYSQL_TABLE, "url_hash"):
        cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN url_hash VARCHAR(40) NULL AFTER url")

    # Drop old uniq_url if present (this is what prevents per-ticker storage)
    if _index_exists(cur, MYSQL_TABLE, "uniq_url"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` DROP INDEX uniq_url")
        except Exception:
            # If it fails, we continue; but you will not get per-ticker rows until removed.
            pass

    # Add new unique index on (company, url_hash)
    if not _index_exists(cur, MYSQL_TABLE, "uniq_company_urlhash"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD UNIQUE KEY uniq_company_urlhash (company(191), url_hash)")
        except Exception:
            pass

    cur.close()
    cnx.close()

def insert_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()

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

# ============================================================
# TheNewsAPI
# ============================================================

def _resolve_thenewsapi_endpoint(cfg: dict) -> str:
    # Backward compat: cfg.thenewsapi.endpoint
    api_cfg = (cfg.get("thenewsapi") or {})
    endpoint = (api_cfg.get("endpoint") or "").strip().lower()
    if endpoint in ("top", "all"):
        return endpoint

    # New config: cfg.query.mode
    qmode = ((cfg.get("query") or {}).get("mode") or "").strip().lower()
    if qmode in ("everything", "all"):
        return "all"
    return "top"

def _resolve_domains(cfg: dict) -> Tuple[List[str], List[str]]:
    # Backward compat: cfg.thenewsapi.domains.include/exclude
    api_cfg = (cfg.get("thenewsapi") or {})
    dom = api_cfg.get("domains") or {}
    inc = dom.get("include") or []
    exc = dom.get("exclude") or []

    # New config: cfg.sources.include/exclude
    src = cfg.get("sources") or {}
    if (src.get("mode") or "").strip().lower() == "domains":
        inc = src.get("include") or inc
        exc = src.get("exclude") or exc

    return list(inc), list(exc)

def fetch_thenewsapi(cfg: dict, search: str, after: datetime, before: datetime) -> List[Dict[str, Any]]:
    endpoint = _resolve_thenewsapi_endpoint(cfg)
    base = f"https://api.thenewsapi.com/v1/news/{endpoint}"

    polling = (cfg.get("polling") or {})
    language = polling.get("language", "en")

    # Your new JSON uses page_size; old code used limit
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

    # Optional: search_fields from old cfg.thenewsapi.search_fields
    api_cfg = (cfg.get("thenewsapi") or {})
    if api_cfg.get("search_fields"):
        params["search_fields"] = api_cfg["search_fields"]

    # Optional: locale from old config
    if api_cfg.get("locale"):
        params["locale"] = ",".join(api_cfg["locale"])

    include_domains, exclude_domains = _resolve_domains(cfg)
    if include_domains:
        params["domains"] = ",".join(include_domains)
    if exclude_domains:
        params["exclude_domains"] = ",".join(exclude_domains)

    # Optional categories from old config
    cats = api_cfg.get("categories") or {}
    if cats.get("include"):
        params["categories"] = ",".join(cats["include"])
    if cats.get("exclude"):
        params["exclude_categories"] = ",".join(cats["exclude"])

    out: List[Dict[str, Any]] = []
    while True:
        r = requests.get(base, params=params, timeout=30)
        if r.status_code >= 400:
            # print server message; this helps with 400 debugging
            raise requests.HTTPError(f"{r.status_code} {r.reason}: {r.text[:800]}", response=r)
        data = r.json() or {}
        items = data.get("data") or []
        out.extend(items if isinstance(items, list) else [])

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

def analyze_article(client: OpenAI, model: str, article: dict) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    t0 = time.time()
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": build_prompt(article)}],
        temperature=0.2,
        max_tokens=650,
        stream=False,
    )
    elapsed = time.time() - t0
    text = (resp.choices[0].message.content or "").strip()
    parsed = extract_json_object(text)

    usage = getattr(resp, "usage", None)
    perf = {
        "elapsed_s": round(elapsed, 3),
        "prompt_tokens": getattr(usage, "prompt_tokens", None) if usage else None,
        "completion_tokens": getattr(usage, "completion_tokens", None) if usage else None,
        "total_tokens": getattr(usage, "total_tokens", None) if usage else None,
    }
    return parsed, perf

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

def build_runtime(cfg: dict) -> dict:
    polling = cfg.get("polling", {}) or {}
    lookback_minutes = int(polling.get("lookback_minutes", 30))

    model_cfg = cfg.get("model", {}) or {}
    ollama_host = model_cfg.get("host")
    if not ollama_host:
        raise RuntimeError("news.json missing model.host")

    preferred_model = model_cfg.get("preferred_model", "")
    fallback_idx = int(model_cfg.get("fallback_model_index", 0))

    # macro companies
    companies = (cfg.get("query") or {}).get("companies") or []

    # tickers (new config)
    tickers_cfg = cfg.get("tickers") or {}
    tickers_mode = (tickers_cfg.get("mode") or "").strip().lower()
    tickers_track = tickers_cfg.get("track") or []
    tickers: List[Dict[str, str]] = []
    if tickers_mode == "explicit" and isinstance(tickers_track, list):
        for t in tickers_track:
            if not isinstance(t, dict):
                continue
            sym = normalize_ticker(t.get("ticker"))
            if not sym:
                continue
            nm = strip_or_none(t.get("name")) or sym
            tickers.append({"ticker": sym, "name": nm})

    if not companies and not tickers:
        raise RuntimeError("news.json must include query.companies and/or tickers.track (mode=explicit).")

    ensure_table()

    models = list_ollama_models(ollama_host)
    chosen_model = pick_model(models, preferred_model, fallback_idx)
    client = OpenAI(base_url=f"{ollama_host}/v1", api_key="ollama")

    return {
        "cfg": cfg,
        "client": client,
        "model": chosen_model,
        "companies": companies,
        "tickers": tickers,
        # dedupe ONLY for avoiding repeated LLM calls in same run:
        # key is (company, url_hash)
        "seen": set(),
        "last_poll": utc_now() - timedelta(minutes=lookback_minutes),
        "cycle": 0,
    }

_STATE: Optional[dict] = None

def run_once() -> Tuple[int, datetime]:
    global _STATE

    if not THENEWSAPI_TOKEN:
        raise RuntimeError("Set THENEWSAPI_TOKEN env var.")
    if not MYSQL_PASSWORD:
        raise RuntimeError("Set MYSQL_PASSWORD env var.")

    if _STATE is None:
        cfg = load_config()
        _STATE = build_runtime(cfg)
        print(f"[news_api] loaded_config={_STATE['cfg'].get('_loaded_from')}")
        print(f"[news_api] tickers_loaded={len(_STATE['tickers'])} companies_loaded={len(_STATE['companies'])}")
        print(f"[news_api] model={_STATE['model']}")

    _STATE["cycle"] += 1
    cfg = _STATE["cfg"]
    client: OpenAI = _STATE["client"]
    model: str = _STATE["model"]

    companies = _STATE["companies"]
    tickers = _STATE["tickers"]
    seen = _STATE["seen"]
    last_poll = _STATE["last_poll"]

    poll_end = utc_now()
    batch: List[Dict[str, Any]] = []

    # A) Macro/company queries
    for c in companies:
        company_name = (c.get("display_name") or c.get("id") or "UNKNOWN").strip()
        search = build_company_search(c)
        if not search:
            continue

        articles = fetch_thenewsapi(cfg, search, last_poll, poll_end)
        for a in articles:
            url = (a.get("url") or "").strip()
            if not url:
                continue

            url_hash = sha1_hex(url)
            seen_key = (company_name, url_hash)
            if seen_key in seen:
                continue
            seen.add(seen_key)

            try:
                analysis, perf = analyze_article(client, model, a)
                batch.append({
                    "cycle": _STATE["cycle"],
                    "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),
                    "company": company_name,

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

                    "sentiment": analysis.get("sentiment"),
                    "confidence_0_to_100": analysis.get("confidence_0_to_100"),
                    "one_sentence_summary": analysis.get("one_sentence_summary"),

                    "market_direction": (analysis.get("market_impact") or {}).get("direction"),
                    "market_time_horizon": (analysis.get("market_impact") or {}).get("time_horizon"),
                    "market_rationale": (analysis.get("market_impact") or {}).get("rationale"),

                    "elapsed_s": perf.get("elapsed_s"),
                    "prompt_tokens": perf.get("prompt_tokens"),
                    "completion_tokens": perf.get("completion_tokens"),
                    "total_tokens": perf.get("total_tokens"),

                    "analysis_json": json.dumps(analysis, ensure_ascii=False),
                    "error": None,
                })
            except Exception as e:
                batch.append({
                    "cycle": _STATE["cycle"],
                    "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),
                    "company": company_name,
                    "uuid": a.get("uuid"),
                    "source": a.get("source"),
                    "published_at": safe_parse_published_at(a.get("published_at")),
                    "title": a.get("title"),
                    "url": url,
                    "url_hash": url_hash,
                    "error": f"analysis_error: {type(e).__name__}: {e}",
                })

    # B) Ticker queries (company=ticker)
    for t in tickers:
        ticker = normalize_ticker(t.get("ticker"))
        name = strip_or_none(t.get("name")) or ticker
        if not ticker:
            continue

        search = build_ticker_search(ticker, name)
        if not search:
            continue

        articles = fetch_thenewsapi(cfg, search, last_poll, poll_end)
        for a in articles:
            url = (a.get("url") or "").strip()
            if not url:
                continue

            url_hash = sha1_hex(url)
            seen_key = (ticker, url_hash)
            if seen_key in seen:
                continue
            seen.add(seen_key)

            try:
                analysis, perf = analyze_article(client, model, a)
                batch.append({
                    "cycle": _STATE["cycle"],
                    "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),
                    "company": ticker,  # IMPORTANT for ai.py joins

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

                    "sentiment": analysis.get("sentiment"),
                    "confidence_0_to_100": analysis.get("confidence_0_to_100"),
                    "one_sentence_summary": analysis.get("one_sentence_summary"),

                    "market_direction": (analysis.get("market_impact") or {}).get("direction"),
                    "market_time_horizon": (analysis.get("market_impact") or {}).get("time_horizon"),
                    "market_rationale": (analysis.get("market_impact") or {}).get("rationale"),

                    "elapsed_s": perf.get("elapsed_s"),
                    "prompt_tokens": perf.get("prompt_tokens"),
                    "completion_tokens": perf.get("completion_tokens"),
                    "total_tokens": perf.get("total_tokens"),

                    "analysis_json": json.dumps(analysis, ensure_ascii=False),
                    "error": None,
                })
            except Exception as e:
                batch.append({
                    "cycle": _STATE["cycle"],
                    "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),
                    "company": ticker,
                    "uuid": a.get("uuid"),
                    "source": a.get("source"),
                    "published_at": safe_parse_published_at(a.get("published_at")),
                    "title": a.get("title"),
                    "url": url,
                    "url_hash": url_hash,
                    "error": f"analysis_error(ticker): {type(e).__name__}: {e}",
                })

    if batch:
        insert_rows(batch)

    _STATE["last_poll"] = poll_end
    return len(batch), poll_end

def run_forever():
    cfg = load_config()
    polling = cfg.get("polling", {}) or {}
    interval_seconds = int(polling.get("interval_seconds", 60))

    global _STATE
    _STATE = build_runtime(cfg)
    print(f"[news_api] loaded_config={_STATE['cfg'].get('_loaded_from')}")
    print(f"[news_api] tickers_loaded={len(_STATE['tickers'])} companies_loaded={len(_STATE['companies'])}")
    print(f"[news_api] model={_STATE['model']}")

    while True:
        inserted, poll_end = run_once()
        print(f"[{iso_z(poll_end)}] inserted={inserted} -> {MYSQL_DB}.{MYSQL_TABLE}")
        time.sleep(interval_seconds)

def main():
    inserted, poll_end = run_once()
    print(f"[{iso_z(poll_end)}] inserted={inserted} -> {MYSQL_DB}.{MYSQL_TABLE}")

if __name__ == "__main__":
    main()
