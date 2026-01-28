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
    """Clean list: strip, drop blanks, dedupe preserving order."""
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
    if kws:
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

def normalize_horizon(h: Any) -> str:
    s = (str(h or "")).strip().lower()
    if s in {"intraday", "days", "weeks", "months", "unclear"}:
        return s
    if not s:
        return "unclear"
    if any(x in s for x in ["today", "hour", "hours", "session", "intraday", "same day"]):
        return "intraday"
    if any(x in s for x in ["day", "days", "this week", "48h", "72h"]):
        return "days"
    if any(x in s for x in ["week", "weeks", "1-4w", "couple of weeks"]):
        return "weeks"
    if any(x in s for x in ["month", "months", "quarter", "quarters", "6m", "12m", "year"]):
        return "months"
    return "unclear"

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
    return len(cur.fetchall()) > 0

def _column_exists(cur, table: str, col: str) -> bool:
    cur.execute(f"SHOW COLUMNS FROM `{table}` LIKE %s", (col,))
    return len(cur.fetchall()) > 0

def ensure_table():
    cnx = mysql_connect()
    cur = cnx.cursor(buffered=True)
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

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
      market_time_horizon TEXT NULL,
      market_rationale TEXT NULL,

      important_keywords JSON NULL,

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
        print("[ensure_table] added column url_hash")

    # Ensure important_keywords exists (for older tables)
    if not _column_exists(cur, MYSQL_TABLE, "important_keywords"):
        cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN important_keywords JSON NULL AFTER market_rationale")
        print("[ensure_table] added column important_keywords")

    # Drop old uniq_url if present
    if _index_exists(cur, MYSQL_TABLE, "uniq_url"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` DROP INDEX uniq_url")
            print("[ensure_table] dropped index uniq_url")
        except Exception as e:
            print(f"[ensure_table] WARNING: failed to drop uniq_url: {type(e).__name__}: {e}")

    # Add unique(company,url_hash)
    if not _index_exists(cur, MYSQL_TABLE, "uniq_company_urlhash"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD UNIQUE KEY uniq_company_urlhash (company(191), url_hash)")
            print("[ensure_table] added index uniq_company_urlhash (company(191), url_hash)")
        except Exception as e:
            print(f"[ensure_table] WARNING: failed to add uniq_company_urlhash: {type(e).__name__}: {e}")

    cur.close()
    cnx.close()
    print("[ensure_table] done")

def insert_rows(rows: List[Dict[str, Any]]) -> None:
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
      important_keywords,
      elapsed_s, prompt_tokens, completion_tokens, total_tokens,
      analysis_json, error
    ) VALUES (
      %s,%s,%s,
      %s,%s,%s,%s,%s,%s,%s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,
      %s,
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
      important_keywords=VALUES(important_keywords),
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
            r.get("important_keywords"),
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

    include_domains, exclude_domains = _resolve_domains(cfg)
    if include_domains:
        params["domains"] = ",".join(include_domains)
    if exclude_domains:
        params["exclude_domains"] = ",".join(exclude_domains)

    out: List[Dict[str, Any]] = []
    while True:
        r = requests.get(base, params=params, timeout=30)
        if r.status_code >= 400:
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
# LLM (SUMMARY + IMPORTANT KEYWORDS IN JSON)
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
    """
    NEW: output includes important_keywords array (5-12 items).
    Force enum-only time_horizon.
    """
    schema = {
        "sentiment": "positive|neutral|negative",
        "confidence_0_to_100": 0,
        "one_sentence_summary": "string (1 sentence, <=25 words)",
        "important_keywords": ["string", "string"],
        "market_impact": {
            "direction": "up|down|mixed|unclear",
            "time_horizon": "intraday|days|weeks|months|unclear",
            "rationale": "string (1-2 sentences)"
        }
    }

    return (
        "Return ONLY valid JSON. No markdown, no extra text.\n"
        "Use ONLY the provided article fields. Do not invent facts.\n"
        "IMPORTANT KEYWORDS: Provide 5 to 12 short keywords/phrases.\n"
        "  - Prefer named entities, tickers, regions, policy items, macro indicators.\n"
        "  - No duplicates. No full sentences.\n"
        "TIME HORIZON RULE: time_horizon MUST be exactly one of: intraday, days, weeks, months, unclear.\n\n"
        f"Schema:\n{json.dumps(schema, indent=2)}\n\n"
        f"Article:\n{json.dumps(article, ensure_ascii=False)}"
    )

def analyze_article(client: OpenAI, model: str, article: dict) -> Tuple[Dict[str, Any], Dict[str, Any], str]:
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
    return parsed, perf, text

def normalize_keywords_list(kws: Any, max_n: int = 12) -> List[str]:
    kws2 = _clean_keywords(kws if isinstance(kws, list) else [])
    return kws2[:max_n]

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
    macro_total = 0
    print("\n[keywords] companies:")
    for c in companies:
        name = (c.get("display_name") or c.get("id") or "UNKNOWN").strip()
        kws = _clean_keywords(c.get("keywords") or [])
        macro_total += len(kws)
        preview = ", ".join(kws[:12])
        suffix = " ..." if len(kws) > 12 else ""
        print(f"  - {name}: {len(kws)} keywords -> {preview}{suffix}")
    print(f"[keywords] companies_total_keywords={macro_total}")

    ticker_total = 0
    ticker_with_kws = 0
    print("\n[keywords] tickers:")
    for t in tickers:
        sym = t.get("ticker") or ""
        nm = t.get("name") or sym
        kws = _clean_keywords(t.get("keywords") or [])
        ticker_total += len(kws)
        if kws:
            ticker_with_kws += 1
        preview = ", ".join(kws[:10])
        suffix = " ..." if len(kws) > 10 else ""
        print(f"  - {sym} ({nm}): {len(kws)} keywords -> {preview}{suffix}")
    print(f"[keywords] tickers_with_keywords={ticker_with_kws}/{len(tickers)} total_ticker_keywords={ticker_total}\n")

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

    return {
        "cfg": cfg,
        "client": client,
        "model": chosen_model,
        "companies": companies,
        "tickers": tickers,
        "seen": set(),  # (company, url_hash)
        "last_poll": utc_now() - timedelta(minutes=lookback_minutes),
        "cycle": 0,
    }

_STATE: Optional[dict] = None

def run_once() -> Tuple[int, datetime]:
    global _STATE

    if not THENEWSAPI_TOKEN:
        raise RuntimeError("Set THENEWSAPI_TOKEN or NEWS_API_TOKEN env var.")
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
            key = (company_name, url_hash)
            if key in seen:
                continue
            seen.add(key)

            try:
                analysis, perf, raw_text = analyze_article(client, model, a)
                mi = (analysis.get("market_impact") or {})
                imp_kws = normalize_keywords_list(analysis.get("important_keywords"))

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

                    "sentiment": clamp_str(analysis.get("sentiment"), 16),
                    "confidence_0_to_100": analysis.get("confidence_0_to_100"),
                    "one_sentence_summary": analysis.get("one_sentence_summary"),

                    "market_direction": clamp_str(mi.get("direction"), 16),
                    "market_time_horizon": normalize_horizon(mi.get("time_horizon")),
                    "market_rationale": mi.get("rationale"),

                    "important_keywords": json.dumps(imp_kws, ensure_ascii=False),

                    "elapsed_s": perf.get("elapsed_s"),
                    "prompt_tokens": perf.get("prompt_tokens"),
                    "completion_tokens": perf.get("completion_tokens"),
                    "total_tokens": perf.get("total_tokens"),

                    "analysis_json": json.dumps(analysis, ensure_ascii=False),
                    "error": None,
                })

                # Optional: display on console
                if imp_kws:
                    print(f"[kw] {company_name}: {', '.join(imp_kws)}")

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

    # B) Ticker queries
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
            url = (a.get("url") or "").strip()
            if not url:
                continue

            url_hash = sha1_hex(url)
            key = (ticker, url_hash)
            if key in seen:
                continue
            seen.add(key)

            try:
                analysis, perf, raw_text = analyze_article(client, model, a)
                mi = (analysis.get("market_impact") or {})
                imp_kws = normalize_keywords_list(analysis.get("important_keywords"))

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
                    "market_time_horizon": normalize_horizon(mi.get("time_horizon")),
                    "market_rationale": mi.get("rationale"),

                    "important_keywords": json.dumps(imp_kws, ensure_ascii=False),

                    "elapsed_s": perf.get("elapsed_s"),
                    "prompt_tokens": perf.get("prompt_tokens"),
                    "completion_tokens": perf.get("completion_tokens"),
                    "total_tokens": perf.get("total_tokens"),

                    "analysis_json": json.dumps(analysis, ensure_ascii=False),
                    "error": None,
                })

                # Optional: display on console
                if imp_kws:
                    print(f"[kw] {ticker}: {', '.join(imp_kws)}")

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

    while True:
        inserted, poll_end = run_once()
        print(f"[{iso_z(poll_end)}] inserted={inserted} -> {MYSQL_DB}.{MYSQL_TABLE}")
        time.sleep(interval_seconds)

def main():
    inserted, poll_end = run_once()
    print(f"[{iso_z(poll_end)}] inserted={inserted} -> {MYSQL_DB}.{MYSQL_TABLE}")

if __name__ == "__main__":
    main()
