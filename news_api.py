#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import json
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from openai import OpenAI
import mysql.connector

# ============================================================
# CONFIG
# ============================================================

THENEWSAPI_TOKEN = os.getenv("THENEWSAPI_TOKEN", "GLW7gjLDEnhMk0iA2bOLz5ZrFwANg1ZXlunXaR2e")

# Repo root (adjust if your layout differs; parents[0] = folder containing this file)
ROOT = Path(__file__).resolve().parents[1]  # repo root
CONFIG_PATH = ROOT / "news_llm" / "json" / "news.json"

# MySQL "app login"
MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "B612b612@")
MYSQL_DB = os.getenv("MYSQL_DB", "LLM")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "news_llm_analysis")


# ----------------------------
# Time helpers
# ----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def thenewsapi_dt(dt: datetime) -> str:
    # TheNewsAPI accepts ISO-like UTC timestamps; keep Z suffix.
    return iso_z(dt)

def strip_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


# ----------------------------
# Watchlist/ticker helpers (NEW)
# ----------------------------
def normalize_ticker(raw: Any) -> str:
    """
    Normalizes a ticker coming from news.json tickers.track:
      - trim, remove spaces
      - convert "-TO" to ".TO"
    """
    if raw is None:
        return ""
    s = str(raw).strip().replace(" ", "")
    if not s or s.lower() in {"none", "nan"}:
        return ""
    s = s.replace("/", "-")
    if s.upper().endswith("-TO"):
        s = s[:-3] + ".TO"
    return s

def build_ticker_search(ticker: str, name: Optional[str]) -> str:
    """
    Builds a TheNewsAPI 'search' expression for a single name/ticker.
    We include both plain and TSX-style tickers because publishers vary.
    """
    t = normalize_ticker(ticker)
    if not t:
        return ""

    # Also include the base symbol without suffix (RY.TO -> RY)
    base = t.split(".")[0] if "." in t else t

    terms: List[str] = []
    if name:
        nm = str(name).strip()
        if nm:
            # Phrase-match for company name, plus a non-phrase token as fallback
            terms.append(f"\"{nm}\"")

    # Include both ticker variants
    terms.append(f"\"{t}\"")
    if base != t:
        terms.append(f"\"{base}\"")

    # A simple OR query is generally more reliable than clever boolean syntax
    return " OR ".join(terms)


# ----------------------------
# Company search builder (existing)
# ----------------------------
def build_company_search(company_obj: dict) -> str:
    kws = [k.strip() for k in (company_obj.get("keywords") or []) if k and k.strip()]
    if not kws:
        name = (company_obj.get("display_name") or company_obj.get("id") or "").strip()
        kws = [name] if name else []
    parts = [f"\"{k}\"" if " " in k else k for k in kws]
    return " | ".join(parts)


# ----------------------------
# TheNewsAPI fetch
# ----------------------------
def fetch_thenewsapi(cfg: dict, search: str, published_after: datetime, published_before: datetime) -> list[dict]:
    api_cfg = cfg.get("thenewsapi", {}) or {}
    endpoint = (api_cfg.get("endpoint") or "top").strip().lower()
    if endpoint not in ("top", "all"):
        raise ValueError("thenewsapi.endpoint must be 'top' or 'all'")

    base = f"https://api.thenewsapi.com/v1/news/{endpoint}"

    polling = cfg.get("polling", {}) or {}
    language = polling.get("language", "en")
    limit = int(polling.get("limit", 50))

    params = {
        "api_token": THENEWSAPI_TOKEN,
        "search": search,
        "language": language,
        "published_after": thenewsapi_dt(published_after),
        "published_before": thenewsapi_dt(published_before),
        "limit": limit,
        "page": 1,
    }

    sf = api_cfg.get("search_fields")
    if sf:
        params["search_fields"] = sf

    locales = api_cfg.get("locale") or []
    if locales:
        params["locale"] = ",".join(locales)

    dom = api_cfg.get("domains", {}) or {}
    if dom.get("include"):
        params["domains"] = ",".join(dom["include"])
    if dom.get("exclude"):
        params["exclude_domains"] = ",".join(dom["exclude"])

    # categories support
    cats = api_cfg.get("categories", {}) or {}
    if cats.get("include"):
        params["categories"] = ",".join(cats["include"])
    if cats.get("exclude"):
        params["exclude_categories"] = ",".join(cats["exclude"])

    out: List[dict] = []
    while True:
        r = requests.get(base, params=params, timeout=30)
        r.raise_for_status()
        data = r.json() or {}
        items = data.get("data") or []
        if not isinstance(items, list):
            break

        out.extend(items)

        # pagination
        meta = data.get("meta") or {}
        next_page = meta.get("next_page")
        if not next_page:
            break
        params["page"] = int(next_page)

        # safety: stop if too many
        if len(out) >= 5000:
            break

    return out


# ----------------------------
# Ollama helpers
# ----------------------------
def list_ollama_models(ollama_host: str) -> list[str]:
    r = requests.get(f"{ollama_host}/api/tags", timeout=20)
    r.raise_for_status()
    j = r.json() or {}
    models = j.get("models") or []
    return [m.get("name") for m in models if m.get("name")]

def pick_model(models: list[str], preferred: str, fallback_index: int) -> str:
    if preferred and preferred in models:
        return preferred
    if not models:
        raise RuntimeError("No models found on the Ollama host.")
    if 0 <= fallback_index < len(models):
        return models[fallback_index]
    return models[0]


# ----------------------------
# MySQL helpers (table + insert)
# ----------------------------
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

def ensure_table_and_migrate():
    # Create DB if missing
    cnx = mysql_connect()
    cur = cnx.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

    # Create table if missing (same schema you already used)
    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()
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
      UNIQUE KEY uniq_url (url(255)),
      KEY idx_company_time (company, timestamp_utc),
      KEY idx_time (timestamp_utc)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    cur.close()
    cnx.close()

def insert_rows_mysql(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()

    sql = f"""
    INSERT INTO `{MYSQL_TABLE}` (
      cycle, timestamp_utc, company,
      uuid, source, published_at, title, url, description, snippet, image_url,
      language, categories, locale, relevance_score,
      sentiment, confidence_0_to_100, one_sentence_summary,
      market_direction, market_time_horizon, market_rationale,
      elapsed_s, prompt_tokens, completion_tokens, total_tokens,
      analysis_json, error
    ) VALUES (
      %s, %s, %s,
      %s, %s, %s, %s, %s, %s, %s, %s,
      %s, %s, %s, %s,
      %s, %s, %s,
      %s, %s, %s,
      %s, %s, %s, %s,
      %s, %s
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
        # timestamp_utc stored as DATETIME (naive UTC ok)
        ts = r.get("timestamp_utc")
        if isinstance(ts, str):
            # If iso_z, remove Z and parse loosely
            # Keep simple: MySQL connector accepts ISO strings for DATETIME in many setups.
            ts_val = ts.replace("Z", "").replace("T", " ")
        else:
            ts_val = ts

        pub = r.get("published_at")
        if isinstance(pub, str):
            pub_val = pub.replace("Z", "").replace("T", " ")
        else:
            pub_val = pub

        vals.append((
            r.get("cycle"),
            ts_val,
            r.get("company"),
            r.get("uuid"),
            r.get("source"),
            pub_val,
            r.get("title"),
            r.get("url"),
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


# ----------------------------
# LLM analysis
# ----------------------------
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
        "You are a news analysis assistant. Use ONLY the provided article fields.\n"
        "Do not invent facts. If insufficient, be neutral and say what is missing.\n\n"
        f"Required JSON schema:\n{json.dumps(schema, indent=2)}\n\n"
        "Article:\n"
        f"{json.dumps(article, ensure_ascii=False)}"
    )

def analyze_article(client: OpenAI, model: str, article: dict) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    start = time.time()
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": build_prompt(article)}],
        temperature=0.2,
        max_tokens=650,
        stream=False,
    )
    elapsed = time.time() - start
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


# ----------------------------
# Runtime builder
# ----------------------------
def load_config(config_path: Path = CONFIG_PATH) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_runtime_from_config(cfg: dict) -> dict:
    polling = cfg.get("polling", {}) or {}
    lookback_minutes = int(polling.get("lookback_minutes", 30))

    model_cfg = cfg.get("model", {}) or {}
    ollama_host = model_cfg["host"]
    preferred_model = model_cfg.get("preferred_model", "")
    fallback_idx = int(model_cfg.get("fallback_model_index", 3))

    companies = (cfg.get("query") or {}).get("companies") or []

    # NEW: tickers support
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
        raise RuntimeError("Config must include query.companies and/or tickers.track.")

    ensure_table_and_migrate()

    models = list_ollama_models(ollama_host)
    chosen_model = pick_model(models, preferred_model, fallback_idx)
    client = OpenAI(base_url=f"{ollama_host}/v1", api_key="ollama")

    state = {
        "cfg": cfg,
        "client": client,
        "model": chosen_model,
        "companies": companies,
        "tickers": tickers,          # NEW
        "seen": set(),               # in-memory dedupe
        "last_poll": utc_now() - timedelta(minutes=lookback_minutes),
        "cycle": 0,
    }
    return state


# ----------------------------
# Public API for main.py
# ----------------------------
_STATE: Optional[dict] = None

def run_once(state: Optional[dict] = None, config_path: Path = CONFIG_PATH) -> Tuple[int, datetime]:
    """
    One polling cycle:
      - fetch articles in [state.last_poll, now)
      - analyze
      - insert rows
    Returns: (inserted_rows_count, poll_end_utc)
    """
    if not THENEWSAPI_TOKEN:
        raise RuntimeError("Set THENEWSAPI_TOKEN environment variable.")
    if not MYSQL_USER or not MYSQL_PASSWORD:
        raise RuntimeError("Set MYSQL_USER and MYSQL_PASSWORD environment variables (app login).")

    if state is None:
        cfg = load_config(config_path)
        state = build_runtime_from_config(cfg)

    cfg = state["cfg"]
    client = state["client"]
    chosen_model = state["model"]
    companies = state.get("companies") or []
    tickers = state.get("tickers") or []      # NEW
    seen = state["seen"]
    last_poll = state["last_poll"]

    poll_end = utc_now()
    batch_rows: List[Dict[str, Any]] = []

    # 1) Macro/company queries (existing behavior)
    for c in companies:
        company_name = c.get("display_name") or c.get("id") or "UNKNOWN"
        search = build_company_search(c)

        try:
            articles = fetch_thenewsapi(cfg=cfg, search=search, published_after=last_poll, published_before=poll_end)
        except Exception as e:
            batch_rows.append({
                "cycle": state["cycle"],
                "timestamp_utc": iso_z(utc_now()),
                "company": company_name,
                "url": "about:blank",
                "error": f"fetch_error: {e}",
            })
            continue

        for a in articles:
            url = (a.get("url") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)

            try:
                analysis, perf = analyze_article(client, chosen_model, a)
                batch_rows.append({
                    "cycle": state["cycle"],
                    "timestamp_utc": iso_z(utc_now()),
                    "company": company_name,

                    "uuid": a.get("uuid"),
                    "source": a.get("source"),
                    "published_at": a.get("published_at"),
                    "title": a.get("title"),
                    "url": url,
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
                batch_rows.append({
                    "cycle": state["cycle"],
                    "timestamp_utc": iso_z(utc_now()),
                    "company": company_name,
                    "uuid": a.get("uuid"),
                    "source": a.get("source"),
                    "published_at": a.get("published_at"),
                    "title": a.get("title"),
                    "url": url or "about:blank",
                    "error": f"analysis_error: {e}",
                })

    # 2) Ticker queries (NEW behavior)
    #    IMPORTANT: company column is set to the ticker symbol so ai.py can query WHERE company=%s using ticker.
    for t in tickers:
        ticker = normalize_ticker(t.get("ticker"))
        name = strip_or_none(t.get("name")) or ticker
        if not ticker:
            continue

        search = build_ticker_search(ticker=ticker, name=name)
        if not search:
            continue

        try:
            articles = fetch_thenewsapi(cfg=cfg, search=search, published_after=last_poll, published_before=poll_end)
        except Exception as e:
            batch_rows.append({
                "cycle": state["cycle"],
                "timestamp_utc": iso_z(utc_now()),
                "company": ticker,            # ticker key
                "url": "about:blank",
                "error": f"fetch_error(ticker): {e}",
            })
            continue

        for a in articles:
            url = (a.get("url") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)

            try:
                analysis, perf = analyze_article(client, chosen_model, a)
                batch_rows.append({
                    "cycle": state["cycle"],
                    "timestamp_utc": iso_z(utc_now()),
                    "company": ticker,          # ticker key (so ai.py can find it)

                    "uuid": a.get("uuid"),
                    "source": a.get("source"),
                    "published_at": a.get("published_at"),
                    "title": a.get("title"),
                    "url": url,
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
                batch_rows.append({
                    "cycle": state["cycle"],
                    "timestamp_utc": iso_z(utc_now()),
                    "company": ticker,
                    "uuid": a.get("uuid"),
                    "source": a.get("source"),
                    "published_at": a.get("published_at"),
                    "title": a.get("title"),
                    "url": url or "about:blank",
                    "error": f"analysis_error(ticker): {e}",
                })

    inserted = 0
    if batch_rows:
        insert_rows_mysql(batch_rows)
        inserted = len(batch_rows)

    # advance window
    state["last_poll"] = poll_end
    return inserted, poll_end


def run_forever(config_path: Path = CONFIG_PATH):
    """
    Continuous runner (uses cfg.polling.interval_seconds and cfg.polling.iterations if present).
    """
    cfg = load_config(config_path)
    polling = cfg.get("polling", {}) or {}
    interval_seconds = int(polling.get("interval_seconds", 60))
    iterations = polling.get("iterations", None)
    if iterations is not None:
        iterations = int(iterations)

    state = build_runtime_from_config(cfg)
    print(f"Using Ollama model: {state['model']}")

    while True:
        state["cycle"] += 1
        if iterations is not None and state["cycle"] > iterations:
            print(f"Reached iterations={iterations}. Exiting.")
            break

        inserted, _poll_end = run_once(state=state, config_path=config_path)
        print(f"[{iso_z(utc_now())}] Cycle {state['cycle']}: inserted {inserted} rows -> {MYSQL_DB}.{MYSQL_TABLE}")

        if iterations is None or state["cycle"] < iterations:
            time.sleep(int(interval_seconds))


# ----------------------------
# Entrypoint
# ----------------------------
def main():
    cfg = load_config(CONFIG_PATH)
    state = build_runtime_from_config(cfg)
    state["cycle"] += 1
    inserted, poll_end = run_once(state=state, config_path=CONFIG_PATH)
    print(f"[{iso_z(poll_end)}] inserted={inserted} -> {MYSQL_DB}.{MYSQL_TABLE}")

if __name__ == "__main__":
    main()
