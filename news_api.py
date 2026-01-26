import os
import re
import json
import time
import requests
from datetime import datetime, timedelta, timezone
from openai import OpenAI
import mysql.connector

THENEWSAPI_TOKEN = os.getenv("THENEWSAPI_TOKEN", "GLW7gjLDEnhMk0iA2bOLz5ZrFwANg1ZXlunXaR2e")
CONFIG_PATH = os.getenv("CONFIG_PATH", "json/news.json")

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
def utc_now():
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def iso_to_mysql_datetime(iso_str: str | None):
    if not iso_str:
        return None
    s = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)

def thenewsapi_dt(dt: datetime) -> str:
    dt2 = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt2.strftime("%Y-%m-%dT%H:%M:%S")

# ----------------------------
# Ollama helpers
# ----------------------------
def list_ollama_models(ollama_host: str) -> list[str]:
    r = requests.get(f"{ollama_host}/api/tags", timeout=15)
    r.raise_for_status()
    data = r.json()
    return [m.get("name") for m in data.get("models", []) if m.get("name")]

def pick_model(models: list[str], preferred: str, fallback_index: int) -> str:
    if not models:
        raise RuntimeError("No models found on the Ollama host.")
    if preferred in models:
        return preferred
    idx = fallback_index - 1
    if 0 <= idx < len(models):
        return models[idx]
    return models[0]

# ----------------------------
# Query builder
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

    sid = api_cfg.get("source_ids", {}) or {}
    if sid.get("include"):
        params["source_ids"] = ",".join(sid["include"])
    if sid.get("exclude"):
        params["exclude_source_ids"] = ",".join(sid["exclude"])

    cat = api_cfg.get("categories", {}) or {}
    if cat.get("include"):
        params["categories"] = ",".join(cat["include"])
    if cat.get("exclude"):
        params["exclude_categories"] = ",".join(cat["exclude"])

    r = requests.get(base, params=params, timeout=30)
    if r.status_code == 429:
        hdrs = {k: v for k, v in r.headers.items() if "rate" in k.lower() or "usage" in k.lower()}
        raise RuntimeError(f"rate_limit_reached (429). headers={hdrs} body={r.text[:500]}")
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"TheNewsAPI error: {data.get('error')}")

    out = []
    for a in (data.get("data") or []):
        out.append({
            "uuid": a.get("uuid"),
            "source": a.get("source"),
            "title": a.get("title"),
            "description": a.get("description"),
            "keywords": a.get("keywords"),
            "snippet": a.get("snippet"),
            "url": a.get("url"),
            "image_url": a.get("image_url"),
            "language": a.get("language"),
            "published_at": a.get("published_at"),
            "categories": a.get("categories"),
            "locale": a.get("locale"),
            "relevance_score": a.get("relevance_score"),
        })
    return out

# ----------------------------
# LLM helpers
# ----------------------------
def extract_json_object(text: str) -> dict:
    if not text:
        raise ValueError("Empty model response")
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not m:
            raise ValueError("Could not locate JSON in model output")
        return json.loads(m.group(0))

def build_analysis_prompt(article: dict) -> str:
    def trunc(s: str, n: int) -> str:
        return s if len(s) <= n else s[:n] + " ...[TRUNCATED]"
    return f"""
Return ONLY valid JSON. No markdown.

Article:
- Source: {trunc(article.get("source") or "", 200)}
- Title: {trunc(article.get("title") or "", 400)}
- Description: {trunc(article.get("description") or "", 1400)}
- Snippet: {trunc(article.get("snippet") or "", 2000)}
- URL: {article.get("url") or ""}

Schema:
{{
  "one_sentence_summary": "string",
  "sentiment": "positive|neutral|negative",
  "confidence_0_to_100": 0,
  "key_points": ["string","string","string"],
  "market_impact": {{
    "direction": "bullish|neutral|bearish|unclear",
    "time_horizon": "short|medium|long",
    "rationale": "string"
  }}
}}
""".strip()

def analyze_article(client: OpenAI, model: str, article: dict) -> tuple[dict, dict]:
    prompt = build_analysis_prompt(article)
    start = time.time()
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=700,
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

# ----------------------------
# MySQL helpers (auto-migrate)
# ----------------------------
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

def ensure_db():
    cnx = mysql_connect()
    cur = cnx.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

def table_exists(cur) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (MYSQL_DB, MYSQL_TABLE),
    )
    return cur.fetchone()[0] > 0

def get_existing_columns(cur) -> set[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
        (MYSQL_DB, MYSQL_TABLE),
    )
    return {r[0] for r in cur.fetchall()}

def index_exists(cur, index_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=%s AND table_name=%s AND index_name=%s",
        (MYSQL_DB, MYSQL_TABLE, index_name),
    )
    return cur.fetchone()[0] > 0

def ensure_table_and_migrate():
    ensure_db()
    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()

    if not table_exists(cur):
        create_sql = f"""
        CREATE TABLE `{MYSQL_TABLE}` (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          cycle INT NULL,
          timestamp_utc DATETIME NULL,
          company VARCHAR(255) NULL,

          uuid CHAR(36) NULL,
          source VARCHAR(255) NULL,
          published_at VARCHAR(64) NULL,
          title TEXT NULL,
          url TEXT NOT NULL,
          description TEXT NULL,
          snippet TEXT NULL,
          image_url TEXT NULL,
          language VARCHAR(16) NULL,
          categories JSON NULL,
          locale VARCHAR(16) NULL,
          relevance_score DOUBLE NULL,

          sentiment VARCHAR(16) NULL,
          confidence_0_to_100 INT NULL,
          one_sentence_summary TEXT NULL,
          market_direction VARCHAR(16) NULL,
          market_time_horizon VARCHAR(16) NULL,
          market_rationale TEXT NULL,
          elapsed_s DOUBLE NULL,
          prompt_tokens INT NULL,
          completion_tokens INT NULL,
          total_tokens INT NULL,
          analysis_json JSON NULL,

          error TEXT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

          PRIMARY KEY (id),
          UNIQUE KEY uniq_url (url(255))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cur.execute(create_sql)
        cur.close()
        cnx.close()
        return

    # If table exists, migrate missing columns
    existing = get_existing_columns(cur)

    # Minimal set needed to fix your current error + future-proof inserts
    desired = {
        "cycle": "INT NULL",
        "timestamp_utc": "DATETIME NULL",
        "company": "VARCHAR(255) NULL",

        "uuid": "CHAR(36) NULL",
        "source": "VARCHAR(255) NULL",
        "published_at": "VARCHAR(64) NULL",
        "title": "TEXT NULL",
        "url": "TEXT NOT NULL",
        "description": "TEXT NULL",
        "snippet": "TEXT NULL",
        "image_url": "TEXT NULL",
        "language": "VARCHAR(16) NULL",
        "categories": "JSON NULL",
        "locale": "VARCHAR(16) NULL",
        "relevance_score": "DOUBLE NULL",

        "sentiment": "VARCHAR(16) NULL",
        "confidence_0_to_100": "INT NULL",
        "one_sentence_summary": "TEXT NULL",
        "market_direction": "VARCHAR(16) NULL",
        "market_time_horizon": "VARCHAR(16) NULL",
        "market_rationale": "TEXT NULL",
        "elapsed_s": "DOUBLE NULL",
        "prompt_tokens": "INT NULL",
        "completion_tokens": "INT NULL",
        "total_tokens": "INT NULL",
        "analysis_json": "JSON NULL",

        "error": "TEXT NULL",
        "created_at": "TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
    }

    missing = [col for col in desired.keys() if col not in existing]
    for col in missing:
        cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN `{col}` {desired[col]};")

    # Ensure UNIQUE index on url(255)
    if not index_exists(cur, "uniq_url"):
        # If table already had duplicate URLs, this will fail.
        # In that case, you need to dedupe first.
        cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD UNIQUE KEY uniq_url (url(255));")

    cur.close()
    cnx.close()

def insert_rows_mysql(rows: list[dict]):
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

    values = []
    for r in rows:
        values.append((
            r.get("cycle"),
            iso_to_mysql_datetime(r.get("timestamp_utc")),
            r.get("company"),

            r.get("uuid"),
            r.get("source"),
            r.get("published_at"),
            r.get("title"),
            r.get("url") or "about:blank",  # url is NOT NULL in our schema
            r.get("description"),
            r.get("snippet"),
            r.get("image_url"),

            r.get("language"),
            r.get("categories"),  # JSON string or None
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

            r.get("analysis_json"),  # JSON string or None
            r.get("error"),
        ))

    cur.executemany(sql, values)
    cur.close()
    cnx.close()

# ----------------------------
# Main
# ----------------------------
def main():
    if not THENEWSAPI_TOKEN:
        raise RuntimeError("Set THENEWSAPI_TOKEN environment variable.")
    if not MYSQL_USER or not MYSQL_PASSWORD:
        raise RuntimeError("Set MYSQL_USER and MYSQL_PASSWORD environment variables (app login).")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    polling = cfg.get("polling", {}) or {}
    interval_seconds = int(polling.get("interval_seconds", 60))
    lookback_minutes = int(polling.get("lookback_minutes", 30))
    iterations = polling.get("iterations", None)
    if iterations is not None:
        iterations = int(iterations)

    model_cfg = cfg.get("model", {}) or {}
    ollama_host = model_cfg["host"]
    preferred_model = model_cfg.get("preferred_model", "")
    fallback_idx = int(model_cfg.get("fallback_model_index", 3))

    companies = (cfg.get("query") or {}).get("companies") or []
    if not companies:
        raise RuntimeError("Config must include query.companies list.")

    # Ensure table (and auto-migrate)
    ensure_table_and_migrate()

    models = list_ollama_models(ollama_host)
    chosen_model = pick_model(models, preferred_model, fallback_idx)
    print(f"Using Ollama model: {chosen_model}")
    client = OpenAI(base_url=f"{ollama_host}/v1", api_key="ollama")

    seen = set()
    last_poll = utc_now() - timedelta(minutes=lookback_minutes)

    cycle = 0
    while True:
        cycle += 1
        if iterations is not None and cycle > iterations:
            print(f"Reached iterations={iterations}. Exiting.")
            break

        poll_end = utc_now()
        batch_rows = []

        for c in companies:
            company_name = c.get("display_name") or c.get("id") or "UNKNOWN"
            search = build_company_search(c)

            try:
                articles = fetch_thenewsapi(cfg=cfg, search=search, published_after=last_poll, published_before=poll_end)
            except Exception as e:
                batch_rows.append({
                    "cycle": cycle,
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
                        "cycle": cycle,
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
                        "cycle": cycle,
                        "timestamp_utc": iso_z(utc_now()),
                        "company": company_name,
                        "uuid": a.get("uuid"),
                        "source": a.get("source"),
                        "published_at": a.get("published_at"),
                        "title": a.get("title"),
                        "url": url,
                        "error": f"analysis_error: {e}",
                    })

        if batch_rows:
            insert_rows_mysql(batch_rows)
            print(f"[{iso_z(utc_now())}] Cycle {cycle}: inserted {len(batch_rows)} rows -> {MYSQL_DB}.{MYSQL_TABLE}")
        else:
            print(f"[{iso_z(utc_now())}] Cycle {cycle}: no rows")

        last_poll = poll_end

        if iterations is None or cycle < iterations:
            time.sleep(interval_seconds)

if __name__ == "__main__":
    main()
