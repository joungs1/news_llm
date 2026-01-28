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

# news.json discovery
ROOT = Path(__file__).resolve().parents[0]
DEFAULT_CONFIG_CANDIDATES = [
    Path(os.getenv("NEWS_JSON", "")) if os.getenv("NEWS_JSON") else None,
    ROOT / "news.json",
    ROOT / "json" / "news.json",
    ROOT / "news_llm" / "json" / "news.json",
]
DEFAULT_CONFIG_CANDIDATES = [p for p in DEFAULT_CONFIG_CANDIDATES if p and str(p).strip()]

# TheNewsAPI search guardrails
MAX_TICKER_KWS = int(os.getenv("NEWS_MAX_TICKER_KWS", "10"))
MAX_COMPANY_KWS = int(os.getenv("NEWS_MAX_COMPANY_KWS", "25"))
MAX_SEARCH_CHARS = int(os.getenv("NEWS_MAX_SEARCH_CHARS", "950"))

# Full-text extraction guardrails
FULLTEXT_ENABLE = os.getenv("NEWS_FULLTEXT_ENABLE", "1").strip().lower() not in {"0", "false", "no"}
FULLTEXT_TIMEOUT_S = int(os.getenv("NEWS_FULLTEXT_TIMEOUT_S", "20"))
FULLTEXT_MAX_CHARS = int(os.getenv("NEWS_FULLTEXT_MAX_CHARS", "18000"))  # keep LLM input bounded
HTTP_UA = os.getenv(
    "NEWS_HTTP_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
)

# LLM guardrails
LLM_MAX_TOKENS = int(os.getenv("NEWS_LLM_MAX_TOKENS", "750"))
LLM_RETRIES = int(os.getenv("NEWS_LLM_RETRIES", "2"))
LLM_TIMEOUT_S = int(os.getenv("NEWS_LLM_TIMEOUT_S", "60"))

# MySQL behavior
MYSQL_STREAMING_INSERTS = os.getenv("NEWS_MYSQL_STREAMING", "1").strip().lower() not in {"0", "false", "no"}

# ============================================================
# TIME / HASH
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def thenewsapi_dt(dt: datetime) -> str:
    # TheNewsAPI expects UTC; send "YYYY-MM-DDTHH:MM:SS"
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

# ============================================================
# SMALL HELPERS
# ============================================================

def strip_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def normalize_ticker(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip().replace(" ", "")
    if not s:
        return ""
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

def clamp_str(v: Any, max_len: int) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    if not s.strip():
        return None
    return s[:max_len]

def safe_parse_published_at(published_at_val: Any) -> Optional[datetime]:
    if not published_at_val:
        return None
    s = str(published_at_val).strip()
    if not s:
        return None
    try:
        # TheNewsAPI often gives ISO 8601 with Z
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
    raise FileNotFoundError("Could not find news.json. Tried: " + ", ".join(str(p) for p in DEFAULT_CONFIG_CANDIDATES))

# ============================================================
# SEARCH BUILDERS
# ============================================================

def _quote_term(term: str) -> str:
    term = term.strip()
    if not term:
        return ""
    # Quote if spaces or special chars
    if re.search(r"\s", term):
        return f"\"{term}\""
    return term

def _join_or(terms: List[str]) -> str:
    terms = [t for t in terms if t.strip()]
    # TheNewsAPI supports boolean-ish syntax; "OR" is commonly accepted.
    return " OR ".join(terms)

def build_company_search(company_obj: dict) -> str:
    kws = clean_keywords(company_obj.get("keywords") or [])
    kws = kws[:MAX_COMPANY_KWS]
    if not kws:
        name = (company_obj.get("display_name") or company_obj.get("id") or "").strip()
        if name:
            kws = [name]
    terms = [_quote_term(k) for k in kws if k]
    q = _join_or(terms)
    return q[:MAX_SEARCH_CHARS]

def build_ticker_search(ticker: str, name: Optional[str], keywords: Optional[List[str]] = None) -> str:
    t = normalize_ticker(ticker)
    if not t:
        return ""

    base = t.split(".")[0] if "." in t else t

    terms: List[str] = []
    if name:
        nm = name.strip()
        if nm:
            terms.append(_quote_term(nm))

    # Always include ticker and base
    terms.append(_quote_term(t))
    if base and base != t:
        terms.append(_quote_term(base))

    kws = clean_keywords(keywords or [])
    kws = kws[:MAX_TICKER_KWS]
    for k in kws:
        terms.append(_quote_term(k))

    q = _join_or(terms)
    return q[:MAX_SEARCH_CHARS]

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

def _index_exists(cur, table: str, index_name: str) -> bool:
    cur.execute(f"SHOW INDEX FROM `{table}` WHERE Key_name=%s", (index_name,))
    return len(cur.fetchall()) > 0

def _column_exists(cur, table: str, col: str) -> bool:
    cur.execute(f"SHOW COLUMNS FROM `{table}` LIKE %s", (col,))
    return len(cur.fetchall()) > 0

def ensure_table() -> None:
    # 1) DB exists
    cnx = mysql_connect()
    cur = cnx.cursor(buffered=True)
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

    # 2) Create table baseline
    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor(buffered=True)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      cycle INT NULL,
      timestamp_utc DATETIME NULL,

      entity_type VARCHAR(32) NULL,         -- 'company' or 'ticker'
      entity_id VARCHAR(64) NULL,           -- company id or ticker symbol
      display_name VARCHAR(255) NULL,       -- company display_name OR "TICKER - Name"

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

      -- Full raw TheNewsAPI article JSON:
      raw_article_json JSON NULL,

      -- Full extracted article body (from URL) and what was sent to LLM:
      full_text MEDIUMTEXT NULL,
      llm_input_text MEDIUMTEXT NULL,

      sentiment VARCHAR(16) NULL,
      confidence_0_to_100 INT NULL,
      one_sentence_summary TEXT NULL,
      important_keywords JSON NULL,

      market_direction VARCHAR(16) NULL,
      market_time_horizon TEXT NULL,  -- TEXT avoids 1406
      market_rationale TEXT NULL,

      elapsed_s DOUBLE NULL,
      prompt_tokens INT NULL,
      completion_tokens INT NULL,
      total_tokens INT NULL,

      analysis_json JSON NULL,
      llm_raw_text MEDIUMTEXT NULL,
      error TEXT NULL,

      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_time (timestamp_utc)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # ---- Safe migrations for older tables ----
    # Ensure url_hash exists
    if not _column_exists(cur, MYSQL_TABLE, "url_hash"):
        cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN url_hash VARCHAR(40) NULL AFTER url")

    # Add entity columns if missing (so you can store tickers + names cleanly)
    for col, ddl in [
        ("entity_type", "ALTER TABLE `{t}` ADD COLUMN entity_type VARCHAR(32) NULL AFTER timestamp_utc"),
        ("entity_id", "ALTER TABLE `{t}` ADD COLUMN entity_id VARCHAR(64) NULL AFTER entity_type"),
        ("display_name", "ALTER TABLE `{t}` ADD COLUMN display_name VARCHAR(255) NULL AFTER entity_id"),
    ]:
        if not _column_exists(cur, MYSQL_TABLE, col):
            cur.execute(ddl.format(t=MYSQL_TABLE))

    # Add raw_article_json / full_text / llm_input_text / llm_raw_text / important_keywords
    for col, ddl in [
        ("raw_article_json", "ALTER TABLE `{t}` ADD COLUMN raw_article_json JSON NULL"),
        ("full_text", "ALTER TABLE `{t}` ADD COLUMN full_text MEDIUMTEXT NULL"),
        ("llm_input_text", "ALTER TABLE `{t}` ADD COLUMN llm_input_text MEDIUMTEXT NULL"),
        ("llm_raw_text", "ALTER TABLE `{t}` ADD COLUMN llm_raw_text MEDIUMTEXT NULL"),
        ("important_keywords", "ALTER TABLE `{t}` ADD COLUMN important_keywords JSON NULL"),
    ]:
        if not _column_exists(cur, MYSQL_TABLE, col):
            cur.execute(ddl.format(t=MYSQL_TABLE))

    # Confidence compatibility (your older scripts used `confidence`)
    if not _column_exists(cur, MYSQL_TABLE, "confidence"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN confidence INT NULL")
        except Exception:
            pass

    # market_time_horizon must be TEXT
    try:
        cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` MODIFY COLUMN market_time_horizon TEXT NULL")
    except Exception:
        pass

    # Uniqueness: we want entity_id + url_hash unique (avoid duplicates across tickers/companies)
    # Drop old uniq indexes if present
    if _index_exists(cur, MYSQL_TABLE, "uniq_company_url"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` DROP INDEX uniq_company_url")
        except Exception:
            pass
    if _index_exists(cur, MYSQL_TABLE, "uniq_company_urlhash"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` DROP INDEX uniq_company_urlhash")
        except Exception:
            pass

    # Add unique index
    if not _index_exists(cur, MYSQL_TABLE, "uniq_entity_urlhash"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD UNIQUE KEY uniq_entity_urlhash (entity_id(64), url_hash)")
        except Exception:
            pass

    cur.close()
    cnx.close()

def insert_row(row: Dict[str, Any]) -> None:
    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor(buffered=True)

    sql = f"""
    INSERT INTO `{MYSQL_TABLE}` (
      cycle, timestamp_utc,
      entity_type, entity_id, display_name,
      uuid, source, published_at, title, url, url_hash, description, snippet, image_url,
      language, categories, locale, relevance_score,
      raw_article_json, full_text, llm_input_text,
      sentiment, confidence_0_to_100, one_sentence_summary, important_keywords,
      market_direction, market_time_horizon, market_rationale,
      elapsed_s, prompt_tokens, completion_tokens, total_tokens,
      analysis_json, llm_raw_text, error,
      confidence
    ) VALUES (
      %s,%s,
      %s,%s,%s,
      %s,%s,%s,%s,%s,%s,%s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,
      %s
    )
    ON DUPLICATE KEY UPDATE
      cycle=VALUES(cycle),
      timestamp_utc=VALUES(timestamp_utc),
      display_name=VALUES(display_name),

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

      raw_article_json=VALUES(raw_article_json),
      full_text=VALUES(full_text),
      llm_input_text=VALUES(llm_input_text),

      sentiment=VALUES(sentiment),
      confidence_0_to_100=VALUES(confidence_0_to_100),
      one_sentence_summary=VALUES(one_sentence_summary),
      important_keywords=VALUES(important_keywords),

      market_direction=VALUES(market_direction),
      market_time_horizon=VALUES(market_time_horizon),
      market_rationale=VALUES(market_rationale),

      elapsed_s=VALUES(elapsed_s),
      prompt_tokens=VALUES(prompt_tokens),
      completion_tokens=VALUES(completion_tokens),
      total_tokens=VALUES(total_tokens),

      analysis_json=VALUES(analysis_json),
      llm_raw_text=VALUES(llm_raw_text),
      error=VALUES(error),

      confidence=VALUES(confidence);
    """

    vals = (
        row.get("cycle"),
        row.get("timestamp_utc"),

        row.get("entity_type"),
        row.get("entity_id"),
        row.get("display_name"),

        row.get("uuid"),
        row.get("source"),
        row.get("published_at"),
        row.get("title"),
        row.get("url"),
        row.get("url_hash"),
        row.get("description"),
        row.get("snippet"),
        row.get("image_url"),

        row.get("language"),
        row.get("categories"),
        row.get("locale"),
        row.get("relevance_score"),

        row.get("raw_article_json"),
        row.get("full_text"),
        row.get("llm_input_text"),

        row.get("sentiment"),
        row.get("confidence_0_to_100"),
        row.get("one_sentence_summary"),
        row.get("important_keywords"),

        row.get("market_direction"),
        row.get("market_time_horizon"),
        row.get("market_rationale"),

        row.get("elapsed_s"),
        row.get("prompt_tokens"),
        row.get("completion_tokens"),
        row.get("total_tokens"),

        row.get("analysis_json"),
        row.get("llm_raw_text"),
        row.get("error"),

        row.get("confidence"),
    )

    cur.execute(sql, vals)
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

    api_cfg = (cfg.get("thenewsapi") or {})
    if api_cfg.get("search_fields"):
        params["search_fields"] = api_cfg["search_fields"]

    if api_cfg.get("locale"):
        params["locale"] = ",".join(api_cfg["locale"])

    include_domains, exclude_domains = _resolve_domains(cfg)
    if include_domains:
        params["domains"] = ",".join(include_domains)
    if exclude_domains:
        params["exclude_domains"] = ",".join(exclude_domains)

    cats = api_cfg.get("categories") or {}
    if cats.get("include"):
        params["categories"] = ",".join(cats["include"])
    if cats.get("exclude"):
        params["exclude_categories"] = ",".join(cats["exclude"])

    out: List[Dict[str, Any]] = []
    while True:
        r = requests.get(base, params=params, timeout=30)
        if r.status_code >= 400:
            raise requests.HTTPError(f"{r.status_code} {r.reason}: {r.text[:800]}", response=r)

        data = r.json() or {}
        items = data.get("data") or []
        if isinstance(items, list):
            out.extend(items)

        meta = data.get("meta") or {}
        next_page = meta.get("next_page")
        if not next_page:
            break
        params["page"] = int(next_page)
        if len(out) >= 5000:
            break

    return out

# ============================================================
# FULL ARTICLE TEXT FETCH + EXTRACTION
# ============================================================

def _rough_html_to_text(html: str) -> str:
    # last-resort extractor if BeautifulSoup is unavailable
    # strip scripts/styles
    html = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", html)
    # remove tags
    text = re.sub(r"(?s)<[^>]+>", " ", html)
    # collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

def fetch_full_article_text(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (text, error). Text may be None if blocked or paywalled.
    """
    if not FULLTEXT_ENABLE:
        return None, "fulltext_disabled"

    if not url or not url.strip():
        return None, "no_url"

    headers = {
        "User-Agent": HTTP_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=FULLTEXT_TIMEOUT_S, allow_redirects=True)
    except Exception as e:
        return None, f"fulltext_request_error: {type(e).__name__}: {e}"

    if resp.status_code >= 400:
        return None, f"fulltext_http_{resp.status_code}"

    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "text/html" not in ctype and "application/xhtml+xml" not in ctype:
        # Many sites still serve HTML without correct content-type; proceed anyway
        pass

    html = resp.text or ""
    if not html.strip():
        return None, "fulltext_empty_html"

    try:
        if BeautifulSoup is not None:
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "noscript", "header", "footer", "svg"]):
                tag.decompose()

            # Prefer article tag if present
            article = soup.find("article")
            target = article if article else soup.body if soup.body else soup

            text = target.get_text(separator="\n", strip=True)
            # remove super short lines and collapse
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            # Heuristic: keep lines >= 25 chars to reduce nav junk, but don’t drop too aggressively
            filtered = [ln for ln in lines if len(ln) >= 25] or lines
            out = "\n".join(filtered).strip()
        else:
            out = _rough_html_to_text(html)

        out = re.sub(r"\n{3,}", "\n\n", out).strip()
        if not out:
            return None, "fulltext_extraction_empty"

        # Cap to keep LLM input bounded
        if len(out) > FULLTEXT_MAX_CHARS:
            out = out[:FULLTEXT_MAX_CHARS].rstrip() + "\n\n[TRUNCATED]"
        return out, None
    except Exception as e:
        return None, f"fulltext_parse_error: {type(e).__name__}: {e}"

# ============================================================
# LLM
# ============================================================

def extract_json_object(text: str) -> Dict[str, Any]:
    if not text or not text.strip():
        raise ValueError("Empty model response")

    raw = text.strip()

    # direct JSON
    try:
        return json.loads(raw)
    except Exception:
        pass

    # strip ``` blocks
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", raw)
        raw = re.sub(r"\s*```\s*$", "", raw).strip()

    # find first complete {...}
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

def build_llm_input_text(article: dict, full_text: Optional[str]) -> str:
    """
    What the LLM sees: prefer extracted full text, fallback to API snippet/description.
    """
    parts: List[str] = []
    title = strip_or_none(article.get("title"))
    if title:
        parts.append(f"TITLE:\n{title}")

    # Prefer full text if available
    if full_text:
        parts.append(f"FULL_TEXT:\n{full_text}")
    else:
        desc = strip_or_none(article.get("description"))
        snip = strip_or_none(article.get("snippet"))
        content = strip_or_none(article.get("content"))
        if desc:
            parts.append(f"DESCRIPTION:\n{desc}")
        if snip and snip != desc:
            parts.append(f"SNIPPET:\n{snip}")
        if content and content not in (desc or "") and content not in (snip or ""):
            parts.append(f"CONTENT:\n{content}")

    # Always include key metadata
    meta = {
        "source": article.get("source"),
        "published_at": article.get("published_at"),
        "url": article.get("url"),
    }
    parts.append(f"METADATA:\n{json.dumps(meta, ensure_ascii=False)}")

    text = "\n\n".join(parts).strip()
    # Hard cap (defensive)
    if len(text) > FULLTEXT_MAX_CHARS + 2500:
        text = text[:FULLTEXT_MAX_CHARS + 2500].rstrip() + "\n\n[TRUNCATED_LLM_INPUT]"
    return text

def build_prompt(entity_display_name: str, llm_input_text: str) -> str:
    """
    Ask for important keywords too (your request).
    """
    schema = {
        "sentiment": "positive|neutral|negative",
        "confidence_0_to_100": 0,
        "one_sentence_summary": "string",
        "important_keywords": ["string", "..."],
        "market_impact": {
            "direction": "up|down|mixed|unclear",
            "time_horizon": "intraday|days|weeks|months|unclear",
            "rationale": "string"
        }
    }

    return (
        "Return ONLY valid JSON. No markdown.\n\n"
        "Use ONLY the provided article text/metadata. Do not invent facts.\n\n"
        f"Entity:\n{entity_display_name}\n\n"
        f"Schema:\n{json.dumps(schema, indent=2)}\n\n"
        "Article Input:\n"
        f"{llm_input_text}"
    )

def analyze_article(client: OpenAI, model: str, entity_display_name: str, llm_input_text: str) -> Tuple[Dict[str, Any], Dict[str, Any], str]:
    """
    Returns (parsed_json, perf_dict, raw_text)
    """
    t0 = time.time()

    last_err: Optional[Exception] = None
    for attempt in range(LLM_RETRIES + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": build_prompt(entity_display_name, llm_input_text)}],
                temperature=0.2,
                max_tokens=LLM_MAX_TOKENS,
                stream=False,
                timeout=LLM_TIMEOUT_S,  # openai python supports this
            )

            raw_text = (resp.choices[0].message.content or "").strip()
            parsed = extract_json_object(raw_text)

            elapsed = time.time() - t0
            usage = getattr(resp, "usage", None)
            perf = {
                "elapsed_s": round(elapsed, 3),
                "prompt_tokens": getattr(usage, "prompt_tokens", None) if usage else None,
                "completion_tokens": getattr(usage, "completion_tokens", None) if usage else None,
                "total_tokens": getattr(usage, "total_tokens", None) if usage else None,
            }
            return parsed, perf, raw_text
        except Exception as e:
            last_err = e
            if attempt < LLM_RETRIES:
                time.sleep(0.8 * (attempt + 1))
                continue
            raise

    raise RuntimeError(f"LLM failed: {last_err}")

# ============================================================
# OLLAMA MODEL PICK
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

# ============================================================
# RUNTIME / MAIN
# ============================================================

def _load_entities(cfg: dict) -> Tuple[List[dict], List[Dict[str, Any]]]:
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
            kws = clean_keywords(t.get("keywords") or [])
            tickers.append({"ticker": sym, "name": nm, "keywords": kws})

    if not companies and not tickers:
        raise RuntimeError("news.json must include query.companies and/or tickers.track (mode=explicit).")

    return companies, tickers

def run_once() -> None:
    if not THENEWSAPI_TOKEN:
        raise RuntimeError("Missing THENEWSAPI_TOKEN / NEWS_API_TOKEN env var.")
    if not MYSQL_PASSWORD:
        raise RuntimeError("Missing MYSQL_PASSWORD env var.")

    cfg = load_config()
    ensure_table()

    polling = (cfg.get("polling") or {})
    lookback_minutes = int(polling.get("lookback_minutes", 60))
    limit = int(polling.get("limit", 50))

    model_cfg = cfg.get("model") or {}
    ollama_host = model_cfg.get("host")
    if not ollama_host:
        raise RuntimeError("news.json missing model.host (Ollama base URL).")

    preferred_model = model_cfg.get("preferred_model", "")
    fallback_idx = int(model_cfg.get("fallback_model_index", 0))

    companies, tickers = _load_entities(cfg)

    # Print loaded entity stats (this answers your "keywords loaded" desire)
    print(f"[news_api] loaded_config={cfg.get('_loaded_from')}")
    print(f"[news_api] companies_loaded={len(companies)} tickers_loaded={len(tickers)}")
    for c in companies:
        dn = (c.get("display_name") or c.get("id") or "UNKNOWN").strip()
        ck = clean_keywords(c.get("keywords") or [])
        print(f"  [company] {dn}: keywords={len(ck)}")
    for t in tickers:
        print(f"  [ticker] {t['ticker']} ({t['name']}): keywords={len(t['keywords'])}")

    # Connect to LLM
    models = list_ollama_models(ollama_host)
    chosen_model = pick_model(models, preferred_model, fallback_idx)
    client = OpenAI(base_url=f"{ollama_host}/v1", api_key="ollama")
    print(f"[news_api] model={chosen_model}")

    # Window
    end = utc_now()
    start = end - timedelta(minutes=lookback_minutes)
    print(f"[news_api] window={iso_z(start)} -> {iso_z(end)} limit={limit}")

    cycle = int(time.time())

    def process_article(entity_type: str, entity_id: str, display_name: str, a: dict) -> None:
        url = (a.get("url") or "").strip()
        if not url:
            return
        url_hash = sha1_hex(url)

        # Full text extraction
        full_text, fulltext_err = (None, None)
        if FULLTEXT_ENABLE:
            full_text, fulltext_err = fetch_full_article_text(url)

        llm_input_text = build_llm_input_text(a, full_text)

        # Run LLM
        try:
            analysis, perf, raw_text = analyze_article(client, chosen_model, display_name, llm_input_text)
            mi = (analysis.get("market_impact") or {})
            important_keywords = analysis.get("important_keywords")
            if not isinstance(important_keywords, list):
                important_keywords = []

            row = {
                "cycle": cycle,
                "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),

                "entity_type": entity_type,
                "entity_id": entity_id,
                "display_name": display_name,

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

                "raw_article_json": json.dumps(a, ensure_ascii=False),

                "full_text": full_text,
                "llm_input_text": llm_input_text,

                "sentiment": clamp_str(analysis.get("sentiment"), 16),
                "confidence_0_to_100": analysis.get("confidence_0_to_100"),
                "one_sentence_summary": analysis.get("one_sentence_summary"),
                "important_keywords": json.dumps(important_keywords, ensure_ascii=False),

                "market_direction": clamp_str(mi.get("direction"), 16),
                "market_time_horizon": clamp_str(mi.get("time_horizon"), 2000),
                "market_rationale": mi.get("rationale"),

                "elapsed_s": perf.get("elapsed_s"),
                "prompt_tokens": perf.get("prompt_tokens"),
                "completion_tokens": perf.get("completion_tokens"),
                "total_tokens": perf.get("total_tokens"),

                "analysis_json": json.dumps(analysis, ensure_ascii=False),
                "llm_raw_text": raw_text,
                "error": None,

                # compatibility
                "confidence": analysis.get("confidence_0_to_100"),
            }

            insert_row(row)

            # If full text failed, keep a trace (but don't fail the pipeline)
            if fulltext_err:
                print(f"  [fulltext_warn] {entity_id} url_hash={url_hash} err={fulltext_err}")

            print(f"  [ok] {entity_id} title={clamp_str(a.get('title'), 80)!r}")

        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            print(f"  [llm_error] {entity_id} url={url} err={err}")

            row = {
                "cycle": cycle,
                "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),

                "entity_type": entity_type,
                "entity_id": entity_id,
                "display_name": display_name,

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

                "raw_article_json": json.dumps(a, ensure_ascii=False),

                "full_text": full_text,
                "llm_input_text": llm_input_text,

                "analysis_json": None,
                "llm_raw_text": None,
                "error": err,
                "confidence": None,
            }
            insert_row(row)

    # A) Companies
    for c in companies:
        company_id = (c.get("id") or "").strip() or "company"
        display_name = (c.get("display_name") or c.get("id") or "UNKNOWN").strip()
        search = build_company_search(c)
        if not search:
            continue

        print(f"\n[company] {display_name}")
        print(f"  search={search[:180]}{'...' if len(search) > 180 else ''}")
        articles = fetch_thenewsapi(cfg, search, start, end)
        print(f"  fetched={len(articles)}")

        for a in articles:
            process_article("company", company_id, display_name, a)

    # B) Tickers
    for t in tickers:
        ticker = normalize_ticker(t.get("ticker"))
        name = strip_or_none(t.get("name")) or ticker
        display_name = f"{ticker} - {name}"
        keywords = t.get("keywords") or []
        search = build_ticker_search(ticker, name, keywords)
        if not search:
            continue

        print(f"\n[ticker] {display_name}")
        print(f"  search={search[:180]}{'...' if len(search) > 180 else ''}")
        articles = fetch_thenewsapi(cfg, search, start, end)
        print(f"  fetched={len(articles)}")

        for a in articles:
            process_article("ticker", ticker, display_name, a)

    print("\nDONE")

def main() -> None:
    run_once()

if __name__ == "__main__":
    main()
