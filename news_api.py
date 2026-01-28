#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import json
import time
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

import requests
import mysql.connector
from openai import OpenAI

# Optional HTML parser (better extraction). If not installed, we fallback.
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None  # type: ignore


class ModelOutputError(Exception):
    """Raised when the model output cannot be parsed as the required JSON."""
    def __init__(self, message: str, raw_text: str = ""):
        super().__init__(message)
        self.raw_text = raw_text or ""

# ============================================================
# CONFIG (env overrides)
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
MAX_ENTITY_KWS = int(os.getenv("NEWS_MAX_ENTITY_KWS", "12"))      # keep searches short & effective
MAX_SEARCH_CHARS = int(os.getenv("NEWS_MAX_SEARCH_CHARS", "700")) # TheNewsAPI tends to fail silently on huge queries
SEARCH_OR_OP = os.getenv("NEWS_SEARCH_OR_OP", "|").strip()        # IMPORTANT: use "|" (works better than "OR")

# Full-text extraction guardrails
FULLTEXT_ENABLE = os.getenv("NEWS_FULLTEXT_ENABLE", "1").strip().lower() not in {"0", "false", "no"}
FULLTEXT_TIMEOUT_S = int(os.getenv("NEWS_FULLTEXT_TIMEOUT_S", "30"))
FULLTEXT_MAX_CHARS = int(os.getenv("NEWS_FULLTEXT_MAX_CHARS", "18000"))
HTTP_UA = os.getenv(
    "NEWS_HTTP_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
)

# LLM guardrails
LLM_MAX_TOKENS = int(os.getenv("NEWS_LLM_MAX_TOKENS", "750"))
LLM_RETRIES = int(os.getenv("NEWS_LLM_RETRIES", "2"))
LLM_TIMEOUT_S = int(os.getenv("NEWS_LLM_TIMEOUT_S", "60"))

# ============================================================
# TIME / HASH
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def thenewsapi_dt(dt: datetime) -> str:
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
    return s.upper()

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
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)
    except Exception:
        return None

# ============================================================
# CONFIG LOAD (unified entities[])
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

def load_entities(cfg: dict) -> List[dict]:
    entities = cfg.get("entities")
    if not entities or not isinstance(entities, list):
        raise RuntimeError("news.json must include a top-level array: entities[]")
    # basic validation
    out = []
    for e in entities:
        if not isinstance(e, dict):
            continue
        eid = strip_or_none(e.get("id")) or ""
        dn = strip_or_none(e.get("display_name")) or eid or "UNKNOWN"
        ticker = e.get("ticker", None)
        kws = clean_keywords(e.get("keywords") or [])
        out.append({
            "id": eid or sha1_hex(dn)[:10],
            "display_name": dn,
            "ticker": normalize_ticker(ticker) if ticker else None,
            "keywords": kws,
        })
    if not out:
        raise RuntimeError("entities[] is empty after validation.")
    return out

# ============================================================
# SEARCH BUILDERS (FIX fetched=0)
# ============================================================

def _quote_term(term: str) -> str:
    term = term.strip()
    if not term:
        return ""
    # Quote if spaces
    if re.search(r"\s", term):
        return f"\"{term}\""
    return term

def _join_or(terms: List[str]) -> str:
    terms = [t for t in terms if t.strip()]
    # TheNewsAPI search behaves better with '|' than literal OR
    return f" {SEARCH_OR_OP} ".join(terms)

def build_entity_search(entity: dict) -> str:
    """
    High-signal short query:
    - If ticker exists: include ticker + base + display name + a few keywords
    - If macro/no ticker: use display name + keywords
    """
    dn = entity.get("display_name") or ""
    ticker = entity.get("ticker")

    kws = clean_keywords(entity.get("keywords") or [])
    kws = kws[:MAX_ENTITY_KWS]

    terms: List[str] = []

    if ticker:
        t = normalize_ticker(ticker)
        base = t.split(".")[0] if "." in t else t
        # ticker terms first
        terms.append(_quote_term(t))
        if base and base != t:
            terms.append(_quote_term(base))
        # name helps catch “Royal Bank of Canada” etc
        if dn:
            terms.append(_quote_term(dn))
        # add only a few keywords (otherwise query too broad/too long)
        for k in kws[:8]:
            terms.append(_quote_term(k))
    else:
        # macro entity: keep broad but not insane
        if dn:
            terms.append(_quote_term(dn))
        for k in kws[:MAX_ENTITY_KWS]:
            terms.append(_quote_term(k))

    q = _join_or(terms).strip()
    if len(q) > MAX_SEARCH_CHARS:
        q = q[:MAX_SEARCH_CHARS].rstrip()
    return q

# ============================================================
# MYSQL (table + migrations)
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
    # DB exists
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

      cycle BIGINT NULL,
      timestamp_utc DATETIME NULL,

      entity_id VARCHAR(128) NULL,
      ticker VARCHAR(64) NULL,
      display_name VARCHAR(255) NULL,

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

      raw_article_json JSON NULL,

      full_text MEDIUMTEXT NULL,
      llm_input_text MEDIUMTEXT NULL,

      sentiment VARCHAR(16) NULL,
      confidence_0_to_100 INT NULL,
      one_sentence_summary TEXT NULL,
      important_keywords JSON NULL,

      market_direction VARCHAR(16) NULL,
      market_time_horizon TEXT NULL,
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

    # migrations
    for col, ddl in [
        ("url_hash", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN url_hash VARCHAR(40) NULL AFTER url"),
        ("raw_article_json", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN raw_article_json JSON NULL"),
        ("full_text", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN full_text MEDIUMTEXT NULL"),
        ("llm_input_text", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN llm_input_text MEDIUMTEXT NULL"),
        ("llm_raw_text", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN llm_raw_text MEDIUMTEXT NULL"),
        ("important_keywords", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN important_keywords JSON NULL"),
        ("entity_id", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN entity_id VARCHAR(128) NULL"),
        ("ticker", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN ticker VARCHAR(64) NULL"),
        ("display_name", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN display_name VARCHAR(255) NULL"),
        ("confidence_0_to_100", f"ALTER TABLE `{MYSQL_TABLE}` ADD COLUMN confidence_0_to_100 INT NULL"),
    ]:
        if not _column_exists(cur, MYSQL_TABLE, col):
            try:
                cur.execute(ddl)
            except Exception:
                pass

    # unique dedupe: entity_id + url_hash
    if not _index_exists(cur, MYSQL_TABLE, "uniq_entity_urlhash"):
        try:
            cur.execute(f"ALTER TABLE `{MYSQL_TABLE}` ADD UNIQUE KEY uniq_entity_urlhash (entity_id(128), url_hash)")
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
      entity_id, ticker, display_name,
      uuid, source, published_at, title, url, url_hash, description, snippet, image_url,
      language, categories, locale, relevance_score,
      raw_article_json, full_text, llm_input_text,
      sentiment, confidence_0_to_100, one_sentence_summary, important_keywords,
      market_direction, market_time_horizon, market_rationale,
      elapsed_s, prompt_tokens, completion_tokens, total_tokens,
      analysis_json, llm_raw_text, error
    ) VALUES (
      %s,%s,
      %s,%s,%s,
      %s,%s,%s,%s,%s,%s,%s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s
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
      error=VALUES(error);
    """

    vals = (
        row.get("cycle"),
        row.get("timestamp_utc"),

        row.get("entity_id"),
        row.get("ticker"),
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
    )

    cur.execute(sql, vals)
    cur.close()
    cnx.close()

# ============================================================
# TheNewsAPI
# ============================================================

def _resolve_domains(cfg: dict) -> Tuple[List[str], List[str]]:
    api_cfg = (cfg.get("thenewsapi") or {})
    dom = api_cfg.get("domains") or {}
    inc = dom.get("include") or []
    exc = dom.get("exclude") or []
    return list(inc), list(exc)

def fetch_thenewsapi(cfg: dict, search: str, after: datetime, before: datetime) -> List[Dict[str, Any]]:
    api_cfg = (cfg.get("thenewsapi") or {})
    endpoint = (api_cfg.get("endpoint") or "all").strip().lower()
    if endpoint not in {"all", "top"}:
        endpoint = "all"

    base = f"https://api.thenewsapi.com/v1/news/{endpoint}"

    polling = (cfg.get("polling") or {})
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

    # domains
    include_domains, exclude_domains = _resolve_domains(cfg)
    if include_domains:
        params["domains"] = ",".join(include_domains)
    if exclude_domains:
        params["exclude_domains"] = ",".join(exclude_domains)

    # NOTE:
    # If you restrict to paywalled domains, TheNewsAPI often returns 0.
    # If you still see fetched=0, temporarily comment out domains include in JSON to test.

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
    html = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def fetch_full_article_text(url: str) -> Tuple[Optional[str], Optional[str]]:
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

    html = resp.text or ""
    if not html.strip():
        return None, "fulltext_empty_html"

    try:
        if BeautifulSoup is not None:
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "noscript", "header", "footer", "svg", "nav"]):
                try:
                    tag.decompose()
                except Exception:
                    pass

            article = soup.find("article")
            target = article if article else soup.body if soup.body else soup
            text = target.get_text(separator="\n", strip=True)

            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            filtered = [ln for ln in lines if len(ln) >= 25] or lines
            out = "\n".join(filtered).strip()
        else:
            out = _rough_html_to_text(html)

        out = re.sub(r"\n{3,}", "\n\n", out).strip()
        if not out:
            return None, "fulltext_extraction_empty"

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
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # strip ``` blocks
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

def build_llm_input_text(article: dict, full_text: Optional[str]) -> str:
    parts: List[str] = []

    title = strip_or_none(article.get("title"))
    if title:
        parts.append(f"TITLE:\n{title}")

    if full_text:
        parts.append(f"FULL_TEXT:\n{full_text}")
    else:
        # fallback to what API gives
        desc = strip_or_none(article.get("description"))
        snip = strip_or_none(article.get("snippet"))
        content = strip_or_none(article.get("content"))
        if desc:
            parts.append(f"DESCRIPTION:\n{desc}")
        if snip and snip != desc:
            parts.append(f"SNIPPET:\n{snip}")
        if content and content not in (desc or "") and content not in (snip or ""):
            parts.append(f"CONTENT:\n{content}")

    meta = {
        "source": article.get("source"),
        "published_at": article.get("published_at"),
        "url": article.get("url"),
    }
    parts.append(f"METADATA:\n{json.dumps(meta, ensure_ascii=False)}")

    text = "\n\n".join(parts).strip()
    if len(text) > (FULLTEXT_MAX_CHARS + 2500):
        text = text[:FULLTEXT_MAX_CHARS + 2500].rstrip() + "\n\n[TRUNCATED_LLM_INPUT]"
    return text

def build_prompt(entity_name: str, llm_input_text: str) -> str:
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
        "Use ONLY the provided text/metadata. Do not invent facts.\n\n"
        f"Entity: {entity_name}\n\n"
        f"Schema:\n{json.dumps(schema, indent=2)}\n\n"
        "Article Input:\n"
        f"{llm_input_text}"
    )

def build_repair_prompt(entity_name: str, llm_input_text: str, bad_output: str) -> str:
    """Second-pass prompt to coerce strict JSON when the first pass wasn't valid JSON."""
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
    bad_output = (bad_output or "").strip()
    if len(bad_output) > 6000:
        bad_output = bad_output[:6000] + "\n[TRUNCATED_BAD_OUTPUT]"
    return (
        "You must output ONLY valid JSON (a single object). No markdown, no commentary.\n"
        "If the previous output contained extra text, discard it and rewrite it as JSON only.\n"
        "Use ONLY the provided article input. Do not invent facts.\n\n"
        f"Entity: {entity_name}\n\n"
        f"Schema:\n{json.dumps(schema, indent=2)}\n\n"
        "Article Input:\n"
        f"{llm_input_text}\n\n"
        "Previous (invalid) output to fix:\n"
        f"{bad_output}"
    )

def analyze_article(client: OpenAI, model: str, entity_name: str, llm_input_text: str) -> Tuple[Dict[str, Any], Dict[str, Any], str]:
    t0 = time.time()
    last_err: Optional[Exception] = None
    last_raw: str = ""

    for attempt in range(LLM_RETRIES + 1):
        try:
            # 1) primary attempt
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": build_prompt(entity_name, llm_input_text)}],
                temperature=0.2,
                max_tokens=LLM_MAX_TOKENS,
                stream=False,
                timeout=LLM_TIMEOUT_S,
            )
            last_raw = ((resp.choices[0].message.content or "")).strip()

            try:
                parsed = extract_json_object(last_raw)
            except Exception as parse_err:
                # 2) repair attempt (single pass, strict JSON rewrite)
                repair = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": build_repair_prompt(entity_name, llm_input_text, last_raw)}],
                    temperature=0.0,
                    max_tokens=max(LLM_MAX_TOKENS, 900),
                    stream=False,
                    timeout=LLM_TIMEOUT_S,
                )
                repaired_raw = ((repair.choices[0].message.content or "")).strip()
                last_raw = repaired_raw or last_raw
                parsed = extract_json_object(last_raw)

            elapsed = time.time() - t0
            usage = getattr(resp, "usage", None)
            perf = {
                "elapsed_s": round(elapsed, 3),
                "prompt_tokens": getattr(usage, "prompt_tokens", None) if usage else None,
                "completion_tokens": getattr(usage, "completion_tokens", None) if usage else None,
                "total_tokens": getattr(usage, "total_tokens", None) if usage else None,
            }
            return parsed, perf, last_raw

        except Exception as e:
            last_err = e
            # retry on transient errors / occasional bad output
            if attempt < LLM_RETRIES:
                time.sleep(0.8 * (attempt + 1))
                continue
            raise ModelOutputError(f"{type(e).__name__}: {e}", raw_text=last_raw)

    raise ModelOutputError(f"LLM failed: {last_err}", raw_text=last_raw)

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
# MAIN
# ============================================================

def run_once() -> None:
    if not THENEWSAPI_TOKEN:
        raise RuntimeError("Missing THENEWSAPI_TOKEN / NEWS_API_TOKEN env var.")
    if not MYSQL_PASSWORD:
        raise RuntimeError("Missing MYSQL_PASSWORD env var.")

    cfg = load_config()
    ensure_table()

    entities = load_entities(cfg)
    polling = cfg.get("polling") or {}
    lookback_minutes = int(polling.get("lookback_minutes", 60))
    limit = int(polling.get("limit", 50))

    model_cfg = cfg.get("model") or {}
    ollama_host = model_cfg.get("host")
    if not ollama_host:
        raise RuntimeError("news.json missing model.host")

    preferred_model = model_cfg.get("preferred_model", "")
    fallback_idx = int(model_cfg.get("fallback_model_index", 0))

    # LLM connect
    models = list_ollama_models(ollama_host)
    chosen_model = pick_model(models, preferred_model, fallback_idx)
    client = OpenAI(base_url=f"{ollama_host}/v1", api_key="ollama")

    # Window
    end = utc_now()
    start = end - timedelta(minutes=lookback_minutes)
    cycle = int(time.time())

    print(f"[news_api] loaded_config={cfg.get('_loaded_from')}")
    print(f"[news_api] entities_loaded={len(entities)} model={chosen_model}")
    print(f"[news_api] window={iso_z(start)} -> {iso_z(end)} limit={limit}")
    print(f"[news_api] search_or_op={SEARCH_OR_OP!r} max_entity_kws={MAX_ENTITY_KWS} max_search_chars={MAX_SEARCH_CHARS}")

    for e in entities:
        eid = e["id"]
        dn = e["display_name"]
        ticker = e.get("ticker")
        search = build_entity_search(e)

        print(f"\n[entity] id={eid} ticker={ticker} name={dn}")
        print(f"  search={search[:220]}{'...' if len(search)>220 else ''}")

        # Fetch
        articles = fetch_thenewsapi(cfg, search, start, end)
        print(f"  fetched={len(articles)}")

        # If fetched=0 consistently, the #1 reason is domains restriction in JSON
        # (paywalled sites or unsupported domains). Test by temporarily removing include[].

        for a in articles:
            url = (a.get("url") or "").strip()
            if not url:
                continue
            url_hash = sha1_hex(url)

            # Full text
            full_text = None
            fulltext_err = None
            if FULLTEXT_ENABLE:
                full_text, fulltext_err = fetch_full_article_text(url)

            llm_input_text = build_llm_input_text(a, full_text)

            try:
                analysis, perf, raw_text = analyze_article(client, chosen_model, dn, llm_input_text)
                mi = (analysis.get("market_impact") or {})
                important_keywords = analysis.get("important_keywords")
                if not isinstance(important_keywords, list):
                    important_keywords = []

                row = {
                    "cycle": cycle,
                    "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),

                    "entity_id": eid,
                    "ticker": ticker,
                    "display_name": dn,

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
                }

                insert_row(row)

                if fulltext_err:
                    print(f"  [fulltext_warn] {eid} url_hash={url_hash} err={fulltext_err}")

                print(f"  [ok] {eid} title={clamp_str(a.get('title'), 90)!r}")

            except Exception as ex:
                raw_failed = getattr(ex, "raw_text", None)
                err = f"{type(ex).__name__}: {ex}"
                print(f"  [llm_error] {eid} url_hash={url_hash} err={err}")

                row = {
                    "cycle": cycle,
                    "timestamp_utc": utc_now().replace(tzinfo=None, microsecond=0),

                    "entity_id": eid,
                    "ticker": ticker,
                    "display_name": dn,

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
                    "llm_raw_text": raw_failed,
                    "error": err,
                }
                insert_row(row)

    print("\nDONE")

def main() -> None:
    run_once()

if __name__ == "__main__":
    main()
