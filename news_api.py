#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import time
import re
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

import requests
from openai import OpenAI

# ============================================================
# 🔧 SWITCH: LLM API MODE
# ============================================================
# False = /v1/responses  (Phi-3, DeepSeek, R1, non-chat)
# True  = /v1/chat/completions (chat-only models)
USE_CHAT_API = False

# ============================================================
# CONFIG
# ============================================================

THENEWSAPI_TOKEN = os.getenv("THENEWSAPI_TOKEN")
OLLAMA_HOST = "http://100.88.217.85:11434"
OLLAMA_MODEL = "phi3:latest"

NEWS_JSON = "./news.json"

# ============================================================
# HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def extract_json_object(text: str) -> Dict[str, Any]:
    text = text.strip()

    try:
        return json.loads(text)
    except Exception:
        pass

    # fallback: extract first {...}
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON found")

    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return json.loads(text[start:i + 1])

    raise ValueError("Unclosed JSON object")

# ============================================================
# NEWS API
# ============================================================

def fetch_thenewsapi(search: str, after: datetime, before: datetime) -> List[dict]:
    url = "https://api.thenewsapi.com/v1/news/all"

    params = {
        "api_token": THENEWSAPI_TOKEN,
        "search": search,
        "language": "en",
        "published_after": after.strftime("%Y-%m-%dT%H:%M:%S"),
        "published_before": before.strftime("%Y-%m-%dT%H:%M:%S"),
        "limit": 50,
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])

# ============================================================
# 🔴 LLM API — BOTH VERSIONS
# ============================================================

def analyze_article(client: OpenAI, article: dict):
    prompt = (
        "Return ONLY valid JSON.\n\n"
        "Schema:\n"
        "{\n"
        '  "sentiment": "positive|neutral|negative",\n'
        '  "confidence_0_to_100": 0,\n'
        '  "one_sentence_summary": "string",\n'
        '  "market_impact": {\n'
        '    "direction": "up|down|mixed|unclear",\n'
        '    "time_horizon": "intraday|days|weeks|months|unclear",\n'
        '    "rationale": "string"\n'
        "  }\n"
        "}\n\n"
        "Article:\n"
        f"{json.dumps(article, ensure_ascii=False)}"
    )

    # ========================================================
    # 🟡 OLD CHAT API (LEGACY)
    # ========================================================
    if USE_CHAT_API:
        resp = client.chat.completions.create(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=800,
        )

        text = resp.choices[0].message.content or ""
        if not text.strip():
            raise RuntimeError("Chat API returned empty output")

        parsed = extract_json_object(text)
        return parsed, text

    # ========================================================
    # ✅ NEW RESPONSES API (CORRECT FOR OLLAMA)
    # ========================================================
    resp = client.responses.create(
        model=OLLAMA_MODEL,
        input=prompt,
        temperature=0.2,
        max_output_tokens=800,
    )

    text = ""
    for item in resp.output:
        if item["type"] == "message":
            for c in item["content"]:
                if c["type"] == "output_text":
                    text += c["text"]

    if not text.strip():
        raise RuntimeError("Responses API returned empty output")

    parsed = extract_json_object(text)
    return parsed, text

# ============================================================
# MAIN
# ============================================================

def main():
    print("RUNNING FILE:", __file__)
    print("USE_CHAT_API =", USE_CHAT_API)

    with open(NEWS_JSON, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    companies = cfg["query"]["companies"]
    tickers = cfg["tickers"]["track"]

    client = OpenAI(
        base_url=f"{OLLAMA_HOST}/v1",
        api_key="ollama"
    )

    now = utc_now()
    after = now - timedelta(hours=6)

    # =======================
    # COMPANIES
    # =======================
    for c in companies:
        label = c["display_name"]
        search = " OR ".join(c["keywords"])

        print(f"\n[COMPANY] {label}")
        articles = fetch_thenewsapi(search, after, now)

        for a in articles[:3]:
            parsed, raw = analyze_article(client, a)
            print("RAW LLM TEXT:", raw[:200], "...")
            print("PARSED JSON:", parsed)

    # =======================
    # TICKERS
    # =======================
    for t in tickers:
        label = t["ticker"]
        search = " OR ".join(t["keywords"][:10])

        print(f"\n[TICKER] {label}")
        articles = fetch_thenewsapi(search, after, now)

        for a in articles[:3]:
            parsed, raw = analyze_article(client, a)
            print("RAW LLM TEXT:", raw[:200], "...")
            print("PARSED JSON:", parsed)

if __name__ == "__main__":
    main()
