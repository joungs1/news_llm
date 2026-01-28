#!/usr/bin/env python3
from __future__ import annotations

import os
import time
import threading
from datetime import datetime, timezone

# Import your modules (same folder)
import news_api
import finance
import ai
import dashboard

# -------------------------
# Schedules (seconds)
# -------------------------
NEWS_EVERY_S = int(os.getenv("NEWS_EVERY_S", "10000"))       # 5 min
FINANCE_EVERY_S = int(os.getenv("FINANCE_EVERY_S", "86400"))  # 1 day
AI_EVERY_S = int(os.getenv("AI_EVERY_S", "36000"))         # 1 hour

DASH_HOST = os.getenv("DASH_HOST", "0.0.0.0")
DASH_PORT = int(os.getenv("DASH_PORT", "8000"))

def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def loop_job(name: str, fn, every_s: int):
    """
    Runs fn() forever, sleeping every_s between runs.
    Exceptions are caught so the loop doesn't die.
    """
    # Run immediately on startup
    next_run = 0.0
    while True:
        now = time.time()
        if now >= next_run:
            print(f"[{utc_now()}] {name}: start")
            try:
                fn()
                print(f"[{utc_now()}] {name}: done")
            except Exception as e:
                print(f"[{utc_now()}] {name}: ERROR -> {e}")
            next_run = time.time() + every_s

        time.sleep(1)

def main():
    # 1) Start dashboard server in a background thread (non-blocking)
    t_dash = threading.Thread(
        target=dashboard.run_server,
        kwargs={"host": DASH_HOST, "port": DASH_PORT},
        daemon=True
    )
    t_dash.start()
    print(f"[{utc_now()}] dashboard: serving on http://{DASH_HOST}:{DASH_PORT}")

    # 2) Start job loops
    threads = [
        threading.Thread(target=loop_job, args=("news_api", news_api.run_once, NEWS_EVERY_S), daemon=True),
        threading.Thread(target=loop_job, args=("finance", finance.run_once, FINANCE_EVERY_S), daemon=True),
        threading.Thread(target=loop_job, args=("ai", ai.run_once, AI_EVERY_S), daemon=True),
    ]
    for t in threads:
        t.start()

    # 3) Keep process alive
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
