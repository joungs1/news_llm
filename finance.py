#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any

import numpy as np
import pandas as pd
import yfinance as yf
import mysql.connector

# ============================================================
# REPO PATHS (GitHub folder-relative)
# Adjust parents[1] if your script is not in repo/src/
# ============================================================
ROOT = Path(__file__).resolve().parents[1]          # repo root
WATCHLIST_JSON = ROOT / "json" / "tickers.json"     # JSON array of objects with "Ticker"

# Sleep between remote calls (yfinance)
SLEEP_BETWEEN_REQUESTS_S = float(os.getenv("SLEEP_BETWEEN_REQUESTS_S", "0.25"))

# ============================================================
# FETCH PLAN (OHLCV)
# ============================================================
FETCH_PLAN: List[Tuple[str, List[str]]] = [
    ("5m",  ["1d", "5d", "1mo"]),
    ("15m", ["1d", "5d", "1mo"]),
    ("60m", ["5d", "1mo", "3mo"]),
    ("1d",  ["1mo", "6mo", "1y", "5y", "max"]),
]

# ============================================================
# MySQL
# ============================================================
MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "B612b612@")
MYSQL_DB = os.getenv("MYSQL_DB", "Finance")

MYSQL_TABLE_BARS = os.getenv("MYSQL_TABLE_FINANCE", "finance_ohlcv_cache")
MYSQL_TABLE_ANALYST_SNAPSHOT = os.getenv("MYSQL_TABLE_ANALYST_SNAPSHOT", "analyst_snapshot")
MYSQL_TABLE_ANALYST_EVENTS = os.getenv("MYSQL_TABLE_ANALYST_EVENTS", "analyst_events")

# ============================================================
# INDICATORS
# ============================================================
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def bollinger(close: pd.Series, window: int = 20, n_std: float = 2.0):
    mid = close.rolling(window).mean()
    sd = close.rolling(window).std(ddof=0)
    upper = mid + n_std * sd
    lower = mid - n_std * sd
    return mid, upper, lower

def vwap(df: pd.DataFrame) -> pd.Series:
    tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
    pv = tp * df["Volume"]
    denom = df["Volume"].cumsum().replace(0, np.nan)
    return pv.cumsum() / denom

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["EMA20"] = ema(out["Close"], 20)
    out["VWAP"] = vwap(out)
    out["RSI14"] = rsi(out["Close"], 14)
    _, _, out["MACD_HIST"] = macd(out["Close"], 12, 26, 9)
    out["BB_MID"], out["BB_UP"], out["BB_LOW"] = bollinger(out["Close"], 20, 2.0)
    return out

# ============================================================
# WATCHLIST JSON
# Expect: [{"Potential":"RBC","Ticker":"RY.TO"}, ...]
# ============================================================
def normalize_ticker(raw: Any) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    s = s.replace(" ", "").replace("/", "-")
    if s.upper().endswith("-TO"):
        s = s[:-3] + ".TO"
    return s

def load_tickers_from_watchlist_json(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Watchlist JSON not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Watchlist JSON must be a JSON array (list) of objects.")
    tickers: List[str] = []
    for row in payload:
        if isinstance(row, dict):
            t = normalize_ticker(row.get("Ticker", ""))
            if t:
                tickers.append(t)
    # dedupe preserve order
    seen, out = set(), []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

# ============================================================
# YFINANCE: OHLCV
# ============================================================
def fetch_ohlcv(ticker: str, interval: str, period: str) -> pd.DataFrame:
    df = yf.download(
        tickers=ticker,
        interval=interval,
        period=period,
        auto_adjust=False,
        progress=False,
        threads=True,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    required = ["Open", "High", "Low", "Close", "Volume"]
    if any(c not in df.columns for c in required):
        return pd.DataFrame()

    return df.dropna(subset=required).copy()

# ============================================================
# YFINANCE: ANALYST DATA
# ============================================================
def json_safe(v):
    if v is None:
        return None
    if isinstance(v, (np.floating, np.integer)):
        return v.item()
    if isinstance(v, float) and np.isnan(v):
        return None
    return v

def build_deterministic_analyst_paragraph(snapshot: Dict[str, Any]) -> Optional[str]:
    """
    No LLM. Produces a compact paragraph from consensus fields.
    """
    s = snapshot.get("summary") or {}
    rk = s.get("recommendationKey")
    rm = s.get("recommendationMean")
    n = s.get("numberOfAnalystOpinions")
    tm = s.get("targetMeanPrice")
    th = s.get("targetHighPrice")
    tl = s.get("targetLowPrice")

    parts = []
    if rk:
        parts.append(f"Consensus: {str(rk).upper()}.")
    if rm is not None:
        parts.append(f"Mean rating: {rm}.")
    if n is not None:
        parts.append(f"Analyst count: {n}.")
    if tm is not None:
        parts.append(f"Target mean: {tm}.")
    if th is not None or tl is not None:
        parts.append(f"Target range: {tl}–{th}." if (tl is not None and th is not None) else f"Target range: low={tl}, high={th}.")
    return " ".join(parts) if parts else None

def fetch_analyst_data(ticker: str) -> Dict[str, Any]:
    """
    Returns:
      - snapshot (consensus + targets + company)
      - events (upgrades/downgrades recent rows)
    """
    t = yf.Ticker(ticker)

    info = {}
    info_err = None
    try:
        info = t.info or {}
    except Exception as e:
        info_err = str(e)
        info = {}

    snapshot = {
        "ticker": ticker,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "recommendationKey": json_safe(info.get("recommendationKey")),
            "recommendationMean": json_safe(info.get("recommendationMean")),
            "numberOfAnalystOpinions": json_safe(info.get("numberOfAnalystOpinions")),
            "targetMeanPrice": json_safe(info.get("targetMeanPrice")),
            "targetHighPrice": json_safe(info.get("targetHighPrice")),
            "targetLowPrice": json_safe(info.get("targetLowPrice")),
        },
        "company": {
            "shortName": json_safe(info.get("shortName")),
            "longName": json_safe(info.get("longName")),
            "sector": json_safe(info.get("sector")),
            "industry": json_safe(info.get("industry")),
        },
        "error": f"info_error: {info_err}" if info_err else None,
    }

    snapshot["paragraph"] = build_deterministic_analyst_paragraph(snapshot)
    snapshot["raw_json"] = info

    events: List[Dict[str, Any]] = []
    try:
        ud = getattr(t, "upgrades_downgrades", None)
        if isinstance(ud, pd.DataFrame) and not ud.empty:
            ud2 = ud.copy().tail(200).reset_index()
            ud2 = ud2.replace({np.nan: None})
            for _, row in ud2.iterrows():
                rec = {k: row.get(k) for k in ud2.columns}
                events.append(rec)
    except Exception as e:
        events.append({"error": f"upgrades_downgrades_error: {e}"})

    return {"snapshot": snapshot, "events": events}

# ============================================================
# MYSQL HELPERS
# ============================================================
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

def ensure_db_and_tables():
    cnx = mysql_connect()
    cur = cnx.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE_BARS}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      ticker VARCHAR(32) NOT NULL,
      interval VARCHAR(16) NOT NULL,
      period VARCHAR(16) NOT NULL,
      ts_utc DATETIME NOT NULL,

      open DOUBLE NULL,
      high DOUBLE NULL,
      low DOUBLE NULL,
      close DOUBLE NULL,
      volume BIGINT NULL,

      ema20 DOUBLE NULL,
      vwap DOUBLE NULL,
      rsi14 DOUBLE NULL,
      macd_hist DOUBLE NULL,
      bb_up DOUBLE NULL,
      bb_mid DOUBLE NULL,
      bb_low DOUBLE NULL,

      fetched_at_utc DATETIME NULL,

      PRIMARY KEY (id),
      UNIQUE KEY uniq_series (ticker, interval, period, ts_utc),
      KEY idx_ts (ts_utc),
      KEY idx_ticker (ticker)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE_ANALYST_SNAPSHOT}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      as_of_utc DATETIME NOT NULL,
      ticker VARCHAR(32) NOT NULL,

      recommendation_key VARCHAR(32) NULL,
      recommendation_mean DOUBLE NULL,
      num_analysts INT NULL,

      target_mean DOUBLE NULL,
      target_high DOUBLE NULL,
      target_low DOUBLE NULL,

      short_name VARCHAR(255) NULL,
      long_name VARCHAR(255) NULL,
      sector VARCHAR(255) NULL,
      industry VARCHAR(255) NULL,

      paragraph TEXT NULL,
      raw_json JSON NULL,
      error TEXT NULL,

      PRIMARY KEY (id),
      UNIQUE KEY uniq_snap (ticker, as_of_utc),
      KEY idx_ticker (ticker),
      KEY idx_asof (as_of_utc)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE_ANALYST_EVENTS}` (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      ticker VARCHAR(32) NOT NULL,
      event_time_utc DATETIME NULL,
      captured_at_utc DATETIME NOT NULL,

      firm VARCHAR(255) NULL,
      action VARCHAR(64) NULL,
      from_grade VARCHAR(64) NULL,
      to_grade VARCHAR(64) NULL,

      note TEXT NULL,
      raw_json JSON NULL,

      PRIMARY KEY (id),
      UNIQUE KEY uniq_event (
        ticker,
        firm(120),
        action(32),
        from_grade(32),
        to_grade(32),
        captured_at_utc
      ),
      KEY idx_ticker (ticker),
      KEY idx_captured (captured_at_utc),
      KEY idx_event_time (event_time_utc)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.close()
    cnx.close()

def to_mysql_dt(ts) -> Optional[datetime]:
    if ts is None or (isinstance(ts, float) and np.isnan(ts)):
        return None
    if isinstance(ts, str):
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(ts, pd.Timestamp):
        dt = ts.to_pydatetime()
    elif isinstance(ts, datetime):
        dt = ts
    else:
        dt = pd.to_datetime(ts, errors="coerce").to_pydatetime()

    if dt.tzinfo is None:
        return dt.replace(tzinfo=None)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)

def n2(v):
    if v is None:
        return None
    if isinstance(v, (np.floating, np.integer)):
        return v.item()
    if isinstance(v, float) and np.isnan(v):
        return None
    return v

# ============================================================
# INSERT/UPSERT: OHLCV BARS
# ============================================================
def upsert_bars(ticker: str, interval: str, period: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")

    df = df.dropna(axis=0, subset=["Open", "High", "Low", "Close", "Volume"]).copy()
    if df.empty:
        return 0

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

    sql = f"""
    INSERT INTO `{MYSQL_TABLE_BARS}` (
      ticker, interval, period, ts_utc,
      open, high, low, close, volume,
      ema20, vwap, rsi14, macd_hist, bb_up, bb_mid, bb_low,
      fetched_at_utc
    ) VALUES (
      %s, %s, %s, %s,
      %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s, %s, %s,
      %s
    )
    ON DUPLICATE KEY UPDATE
      open=VALUES(open),
      high=VALUES(high),
      low=VALUES(low),
      close=VALUES(close),
      volume=VALUES(volume),
      ema20=VALUES(ema20),
      vwap=VALUES(vwap),
      rsi14=VALUES(rsi14),
      macd_hist=VALUES(macd_hist),
      bb_up=VALUES(bb_up),
      bb_mid=VALUES(bb_mid),
      bb_low=VALUES(bb_low),
      fetched_at_utc=VALUES(fetched_at_utc);
    """

    values = []
    for ts, row in df.iterrows():
        ts_utc = to_mysql_dt(ts)
        if ts_utc is None:
            continue

        vol = row.get("Volume")
        vol_val = int(vol) if pd.notna(vol) else None

        values.append((
            ticker, interval, period, ts_utc,
            n2(row.get("Open")), n2(row.get("High")), n2(row.get("Low")), n2(row.get("Close")), vol_val,
            n2(row.get("EMA20")), n2(row.get("VWAP")), n2(row.get("RSI14")), n2(row.get("MACD_HIST")),
            n2(row.get("BB_UP")), n2(row.get("BB_MID")), n2(row.get("BB_LOW")),
            now_utc
        ))

    if not values:
        return 0

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()
    cur.executemany(sql, values)
    cur.close()
    cnx.close()
    return len(values)

# ============================================================
# INSERT: ANALYST SNAPSHOT
# ============================================================
def insert_analyst_snapshot(snapshot: Dict[str, Any]) -> int:
    as_of = datetime.now(timezone.utc).replace(tzinfo=None)

    s = snapshot.get("summary") or {}
    c = snapshot.get("company") or {}

    sql = f"""
    INSERT INTO `{MYSQL_TABLE_ANALYST_SNAPSHOT}` (
      as_of_utc, ticker,
      recommendation_key, recommendation_mean, num_analysts,
      target_mean, target_high, target_low,
      short_name, long_name, sector, industry,
      paragraph, raw_json, error
    ) VALUES (
      %s, %s,
      %s, %s, %s,
      %s, %s, %s,
      %s, %s, %s, %s,
      %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
      recommendation_key=VALUES(recommendation_key),
      recommendation_mean=VALUES(recommendation_mean),
      num_analysts=VALUES(num_analysts),
      target_mean=VALUES(target_mean),
      target_high=VALUES(target_high),
      target_low=VALUES(target_low),
      short_name=VALUES(short_name),
      long_name=VALUES(long_name),
      sector=VALUES(sector),
      industry=VALUES(industry),
      paragraph=VALUES(paragraph),
      raw_json=VALUES(raw_json),
      error=VALUES(error);
    """

    raw_json_str = None
    try:
        raw_json_str = json.dumps(snapshot.get("raw_json"), ensure_ascii=False)
    except Exception:
        raw_json_str = None

    vals = (
        as_of,
        snapshot.get("ticker"),
        s.get("recommendationKey"),
        n2(s.get("recommendationMean")),
        int(s.get("numberOfAnalystOpinions")) if s.get("numberOfAnalystOpinions") is not None else None,
        n2(s.get("targetMeanPrice")),
        n2(s.get("targetHighPrice")),
        n2(s.get("targetLowPrice")),
        c.get("shortName"),
        c.get("longName"),
        c.get("sector"),
        c.get("industry"),
        snapshot.get("paragraph"),
        raw_json_str,
        snapshot.get("error"),
    )

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()
    cur.execute(sql, vals)
    cur.close()
    cnx.close()
    return 1

# ============================================================
# INSERT: ANALYST EVENTS
# ============================================================
def normalize_event_record(ticker: str, rec: Dict[str, Any], captured_at: datetime) -> Dict[str, Any]:
    firm = rec.get("Firm") or rec.get("firm")
    action = rec.get("Action") or rec.get("action")
    from_grade = rec.get("FromGrade") or rec.get("fromGrade") or rec.get("from_grade")
    to_grade = rec.get("ToGrade") or rec.get("toGrade") or rec.get("to_grade")

    event_time = rec.get("GradeDate") or rec.get("gradeDate") or rec.get("Date") or rec.get("date")
    event_time_dt = to_mysql_dt(event_time) if event_time is not None else None

    return {
        "ticker": ticker,
        "captured_at_utc": captured_at,
        "event_time_utc": event_time_dt,
        "firm": firm,
        "action": action,
        "from_grade": from_grade,
        "to_grade": to_grade,
        "note": None,
        "raw_json": rec,
    }

def insert_analyst_events(ticker: str, events: List[Dict[str, Any]]) -> int:
    if not events:
        return 0

    captured_at = datetime.now(timezone.utc).replace(tzinfo=None)

    sql = f"""
    INSERT INTO `{MYSQL_TABLE_ANALYST_EVENTS}` (
      ticker, event_time_utc, captured_at_utc,
      firm, action, from_grade, to_grade,
      note, raw_json
    ) VALUES (
      %s, %s, %s,
      %s, %s, %s, %s,
      %s, %s
    )
    ON DUPLICATE KEY UPDATE
      event_time_utc=VALUES(event_time_utc),
      note=VALUES(note),
      raw_json=VALUES(raw_json);
    """

    values = []
    for rec in events:
        if "error" in rec and len(rec.keys()) == 1:
            norm = {
                "ticker": ticker,
                "captured_at_utc": captured_at,
                "event_time_utc": None,
                "firm": None,
                "action": None,
                "from_grade": None,
                "to_grade": None,
                "note": None,
                "raw_json": rec,
            }
        else:
            norm = normalize_event_record(ticker, rec, captured_at)

        raw_str = None
        try:
            raw_str = json.dumps(norm["raw_json"], ensure_ascii=False)
        except Exception:
            raw_str = None

        values.append((
            norm["ticker"],
            norm["event_time_utc"],
            norm["captured_at_utc"],
            norm["firm"],
            norm["action"],
            norm["from_grade"],
            norm["to_grade"],
            norm["note"],
            raw_str,
        ))

    if not values:
        return 0

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()
    cur.executemany(sql, values)
    cur.close()
    cnx.close()
    return len(values)

# ============================================================
# CONFIG + STATE + RUNNERS (NEW)
# ============================================================
def load_config(watchlist_path: Path = WATCHLIST_JSON) -> Dict[str, Any]:
    """
    Simple config loader for orchestration use.
    """
    tickers = load_tickers_from_watchlist_json(watchlist_path)
    return {
        "tickers": tickers,
        "sleep_s": float(os.getenv("SLEEP_BETWEEN_REQUESTS_S", str(SLEEP_BETWEEN_REQUESTS_S))),
        "fetch_plan": FETCH_PLAN,
        "watchlist_path": str(watchlist_path),
    }

def build_state(cfg: Optional[Dict[str, Any]] = None, watchlist_path: Path = WATCHLIST_JSON) -> Dict[str, Any]:
    """
    Prepares runtime state; safe to reuse across run_once calls.
    """
    if cfg is None:
        cfg = load_config(watchlist_path)
    ensure_db_and_tables()
    return {
        "cfg": cfg,
        "tickers": cfg["tickers"],
        "sleep_s": cfg["sleep_s"],
        "fetch_plan": cfg["fetch_plan"],
        "cycle": 0,
    }

def run_once(
    state: Optional[Dict[str, Any]] = None,
    watchlist_path: Path = WATCHLIST_JSON,
    *,
    do_analyst: bool = True,
    do_bars: bool = True,
    max_tickers: Optional[int] = None,
) -> Dict[str, Any]:
    """
    One full cycle over tickers:
      - optional analyst snapshot + events per ticker
      - optional OHLCV+indicators upsert per interval/period

    Returns summary dict (for logging).
    """
    if state is None:
        state = build_state(None, watchlist_path)

    state["cycle"] += 1
    tickers = list(state["tickers"])
    if max_tickers is not None:
        tickers = tickers[: int(max_tickers)]

    sleep_s = float(state.get("sleep_s", 0.25))
    fetch_plan = state.get("fetch_plan", FETCH_PLAN)

    total_bars = 0
    total_events = 0
    total_snapshots = 0
    errors: List[str] = []

    for ticker in tickers:
        # ---- Analyst
        if do_analyst:
            try:
                analyst = fetch_analyst_data(ticker)
                snapshot = analyst["snapshot"]
                events = analyst["events"]

                insert_analyst_snapshot(snapshot)
                total_snapshots += 1

                n_ev = insert_analyst_events(ticker, events)
                total_events += n_ev
            except Exception as e:
                errors.append(f"ANALYST_ERR {ticker}: {e}")

            if sleep_s:
                time.sleep(sleep_s)

        # ---- Bars
        if do_bars:
            for interval, periods in fetch_plan:
                for period in periods:
                    try:
                        df = fetch_ohlcv(ticker, interval, period)
                        if df is None or df.empty:
                            continue
                        df2 = compute_indicators(df)
                        n = upsert_bars(ticker, interval, period, df2)
                        total_bars += n
                    except Exception as e:
                        errors.append(f"BARS_ERR {ticker} {interval} {period}: {e}")

                    if sleep_s:
                        time.sleep(sleep_s)

    return {
        "cycle": state["cycle"],
        "tickers": len(tickers),
        "bars_upserted": total_bars,
        "analyst_snapshots": total_snapshots,
        "analyst_events": total_events,
        "errors": errors[:50],  # cap
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }

def run_forever(
    watchlist_path: Path = WATCHLIST_JSON,
    *,
    interval_seconds: int = 900,
    do_analyst: bool = True,
    do_bars: bool = True,
):
    """
    Convenience loop, if you want finance.py itself to run continuously.
    """
    state = build_state(None, watchlist_path)
    while True:
        summary = run_once(state, watchlist_path, do_analyst=do_analyst, do_bars=do_bars)
        print(f"[FINANCE] {summary}")
        time.sleep(int(interval_seconds))

# ============================================================
# SCRIPT MAIN (keeps your original behavior)
# ============================================================
def main():
    tickers = load_tickers_from_watchlist_json(WATCHLIST_JSON)
    if not tickers:
        raise RuntimeError(f"No tickers found in watchlist JSON: {WATCHLIST_JSON}")

    ensure_db_and_tables()
    print(f"Loaded {len(tickers)} tickers from: {WATCHLIST_JSON}")

    total_bars = 0
    total_events = 0
    total_snapshots = 0

    for ticker in tickers:
        print(f"\n=== {ticker} ===")

        # ---- Analyst: snapshot + events
        try:
            analyst = fetch_analyst_data(ticker)
            snapshot = analyst["snapshot"]
            events = analyst["events"]

            insert_analyst_snapshot(snapshot)
            total_snapshots += 1
            print("  [ANALYST_SNAPSHOT] inserted")

            n_ev = insert_analyst_events(ticker, events)
            total_events += n_ev
            print(f"  [ANALYST_EVENTS] inserted/upserted: {n_ev}")
        except Exception as e:
            print(f"  [ANALYST_ERR] {ticker}: {e}")

        time.sleep(SLEEP_BETWEEN_REQUESTS_S)

        # ---- OHLCV + indicators
        for interval, periods in FETCH_PLAN:
            for period in periods:
                try:
                    df = fetch_ohlcv(ticker, interval, period)
                    if df.empty:
                        print(f"  [MISS]   {interval} {period}")
                        continue
                    df2 = compute_indicators(df)
                    n = upsert_bars(ticker, interval, period, df2)
                    total_bars += n
                    print(f"  [BARS]   {interval} {period} -> upserted {n} rows")
                except Exception as e:
                    print(f"  [BARS_ERR] {interval} {period}: {e}")

                time.sleep(SLEEP_BETWEEN_REQUESTS_S)

    print("\nDone.")
    print(f"Total bars upserted: {total_bars}")
    print(f"Total analyst snapshots inserted: {total_snapshots}")
    print(f"Total analyst events inserted/upserted: {total_events}")

if __name__ == "__main__":
    main()
