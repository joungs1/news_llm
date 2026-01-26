#!/usr/bin/env python3
from __future__ import annotations

import os, json, time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
import yfinance as yf
import mysql.connector

# ============================================================
# CONFIG
# ============================================================
WATCHLIST_JSON = ROOT / "json" / "tickers.json"

FETCH_PLAN: List[Tuple[str, List[str]]] = [
    ("5m",  ["1d", "5d", "1mo"]),
    ("15m", ["1d", "5d", "1mo"]),
    ("60m", ["5d", "1mo", "3mo"]),
    ("1d",  ["1mo", "6mo", "1y", "5y", "max"]),
]

SLEEP_BETWEEN_REQUESTS_S = 1440

# MySQL "app login"
MYSQL_HOST = os.getenv("MYSQL_HOST", "100.117.198.80")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "B612b612@")
MYSQL_DB = os.getenv("MYSQL_DB", "Finance")
MYSQL_TABLE = os.getenv("MYSQL_TABLE_FINANCE", "finance_ohlcv_cache")

# ============================================================
# INDICATORS
# ============================================================
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
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
# WATCHLIST (list of {"Potential":..., "Ticker":...})
# ============================================================
def normalize_ticker(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    s = s.replace(" ", "")
    s = s.replace("/", "-")
    if s.upper().endswith("-TO"):
        s = s[:-3] + ".TO"
    return s

def load_tickers_from_watchlist_json(path: str) -> List[str]:
    p = Path(path)
    payload = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Watchlist JSON must be a JSON array (list) of objects.")
    tickers: List[str] = []
    for row in payload:
        if isinstance(row, dict):
            t = normalize_ticker(row.get("Ticker", ""))
            if t:
                tickers.append(t)
    # dedupe, preserve order
    seen, out = set(), []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

# ============================================================
# YFINANCE FETCH
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

    df = df.dropna(subset=required).copy()
    return df

# ============================================================
# MYSQL
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

def ensure_db_and_table():
    cnx = mysql_connect()
    cur = cnx.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4")
    cur.close()
    cnx.close()

    cnx = mysql_connect(MYSQL_DB)
    cur = cnx.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE}` (
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
        # yfinance often returns timezone-aware; if not, treat as UTC
        return dt.replace(tzinfo=None)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)

def n2(v):
    if v is None:
        return None
    if isinstance(v, (np.floating, np.integer)):
        v = v.item()
    if isinstance(v, float) and np.isnan(v):
        return None
    return v

def upsert_rows(ticker: str, interval: str, period: str, df: pd.DataFrame):
    if df.empty:
        return 0

    # Ensure datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.dropna(axis=0, subset=["Open", "High", "Low", "Close", "Volume"]).copy()

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

    sql = f"""
    INSERT INTO `{MYSQL_TABLE}` (
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
        values.append((
            ticker, interval, period, ts_utc,
            n2(row.get("Open")), n2(row.get("High")), n2(row.get("Low")), n2(row.get("Close")), int(row.get("Volume")) if pd.notna(row.get("Volume")) else None,
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
# MAIN
# ============================================================
def main():
    tickers = load_tickers_from_watchlist_json(WATCHLIST_JSON)
    if not tickers:
        raise RuntimeError("No tickers found in watchlist JSON.")

    ensure_db_and_table()

    total = 0
    for ticker in tickers:
        print(f"\n=== {ticker} ===")
        for interval, periods in FETCH_PLAN:
            for period in periods:
                try:
                    df = fetch_ohlcv(ticker, interval, period)
                    if df.empty:
                        print(f"  [MISS] {interval} {period}")
                        continue
                    df2 = compute_indicators(df)
                    n = upsert_rows(ticker, interval, period, df2)
                    total += n
                    print(f"  [UPSERT] {interval} {period} -> {n} rows")
                except Exception as e:
                    print(f"  [ERR] {interval} {period}: {e}")
                time.sleep(SLEEP_BETWEEN_REQUESTS_S)

    print(f"\nDone. Total rows upserted: {total}")

if __name__ == "__main__":
    main()
