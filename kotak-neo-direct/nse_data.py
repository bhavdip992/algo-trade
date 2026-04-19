"""
nse_data.py
═══════════
Real NSE historical data fetcher for BANKNIFTY / NIFTY backtesting.

Data sources (tried in order):
  0. Dhan API    — real 5-min intraday (BEST — set DHAN_CLIENT_ID + DHAN_ACCESS_TOKEN in .env)
  1. OpenChart   — 5-min intraday NFO futures data
  2. nsepy       — Daily OHLCV index data
  3. nselib      — Daily index data
  4. NSE REST    — Direct NSE website scraping

Usage:
  py -3.11 nse_data.py --index NIFTY --days 90 --save
  py -3.11 nse_data.py --source dhan --index NIFTY --days 30 --save
  py -3.11 nse_data.py --check

Install:
  py -3.11 -m pip install nsepy nselib openchart-python requests pandas python-dotenv
"""

import os, sys, json, time, logging, argparse
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("NSEData")

# ─────────────────────────────────────────────────────────────
# INDEX SYMBOL MAPS
# ─────────────────────────────────────────────────────────────
NSE_INDEX_MAP = {
    "BANKNIFTY":  "NIFTY BANK",
    "NIFTY":      "NIFTY 50",
    "FINNIFTY":   "NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NIFTY MID SELECT",
}

OPENCHART_FUTURES = {
    "BANKNIFTY": "BANKNIFTY",
    "NIFTY":     "NIFTY",
    "FINNIFTY":  "FINNIFTY",
}


# ═════════════════════════════════════════════════════════════
# SOURCE 0: Dhan API — real 5-min intraday (BEST)
# ═════════════════════════════════════════════════════════════
def fetch_dhan(index: str, tf_min: int, days: int) -> pd.DataFrame:
    """
    Dhan intraday API — real 5-min OHLCV for NSE indices.
    Requires DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env
    POST https://api.dhan.co/v2/charts/intraday
    """
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
        load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    except Exception:
        pass

    client_id    = os.getenv("DHAN_CLIENT_ID", "").strip()
    access_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()

    if not client_id or not access_token:
        log.info("Dhan: DHAN_CLIENT_ID/DHAN_ACCESS_TOKEN not set — skipping")
        return pd.DataFrame()

    DHAN_SECURITY_ID = {
        "NIFTY": "13", "BANKNIFTY": "25",
        "FINNIFTY": "27", "MIDCPNIFTY": "442",
    }
    DHAN_INTERVAL = {1:"1", 3:"3", 5:"5", 10:"10", 15:"15", 25:"25", 60:"60"}

    security_id = DHAN_SECURITY_ID.get(index.upper())
    if not security_id:
        log.warning(f"Dhan: no security ID for {index}")
        return pd.DataFrame()

    interval = DHAN_INTERVAL.get(tf_min, "5")
    now      = datetime.now()
    from_dt  = now - timedelta(days=days)

    payload = {
        "securityId":      security_id,
        "exchangeSegment": "IDX_I",
        "instrument":      "INDEX",
        "interval":        interval,
        "fromDate":        from_dt.strftime("%Y-%m-%d"),
        "toDate":          now.strftime("%Y-%m-%d"),
    }
    headers = {
        "access-token": access_token,
        "client-id":    client_id,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }

    log.info(f"Dhan: {index} {tf_min}min {payload['fromDate']} → {payload['toDate']}")
    try:
        resp = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            headers=headers, json=payload, timeout=15,
        )
        if resp.status_code != 200:
            log.warning(f"Dhan API {resp.status_code}: {resp.text[:200]}")
            return pd.DataFrame()

        data       = resp.json()
        opens      = data.get("open",      [])
        highs      = data.get("high",      [])
        lows       = data.get("low",       [])
        closes     = data.get("close",     [])
        volumes    = data.get("volume",    [])
        timestamps = data.get("timestamp", data.get("start_Time", []))

        if not closes:
            log.warning(f"Dhan returned empty data: {str(data)[:200]}")
            return pd.DataFrame()

        rows = []
        for i in range(len(closes)):
            try:
                cl = float(closes[i])
                if cl <= 0: continue
                o  = float(opens[i])   if i < len(opens)   else cl
                h  = float(highs[i])   if i < len(highs)   else cl
                l  = float(lows[i])    if i < len(lows)    else cl
                v  = float(volumes[i]) if i < len(volumes) else 0.0
                ts = timestamps[i]     if i < len(timestamps) else None
                rows.append({"timestamp": ts, "open": o, "high": h,
                             "low": l, "close": cl, "volume": v})
            except Exception:
                continue

        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df = df.set_index("timestamp").sort_index()
        df = df[["open", "high", "low", "close", "volume"]].astype(float)
        df = df.between_time("09:15", "15:30")
        df = df[df.index.dayofweek < 5]
        log.info(f"Dhan: loaded {len(df)} candles "
                 f"({df.index[0].date()} → {df.index[-1].date()})")
        return df

    except Exception as e:
        log.warning(f"Dhan request failed: {e}")
        return pd.DataFrame()


# ═════════════════════════════════════════════════════════════
# SOURCE 1: OpenChart (5-min intraday)
# ═════════════════════════════════════════════════════════════
def fetch_openchart(index: str, tf_min: int, days: int) -> pd.DataFrame:
    try:
        from openchart import NSEData
        nse = NSEData()
        nse.download()

        end_dt   = datetime.now()
        start_dt = end_dt - timedelta(days=days + 10)

        sym_map = {
            "BANKNIFTY": "NIFTY BANK",
            "NIFTY":     "NIFTY 50",
            "FINNIFTY":  "NIFTY FIN SERVICE",
        }
        symbol = sym_map.get(index, "NIFTY BANK")

        log.info(f"OpenChart: fetching {symbol} {tf_min}m data...")
        df = nse.historical(
            symbol=symbol, exchange="NSE",
            start=start_dt, end=end_dt, interval=f"{tf_min}m",
        )

        if df is not None and not df.empty:
            df.columns = [c.lower() for c in df.columns]
            df.index.name = "timestamp"
            if "volume" not in df.columns:
                df["volume"] = 1000000
            df = df[["open","high","low","close","volume"]].astype(float)
            df = df.between_time("09:15", "15:30")
            df = df[df.index.dayofweek < 5]
            log.info(f"OpenChart: loaded {len(df)} {tf_min}-min candles")
            return df

    except ImportError:
        log.info("openchart not installed → py -3.11 -m pip install openchart")
    except Exception as e:
        log.warning(f"OpenChart failed: {e}")

    return pd.DataFrame()


# ═════════════════════════════════════════════════════════════
# SOURCE 2: nsepy — daily index OHLCV
# ═════════════════════════════════════════════════════════════
def fetch_nsepy(index: str, days: int) -> pd.DataFrame:
    try:
        from nsepy import get_history

        end_dt   = date.today()
        start_dt = end_dt - timedelta(days=days + 30)

        sym_map = {
            "BANKNIFTY":  "NIFTY BANK",
            "NIFTY":      "NIFTY 50",
            "FINNIFTY":   "NIFTY FIN SERVICE",
            "MIDCPNIFTY": "NIFTY MIDCAP SELECT",
        }
        symbol = sym_map.get(index, "NIFTY BANK")

        log.info(f"nsepy: fetching {symbol} daily data from {start_dt} to {end_dt}...")
        df = get_history(symbol=symbol, start=start_dt, end=end_dt, index=True)

        if df is not None and not df.empty:
            df.columns = [c.lower().strip() for c in df.columns]
            rename = {}
            for col in df.columns:
                if "open" in col:  rename[col] = "open"
                if "high" in col:  rename[col] = "high"
                if "low"  in col:  rename[col] = "low"
                if "close" in col and "prev" not in col: rename[col] = "close"
                if "volume" in col or "turnover" in col: rename[col] = "volume"
            df = df.rename(columns=rename)
            df.index = pd.to_datetime(df.index)
            df.index.name = "timestamp"
            if "volume" not in df.columns:
                df["volume"] = 1000000
            df = df[["open","high","low","close","volume"]].astype(float).dropna()
            log.info(f"nsepy: loaded {len(df)} daily candles")
            return df

    except ImportError:
        log.info("nsepy not installed → py -3.11 -m pip install nsepy")
    except Exception as e:
        log.warning(f"nsepy failed: {e}")

    return pd.DataFrame()


# ═════════════════════════════════════════════════════════════
# SOURCE 3: nselib — daily index OHLCV
# ═════════════════════════════════════════════════════════════
def fetch_nselib(index: str, days: int) -> pd.DataFrame:
    try:
        from nselib import capital_market

        end_dt   = date.today()
        start_dt = end_dt - timedelta(days=days + 30)

        sym_map = {
            "BANKNIFTY":  "Nifty Bank",
            "NIFTY":      "Nifty 50",
            "FINNIFTY":   "Nifty Financial Services",
            "MIDCPNIFTY": "Nifty Midcap Select",
        }
        symbol = sym_map.get(index, "Nifty Bank")

        log.info(f"nselib: fetching {symbol} daily data...")

        all_chunks = []
        chunk_start = start_dt
        while chunk_start < end_dt:
            chunk_end = min(chunk_start + timedelta(days=365), end_dt)
            try:
                chunk = capital_market.index_data(
                    index=symbol,
                    from_date=chunk_start.strftime("%d-%m-%Y"),
                    to_date=chunk_end.strftime("%d-%m-%Y"),
                )
                if chunk is not None and not chunk.empty:
                    all_chunks.append(chunk)
            except Exception as e:
                log.warning(f"  chunk {chunk_start}–{chunk_end} failed: {e}")
            chunk_start = chunk_end + timedelta(days=1)

        if not all_chunks:
            return pd.DataFrame()

        df = pd.concat(all_chunks)
        df.columns = [c.lower().strip() for c in df.columns]

        for col in ["timestamp", "date", "eod_timestamp"]:
            if col in df.columns:
                df.index = pd.to_datetime(df[col], dayfirst=True)
                break
        df.index.name = "timestamp"

        col_map = {}
        for col in df.columns:
            cl = col.lower()
            if "open"  in cl and "index" in cl:  col_map[col] = "open"
            if "high"  in cl and "index" in cl:  col_map[col] = "high"
            if "low"   in cl and "index" in cl:  col_map[col] = "low"
            if "close" in cl and "index" in cl and "prev" not in cl: col_map[col] = "close"
        df = df.rename(columns=col_map)

        for c in ["open","high","low","close"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c].astype(str).str.replace(",",""), errors="coerce")
        if "volume" not in df.columns:
            df["volume"] = 1000000

        df = df[["open","high","low","close","volume"]].astype(float).dropna()
        df = df.sort_index()
        log.info(f"nselib: loaded {len(df)} daily candles")
        return df

    except ImportError:
        log.info("nselib not installed → py -3.11 -m pip install nselib")
    except Exception as e:
        log.warning(f"nselib failed: {e}")

    return pd.DataFrame()


# ═════════════════════════════════════════════════════════════
# SOURCE 4: NSE direct REST API
# ═════════════════════════════════════════════════════════════
def fetch_nse_rest(index: str, days: int) -> pd.DataFrame:
    sym_map = {
        "BANKNIFTY":  "NIFTY BANK",
        "NIFTY":      "NIFTY 50",
        "FINNIFTY":   "NIFTY FIN SERVICE",
        "MIDCPNIFTY": "NIFTY MID SELECT",
    }
    symbol = sym_map.get(index, "NIFTY BANK")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/",
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    })

    try:
        log.info("NSE REST: getting session cookies...")
        session.get("https://www.nseindia.com", timeout=10)
        session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
        time.sleep(1)
    except Exception as e:
        log.warning(f"NSE REST cookie fetch failed: {e}")
        return pd.DataFrame()

    all_rows = []
    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=days + 10)

    chunk_days = 45
    cur_end = end_dt
    while cur_end > start_dt:
        cur_start = max(cur_end - timedelta(days=chunk_days), start_dt)
        url = (
            "https://www.nseindia.com/api/historical/indicesHistory"
            f"?indexType={requests.utils.quote(symbol)}"
            f"&from={cur_start.strftime('%d-%m-%Y')}"
            f"&to={cur_end.strftime('%d-%m-%Y')}"
        )
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
            records = data.get("data", {}).get("indexCloseOnlineRecords", [])
            for item in records:
                try:
                    all_rows.append({
                        "timestamp": pd.Timestamp(item.get("EOD_TIMESTAMP", "")),
                        "open":  float(str(item.get("EOD_OPEN_INDEX_VAL",  0)).replace(",","")),
                        "high":  float(str(item.get("EOD_HIGH_INDEX_VAL",  0)).replace(",","")),
                        "low":   float(str(item.get("EOD_LOW_INDEX_VAL",   0)).replace(",","")),
                        "close": float(str(item.get("EOD_CLOSE_INDEX_VAL", 0)).replace(",","")),
                        "volume": float(str(item.get("EOD_TRADED_QTY", 1000000)).replace(",","")),
                    })
                except Exception:
                    pass
            log.info(f"  NSE REST: {cur_start} → {cur_end}: {len(records)} records")
        except Exception as e:
            log.warning(f"  NSE REST chunk failed {cur_start}–{cur_end}: {e}")

        cur_end = cur_start - timedelta(days=1)
        time.sleep(0.5)

    if not all_rows:
        log.warning("NSE REST: no data returned")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows).set_index("timestamp").sort_index()
    df = df[df.index.notna()].drop_duplicates()
    df = df[["open","high","low","close","volume"]].astype(float).dropna()
    log.info(f"NSE REST: loaded {len(df)} daily candles")
    return df


# ═════════════════════════════════════════════════════════════
# RESAMPLE DAILY → INTRADAY
# ═════════════════════════════════════════════════════════════
def resample_daily_to_intraday(df_daily: pd.DataFrame, tf_min: int) -> pd.DataFrame:
    bars_per_day = int(375 / tf_min)
    rows = []
    np.random.seed(42)

    for ts, row in df_daily.iterrows():
        day = pd.Timestamp(ts).date() if hasattr(ts, 'date') else ts
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        daily_vol   = float(row.get("volume", 1000000))

        n    = bars_per_day
        path = [o]
        for i in range(n - 1):
            drift = (c - path[-1]) / (n - len(path))
            noise = (h - l) * 0.01 * np.random.randn()
            nxt   = max(l, min(h, path[-1] + drift + noise))
            path.append(nxt)
        path[-1] = c

        vol_weights = np.ones(n)
        vol_weights[:int(n*0.25)]  *= 1.8
        vol_weights[-int(n*0.15):] *= 1.5
        vol_weights = vol_weights / vol_weights.sum()
        bar_vols    = (vol_weights * daily_vol).astype(int)

        for i in range(n):
            bar_open  = path[i]
            bar_close = path[i+1] if i < n-1 else c
            bar_noise = (h - l) * 0.005
            bar_h     = min(max(bar_open, bar_close) + abs(np.random.randn()) * bar_noise, h)
            bar_l     = max(min(bar_open, bar_close) - abs(np.random.randn()) * bar_noise, l)
            bar_ts    = pd.Timestamp(datetime(day.year, day.month, day.day, 9, 15) + timedelta(minutes=i * tf_min))
            rows.append({
                "timestamp": bar_ts,
                "open": round(bar_open, 2), "high": round(bar_h, 2),
                "low":  round(bar_l,    2), "close":round(bar_close, 2),
                "volume": float(bar_vols[i]),
            })

    df = pd.DataFrame(rows).set_index("timestamp")
    log.info(f"Resampled {len(df_daily)} daily bars → {len(df)} {tf_min}-min bars")
    return df


# ═════════════════════════════════════════════════════════════
# MASTER FETCHER
# ═════════════════════════════════════════════════════════════
def get_historical_data(
    index:   str  = "BANKNIFTY",
    tf_min:  int  = 5,
    days:    int  = 90,
    source:  str  = "auto",   # auto | dhan | openchart | nsepy | nselib | nse_rest
    save_csv:bool = False,
    csv_dir: str  = ".",
) -> pd.DataFrame:
    """
    Priority: dhan → openchart → nsepy → nselib → nse_rest
    Daily sources are resampled to tf_min intraday candles.
    """
    df = pd.DataFrame()

    if source in ("auto", "dhan"):
        df = fetch_dhan(index, tf_min, days)

    if df.empty and source in ("auto", "openchart"):
        df = fetch_openchart(index, tf_min, days)

    if df.empty and source in ("auto", "nsepy"):
        df = fetch_nsepy(index, days)
        if not df.empty:
            log.info(f"Got daily data — resampling to {tf_min}-min intraday bars")
            df = resample_daily_to_intraday(df, tf_min)

    if df.empty and source in ("auto", "nselib"):
        df = fetch_nselib(index, days)
        if not df.empty:
            df = resample_daily_to_intraday(df, tf_min)

    if df.empty and source in ("auto", "nse_rest"):
        df = fetch_nse_rest(index, days)
        if not df.empty:
            df = resample_daily_to_intraday(df, tf_min)

    if df.empty:
        log.error("All data sources failed.")
        log.error("Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env for best results.")
        return df

    df = df.sort_index()
    df = df[~df.index.duplicated(keep="first")]
    df = df[["open","high","low","close","volume"]].astype(float).dropna()
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    df = df[df.index >= cutoff]

    log.info(f"Final dataset: {len(df)} bars | {df.index[0].date()} → {df.index[-1].date()}")

    if save_csv:
        fname = os.path.join(csv_dir, f"{index}_{tf_min}min_{days}d.csv")
        df.to_csv(fname)
        log.info(f"Saved to: {fname}")
        print(f"\nCSV saved: {fname}")
        print("Use for future backtests: py -3.11 backtest.py --csv " + fname)

    return df


# ═════════════════════════════════════════════════════════════
# INSTALL CHECKER
# ═════════════════════════════════════════════════════════════
def check_sources():
    print("\n" + "="*55)
    print("  NSE Data Source Availability")
    print("="*55)

    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
        load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    except Exception:
        pass

    dhan_ok = bool(os.getenv("DHAN_CLIENT_ID") and os.getenv("DHAN_ACCESS_TOKEN"))
    mark   = "✓" if dhan_ok else "✗"
    status = "CONFIGURED" if dhan_ok else "NOT CONFIGURED"
    print(f"  {mark} {'dhan':<12} {status:<16} real 5-min intraday ← BEST (set in .env)")

    for pkg, desc in [
        ("openchart", "5-min intraday NFO"),
        ("nsepy",     "daily index OHLCV"),
        ("nselib",    "daily index OHLCV"),
        ("requests",  "NSE REST (always available)"),
    ]:
        try:
            __import__(pkg)
            print(f"  ✓ {pkg:<12} {'INSTALLED':<16} {desc}")
        except ImportError:
            print(f"  ✗ {pkg:<12} {'NOT INSTALLED':<16} {desc}")

    print()
    print("  Best: add DHAN_CLIENT_ID + DHAN_ACCESS_TOKEN to .env")
    print("  Fallback: py -3.11 -m pip install openchart nsepy nselib")
    print("="*55 + "\n")


# ═════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch real NSE historical data")
    parser.add_argument("--index",  type=str, default="BANKNIFTY",
                        choices=["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"])
    parser.add_argument("--tf",     type=int, default=5,   help="Timeframe in minutes")
    parser.add_argument("--days",   type=int, default=90,  help="Number of days")
    parser.add_argument("--source", type=str, default="auto",
                        choices=["auto","dhan","openchart","nsepy","nselib","nse_rest"])
    parser.add_argument("--save",   action="store_true", help="Save CSV for reuse")
    parser.add_argument("--check",  action="store_true", help="Check installed sources")
    args = parser.parse_args()

    if args.check:
        check_sources()
        sys.exit(0)

    check_sources()

    df = get_historical_data(
        index=args.index,
        tf_min=args.tf,
        days=args.days,
        source=args.source,
        save_csv=args.save,
    )

    if not df.empty:
        print(f"\n  Index     : {args.index}")
        print(f"  Timeframe : {args.tf} min")
        print(f"  Bars      : {len(df)}")
        print(f"  From      : {df.index[0]}")
        print(f"  To        : {df.index[-1]}")
        print(f"\n  Sample (last 5 bars):")
        print(df.tail().to_string())
        print()
        if not args.save:
            print(f"  Tip: add --save to write CSV for faster future runs")
