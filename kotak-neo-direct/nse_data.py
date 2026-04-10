"""
nse_data.py
═══════════
Real NSE historical data fetcher for BANKNIFTY / NIFTY backtesting.
100% free — no paid subscription, no broker login needed.

Supports 4 data sources (tried in order):
  1. OpenChart  — 5-min intraday NFO futures data (best for options backtesting)
  2. nsepy      — Daily OHLCV index data (pip install nsepy)
  3. nselib     — Daily index data (pip install nselib)
  4. NSE REST   — Direct NSE website scraping (no install needed)

Usage:
  py -3.11 nse_data.py                         # fetch BANKNIFTY 90 days
  py -3.11 nse_data.py --index NIFTY --days 60
  py -3.11 nse_data.py --source openchart
  py -3.11 nse_data.py --save                  # save to CSV for reuse

Install all sources:
  py -3.11 -m pip install nsepy nselib openchart-python requests pandas
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

# OpenChart futures symbols for intraday data
OPENCHART_FUTURES = {
    "BANKNIFTY": "BANKNIFTY",
    "NIFTY":     "NIFTY",
    "FINNIFTY":  "FINNIFTY",
}

# ═════════════════════════════════════════════════════════════
# SOURCE 1: OpenChart (5-min intraday — best for options BT)
# ═════════════════════════════════════════════════════════════
def fetch_openchart(index: str, tf_min: int, days: int) -> pd.DataFrame:
    """
    OpenChart: free NSE/NFO intraday data library.
    Gives 5-min, 15-min candles for index futures.
    Install: py -3.11 -m pip install openchart

    Returns OHLCV DataFrame with DatetimeIndex.
    """
    try:
        from openchart import NSEData
        nse = NSEData()
        nse.download()   # downloads master contract (cached after first run)

        end_dt   = datetime.now()
        start_dt = end_dt - timedelta(days=days + 10)

        # Search for the continuous futures symbol
        sym_map = {
            "BANKNIFTY": "NIFTY BANK",
            "NIFTY":     "NIFTY 50",
            "FINNIFTY":  "NIFTY FIN SERVICE",
        }
        symbol = sym_map.get(index, "NIFTY BANK")

        log.info(f"OpenChart: fetching {symbol} {tf_min}m data...")
        df = nse.historical(
            symbol=symbol,
            exchange="NSE",
            start=start_dt,
            end=end_dt,
            interval=f"{tf_min}m",
        )

        if df is not None and not df.empty:
            df.columns = [c.lower() for c in df.columns]
            df.index.name = "timestamp"
            if "volume" not in df.columns:
                df["volume"] = 1000000
            df = df[["open","high","low","close","volume"]].astype(float)
            # Filter to IST market hours 9:15–15:30
            df = df.between_time("09:15", "15:30")
            df = df[df.index.dayofweek < 5]  # remove weekends
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
    """
    nsepy: get_history() for index daily OHLCV.
    Install: py -3.11 -m pip install nsepy

    Note: NSE changed their API in 2023. If nsepy fails,
    try: py -3.11 -m pip install git+https://github.com/gvkool/nsepy
    """
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
    """
    nselib: capital_market.index_data()
    Install: py -3.11 -m pip install nselib
    """
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

        # nselib only allows ~2 year chunks
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

        # Find timestamp column
        for col in ["timestamp", "date", "eod_timestamp"]:
            if col in df.columns:
                df.index = pd.to_datetime(df[col], dayfirst=True)
                break
        df.index.name = "timestamp"

        # Map columns
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
                df[c] = pd.to_numeric(
                    df[c].astype(str).str.replace(",",""), errors="coerce"
                )
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
# SOURCE 4: NSE direct REST API (no install, just requests)
# ═════════════════════════════════════════════════════════════
def fetch_nse_rest(index: str, days: int) -> pd.DataFrame:
    """
    NSE public REST API — no login, no install beyond requests.
    Fetches daily OHLCV in 50-day chunks (NSE limit per request).
    """
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

    # Hit homepage first to get cookies
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

    # NSE allows max ~50 days per request — chunk it
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
                        "volume": float(str(item.get("EOD_TRADED_QTY",    1000000)).replace(",","")),
                    })
                except Exception:
                    pass
            log.info(f"  NSE REST: {cur_start} → {cur_end}: {len(records)} records")
        except Exception as e:
            log.warning(f"  NSE REST chunk failed {cur_start}–{cur_end}: {e}")

        cur_end = cur_start - timedelta(days=1)
        time.sleep(0.5)  # be polite to NSE servers

    if not all_rows:
        log.warning("NSE REST: no data returned")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows).set_index("timestamp").sort_index()
    df = df[df.index.notna()].drop_duplicates()
    df = df[["open","high","low","close","volume"]].astype(float).dropna()
    log.info(f"NSE REST: loaded {len(df)} daily candles")
    return df


# ═════════════════════════════════════════════════════════════
# RESAMPLE DAILY → INTRADAY (for testing when no intraday data)
# ═════════════════════════════════════════════════════════════
def resample_daily_to_intraday(df_daily: pd.DataFrame, tf_min: int) -> pd.DataFrame:
    """
    Simulate intraday candles from daily OHLCV data.
    Each daily bar is split into N intraday bars (9:15–15:30)
    using realistic intraday price paths.

    NOTE: This is for strategy testing only — not real intraday data.
    For real 5-min data use openchart or your broker CSV.
    """
    bars_per_day = int(375 / tf_min)   # 375 min in trading session
    rows = []
    np.random.seed(42)

    for ts, row in df_daily.iterrows():
        day = pd.Timestamp(ts).date() if hasattr(ts, 'date') else ts
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        daily_vol   = float(row.get("volume", 1000000))

        # Generate realistic intraday price path using GBM
        # Pin open to daily open, close to daily close
        n    = bars_per_day
        path = [o]
        for i in range(n - 1):
            # Drift toward daily close
            drift   = (c - path[-1]) / (n - len(path))
            noise   = (h - l) * 0.01 * np.random.randn()
            nxt     = path[-1] + drift + noise
            nxt     = max(l, min(h, nxt))
            path.append(nxt)
        path[-1] = c  # force last bar to close at daily close

        # Volume distribution: higher in morning and last hour
        vol_weights = np.ones(n)
        vol_weights[:int(n*0.25)]  *= 1.8   # morning volume spike
        vol_weights[-int(n*0.15):] *= 1.5   # last hour
        vol_weights = vol_weights / vol_weights.sum()
        bar_vols    = (vol_weights * daily_vol).astype(int)

        for i in range(n):
            bar_open  = path[i]
            bar_close = path[i+1] if i < n-1 else c
            bar_noise = (h - l) * 0.005
            bar_h     = max(bar_open, bar_close) + abs(np.random.randn()) * bar_noise
            bar_l     = min(bar_open, bar_close) - abs(np.random.randn()) * bar_noise
            bar_h     = min(bar_h, h)
            bar_l     = max(bar_l, l)

            bar_ts = pd.Timestamp(
                datetime(day.year, day.month, day.day, 9, 15) + timedelta(minutes=i * tf_min)
            )
            rows.append({
                "timestamp": bar_ts,
                "open": round(bar_open, 2),
                "high": round(bar_h,    2),
                "low":  round(bar_l,    2),
                "close":round(bar_close,2),
                "volume": float(bar_vols[i]),
            })

    df = pd.DataFrame(rows).set_index("timestamp")
    log.info(f"Resampled {len(df_daily)} daily bars → {len(df)} {tf_min}-min bars")
    return df


# ═════════════════════════════════════════════════════════════
# MASTER FETCHER — tries all sources in order
# ═════════════════════════════════════════════════════════════
def get_historical_data(
    index:   str = "BANKNIFTY",
    tf_min:  int = 5,
    days:    int = 90,
    source:  str = "auto",   # auto | openchart | nsepy | nselib | nse_rest
    save_csv:bool = False,
    csv_dir: str  = ".",
) -> pd.DataFrame:
    """
    Master function — returns clean OHLCV DataFrame ready for backtesting.

    Priority:
      auto → openchart (5-min intraday) → nsepy daily → nselib daily → nse_rest daily

    All daily sources are resampled to tf_min intraday candles.
    """
    df = pd.DataFrame()

    if source in ("auto", "openchart"):
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
        log.error("Install a data source:  py -3.11 -m pip install openchart")
        log.error("Or use CSV:             py -3.11 backtest.py --csv your_file.csv")
        return df

    # Final clean
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="first")]
    df = df[["open","high","low","close","volume"]].astype(float)
    df = df.dropna()

    # Keep only last N days
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

    sources = {
        "openchart": ("openchart",  "5-min intraday NFO  ← BEST for options BT"),
        "nsepy":     ("nsepy",       "daily index OHLCV"),
        "nselib":    ("nselib",      "daily index OHLCV"),
        "requests":  ("requests",   "NSE REST (always available)"),
    }

    for pkg, (imp, desc) in sources.items():
        try:
            __import__(imp)
            status = "INSTALLED"
            mark   = "✓"
        except ImportError:
            status = "NOT INSTALLED"
            mark   = "✗"
        print(f"  {mark} {pkg:<12} {status:<16} {desc}")

    print()
    print("  Install best source:")
    print("  py -3.11 -m pip install openchart")
    print()
    print("  Or install all:")
    print("  py -3.11 -m pip install openchart nsepy nselib requests")
    print("="*55 + "\n")


# ═════════════════════════════════════════════════════════════
# MAIN — standalone usage
# ═════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch real NSE historical data")
    parser.add_argument("--index",  type=str, default="BANKNIFTY",
                        choices=["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"])
    parser.add_argument("--tf",     type=int, default=5,   help="Timeframe in minutes")
    parser.add_argument("--days",   type=int, default=90,  help="Number of days")
    parser.add_argument("--source", type=str, default="auto",
                        choices=["auto","openchart","nsepy","nselib","nse_rest"])
    parser.add_argument("--save",   action="store_true",   help="Save CSV for reuse")
    parser.add_argument("--check",  action="store_true",   help="Check installed sources")
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
