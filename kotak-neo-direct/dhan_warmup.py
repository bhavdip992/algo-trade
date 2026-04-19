"""
dhan_warmup.py
==============
Fetches historical OHLCV candles from Dhan API and feeds them into
the signal engine for indicator warmup.

Dhan intraday API:
  POST https://api.dhan.co/v2/charts/intraday
  Headers: access-token, client-id
  Body: securityId, exchangeSegment, instrument, interval, fromDate, toDate

Supported intervals: 1, 3, 5, 10, 15, 25, 60 (minutes)
"""

import os
import logging
import requests
from datetime import datetime, timedelta

log = logging.getLogger("DhanWarmup")

# Dhan security IDs for NSE indices
DHAN_SECURITY_ID = {
    "NIFTY":      "13",
    "BANKNIFTY":  "25",
    "FINNIFTY":   "27",
    "MIDCPNIFTY": "442",
}

# Dhan interval mapping (minutes → string)
DHAN_INTERVAL = {
    1: "1", 3: "3", 5: "5", 10: "10", 15: "15", 25: "25", 60: "60"
}


def fetch_dhan_candles(index: str, tf_min: int, days: int) -> list:
    """
    Fetch intraday OHLCV candles from Dhan API.
    Returns list of (timestamp, open, high, low, close, volume) tuples.
    """
    client_id    = os.getenv("DHAN_CLIENT_ID", "").strip()
    access_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()

    if not client_id or not access_token:
        log.warning("DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN not set in .env")
        return []

    security_id = DHAN_SECURITY_ID.get(index.upper())
    if not security_id:
        log.warning(f"No Dhan security ID for index: {index}")
        return []

    interval = DHAN_INTERVAL.get(tf_min, "5")

    now     = datetime.now()
    from_dt = now - timedelta(days=days)

    # Skip to last weekday if today is weekend
    while from_dt.weekday() >= 5:
        from_dt -= timedelta(days=1)

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

    log.info(
        f"Dhan: {index} {tf_min}min "
        f"{payload['fromDate']} \u2192 {payload['toDate']}"
    )

    try:
        resp = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            headers=headers,
            json=payload,
            timeout=15,
        )

        if resp.status_code != 200:
            log.warning(f"Dhan API {resp.status_code}: {resp.text[:200]}")
            return []

        data = resp.json()

        # Dhan response: parallel arrays
        opens      = data.get("open",       [])
        highs      = data.get("high",       [])
        lows       = data.get("low",        [])
        closes     = data.get("close",      [])
        volumes    = data.get("volume",     [])
        timestamps = data.get("timestamp",  data.get("start_Time", []))

        if not closes:
            log.warning(f"Dhan returned empty candles. Response: {str(data)[:300]}")
            return []

        candles = []
        for i in range(len(closes)):
            try:
                cl = float(closes[i])
                if cl <= 0:
                    continue
                o  = float(opens[i])      if i < len(opens)      else cl
                h  = float(highs[i])      if i < len(highs)      else cl
                l  = float(lows[i])       if i < len(lows)       else cl
                v  = float(volumes[i])    if i < len(volumes)    else 0.0
                ts = timestamps[i]        if i < len(timestamps) else None
                candles.append((ts, o, h, l, cl, v))
            except Exception:
                continue

        log.info(f"Dhan: fetched {len(candles)} candles")
        return candles

    except Exception as e:
        log.warning(f"Dhan request failed: {e}")
        return []


def warmup_from_dhan(engine, index: str, tf_min: int, days: int = 5) -> int:
    """
    Feed Dhan historical candles into the signal engine.
    Returns number of bars loaded.
    """
    candles = fetch_dhan_candles(index, tf_min, days)
    if not candles:
        return 0

    count = 0
    for ts, o, h, l, cl, v in candles:
        try:
            engine.update(ts, o, h, l, cl, v)
            count += 1
        except Exception:
            continue

    log.info(f"\u2705 Dhan warmup: {count} bars loaded into engine")
    return count
