"""
data_feed.py — Kotak Neo WebSocket + Historical Data  (standalone helper)

PURPOSE
───────
This is a DIAGNOSTIC and HELPER module.  You do NOT need to import it in
kotak_live.py — all fixes are already IN kotak_live.py.

Use this file to:
  1. Test your WebSocket connection in isolation before running the main bot
  2. Manually fetch historical candles and inspect the DataFrame
  3. Understand the expected SDK call signatures (history / subscribe)

USAGE
─────
  python data_feed.py --test-ws          # test WS for 60 seconds
  python data_feed.py --test-history     # fetch 5d/5min NIFTY history
  python data_feed.py --test-symbol      # test symbol generation + token lookup

REQUIREMENTS
────────────
  pip install neo_api_client pyotp python-dotenv pandas
"""

import os, sys, time, json, logging, argparse
from datetime import datetime, date, timedelta

import pyotp
from dotenv import load_dotenv

# ── Load .env ─────────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

_ENV_FILE = os.path.join(_SCRIPT_DIR, ".env")
if not os.path.exists(_ENV_FILE):
    _ENV_FILE = os.path.join(_SCRIPT_DIR, "..", ".env")
load_dotenv(_ENV_FILE)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("DataFeed")

try:
    from neo_api_client import NeoAPI
except ImportError:
    log.error("neo_api_client not installed.")
    sys.exit(1)

# ── Config from .env ──────────────────────────────────────────────────────
def _s(k, d=""): return os.getenv(k, str(d)).strip()
def _i(k, d): 
    try: return int(_s(k, d))
    except: return int(d)

CFG = {
    "consumer_key": _s("KOTAK_CONSUMER_KEY"),
    "mobile":       _s("KOTAK_MOBILE"),
    "ucc":          _s("KOTAK_UCC"),
    "mpin":         _s("KOTAK_MPIN"),
    "totp_secret":  _s("KOTAK_TOTP_SECRET"),
    "environment":  _s("ENVIRONMENT", "prod"),
    "index":        _s("INDEX", "NIFTY").upper(),
    "strike_step":  _i("STRIKE_STEP", 50),
}

INDEX_NAME_MAP = {
    "NIFTY":      "Nifty 50",
    "BANKNIFTY":  "Nifty Bank",
    "FINNIFTY":   "Nifty Fin Service",
    "MIDCPNIFTY": "Nifty MidCap 100",
}

INDEX_TOKENS = {
    "NIFTY":     {"instrument_token": "26000", "exchange_segment": "nse_cm"},
    "BANKNIFTY": {"instrument_token": "26009", "exchange_segment": "nse_cm"},
    "FINNIFTY":  {"instrument_token": "26037", "exchange_segment": "nse_cm"},
}

NSE_SEG = "nse_cm"
NFO_SEG = "nfo_fo"

# ── Login ─────────────────────────────────────────────────────────────────
def login() -> NeoAPI:
    """
    Full TOTP + MPIN login.
    Returns authenticated NeoAPI client.

    SDK signature (v2, no consumer_secret):
        NeoAPI(consumer_key, environment, access_token=None, neo_fin_key=None)
    """
    log.info("🔑 Logging in...")
    client = NeoAPI(
        consumer_key=CFG["consumer_key"],
        environment=CFG["environment"],
        access_token=None,
        neo_fin_key=None,
    )

    totp = pyotp.TOTP(CFG["totp_secret"]).now()
    log.info(f"   TOTP: {totp}")

    r1 = client.totp_login(
        mobile_number=CFG["mobile"],
        ucc=CFG["ucc"],
        totp=totp,
    )
    if not r1 or not r1.get("data", {}).get("token"):
        raise RuntimeError(f"TOTP login failed: {r1}")
    log.info("   TOTP ✓")

    r2 = client.totp_validate(mpin=CFG["mpin"])
    if not r2 or not r2.get("data", {}).get("token"):
        raise RuntimeError(f"MPIN failed: {r2}")
    log.info("   MPIN ✓  — login successful")

    return client


# ── WebSocket test ────────────────────────────────────────────────────────
def test_websocket(client: NeoAPI, duration_secs: int = 60):
    """
    Subscribe NIFTY index via WebSocket and print ticks for duration_secs.

    Expected behaviour:
      - _on_open fires once with "Connection established" or similar
      - _on_message fires every ~1-2 seconds with NIFTY spot price
      - _on_close fires when we stop

    If you see "Connection is already closed" → your SDK version is old.
    Update: pip install "git+https://github.com/Kotak-Neo/kotak-neo-api.git"
    """
    ticks_received = []
    started_at = time.time()

    def _on_message(msg):
        ts = datetime.now().strftime("%H:%M:%S")
        try:
            if isinstance(msg, (str, bytes)):
                data = json.loads(msg)
            else:
                data = msg
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict): continue
                ltp = None
                for k in ("lp", "ltP", "ltp", "LTP", "last_price"):
                    v = item.get(k)
                    if v is not None:
                        try:
                            ltp = float(v)
                            break
                        except Exception:
                            pass
                if ltp and ltp > 0:
                    ticks_received.append(ltp)
                    log.info(f"  WS TICK [{ts}]  LTP = ₹{ltp:.2f}  "
                             f"(total ticks: {len(ticks_received)})")
                else:
                    log.debug(f"  WS raw: {str(msg)[:120]}")
        except Exception as e:
            log.debug(f"  WS parse: {e}  raw: {str(msg)[:80]}")

    def _on_open(msg):
        log.info(f"✅ WS OPEN: {msg}")

    def _on_close(msg):
        log.info(f"WS CLOSED: {msg}")

    def _on_error(err):
        # Safe stringify — SDK may return objects with broken __str__
        try:
            s = str(err)
            if not isinstance(s, str): s = repr(err)
        except Exception:
            s = repr(err)
        log.error(f"WS ERROR: {s}")

    index_name = INDEX_NAME_MAP.get(CFG["index"], "Nifty 50")
    log.info(f"📡 Subscribing {CFG['index']} ({index_name!r}) via WS for {duration_secs}s...")

    client.on_message = _on_message
    client.on_open    = _on_open
    client.on_close   = _on_close
    client.on_error   = _on_error

    client.subscribe(
        instrument_tokens=[{
            "instrument_token": index_name,
            "exchange_segment": NSE_SEG,
        }],
        isIndex=True,
        isDepth=False,
    )

    # Wait for ticks
    while time.time() - started_at < duration_secs:
        time.sleep(1)
        elapsed = int(time.time() - started_at)
        if elapsed % 10 == 0:
            log.info(f"  [{elapsed}s] Ticks received: {len(ticks_received)}")

    log.info(f"\n── WS Test Results ──────────────────────────────")
    log.info(f"  Duration  : {duration_secs}s")
    log.info(f"  Ticks     : {len(ticks_received)}")
    if ticks_received:
        log.info(f"  Min LTP   : ₹{min(ticks_received):.2f}")
        log.info(f"  Max LTP   : ₹{max(ticks_received):.2f}")
        log.info(f"  Last LTP  : ₹{ticks_received[-1]:.2f}")
        log.info("  ✅ WebSocket is WORKING")
    else:
        log.warning("  ❌ No ticks received — check:")
        log.warning("     1. Is market open? (9:15–15:30 IST, Mon–Fri)")
        log.warning("     2. Is consumer_key correct?")
        log.warning("     3. Try updating SDK: pip install -U neo_api_client")


# ── Historical data test ──────────────────────────────────────────────────
def test_historical(client: NeoAPI, tf_min: int = 5, days: int = 5):
    """
    Fetch historical OHLCV candles and print summary.

    Kotak Neo SDK:  client.history(exchange_segment, instrument_token,
                                   from_date, to_date, interval)

    Date format  :  DD/MM/YYYY  (NOT YYYY-MM-DD)
    Interval     :  "1" | "3" | "5" | "10" | "15" | "30" | "60" | "D"
    """
    import pandas as pd

    tok = INDEX_TOKENS.get(CFG["index"], INDEX_TOKENS["NIFTY"])
    to_dt   = datetime.now()
    from_dt = to_dt - timedelta(days=days)
    interval = str(tf_min)

    log.info(f"📅 Fetching {CFG['index']} {tf_min}min candles "
             f"{from_dt.strftime('%d/%m/%Y')} → {to_dt.strftime('%d/%m/%Y')}")
    log.info(f"   exchange_segment = {tok['exchange_segment']!r}")
    log.info(f"   instrument_token = {tok['instrument_token']!r}")
    log.info(f"   interval         = {interval!r}")

    try:
        resp = client.history(
            exchange_segment = tok["exchange_segment"],
            instrument_token = tok["instrument_token"],
            from_date        = from_dt.strftime("%d/%m/%Y"),
            to_date          = to_dt.strftime("%d/%m/%Y"),
            interval         = interval,
        )
    except AttributeError:
        log.error("❌ client.history() not found in your SDK version.")
        log.error("   Update: pip install -U neo_api_client")
        return None
    except Exception as e:
        log.error(f"❌ history() failed: {e}")
        return None

    # Parse response
    data = resp if isinstance(resp, list) else (resp or {}).get("data", [])
    log.info(f"   Raw response type : {type(resp).__name__}")
    log.info(f"   Data rows         : {len(data)}")

    if not data:
        log.warning("⚠️  No data returned.  Possible reasons:")
        log.warning("   1. Market was closed for all requested days")
        log.warning("   2. instrument_token is wrong")
        log.warning("   3. Kotak API downtime")
        return None

    rows = []
    for c in data:
        try:
            if isinstance(c, (list, tuple)) and len(c) >= 6:
                ts_raw = c[0]
                o, h, l, cl, v = float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])
            elif isinstance(c, dict):
                ts_raw = c.get("datetime") or c.get("time") or c.get("t")
                o  = float(c.get("open",   c.get("o", 0)))
                h  = float(c.get("high",   c.get("h", 0)))
                l  = float(c.get("low",    c.get("l", 0)))
                cl = float(c.get("close",  c.get("c", 0)))
                v  = float(c.get("volume", c.get("v", 0)))
            else:
                continue

            ts = pd.to_datetime(ts_raw, errors="coerce")
            if pd.isna(ts) or cl <= 0:
                continue
            rows.append({"datetime": ts, "open": o, "high": h,
                         "low": l, "close": cl, "volume": v})
        except Exception:
            continue

    if not rows:
        log.warning("⚠️  Parsed 0 valid rows from response.")
        log.info(f"   First raw item: {data[0]}")
        return None

    df = pd.DataFrame(rows).set_index("datetime").sort_index()
    log.info(f"\n── Historical Data Results ─────────────────────")
    log.info(f"  Bars      : {len(df)}")
    log.info(f"  From      : {df.index[0]}")
    log.info(f"  To        : {df.index[-1]}")
    log.info(f"  Close range: ₹{df['close'].min():.0f} – ₹{df['close'].max():.0f}")
    log.info(f"  Last close : ₹{df['close'].iloc[-1]:.2f}")
    log.info(f"\n{df.tail(5).to_string()}")
    log.info("  ✅ Historical data WORKING")
    return df


# ── Symbol generation test ────────────────────────────────────────────────
def test_symbol_generation(client: NeoAPI, spot_price: float = None):
    """
    Test option symbol generation and token lookup.
    Prints the exact symbol strings and their Kotak pSymbol tokens.
    """
    import re

    if spot_price is None:
        # Fetch live index LTP via REST
        index_name = INDEX_NAME_MAP.get(CFG["index"], "Nifty 50")
        try:
            resp = client.quotes(
                instrument_tokens=[{
                    "instrument_token": index_name,
                    "exchange_segment": NSE_SEG,
                }],
                quote_type="ltp",
            )
            for row in (resp if isinstance(resp, list) else [resp]):
                if isinstance(row, dict):
                    for k in ("ltp", "ltP", "LTP", "lp"):
                        v = row.get(k)
                        if v:
                            try:
                                spot_price = float(v)
                                break
                            except Exception:
                                pass
            if spot_price:
                log.info(f"Live spot: ₹{spot_price:.2f}")
            else:
                spot_price = 24000.0
                log.warning(f"Could not fetch live spot — using dummy ₹{spot_price}")
        except Exception as e:
            spot_price = 24000.0
            log.warning(f"REST quote failed ({e}) — using dummy ₹{spot_price}")

    step = CFG["strike_step"]

    # Correct ATM calculation (floor-round, not banker's round)
    atm = int((spot_price + step / 2) // step) * step
    log.info(f"\n── Symbol Generation ───────────────────────────")
    log.info(f"  Spot       : ₹{spot_price:.2f}")
    log.info(f"  Step       : {step}")
    log.info(f"  ATM Strike : {atm}  (banker's round would give: {int(round(spot_price/step))*step})")

    # Next expiry
    from datetime import date as _date
    today = _date.today()
    # Find next Tuesday (weekday=1)
    ahead = (1 - today.weekday()) % 7
    if ahead == 0: ahead = 7
    expiry_date = today + timedelta(days=ahead)
    expiry = expiry_date.strftime("%d%b%y").upper()
    log.info(f"  Expiry     : {expiry}")

    # Build test symbols
    test_symbols = []
    for mode, offset in [("ATM", 0), ("OTM1", 1), ("OTM2", 2)]:
        ce_strike = atm + offset * step
        pe_strike = atm - offset * step
        test_symbols.append((mode, "CE", f"{CFG['index']}{expiry}{ce_strike}CE"))
        test_symbols.append((mode, "PE", f"{CFG['index']}{expiry}{pe_strike}PE"))

    log.info(f"\n  {'MODE':<6} {'TYPE'} {'SYMBOL':<26} {'TOKEN'}")
    log.info(f"  {'─'*6} {'─'*4} {'─'*26} {'─'*10}")

    for mode, opt_type, symbol in test_symbols:
        # Token lookup
        m = re.match(r"^([A-Z]+?)(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$", symbol)
        token = "N/A"
        if m:
            _idx, _exp, _strike, _opt = m.groups()
            try:
                resp = client.search_scrip(exchange_segment=NFO_SEG, symbol=_idx)
                items = resp if isinstance(resp, list) else (resp or {}).get("data", [])
                for inst in (items or []):
                    if not isinstance(inst, dict): continue
                    ot = (inst.get("pOptionType") or "").strip().upper()
                    if ot != _opt: continue
                    raw_strike = str(inst.get("dStrikePrice") or "").rstrip(";")
                    try:
                        if str(int(float(raw_strike))) != _strike: continue
                    except Exception: continue
                    # Check expiry (simplified)
                    raw_exp = str(inst.get("pExpiryDate") or "").replace("-","").upper()
                    if _exp not in raw_exp and raw_exp[:5] not in _exp: continue
                    for f in ("pSymbol", "pTok", "tok"):
                        v = str(inst.get(f) or "").strip()
                        if v and v not in ("0", "-1", "None", ""):
                            token = v
                            break
                    if token != "N/A": break
            except Exception as e:
                token = f"ERR:{e}"

        log.info(f"  {mode:<6} {opt_type}   {symbol:<26} {token}")

    log.info(f"\n  ✅ Symbol generation complete")


# ── CLI ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Kotak Neo Data Feed Diagnostics")
    ap.add_argument("--test-ws",      action="store_true", help="Test WebSocket for 60s")
    ap.add_argument("--test-history", action="store_true", help="Fetch historical candles")
    ap.add_argument("--test-symbol",  action="store_true", help="Test symbol generation + token lookup")
    ap.add_argument("--all",          action="store_true", help="Run all tests")
    ap.add_argument("--ws-secs",      type=int, default=60, help="WS test duration (default 60)")
    args = ap.parse_args()

    if not any([args.test_ws, args.test_history, args.test_symbol, args.all]):
        ap.print_help()
        sys.exit(0)

    client = login()

    if args.test_ws or args.all:
        test_websocket(client, duration_secs=args.ws_secs)

    if args.test_history or args.all:
        test_historical(client, tf_min=5, days=5)

    if args.test_symbol or args.all:
        test_symbol_generation(client)
