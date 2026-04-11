"""
╔══════════════════════════════════════════════════════════════════════════╗
║   KOTAK NEO — Automated Options Trader  v6.0  (CE + PE)                ║
║   Fully automated: Login → Warmup → Signal → BUY CE/PE → SL → Trail   ║
╠══════════════════════════════════════════════════════════════════════════╣
║  HOW IT WORKS                                                           ║
║  1. Login via TOTP + MPIN at startup                                   ║
║  2. Load 5 days of history from yfinance to warm up indicators          ║
║  3. Poll live index LTP every 1s via REST API  (primary feed)           ║
║  4. Build 5-min OHLC candles from ticks                                 ║
║  5. On each candle close → run 8-confirmation signal engine             ║
║  6. BUY CE signal → build ATM CE symbol → get LTP → place BUY order   ║
║  7. BUY PE signal → build ATM PE symbol → get LTP → place BUY order   ║
║  8. Place SL order immediately after entry                              ║
║  9. TrailMonitor thread watches open positions every 5s                 ║
║     → Trails SL up as option gains (15% below peak LTP)                ║
║     → Exits on target / SL hit / EOD 15:10                             ║
║ 10. Auto square-off all at 15:10 IST                                   ║
╠══════════════════════════════════════════════════════════════════════════╣
║  RUN                                                                    ║
║    py -3.11 kotak_live.py              ← live mode                     ║
║    py -3.11 kotak_live.py --paper      ← paper trade (no real orders)  ║
║    py -3.11 kotak_live.py --debug      ← verbose per-candle output     ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import os, sys, time, logging, threading, json, argparse, warnings, socket
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

import pyotp
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

# ════════════════════════════════════════════════════════════════════════
# CLI ARGS
# ════════════════════════════════════════════════════════════════════════
ap = argparse.ArgumentParser(description="Kotak Neo Options Auto-Trader")
ap.add_argument("--paper", action="store_true", help="Paper trade — no real orders")
ap.add_argument("--debug", action="store_true", help="Verbose per-candle output")
args, _ = ap.parse_known_args()

# ════════════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.DEBUG if args.debug else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kotak_live.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("KotakLive")

for _lib in ("yfinance", "urllib3", "peewee", "filelock",
             "requests", "httpx", "charset_normalizer", "neo_api_client"):
    logging.getLogger(_lib).setLevel(logging.WARNING)

# ════════════════════════════════════════════════════════════════════════
# SDK
# ════════════════════════════════════════════════════════════════════════
try:
    from neo_api_client import NeoAPI
except ImportError:
    log.error("neo_api_client not installed.")
    log.error('pip install "git+https://github.com/Kotak-Neo/kotak-neo-api.git#egg=neo_api_client"')
    sys.exit(1)

# ════════════════════════════════════════════════════════════════════════
# GLOBAL SOCKET TIMEOUT — PRIMARY hang prevention (works on Windows)
# ════════════════════════════════════════════════════════════════════════
# Without this, any dropped network connection causes the SDK's internal
# requests.get() / socket.recv() to block forever, freezing the LTPPoller
# thread for hours with zero log output.
# socket.setdefaulttimeout(8) applies to ALL socket operations in this process,
# including every HTTP call made by the Kotak Neo SDK.
socket.setdefaulttimeout(8)
log.info("🔒 Global socket timeout set to 8s (prevents thread hang on network drop)")

from signal_engine import SignalEngine, EngineConfig, SignalResult


# ════════════════════════════════════════════════════════════════════════
# ENV HELPERS
# ════════════════════════════════════════════════════════════════════════
def _s(k, d=""): return os.getenv(k, str(d)).strip()
def _b(k, d="false"): return _s(k, d).lower() in ("true", "1", "yes")
def _i(k, d):
    try: return int(_s(k, d))
    except: return int(d)
def _f(k, d):
    try: return float(_s(k, d))
    except: return float(d)


# ════════════════════════════════════════════════════════════════════════
# CONFIG  (all values from .env)
# ════════════════════════════════════════════════════════════════════════
PAPER = args.paper or _b("PAPER_TRADE", "true")

CFG = {
    "consumer_key":   _s("KOTAK_CONSUMER_KEY"),
    "mobile":         _s("KOTAK_MOBILE"),
    "ucc":            _s("KOTAK_UCC"),
    "mpin":           _s("KOTAK_MPIN"),
    "totp_secret":    _s("KOTAK_TOTP_SECRET"),
    "environment":    _s("ENVIRONMENT", "prod"),
    "paper":          PAPER,

    "index":          _s("INDEX", "NIFTY").upper(),
    "lot_size":       _i("LOT_SIZE",    30),
    "strike_step":    _i("STRIKE_STEP", 100),
    "expiry_weekday": _s("EXPIRY_WEEKDAY", "TUE").upper(),
    "expiry_type":    _s("EXPIRY_TYPE",  "Weekly"),
    "strike_mode":    _s("STRIKE_MODE",  "OTM1"),
    "option_type":    _s("OPTION_TYPE",  "AUTO"),   # AUTO | CE Only | PE Only
    "timeframe":      _i("TIMEFRAME",    5),

    "capital":        _f("MAX_CAPITAL",    20000),
    "max_trades":     _i("MAX_TRADES",     3),
    "max_daily_loss": _f("MAX_DAILY_LOSS", 3),
    "max_premium":    _f("MAX_PREMIUM",    500),
    "risk_pct":       _f("RISK_PCT",       2.0),
    "max_lots":       _i("MAX_LOTS",       1),

    "ema_fast":   _i("EMA_FAST",  9),
    "ema_mid":    _i("EMA_MID",   21),
    "ema_slow":   _i("EMA_SLOW",  50),
    "ema_trend":  _i("EMA_TREND", 200),
    "st_len":     _i("ST_ATR_LEN",  10),
    "st_factor":  _f("ST_FACTOR",   3.0),
    "rsi_len":    _i("RSI_LEN",  14),
    "rsi_ob":     _i("RSI_OB",   70),
    "rsi_os":     _i("RSI_OS",   30),
    "adx_thresh": _i("ADX_THRESH", 20),
    "vol_mult":   _f("VOL_MULT",   1.2),
    "bb_len":     _i("BB_LEN",    20),
    "bb_std":     _f("BB_STD",    2.0),
    "min_conf":   _i("MIN_CONFIRMATIONS", 5),
    "sl_mode":    _s("SL_MODE",      "ATR"),
    "sl_atr":     _f("SL_ATR_MULT",  1.5),
    "sl_pct":     _f("SL_PCT",       0.8),
    "tgt_mode":   _s("TGT_MODE",     "2:1 RR"),
    "tgt_pct":    _f("TGT_PCT",      1.5),
    "tgt_atr":    _f("TGT_ATR_MULT", 3.0),
    "use_trail":  _b("USE_TRAIL",    "true"),
    "trail_trig": _f("TRAIL_TRIG",   0.5),
    "trail_step": _f("TRAIL_STEP",   0.3),

    "warmup_bars": 55,   # bars needed before signals fire (EMA50 + buffer)
}

_WDAY = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4}
CFG["expiry_wday"] = _WDAY.get(CFG["expiry_weekday"], 2)

NFO_SEG  = "nfo_fo"
NSE_SEG  = "nse_cm"
PRODUCT  = "INTRADAY"
VALIDITY = "DAY"

# Kotak Neo pSymbol values (confirmed from search_scrip diagnostic)
# These are used for quotes() REST API and WS subscribe
INDEX_TOKENS = {
    "BANKNIFTY":  {"instrument_token": "26009",  "exchange_segment": NSE_SEG},
    "NIFTY":      {"instrument_token": "26000",  "exchange_segment": NSE_SEG},
    "FINNIFTY":   {"instrument_token": "26037",  "exchange_segment": NSE_SEG},
    "MIDCPNIFTY": {"instrument_token": "26014",  "exchange_segment": NSE_SEG},
}

YF_TICKERS = {
    "BANKNIFTY":  "^NSEBANK",
    "NIFTY":      "^NSEI",
    "FINNIFTY":   "NIFTY_FIN_SERVICE.NS",
    "MIDCPNIFTY": "NIFTY_MIDCAP_100.NS",
}


# ════════════════════════════════════════════════════════════════════════
# SIGNAL ENGINE
# ════════════════════════════════════════════════════════════════════════
engine = SignalEngine(EngineConfig(
    fast_len=CFG["ema_fast"],       mid_len=CFG["ema_mid"],
    slow_len=CFG["ema_slow"],       trend_len=CFG["ema_trend"],
    st_atr_len=CFG["st_len"],       st_factor=CFG["st_factor"],
    rsi_len=CFG["rsi_len"],         rsi_ob=CFG["rsi_ob"],    rsi_os=CFG["rsi_os"],
    use_vwap=True,
    vol_mult=CFG["vol_mult"],       vol_required=False,
    adx_len=14,                     adx_thresh=CFG["adx_thresh"],
    bb_len=CFG["bb_len"],           bb_std=CFG["bb_std"],
    min_confirmations=CFG["min_conf"],
    sl_mode=CFG["sl_mode"],         sl_atr_mult=CFG["sl_atr"],   sl_pct=CFG["sl_pct"],
    tgt_mode=CFG["tgt_mode"],       tgt_pct=CFG["tgt_pct"],      tgt_atr_mult=CFG["tgt_atr"],
    use_trail=CFG["use_trail"],     trail_trig=CFG["trail_trig"], trail_step=CFG["trail_step"],
    capital=CFG["capital"],         risk_pct=CFG["risk_pct"],
    lot_size=CFG["lot_size"],       max_lots=CFG["max_lots"],
    option_type=CFG["option_type"],
))


# ════════════════════════════════════════════════════════════════════════
# KOTAK SESSION
# ════════════════════════════════════════════════════════════════════════
class KotakSession:
    def __init__(self):
        self.client:              NeoAPI   = None
        self.logged_in:           bool     = False
        self.login_time:          datetime = None
        self._lock                         = threading.Lock()
        self._consecutive_failures: int   = 0   # FIX: track API failures for auto-relogin

    def login(self) -> bool:
        # FIX: _lock is NOT held inside login body to avoid deadlock when
        # ensure_fresh() → login() is called from multiple threads simultaneously.
        # The outer ensure_fresh() call is already serialised by the lock.
        print("🔑 Logging into Kotak Neo...", CFG)
        log.info("🔑 Logging into Kotak Neo...")
        client = NeoAPI(
            consumer_key=CFG["consumer_key"],
            environment=CFG["environment"],
            access_token=None,
            neo_fin_key=None,
        )
        totp = pyotp.TOTP(CFG["totp_secret"]).now()
        log.info(f"TOTP: {totp}")

        r1 = client.totp_login(
            mobile_number=CFG["mobile"], ucc=CFG["ucc"], totp=totp)
        if not r1 or not r1.get("data", {}).get("token"):
            raise RuntimeError(f"TOTP login failed: {r1}")
        log.info("TOTP ✓")

        r2 = client.totp_validate(mpin=CFG["mpin"])
        if not r2 or not r2.get("data", {}).get("token"):
            raise RuntimeError(f"MPIN failed: {r2}")
        log.info("MPIN ✓")

        # Atomically update state only after full success
        with self._lock:
            self.client                  = client
            self.logged_in               = True
            self.login_time              = datetime.now()
            self._consecutive_failures   = 0
        log.info("✅ Login successful")
        return True

    def mark_failure(self):
        """Called by API consumers when a request fails — triggers re-login after 3 failures."""
        with self._lock:
            self._consecutive_failures += 1
            failures = self._consecutive_failures
        if failures >= 3:
            log.warning(f"⚠️  {failures} consecutive API failures — forcing re-login")
            try:
                self.login()
            except Exception as e:
                log.error(f"Force re-login failed: {e}")

    def ensure_fresh(self):
        """Re-login if session age > 6 hours OR if client is not set."""
        # FIX: use a short-lived lock only for reading state, not for the whole login
        with self._lock:
            if not self.logged_in or self.client is None:
                need_login = True
                age_h      = 999.0
            else:
                age_h      = (datetime.now() - self.login_time).total_seconds() / 3600
                need_login = age_h > 6.0   # FIX: reduced from 6.5 → 6.0 for safety margin

        if need_login:
            log.info(f"🔄 Session refresh (age={age_h:.1f}h)")
            try:
                self.login()
            except Exception as e:
                log.error(f"Session refresh failed: {e}")


sess = KotakSession()


# ════════════════════════════════════════════════════════════════════════
# OPTION SYMBOL BUILDER
# ════════════════════════════════════════════════════════════════════════
def _next_expiry() -> str:
    today  = date.today()
    exp_wd = CFG["expiry_wday"]
    if CFG["expiry_type"] == "Monthly":
        y, m = today.year, today.month
        cands = []
        for d in range(1, 32):
            try:
                dd = date(y, m, d)
                if dd.weekday() == exp_wd:
                    cands.append(dd)
            except ValueError:
                break
        expiry = cands[-1] if cands else today
    else:
        ahead = (exp_wd - today.weekday()) % 7
        if ahead == 0:
            ahead = 7
        expiry = today + timedelta(days=ahead)
    return expiry.strftime("%d%b%y").upper()


def build_option_symbol(option: str, underlying_px: float, strike_mode: str = None) -> str:
    mode      = strike_mode or CFG["strike_mode"]
    step      = CFG["strike_step"]
    # ATM: round to nearest strike step
    atm       = int(round(underlying_px / step) * step)
    offsets   = {"ATM": 0, "OTM1": 1, "OTM2": 2, "ITM1": -1, "ITM2": -2}
    # For CE: OTM is higher strike (+), for PE: OTM is lower strike (-)
    direction = 1 if option == "CE" else -1
    offset    = offsets.get(mode, 0)
    strike    = atm + offset * step * direction
    expiry    = _next_expiry()
    symbol    = f"{CFG['index']}{expiry}{strike}{option}"
    log.info(
        f"📋 Symbol: {symbol}  "
        f"(spot={underlying_px:.0f} ATM={atm} mode={mode} "
        f"offset={offset} step={step} K={strike})"
    )
    return symbol


# ════════════════════════════════════════════════════════════════════════
# BROKER
# ════════════════════════════════════════════════════════════════════════
class Broker:
    _token_cache: dict = {}
    _cache_date:  date = None

    def _api(self) -> NeoAPI:
        sess.ensure_fresh()
        return sess.client

    def get_token(self, symbol: str, segment: str = NFO_SEG) -> str:
        """
        Get pSymbol token for an option contract.

        Strategy:
          1. Try search_scrip(symbol=full_symbol) — exact match on pTrdSymbol
          2. If no match, try search_scrip(symbol=index_name) — broader lookup,
             then match by parsed strike / expiry / option type from the symbol string.
          3. Log actual pTrdSymbol values so we can confirm format.
        """
        today = date.today()
        key   = f"{segment}:{symbol}"
        if self._cache_date != today:
            self._token_cache.clear()
            self._cache_date = today
        if key in self._token_cache:
            return self._token_cache[key]

        def _extract_tok(inst: dict) -> str:
            tok = str(inst.get("pSymbol") or inst.get("pTok") or inst.get("tok") or "")
            return tok if tok and tok not in ("0", "-1") else ""

        def _search_items(query: str) -> list:
            try:
                resp = self._api().search_scrip(exchange_segment=segment, symbol=query)
                if isinstance(resp, list):
                    return resp
                if isinstance(resp, dict):
                    for k in ("data", "Data", "result"):
                        if resp.get(k):
                            v = resp[k]
                            return v if isinstance(v, list) else [v]
            except Exception as e:
                log.debug(f"search_scrip({query!r}): {e}")
            return []

        try:
            # ── Pass 1: exact full-symbol search ──────────────────────────
            items = _search_items(symbol)

            if items:
                log.debug(f"search_scrip({symbol!r}) → {len(items)} result(s)")
                for _i, _inst in enumerate(items[:5]):
                    log.debug(
                        f"  scrip[{_i}]: pTrdSymbol={_inst.get('pTrdSymbol')!r} "
                        f"pSymbol={_inst.get('pSymbol')} "
                        f"pOptionType={_inst.get('pOptionType')} "
                        f"dStrikePrice={_inst.get('dStrikePrice')} "
                        f"pExpiryDate={_inst.get('pExpiryDate')}"
                    )
                for inst in items:
                    if not isinstance(inst, dict): continue
                    trd = (inst.get("pTrdSymbol") or inst.get("trdSym") or
                           inst.get("sym") or "")
                    if trd.strip().upper() == symbol.strip().upper():
                        tok = _extract_tok(inst)
                        if tok:
                            self._token_cache[key] = tok
                            log.info(f"✅ Token (exact) {symbol} → {tok}")
                            return tok
            else:
                log.debug(f"search_scrip({symbol!r}) returned no results")

            # ── Pass 2: broad index-name search + field-level matching ────
            # Parse symbol like  NIFTY07APR2622950CE  →  expiry=07APR26, strike=22950, opt=CE
            import re
            m = re.match(
                r'^([A-Z]+?)(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$', symbol.upper()
            )
            if m:
                _idx, _exp, _strike, _opt = m.groups()
                log.debug(f"Parsed: idx={_idx} exp={_exp} strike={_strike} opt={_opt}")
                broad_items = _search_items(_idx)
                log.debug(f"Broad search({_idx!r}) → {len(broad_items)} result(s)")
                for _i, _inst in enumerate(broad_items[:5]):
                    log.debug(
                        f"  broad[{_i}]: pTrdSymbol={_inst.get('pTrdSymbol')!r} "
                        f"pSymbol={_inst.get('pSymbol')} "
                        f"pOptionType={_inst.get('pOptionType')} "
                        f"dStrikePrice={_inst.get('dStrikePrice')} "
                        f"pExpiryDate={_inst.get('pExpiryDate')}"
                    )
                for inst in broad_items:
                    if not isinstance(inst, dict): continue
                    # Match option type
                    opt_type = (inst.get("pOptionType") or inst.get("optionType") or "").strip().upper()
                    if opt_type != _opt:
                        continue
                    # Match strike price (API may store as float string "22950.0000")
                    raw_strike = str(inst.get("dStrikePrice") or inst.get("strikePrice") or "")
                    try:
                        inst_strike = str(int(float(raw_strike)))
                    except Exception:
                        inst_strike = raw_strike.strip()
                    if inst_strike != _strike:
                        continue
                    # Match expiry — pExpiryDate may be "07-APR-2026" or "07APR26" etc.
                    raw_exp = str(inst.get("pExpiryDate") or inst.get("expiryDate") or "")
                    # Normalise to DDMMMYY format for comparison
                    raw_exp_norm = raw_exp.replace("-", "").upper()
                    exp_norm     = _exp.upper()  # e.g. "07APR26"
                    if exp_norm not in raw_exp_norm and raw_exp_norm[:7] != exp_norm:
                        continue
                    tok = _extract_tok(inst)
                    if tok:
                        actual_trd = inst.get("pTrdSymbol") or ""
                        log.info(f"✅ Token (field-match) {symbol} → {tok}  [pTrdSymbol={actual_trd!r}]")
                        self._token_cache[key] = tok
                        return tok

            log.debug(f"No token found for {symbol} after both search passes")

        except Exception as e:
            log.error(f"Token ({symbol}): {e}")
        return ""   # empty = will fall back to symbol string in get_option_ltp

    def get_option_ltp(self, symbol: str) -> float:
        """
        LTP of an option contract (NFO segment).

        Tries in order:
          1. Numeric pSymbol token from search_scrip (most reliable)
          2. Trading symbol string as token (fallback)

        The "Invalid neosymbol values" error means the symbol string
        format doesn't match what Kotak Neo expects. The numeric pSymbol
        token bypasses this validation entirely.
        """
        if CFG["paper"]: return 0.0
        try:
            tok = self.get_token(symbol, NFO_SEG)

            # Build list of token values to try
            candidates = []
            if tok and tok != symbol:
                candidates.append(tok)       # numeric pSymbol token (preferred)
            candidates.append(symbol)         # symbol string (fallback)

            for tok_val in candidates:
                if not tok_val:
                    continue
                try:
                    resp = self._api().quotes(
                        instrument_tokens=[{
                            "instrument_token": tok_val,
                            "exchange_segment":  NFO_SEG,
                        }],
                        quote_type="ltp",
                    )
                    log.debug(f"option quotes({tok_val!r}): {str(resp)[:200]}")

                    rows = []
                    if isinstance(resp, list):
                        rows = resp
                    elif isinstance(resp, dict):
                        for k in ("message", "data", "Data", "result"):
                            if resp.get(k):
                                v = resp[k]
                                rows = v if isinstance(v, list) else [v]
                                break
                        if not rows and "Error" not in resp and "fault" not in resp:
                            rows = [resp]

                    for row in rows:
                        if not isinstance(row, dict): continue
                        for f in ("last_traded_price", "ltp", "ltP", "LTP",
                                  "last_price", "lp", "lastPrice"):
                            v = row.get(f)
                            if v is not None:
                                try:
                                    ltp = float(v)
                                    if ltp > 0:
                                        log.debug(f"Option LTP {symbol}: ₹{ltp:.1f}")
                                        return ltp
                                except (TypeError, ValueError):
                                    pass
                except Exception as e:
                    log.debug(f"option quotes({tok_val!r}): {e}")

        except Exception as e:
            log.error(f"Option LTP ({symbol}): {e}")
        return 0.0

    def get_index_ltp(self) -> float:
        """
        Get live BANKNIFTY/NIFTY spot price.

        CONFIRMED from Kotak Neo official docs (webSocket.md):
          "Exchange Identifier is not a number in case of Indexes.
           Use the Index Name string in place of instrument_token."
          Example: {"instrument_token": "Nifty Bank", "exchange_segment": "nse_cm"}

        Index name strings:
          BANKNIFTY  → "Nifty Bank"
          NIFTY      → "Nifty 50"
          FINNIFTY   → "Nifty Fin Service"
          MIDCPNIFTY → "Nifty MidCap 100"
        """
        if CFG["paper"]: return 0.0
        index = CFG["index"]

        INDEX_NAME_MAP = {
            "BANKNIFTY":  "Nifty Bank",
            "NIFTY":      "Nifty 50",
            "FINNIFTY":   "Nifty Fin Service",
            "MIDCPNIFTY": "Nifty MidCap 100",
        }
        index_name = INDEX_NAME_MAP.get(index, "")
        if not index_name:
            log.warning(f"No index name mapping for {index}")
            return 0.0

        try:
            resp = self._api().quotes(
                instrument_tokens=[{
                    "instrument_token": index_name,
                    "exchange_segment":  NSE_SEG,
                }],
                quote_type="ltp",
            )
            log.debug(f"index quotes({index_name!r}): {str(resp)[:300]}")

            rows = []
            if isinstance(resp, list):
                rows = resp
            elif isinstance(resp, dict):
                for k in ("message", "data", "Data", "result"):
                    if resp.get(k):
                        v = resp[k]
                        rows = v if isinstance(v, list) else [v]
                        break
                if not rows and "Error" not in resp and "fault" not in resp:
                    rows = [resp]

            for row in rows:
                if not isinstance(row, dict):
                    continue
                for f in ("last_traded_price", "ltp", "ltP", "LTP",
                          "last_price", "lp", "lastPrice", "close", "Close"):
                    v = row.get(f)
                    if v is not None:
                        try:
                            ltp = float(v)
                            if ltp > 5000:
                                log.debug(f"✅ Index LTP ₹{ltp:.1f}")
                                return ltp
                        except (TypeError, ValueError):
                            pass

            log.debug(f"No valid LTP in response rows: {rows[:2]}")

        except Exception as e:
            # FIX: log at WARNING not DEBUG so session failures are visible in log
            log.warning(f"Index LTP error: {e}")
            sess.mark_failure()   # FIX: track failures → auto re-login after 3 consecutive

        return 0.0

    def get_margin(self) -> float:
        if CFG["paper"]: return CFG["capital"]
        try:
            resp = self._api().limits(segment="FO", exchange="NFO", product="MIS")
            if resp and resp.get("data"):
                d = resp["data"]
                return float(
                    d.get("net", 0) or d.get("marginAvailable", 0) or
                    d.get("cashmarginavailable", 0) or 0
                )
        except Exception as e:
            log.error(f"Margin: {e}")
        return CFG["capital"]

    def place_order(self, symbol: str, qty: int,
                    tx: str = "B", order_type: str = "MKT",
                    price: float = 0, trigger: float = 0) -> str:
        if CFG["paper"]:
            oid = f"PAPER_{tx}_{symbol[-10:]}_{int(time.time())}"
            log.info(f"[PAPER] {tx} {qty}×{symbol} {order_type} → {oid}")
            return oid
        try:
            resp = self._api().place_order(
                exchange_segment=NFO_SEG, product=PRODUCT,
                price=str(round(price, 1)) if price else "0",
                order_type=order_type, quantity=str(qty), validity=VALIDITY,
                trading_symbol=symbol, transaction_type=tx,
                amo="NO", disclosed_quantity="0", market_protection="0", pf="N",
                trigger_price=str(round(trigger, 1)) if trigger else "0",
                tag="OPTIONS_AUTO",
            )
            log.info(f"Order resp: {resp}")
            if resp and resp.get("data"):
                oid = str(
                    resp["data"].get("nOrdNo") or resp["data"].get("orderId") or
                    resp["data"].get("order_id") or ""
                )
                log.info(f"✅ Order: {oid} | {tx} {qty}×{symbol}")
                return oid
            if resp and resp.get("nOrdNo"):
                return str(resp["nOrdNo"])
            raise RuntimeError(f"Unexpected response: {resp}")
        except Exception as e:
            log.error(f"place_order FAILED: {e}")
            raise

    def modify_order(self, order_id: str, symbol: str, qty: int,
                     new_trigger: float, new_price: float) -> str:
        if CFG["paper"]:
            log.info(f"[PAPER] MODIFY {order_id} trig={new_trigger:.1f}")
            return order_id
        try:
            resp = self._api().modify_order(
                order_id=order_id, price=str(round(new_price, 1)),
                quantity=str(qty), validity=VALIDITY, disclosed_quantity="0",
                trigger_price=str(round(new_trigger, 1)), order_type="SL",
                exchange_segment=NFO_SEG, product=PRODUCT, trading_symbol=symbol,
            )
            if resp and resp.get("data"):
                return str(resp["data"].get("nOrdNo") or order_id)
        except Exception as e:
            log.error(f"modify_order: {e}")
        return order_id

    def cancel_order(self, order_id: str) -> bool:
        if CFG["paper"]: return True
        try:
            self._api().cancel_order(order_id=order_id, isVerify=False)
            return True
        except Exception as e:
            log.error(f"cancel_order: {e}")
            return False

    def get_positions(self) -> list:
        if CFG["paper"]: return []
        try:
            r = self._api().positions()
            return r.get("data", []) if r else []
        except Exception as e:
            log.error(f"positions: {e}")
            return []


broker = Broker()


# ════════════════════════════════════════════════════════════════════════
# ORDER HELPER — retry on transient failure
# ════════════════════════════════════════════════════════════════════════
def _place_with_retry(symbol: str, qty: int, tx: str,
                      order_type: str = "MKT",
                      price: float = 0, trigger: float = 0,
                      retries: int = 3) -> str:
    """
    Places an order with up to `retries` attempts.
    Returns order ID on success, raises on final failure.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return broker.place_order(
                symbol, qty, tx=tx, order_type=order_type,
                price=price, trigger=trigger,
            )
        except Exception as e:
            last_err = e
            if attempt < retries:
                log.warning(f"Order attempt {attempt} failed ({e}) — retrying in 1s...")
                time.sleep(1)
    raise RuntimeError(f"Order failed after {retries} attempts: {last_err}")


# ════════════════════════════════════════════════════════════════════════
# TRADING STATE
# ════════════════════════════════════════════════════════════════════════
@dataclass
class Position:
    symbol:       str
    option:       str      # CE | PE
    entry_ltp:    float    # option premium at entry
    sl_prem:      float    # SL level in option premium
    tgt_prem:     float    # hard target (2.5:1 RR) — used as reference
    lots:         int
    qty:          int
    entry_oid:    str
    sl_oid:       str
    peak_ltp:     float = 0.0
    pnl:          float = 0.0
    status:       str   = "OPEN"   # OPEN | SL_HIT | TGT_HIT | TRAIL_SL | EOD | MANUAL
    entry_time:   str   = ""
    exit_time:    str   = ""
    exit_ltp:     float = 0.0
    partial_done: bool  = False   # True after 50% partial exit at 2× entry
    be_done:      bool  = False   # True after breakeven SL shift


@dataclass
class DayState:
    trades:    int   = 0
    pnl:       float = 0.0
    halted:    bool  = False
    positions: dict  = field(default_factory=dict)
    trade_log: list  = field(default_factory=list)
    candles:   int   = 0
    _today:    date  = field(default_factory=date.today)

    def reset_if_new_day(self):
        today = date.today()
        if self._today != today:
            log.info("🗓 New trading day — resetting counters")
            self.trades  = 0
            self.pnl     = 0.0
            self.halted  = False
            self.candles = 0
            self._today  = today

    def can_trade(self) -> tuple:
        self.reset_if_new_day()
        if self.halted:
            return False, "Trading HALTED"
        if self.trades >= CFG["max_trades"]:
            return False, f"Max {CFG['max_trades']} trades/day reached"
        loss_limit = CFG["capital"] * CFG["max_daily_loss"] / 100
        if self.pnl <= -loss_limit:
            self.halted = True
            return False, f"Daily loss ₹{loss_limit:.0f} hit — HALTED"
        return True, "OK"

    def open_positions(self) -> dict:
        return {s: p for s, p in self.positions.items() if p.status == "OPEN"}


state      = DayState()
state_lock = threading.Lock()


# ════════════════════════════════════════════════════════════════════════
# LOT CALCULATOR
# ════════════════════════════════════════════════════════════════════════
def calc_lots(opt_ltp: float) -> int:
    if opt_ltp <= 0:
        return CFG["max_lots"]
    try:
        margin = min(broker.get_margin(), CFG["capital"])
    except Exception:
        margin = CFG["capital"]
    usable  = margin * 0.80
    per_lot = opt_ltp * CFG["lot_size"]
    lots    = max(1, int(usable / per_lot))
    lots    = min(lots, CFG["max_lots"])
    log.info(
        f"Lot calc: margin=₹{margin:.0f}  usable=₹{usable:.0f}  "
        f"per_lot=₹{per_lot:.0f}  → {lots} lot(s)"
    )
    return lots


# ════════════════════════════════════════════════════════════════════════
# SQUARE OFF
# ════════════════════════════════════════════════════════════════════════
def square_off(symbol: str, reason: str = "MANUAL") -> dict:
    with state_lock:
        pos = state.positions.get(symbol)
    if not pos or pos.status != "OPEN":
        return {"error": f"No open position: {symbol}"}
    try:
        ltp = broker.get_option_ltp(symbol)
        if ltp <= 0 and CFG["paper"]:
            ltp = pos.entry_ltp
        oid = broker.place_order(symbol, pos.qty, tx="S", order_type="MKT")
        if pos.sl_oid and not CFG["paper"]:
            broker.cancel_order(pos.sl_oid)
        pnl = (ltp - pos.entry_ltp) * pos.qty
        with state_lock:
            pos.status    = reason
            pos.pnl       = round(pnl, 2)
            pos.exit_ltp  = ltp
            pos.exit_time = datetime.now().isoformat()
            state.pnl    += pnl
        emoji = "✅" if pnl >= 0 else "❌"
        log.info(
            f"{emoji} EXIT [{reason}] {symbol} "
            f"| Entry ₹{pos.entry_ltp:.1f} → Exit ₹{ltp:.1f} "
            f"| P&L ₹{pnl:.0f}"
        )
        return {"symbol": symbol, "status": reason, "pnl": round(pnl, 2), "oid": oid}
    except Exception as e:
        log.error(f"square_off failed ({symbol}): {e}")
        return {"error": str(e)}


def square_off_all(reason: str = "MANUAL") -> list:
    with state_lock:
        syms = list(state.open_positions().keys())
    results = [square_off(s, reason) for s in syms]
    if results:
        log.info(f"Square-off all ({reason}): {len(results)} position(s)")
    return results


# ════════════════════════════════════════════════════════════════════════
# TRADE EXECUTOR
# ════════════════════════════════════════════════════════════════════════
def execute_trade(result: SignalResult) -> bool:
    """
    Places a BUY CE or BUY PE order based on signal.
    Steps:
      1. Validate signal type and time gate
      2. Build option symbol
      3. Get option LTP
      4. Apply premium cap (try OTM1 if ATM too expensive)
      5. Calculate lots
      6. Place BUY (entry) order
      7. Place SL (stop-loss) order
      8. Track position in state
    """
    option = "CE" if result.signal == "BUY_CE" else "PE"

    # Option type filter
    ot = CFG["option_type"]
    if ot == "CE Only" and option != "CE":
        log.info(f"Blocked: option_type=CE Only"); return False
    if ot == "PE Only" and option != "PE":
        log.info(f"Blocked: option_type=PE Only"); return False

    # Time gate 9:20 – 15:00 IST
    now     = datetime.now()
    bar_min = now.hour * 60 + now.minute
    if bar_min < 9 * 60 + 20:
        log.info("⏰ Before 9:20 — signal skipped"); return False
    if bar_min >= 15 * 60:
        log.info("⏰ After 15:00 — signal skipped"); return False

    # Day state checks
    with state_lock:
        ok, reason = state.can_trade()
    if not ok:
        log.warning(f"🚫 {reason}"); return False

    # Build symbol
    symbol = build_option_symbol(option, result.close)

    # Duplicate check
    with state_lock:
        if symbol in state.positions and state.positions[symbol].status == "OPEN":
            log.info(f"Already in {symbol} — skip"); return False

    # Get option LTP
    opt_ltp = broker.get_option_ltp(symbol)
    if opt_ltp <= 0:
        if CFG["paper"]:
            opt_ltp = round(result.atr * 1.5, 1) if result.atr > 0 else 150.0
            log.info(f"[PAPER] Synthetic LTP ₹{opt_ltp}")
        else:
            log.error(f"❌ LTP=0 for {symbol} — check market hours / symbol")
            return False

    # Premium cap
    if opt_ltp > CFG["max_premium"] and not CFG["paper"]:
        log.warning(f"Premium ₹{opt_ltp:.0f} > cap ₹{CFG['max_premium']:.0f} → try OTM1")
        symbol  = build_option_symbol(option, result.close, strike_mode="OTM1")
        opt_ltp = broker.get_option_ltp(symbol)
        if opt_ltp > CFG["max_premium"]:
            log.error(f"❌ OTM1 ₹{opt_ltp:.0f} still > cap"); return False

    # Lots & cost
    lots = calc_lots(opt_ltp)
    qty  = lots * CFG["lot_size"]
    cost = opt_ltp * qty

    if cost > CFG["capital"] * 0.90 and not CFG["paper"]:
        log.error(f"❌ Cost ₹{cost:.0f} > 90% capital"); return False

    # ── SL & Target — ATR-based option premium % ─────────────────────────
    # OTM options amplify index moves: use ATR/spot as proxy for move size,
    # then scale into option SL%. Clamp between 15%–35% of option premium.
    atr_pct  = (result.atr / result.close) if result.close > 0 else 0.004
    sl_pct   = min(0.35, max(0.15, atr_pct * 1.5))   # 15%–35% of premium
    sl_prem  = max(10.0, round(opt_ltp * (1.0 - sl_pct), 1))
    tgt_prem = round(opt_ltp * (1.0 + sl_pct * 2.5), 1)   # 2.5:1 RR

    mode = "PAPER" if CFG["paper"] else "LIVE"
    log.info(
        f"\n{'='*62}\n"
        f"  [{mode}] {'BUY CALL (CE)' if option=='CE' else 'BUY PUT  (PE)'}\n"
        f"  Symbol  : {symbol}\n"
        f"  LTP     : ₹{opt_ltp:.1f}   Qty: {qty} ({lots}L)   Cost: ₹{cost:.0f}\n"
        f"  SL      : ₹{sl_prem:.1f}   Target: ₹{tgt_prem:.1f}\n"
        f"  Spot    : ₹{result.close:.1f}   Signal: {result.signal}\n"
        f"  Conf    : [{result.conf_str()}]  {result.confirmations}/8\n"
        f"  Reason  : {result.signal_reason}\n"
        f"{'='*62}"
    )

    # Place ENTRY (BUY) order  — with retry
    try:
        entry_oid = _place_with_retry(symbol, qty, tx="B", order_type="MKT")
    except Exception as e:
        log.error(f"Entry order FAILED: {e}"); return False

    # Place SL (SELL SL-M) order  — SL-M fills at market on trigger hit
    sl_oid = ""
    try:
        sl_trigger = round(sl_prem * 0.995, 1)   # trigger slightly below SL price
        sl_oid = _place_with_retry(
            symbol, qty, tx="S", order_type="SL-M",
            trigger=sl_trigger,   # SL-M only needs trigger, no limit price
        )
        log.info(f"🛑 SL-M order: {sl_oid} @ trig ₹{sl_trigger:.1f}")
    except Exception as e:
        log.warning(f"SL order failed — TrailMonitor will watch manually: {e}")

    # Track position
    pos = Position(
        symbol=symbol, option=option,
        entry_ltp=opt_ltp, sl_prem=sl_prem, tgt_prem=tgt_prem,
        lots=lots, qty=qty,
        entry_oid=entry_oid, sl_oid=sl_oid,
        peak_ltp=opt_ltp,
        entry_time=datetime.now().isoformat(),
    )

    with state_lock:
        state.positions[symbol] = pos
        state.trades += 1
        state.trade_log.append({
            "time":       pos.entry_time,
            "symbol":     symbol,
            "signal":     result.signal,
            "option":     option,
            "entry_ltp":  opt_ltp,
            "sl_prem":    sl_prem,
            "tgt_prem":   tgt_prem,
            "lots":       lots,
            "qty":        qty,
            "cost":       round(cost, 2),
            "entry_oid":  entry_oid,
            "sl_oid":     sl_oid,
            "mode":       mode,
            "spot":       result.close,
            "confs":      result.confirmations,
            "reason":     result.signal_reason,
        })

    log.info(
        f"🚀 [{mode}] TRADE OPEN: {symbol} | "
        f"{lots}L @ ₹{opt_ltp:.1f} | "
        f"Cost ₹{cost:.0f} | SL ₹{sl_prem:.1f} | Tgt ₹{tgt_prem:.1f}"
    )
    return True


# ════════════════════════════════════════════════════════════════════════
# TRAILING SL MONITOR
# ════════════════════════════════════════════════════════════════════════
class TrailMonitor(threading.Thread):
    """
    Runs every 5s. For each open position:
    - Checks target hit → exits
    - Checks SL hit → exits
    - Trails SL up at 15% below peak LTP (greedy trail)
    - EOD at 15:10 → exits all
    """
    def __init__(self, interval: int = 5):
        super().__init__(daemon=True, name="TrailMonitor")
        self.interval = interval

    def run(self):
        log.info(f"🔁 TrailMonitor started (every {self.interval}s)")
        while True:
            try:
                self._tick()
            except Exception as e:
                log.error(f"TrailMonitor: {e}")
            time.sleep(self.interval)

    def _tick(self):
        # EOD
        now = datetime.now()
        if now.weekday() < 5 and now.hour == 15 and 10 <= now.minute <= 12:
            if state.open_positions():
                log.info("⏰ 15:10 EOD — squaring off all")
                square_off_all("EOD")
            return

        with state_lock:
            open_pos = list(state.open_positions().items())

        for sym, pos in open_pos:
            try:
                ltp = broker.get_option_ltp(sym)
                if ltp <= 0:
                    continue
                self._manage(pos, sym, ltp)
            except Exception as e:
                log.warning(f"Position check ({sym}): {e}")

    def _manage(self, pos: Position, sym: str, ltp: float):
        """
        Multi-stage position management:
          Stage 0 — SL hit → exit immediately
          Stage 1 — Breakeven: at 1:1 RR, move SL to entry+0.5%
          Stage 2 — Partial exit: at 2× entry, sell 50% qty
          Stage 3 — Trail: above 1.5× entry, trail 18% below peak
          Stage 4 — Full target (2.5× entry) → exit rest
        """
        # Update peak
        if ltp > pos.peak_ltp:
            pos.peak_ltp = ltp

        # ── Stage 0: SL hit ───────────────────────────────────────────────
        if ltp <= pos.sl_prem:
            log.info(f"🛑 SL HIT: {sym} ₹{ltp:.1f} <= ₹{pos.sl_prem:.1f}")
            square_off(sym, "SL_HIT"); return

        # ── Stage 1: Breakeven shift — at 1:1 RR, move SL to entry+0.5% ──
        sl_range = pos.entry_ltp - pos.sl_prem          # original risk in premium
        be_trigger = pos.entry_ltp + sl_range           # 1:1 from entry
        if not pos.be_done and ltp >= be_trigger:
            new_be_sl = round(pos.entry_ltp * 1.005, 1) # entry + 0.5%
            if new_be_sl > pos.sl_prem:
                old_sl = pos.sl_prem
                pos.sl_prem = new_be_sl
                pos.be_done = True
                log.info(
                    f"🔒 BREAKEVEN {sym}: SL ₹{old_sl:.1f} → ₹{new_be_sl:.1f} "
                    f"(entry=₹{pos.entry_ltp:.1f})"
                )
                if pos.sl_oid and not CFG["paper"]:
                    new_oid = broker.modify_order(
                        pos.sl_oid, sym, pos.qty,
                        new_trigger=round(new_be_sl * 0.995, 1),
                        new_price=new_be_sl,
                    )
                    with state_lock:
                        pos.sl_oid = new_oid

        # ── Stage 2: Partial exit — at 2× entry, sell 50% qty ─────────────
        if not pos.partial_done and ltp >= pos.entry_ltp * 2.0:
            half_qty = (pos.qty // 2) if pos.qty > 1 else 0
            if half_qty > 0:
                try:
                    _place_with_retry(sym, half_qty, tx="S", order_type="MKT")
                    pos.qty -= half_qty
                    pos.partial_done = True
                    partial_pnl = (ltp - pos.entry_ltp) * half_qty
                    with state_lock:
                        state.pnl += partial_pnl
                    log.info(
                        f"💰 PARTIAL EXIT {sym}: {half_qty} @ ₹{ltp:.1f} "
                        f"P&L ₹{partial_pnl:.0f} | keeping {pos.qty}"
                    )
                except Exception as e:
                    log.warning(f"Partial exit failed: {e}")

        # ── Stage 3: Full target (2.5× entry) → exit remainder ───────────
        if ltp >= pos.tgt_prem:
            log.info(f"🎯 TARGET: {sym} ₹{ltp:.1f} >= ₹{pos.tgt_prem:.1f}")
            square_off(sym, "TGT_HIT"); return

        # ── Stage 4: Trail — activate at +50%, trail 18% below peak ───────
        trail_activate = pos.entry_ltp * 1.5   # trail kicks in at +50% gain
        if ltp >= trail_activate:
            new_sl = round(pos.peak_ltp * 0.82, 1)   # 18% below peak
            if new_sl > pos.sl_prem:
                old_sl = pos.sl_prem
                pos.sl_prem = new_sl
                log.info(
                    f"📈 TRAIL SL {sym}: ₹{old_sl:.1f} → ₹{new_sl:.1f} "
                    f"(peak=₹{pos.peak_ltp:.1f})"
                )
                if pos.sl_oid and not CFG["paper"]:
                    new_oid = broker.modify_order(
                        pos.sl_oid, sym, pos.qty,
                        new_trigger=round(new_sl * 0.995, 1),
                        new_price=new_sl,
                    )
                    with state_lock:
                        pos.sl_oid = new_oid


# ════════════════════════════════════════════════════════════════════════
# CANDLE BUILDER
# ════════════════════════════════════════════════════════════════════════
@dataclass
class Candle:
    ts:     datetime
    open:   float = 0.0
    high:   float = 0.0
    low:    float = float("inf")
    close:  float = 0.0
    volume: float = 0.0


class CandleBuilder:
    def __init__(self, tf_min: int, on_close_fn):
        self.tf       = tf_min
        self.on_close = on_close_fn
        self.cur      = None
        self._lock    = threading.Lock()
        self.ticks    = 0
        self._last_ts = None

    def _floor(self, dt: datetime) -> datetime:
        m = dt.hour * 60 + dt.minute
        f = (m // self.tf) * self.tf
        return dt.replace(hour=f // 60, minute=f % 60, second=0, microsecond=0)

    def tick(self, ltp: float, vol: float = 0.0, dt: datetime = None):
        if ltp <= 0: return
        if dt is None: dt = datetime.now()
        with self._lock:
            self.ticks += 1
            ts = self._floor(dt)
            if self.cur is None or ts > self.cur.ts:
                if self.cur is not None and self.cur.ts != self._last_ts:
                    self._last_ts = self.cur.ts
                    closed = self.cur
                    log.info(
                        f"📊 Candle {closed.ts.strftime('%H:%M')} | "
                        f"O={closed.open:.0f} H={closed.high:.0f} "
                        f"L={closed.low:.0f} C={closed.close:.0f}"
                    )
                    threading.Thread(
                        target=self.on_close, args=(closed,), daemon=True
                    ).start()
                self.cur = Candle(
                    ts=ts, open=ltp, high=ltp, low=ltp, close=ltp, volume=vol
                )
            else:
                self.cur.high    = max(self.cur.high, ltp)
                self.cur.low     = min(self.cur.low,  ltp)
                self.cur.close   = ltp
                self.cur.volume += vol

    def flush(self):
        """
        FIX: Force-close the current in-progress candle.
        Call this at EOD (15:15) or on recovery after a feed interruption
        so the last partial candle is not silently dropped.
        Without this, any candle interrupted mid-period (e.g. poller restart)
        is lost and the signal engine never sees it.
        """
        with self._lock:
            if self.cur is not None and self.cur.ts != self._last_ts:
                self._last_ts = self.cur.ts
                closed = self.cur
                self.cur = None
                log.info(
                    f"📊 Candle FLUSH {closed.ts.strftime('%H:%M')} | "
                    f"O={closed.open:.0f} H={closed.high:.0f} "
                    f"L={closed.low:.0f} C={closed.close:.0f} (partial)"
                )
                threading.Thread(
                    target=self.on_close, args=(closed,), daemon=True
                ).start()


candle_builder = CandleBuilder(CFG["timeframe"], lambda c: on_candle_close(c))


# ════════════════════════════════════════════════════════════════════════
# CANDLE CLOSE HANDLER
# ════════════════════════════════════════════════════════════════════════
def on_candle_close(candle: Candle):
    result = engine.update(
        timestamp=candle.ts,
        open_=candle.open, high=candle.high,
        low=candle.low,    close=candle.close,
        volume=candle.volume,
    )
    with state_lock:
        state.candles += 1

    _print_dashboard(result, candle)
    if args.debug:
        _print_debug(result)

    if len(engine.df) < CFG["warmup_bars"]:
        return

    if result.signal in ("BUY_CE", "BUY_PE"):
        execute_trade(result)


# ════════════════════════════════════════════════════════════════════════
# TERMINAL DASHBOARD
# ════════════════════════════════════════════════════════════════════════
GRN  = "\033[92m"; RED  = "\033[91m"; YEL = "\033[93m"
GREY = "\033[90m"; BLU  = "\033[94m"; RST = "\033[0m"


def _print_dashboard(result: SignalResult, candle: Candle):
    now   = datetime.now().strftime("%H:%M:%S")
    mode  = f"{YEL}[PAPER]{RST}" if CFG["paper"] else f"{RED}[LIVE]{RST}"
    bars  = len(engine.df)
    need  = CFG["warmup_bars"]

    if bars < need:
        filled = int(bars / need * 20)
        print(
            f"\r{mode} {now} {CFG['index']} ₹{candle.close:.0f}  "
            f"{YEL}WARMUP [{'█'*filled}{'░'*(20-filled)}] {bars}/{need}{RST}",
            end="", flush=True
        )
        return

    sig = result.signal
    sc  = GRN if sig == "BUY_CE" else RED if sig == "BUY_PE" else GREY

    with state_lock:
        n_open  = len(state.open_positions())
        day_pnl = state.pnl
        trades  = state.trades

    pc = GRN if day_pnl >= 0 else RED
    print(
        f"\r{mode} {now} {CFG['index']:10s}"
        f" ₹{candle.close:>9.1f}"
        f"  RSI:{result.rsi:>5.1f}"
        f"  ADX:{result.adx:>5.1f}"
        f"  ATR:{result.atr:>6.1f}"
        f"  [{result.conf_str()}]{BLU}{result.confirmations}/8{RST}"
        f"  {sc}{sig:<8}{RST}"
        f"  T:{trades}/{CFG['max_trades']}"
        f"  Pos:{n_open}"
        f"  {pc}PnL:₹{day_pnl:.0f}{RST}",
        end="", flush=True
    )
    if sig != "NONE":
        print()


def _print_debug(result: SignalResult):
    print(
        f"\n  DEBUG | spot={result.close:.1f} RSI={result.rsi:.1f} "
        f"ADX={result.adx:.1f} ATR={result.atr:.1f} VWAP={result.vwap:.1f}"
    )
    print(
        f"  E={result.conf_ema_cross} T={result.conf_trend} "
        f"S={result.conf_supertrend} V={result.conf_vwap} "
        f"R={result.conf_rsi} A={result.conf_adx} "
        f"L={result.conf_volume} B={result.conf_no_squeeze}"
    )
    print(f"  → {result.signal_reason}")


# ════════════════════════════════════════════════════════════════════════
# REST LTP POLLER  — PRIMARY price feed
# ════════════════════════════════════════════════════════════════════════
class LTPPoller(threading.Thread):
    """
    Polls Kotak Neo REST API for index spot LTP every second.
    This is the PRIMARY feed — WebSocket is optional secondary.

    FIX v6.1:
    - Tracks consecutive LTP=0 returns and forces session re-login after 10
    - Catches NoneType __str__ SDK bug safely
    - Logs at WARNING (not DEBUG) when silent, so failures appear in log
    """
    def __init__(self, interval: float = 1.0):
        super().__init__(daemon=True, name="LTPPoller")
        self.interval      = interval
        self.errors        = 0
        self.last_ltp      = 0.0
        self._zero_streak  = 0          # FIX: consecutive zero-LTP counter
        self._last_candle  = time.time()  # FIX: watchdog: time of last candle tick

    def _market_open(self) -> bool:
        now = datetime.now()
        return (
            now.weekday() < 5 and
            (now.hour > 9 or (now.hour == 9 and now.minute >= 14)) and
            now.hour < 16
        )

    def run(self):
        log.info(f"📊 LTPPoller started — REST poll every {self.interval}s")
        # FIX: single-thread executor used to enforce a hard timeout on each
        # SDK HTTP call. Without this, a dropped network connection causes the
        # requests library to block forever — freezing this thread silently.
        _executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="LTPCall")

        while True:
            try:
                if not self._market_open():
                    self._zero_streak = 0
                    time.sleep(10)
                    continue

                # FIX: proactively refresh session before polling
                # Also wrapped with timeout in case login() hangs on network drop
                try:
                    fresh_future = _executor.submit(sess.ensure_fresh)
                    fresh_future.result(timeout=15)
                except FuturesTimeoutError:
                    log.warning("⚠️  ensure_fresh() timed out — network issue. Will retry.")
                    _executor.shutdown(wait=False)
                    _executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="LTPCall")
                    time.sleep(3)
                    continue
                except Exception as ef_e:
                    err_str = str(ef_e) if ef_e is not None else repr(ef_e)
                    log.warning(f"ensure_fresh error: {err_str}")

                # FIX: enforce 10-second timeout on the SDK HTTP call (backup to socket timeout)
                # Primary protection: socket.setdefaulttimeout(8) raises socket.timeout in the SDK
                # Backup protection: future.result(timeout=10) catches anything the socket missed
                try:
                    future = _executor.submit(broker.get_index_ltp)
                    ltp = future.result(timeout=10)
                except FuturesTimeoutError:
                    self.errors += 1
                    log.warning(
                        f"⚠️  Index LTP call timed out (>5s) — "
                        f"network issue or API unresponsive. Forcing re-login."
                    )
                    # Recreate executor so the hung thread doesn't block future calls
                    _executor.shutdown(wait=False)
                    _executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="LTPCall")
                    try:
                        sess.login()
                    except Exception as re_e:
                        log.error(f"Re-login after timeout failed: {re_e}")
                    time.sleep(2)
                    continue

                if ltp > 0:
                    self.errors       = 0
                    self._zero_streak = 0
                    if abs(ltp - self.last_ltp) > 0.01:
                        log.debug(f"Poll ₹{ltp:.1f}")
                    self.last_ltp     = ltp
                    self._last_candle = time.time()
                    candle_builder.tick(ltp, 0.0, datetime.now())
                else:
                    self._zero_streak += 1
                    self.errors       += 1

                    if self._zero_streak % 10 == 0:
                        log.warning(
                            f"⚠️  Index LTP=0 for {self._zero_streak} consecutive polls "
                            f"— forcing session re-login"
                        )
                        try:
                            sess.login()
                        except Exception as re_e:
                            log.error(f"Re-login after zero-streak failed: {re_e}")

                    elif self._zero_streak % 3 == 0:
                        log.warning(
                            f"⚠️  Index LTP=0 ({self._zero_streak}x) — "
                            f"check session / market hours"
                        )

            except Exception as e:
                self.errors += 1
                # FIX: safely convert exception to string — Kotak SDK sometimes
                # returns exceptions whose __str__() returns None (NoneType bug)
                err_str = str(e) if e is not None else repr(e)
                log.warning(f"LTPPoller error ({type(e).__name__}): {err_str}")
                # FIX: force re-login on any API exception after 5 errors
                if self.errors % 5 == 0:
                    log.warning("LTPPoller: forcing session re-login after repeated errors")
                    try:
                        sess.login()
                    except Exception as re_e:
                        log.error(f"Re-login failed: {re_e}")

            time.sleep(self.interval)


# ════════════════════════════════════════════════════════════════════════
# WEBSOCKET  — SECONDARY price feed
# ════════════════════════════════════════════════════════════════════════
_ws_last_tick   = time.time()
_ws_retry_count = 0


def _extract_ltp(data: dict) -> float:
    for key in ("lp", "ltP", "last_price", "ltp", "LTP", "c", "close"):
        v = data.get(key)
        if v is not None:
            try:
                f = float(v)
                if f > 0: return f
            except (TypeError, ValueError):
                pass
    return 0.0


def _on_tick(message):
    global _ws_last_tick
    _ws_last_tick = time.time()
    try:
        if isinstance(message, (str, bytes)):
            try: data = json.loads(message)
            except: return
        else:
            data = message
        ticks = data if isinstance(data, list) else [data]
        for tick in ticks:
            if not isinstance(tick, dict): continue
            ltp = _extract_ltp(tick)
            if ltp > 0:
                log.debug(f"WS ₹{ltp:.1f}")
                candle_builder.tick(ltp, 0.0, datetime.now())
    except Exception as e:
        log.debug(f"WS tick: {e}")


def _on_error(e):   log.error(f"WS error: {e}")
def _on_open(m):
    global _ws_retry_count
    _ws_retry_count = 0
    log.info(f"✅ WS connected: {m}")
def _on_close(m):   log.warning(f"WS closed: {m}")


def _start_ws():
    global _ws_retry_count
    # For WS subscribe, index also needs the name string not numeric token
    INDEX_NAME_MAP = {
        "BANKNIFTY":  "Nifty Bank",
        "NIFTY":      "Nifty 50",
        "FINNIFTY":   "Nifty Fin Service",
        "MIDCPNIFTY": "Nifty MidCap 100",
    }
    index_name = INDEX_NAME_MAP.get(CFG["index"], CFG["index"])
    tok = {"instrument_token": index_name, "exchange_segment": NSE_SEG}
    try:
        sess.ensure_fresh()
        c = sess.client
        c.on_message = _on_tick
        c.on_error   = _on_error
        c.on_close   = _on_close
        c.on_open    = _on_open
        log.info(f"📡 WS: {CFG['index']} name={index_name!r} seg={NSE_SEG}")
        c.subscribe(instrument_tokens=[tok], isIndex=True, isDepth=False)
        _ws_retry_count = 0
    except Exception as e:
        log.warning(f"WS subscribe: {e} (REST poller continues)")


class WSWatchdog(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True, name="WSWatchdog")

    def run(self):
        global _ws_retry_count
        log.info("👁 WSWatchdog started")
        time.sleep(60)
        while True:
            try:
                silence = time.time() - _ws_last_tick
                now = datetime.now()
                in_mkt = (
                    now.weekday() < 5 and
                    (now.hour > 9 or (now.hour == 9 and now.minute >= 15)) and
                    now.hour < 15
                )
                if in_mkt and silence > 300:
                    log.info(f"🔄 WS silent {silence/60:.0f}m — reconnecting")
                    _ws_retry_count += 1
                    _start_ws()
            except Exception as e:
                log.error(f"WSWatchdog: {e}")
            time.sleep(60)


# ════════════════════════════════════════════════════════════════════════
# HISTORICAL WARMUP
# ════════════════════════════════════════════════════════════════════════
def warmup(days: int = 5):
    needed = CFG["warmup_bars"]
    log.info(f"📚 Loading {days}d history ({needed} bars needed)...")

    # Try Kotak SDK first
    tok = INDEX_TOKENS.get(CFG["index"], {})
    if tok and sess.client:
        try:
            now     = datetime.now()
            from_dt = now - timedelta(days=days)
            resp = sess.client.historical_candles(
                instrument_token=tok["instrument_token"],
                exchange_segment=tok["exchange_segment"],
                to_date=now.strftime("%Y-%m-%d %H:%M:%S"),
                from_date=from_dt.strftime("%Y-%m-%d %H:%M:%S"),
                interval=str(CFG["timeframe"]),
            )
            if resp and resp.get("data"):
                count = 0
                for c in resp["data"]:
                    try:
                        if isinstance(c, list) and len(c) >= 6:
                            ts, o, h, l, cl, v = (
                                c[0], float(c[1]), float(c[2]),
                                float(c[3]), float(c[4]), float(c[5])
                            )
                        elif isinstance(c, dict):
                            ts = c.get("datetime") or c.get("time") or c.get("t")
                            o, h, l, cl, v = [
                                float(c.get(k, 0))
                                for k in ("open", "high", "low", "close", "volume")
                            ]
                        else:
                            continue
                        if cl > 0:
                            engine.update(ts, o, h, l, cl, v); count += 1
                    except Exception:
                        continue
                log.info(f"✅ SDK history: {count} candles")
                if len(engine.df) >= needed:
                    return
        except AttributeError:
            log.warning("historical_candles() not in SDK — trying yfinance")
        except Exception as e:
            log.warning(f"SDK history: {e}")

    # yfinance fallback
    ticker = YF_TICKERS.get(CFG["index"])
    if ticker:
        try:
            import pandas as _pd
            import yfinance as yf

            tf_map = {1: "1m", 3: "5m", 5: "5m", 10: "15m", 15: "15m"}
            yf_tf  = tf_map.get(CFG["timeframe"], "5m")
            period = "5d" if days <= 5 else "10d" if days <= 10 else "30d"
            log.info(f"yfinance: {ticker} {yf_tf} {period}")

            df = yf.download(
                ticker, period=period, interval=yf_tf,
                auto_adjust=True, progress=False, multi_level_index=False,
            )

            if df is not None and not df.empty:
                if isinstance(df.columns, _pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                def _val(v, default=0.0):
                    if isinstance(v, _pd.Series):
                        return float(v.iloc[0]) if len(v) > 0 else default
                    try: return float(v)
                    except: return default

                count = 0
                for ts, row in df.iterrows():
                    try:
                        o  = _val(row["Open"])
                        h  = _val(row["High"])
                        l  = _val(row["Low"])
                        cl = _val(row["Close"])
                        v  = _val(row.get("Volume", 1_000_000))
                        if cl > 0:
                            engine.update(ts, o, h, l, cl, v); count += 1
                    except Exception:
                        continue
                log.info(f"✅ yfinance: {count} candles → {len(engine.df)} in engine")
                if len(engine.df) >= needed:
                    return

        except ImportError:
            log.warning("yfinance not installed — pip install yfinance")
        except Exception as e:
            log.warning(f"yfinance: {e}")

    rem = max(0, needed - len(engine.df))
    if rem > 0:
        log.warning(
            f"⚠️  Cold start — need {rem} more live candles "
            f"(≈{rem * CFG['timeframe']} min). Signals fire once warmed up."
        )
    else:
        log.info(f"✅ Warmup complete — {len(engine.df)} bars")


# ════════════════════════════════════════════════════════════════════════
# MARKET HOURS GATE
# ════════════════════════════════════════════════════════════════════════
def wait_for_market():
    logged = False
    while True:
        try:
            now = datetime.now()
            if now.weekday() >= 5:
                if not logged:
                    log.info("Weekend — waiting for Monday 9:15")
                    logged = True
                time.sleep(60); continue

            mo = now.replace(hour=9, minute=15, second=0, microsecond=0)
            mc = now.replace(hour=15, minute=30, second=0, microsecond=0)

            if mo <= now <= mc:
                log.info("🟢 Market is open"); return

            if now > mc:
                if not logged:
                    log.info("Market closed — waiting for tomorrow 9:15")
                    logged = True
                time.sleep(60); continue

            secs = (mo - now).total_seconds()
            if not logged or int(secs) % 300 < 30:
                log.info(f"⏰ Market opens in {int(secs/60)}m {int(secs%60)}s")
                logged = True
            time.sleep(min(30, max(1, secs - 5)))

        except KeyboardInterrupt:
            log.info("Ctrl+C — exiting"); sys.exit(0)


# ════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    mode_lbl = f"{YEL}PAPER TRADE{RST}" if CFG["paper"] else f"{RED}LIVE TRADE — REAL MONEY{RST}"
    print("=" * 65)
    print(f"  Kotak Neo Options Auto-Trader  v6.0")
    print(f"  Mode         : {mode_lbl}")
    print(f"  Index        : {CFG['index']}")
    print(f"  Lot Size     : {CFG['lot_size']}")
    print(f"  Strike Step  : {CFG['strike_step']}")
    print(f"  Expiry       : {CFG['expiry_type']} {CFG['expiry_weekday']}")
    print(f"  Strike Mode  : {CFG['strike_mode']}")
    print(f"  Option Type  : {CFG['option_type']}  (AUTO=CE+PE | CE Only | PE Only)")
    print(f"  Timeframe    : {CFG['timeframe']} min")
    print(f"  Capital      : ₹{CFG['capital']:,.0f}")
    print(f"  Max Trades   : {CFG['max_trades']}/day")
    print(f"  Loss Limit   : {CFG['max_daily_loss']}% = ₹{CFG['capital']*CFG['max_daily_loss']/100:.0f}")
    print(f"  Max Premium  : ₹{CFG['max_premium']:.0f}/share")
    print(f"  Min Confs    : {CFG['min_conf']}/8")
    print(f"  Warmup Bars  : {CFG['warmup_bars']}")
    print(f"  Price Feed   : REST poll 1s (primary) + WebSocket (secondary)")
    print("=" * 65)
    print("  Conf: E=EMACross T=Trend S=SuperTrend V=VWAP")
    print("        R=RSI A=ADX L=Volume B=NoBBsqueeze")
    print()

    missing = [k for k in ("consumer_key","mobile","ucc","mpin","totp_secret") if not CFG[k]]
    if missing:
        log.error(f"Missing .env keys: {[k.upper() for k in missing]}")
        sys.exit(1)

    if not CFG["paper"]:
        print(f"  {RED}⚠️  LIVE MODE — REAL MONEY ORDERS WILL BE PLACED!{RST}")
        print("  Ctrl+C within 5s to abort...")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("  Aborted."); sys.exit(0)

    # 1. Login
    sess.login()

    # 1b. Find correct index instrument token via scrip master
    log.info(f"🔍 Finding {CFG['index']} instrument token...")
    try:
        _diag = sess.client.search_scrip(
            exchange_segment=NSE_SEG, symbol=CFG["index"])
        # Log ALL keys from first item to understand field names
        if isinstance(_diag, list) and _diag:
            _first = _diag[0]
            log.info(f"  search_scrip keys: {list(_first.keys())}")
            log.info(f"  first item: {_first}")
        elif isinstance(_diag, dict):
            _items = _diag.get("data", [])
            if _items:
                log.info(f"  search_scrip keys: {list(_items[0].keys())}")
                log.info(f"  first item: {_items[0]}")
            else:
                log.info(f"  search_scrip dict keys: {list(_diag.keys())}")
                log.info(f"  full resp: {_diag}")
        else:
            log.info(f"  search_scrip returned: type={type(_diag)} val={str(_diag)[:200]}")
    except Exception as _e:
        log.warning(f"  search_scrip diagnostic failed: {_e}")

    # 2. Warmup
    warmup(days=5)

    # 3. Wait for market
    wait_for_market()
    log.info("🟢 Market open — auto-trader starting")

    # 4. TrailMonitor
    TrailMonitor(interval=5).start()

    # 5. REST LTP Poller (PRIMARY)
    LTPPoller(interval=1.0).start()
    log.info("📊 REST LTP poller active (primary feed)")

    # 6. WebSocket (SECONDARY)
    _ws_last_tick = time.time()
    _start_ws()

    # 7. WS Watchdog
    WSWatchdog().start()

    # 8. Main loop
    log.info("✅ Running — Ctrl+C to stop and square off all")
    try:
        while True:
            time.sleep(30)
            try:
                sess.ensure_fresh()
            except Exception as e:
                log.warning(f"Session refresh: {e}")

            # EOD backup (TrailMonitor is primary EOD handler)
            now = datetime.now()
            if now.weekday() < 5 and now.hour == 15 and 10 <= now.minute <= 14:
                # FIX: flush partial candle before EOD square-off
                candle_builder.flush()
                if state.open_positions():
                    log.info("⏰ 15:10 EOD backup — squaring off")
                    square_off_all("EOD")
                    time.sleep(120)

    except KeyboardInterrupt:
        log.info("\nCtrl+C — squaring off all...")
        square_off_all("MANUAL")
        print()
        print(f"  ── Session Summary ──────────────────────")
        print(f"  Trades   : {state.trades}")
        print(f"  P&L      : ₹{state.pnl:.2f}")
        print(f"  Candles  : {state.candles}")
        print(f"  Log      : kotak_live.log")
        print()