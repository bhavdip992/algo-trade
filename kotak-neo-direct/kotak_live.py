"""
╔══════════════════════════════════════════════════════════════════════════╗
║   KOTAK NEO — Automated Options Trader  v7.0  (CE + PE)                ║
║   Fully automated: Login → Warmup → Signal → BUY CE/PE → SL → Trail   ║
╠══════════════════════════════════════════════════════════════════════════╣
║  ARCHITECTURE v7.0                                                      ║
║  ─────────────────────────────────────────────────────────────────────  ║
║  PRICE FEED: WebSocket PRIMARY → REST FALLBACK                          ║
║    • WS subscribes NIFTY index on startup → feeds CandleBuilder        ║
║    • WS also subscribes option contract after each trade entry          ║
║    • REST LTPPoller activates ONLY when WS is silent >30s              ║
║    • LiveLTPCache: thread-safe dict {token → ltp} updated by WS        ║
║                                                                         ║
║  TOKEN LOOKUP (3-pass robust):                                         ║
║    Pass 1: search_scrip(full_symbol) exact pTrdSymbol match            ║
║    Pass 2: search_scrip(index) broad filter: strike+expiry+opttype     ║
║            Handles ALL Kotak expiry formats:                           ║
║            "14APR26","14-APR-2026","14APR2026","2026-04-14"           ║
║    Pass 3: search_scrip(index+strike) narrow fallback                  ║
║    Token cached per-day. Empty token → SDK symbol string fallback.     ║
║                                                                         ║
║  TRADE EXECUTION:                                                       ║
║    1. Signal fires → build symbol (OTM1 default)                       ║
║    2. get_ltp_with_retry(): WS cache → REST, up to 3 attempts         ║
║    3. Fallback chain: OTM1 → ATM if LTP=0                             ║
║    4. Validate: token exists, LTP>0, session active                    ║
║    5. Subscribe option to WS for real-time exit monitoring             ║
║    6. Place BUY + SL-M orders                                          ║
╠══════════════════════════════════════════════════════════════════════════╣
║  RUN                                                                    ║
║    python3 kotak_live.py              ← live mode                      ║
║    python3 kotak_live.py --paper      ← paper trade (no real orders)   ║
║    python3 kotak_live.py --debug      ← verbose per-candle output      ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import os, sys, time, logging, threading, json, argparse, warnings, socket
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

import pyotp
from dotenv import load_dotenv

# ── Ensure script directory is on sys.path (for historical_data.py import) ──
import sys as _sys
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in _sys.path:
    _sys.path.insert(0, _SCRIPT_DIR)

# ── Load .env from same directory as this script ─────────────────────────────
_ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if not os.path.exists(_ENV_FILE):
    _up = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    if os.path.exists(_up):
        _ENV_FILE = _up
load_dotenv(_ENV_FILE)

# ════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════
ap = argparse.ArgumentParser(description="Kotak Neo Options Auto-Trader v7")
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

# Global socket timeout — prevents SDK HTTP calls from hanging forever
socket.setdefaulttimeout(8)

from signal_engine import SignalEngine, EngineConfig, SignalResult
from telegram_notify import (
    notify_startup, notify_trade_open, notify_trade_exit,
    notify_sl_trail, notify_daily_summary, TelegramCommandListener,
)
from dhan_warmup import warmup_from_dhan


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
# CONFIG
# ════════════════════════════════════════════════════════════════════════
PAPER = args.paper or _b("PAPER_TRADE", "true")

CFG = {
    "consumer_key":    _s("KOTAK_CONSUMER_KEY"),
    "mobile":          _s("KOTAK_MOBILE"),
    "ucc":             _s("KOTAK_UCC"),
    "mpin":            _s("KOTAK_MPIN"),
    "totp_secret":     _s("KOTAK_TOTP_SECRET"),
    "environment":     _s("ENVIRONMENT", "prod"),
    "paper":           PAPER,

    "index":           _s("INDEX",          "NIFTY").upper(),
    "lot_size":        _i("LOT_SIZE",        65),
    "strike_step":     _i("STRIKE_STEP",     50),
    "expiry_weekday":  _s("EXPIRY_WEEKDAY",  "TUE").upper(),
    "expiry_type":     _s("EXPIRY_TYPE",     "Weekly"),
    "strike_mode":     _s("STRIKE_MODE",     "OTM1"),
    "option_type":     _s("OPTION_TYPE",     "AUTO"),
    "timeframe":       _i("TIMEFRAME",       5),

    "capital":         _f("MAX_CAPITAL",     100000),
    "max_trades":      _i("MAX_TRADES",      3),
    "max_daily_loss":  _f("MAX_DAILY_LOSS",  2.0),
    "max_premium":     _f("MAX_PREMIUM",     500),
    "risk_pct":        _f("RISK_PCT",        2.0),
    "max_lots":        _i("MAX_LOTS",        1),

    "ema_fast":    _i("EMA_FAST",       9),
    "ema_mid":     _i("EMA_MID",       21),
    "ema_slow":    _i("EMA_SLOW",      50),
    "ema_trend":   _i("EMA_TREND",    200),
    "st_len":      _i("ST_ATR_LEN",    10),
    "st_factor":   _f("ST_FACTOR",    3.0),
    "rsi_len":     _i("RSI_LEN",       14),
    "rsi_ob":      _i("RSI_OB",        70),
    "rsi_os":      _i("RSI_OS",        30),
    "adx_thresh":  _i("ADX_THRESH",    20),
    "vol_mult":    _f("VOL_MULT",      1.2),
    "bb_len":      _i("BB_LEN",        20),
    "bb_std":      _f("BB_STD",       2.0),
    "min_conf":    _i("MIN_CONFIRMATIONS", 5),
    "sl_mode":     _s("SL_MODE",      "ATR"),
    "sl_atr":      _f("SL_ATR_MULT",  1.5),
    "sl_pct":      _f("SL_PCT",       0.8),
    "tgt_mode":    _s("TGT_MODE",     "2:1 RR"),
    "tgt_pct":     _f("TGT_PCT",      1.5),
    "tgt_atr":     _f("TGT_ATR_MULT", 3.0),
    "use_trail":   _b("USE_TRAIL",    "true"),
    "trail_trig":  _f("TRAIL_TRIG",   0.5),
    "trail_step":  _f("TRAIL_STEP",   0.3),

    "warmup_bars": 55,
    # WS becomes inactive and REST kicks in if no tick for this many seconds
    "ws_fallback_secs": 30,
}

_WDAY = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4}
CFG["expiry_wday"] = _WDAY.get(CFG["expiry_weekday"], 1)  # TUE default

NFO_SEG  = "nfo_fo"
NSE_SEG  = "nse_cm"
PRODUCT  = "INTRADAY"
VALIDITY = "DAY"

# Index name strings for WS subscribe (confirmed from Kotak docs)
INDEX_NAME_MAP = {
    "NIFTY":      "Nifty 50",
    "BANKNIFTY":  "Nifty Bank",
    "FINNIFTY":   "Nifty Fin Service",
    "MIDCPNIFTY": "Nifty MidCap 100",
}

# Numeric tokens for REST quotes (confirmed from Kotak scrip master)
INDEX_TOKENS = {
    "NIFTY":      {"instrument_token": "26000", "exchange_segment": NSE_SEG},
    "BANKNIFTY":  {"instrument_token": "26009", "exchange_segment": NSE_SEG},
    "FINNIFTY":   {"instrument_token": "26037", "exchange_segment": NSE_SEG},
    "MIDCPNIFTY": {"instrument_token": "26014", "exchange_segment": NSE_SEG},
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
# LIVE LTP CACHE  — shared memory updated by WebSocket ticks
# ════════════════════════════════════════════════════════════════════════
class LiveLTPCache:
    """
    Thread-safe store: instrument_token (str) → latest LTP (float).
    Updated by WebSocket on_message callback.
    Read by TrailMonitor and execute_trade for near-zero latency.

    Two namespaces:
      index_ltp  — current NIFTY/BANKNIFTY spot price (updated every WS tick)
      option_ltp — current LTP per subscribed option contract
    """
    def __init__(self):
        self._lock      = threading.Lock()
        self._prices    = {}      # token → ltp
        self._ts        = {}      # token → last update time
        self.index_ltp  = 0.0    # shortcut for the index itself
        self.index_ts   = 0.0    # time.time() of last index update

    def set(self, token: str, ltp: float):
        if ltp <= 0: return
        with self._lock:
            self._prices[token] = ltp
            self._ts[token]     = time.time()
            # If this looks like an index tick (>5000 for NIFTY/BANKNIFTY range)
            if ltp > 5000:
                self.index_ltp = ltp
                self.index_ts  = time.time()

    def get(self, token: str, max_age: float = 10.0) -> float:
        """Return cached LTP if fresher than max_age seconds, else 0."""
        with self._lock:
            ltp = self._prices.get(token, 0.0)
            ts  = self._ts.get(token, 0.0)
        if ltp > 0 and (time.time() - ts) < max_age:
            return ltp
        return 0.0

    def get_index(self, max_age: float = 5.0) -> float:
        """Return index LTP if fresh, else 0."""
        if self.index_ltp > 0 and (time.time() - self.index_ts) < max_age:
            return self.index_ltp
        return 0.0

    def age(self, token: str) -> float:
        """Seconds since last update for a token."""
        ts = self._ts.get(token, 0.0)
        return time.time() - ts if ts > 0 else 9999.0


ltp_cache = LiveLTPCache()


# ════════════════════════════════════════════════════════════════════════
# KOTAK SESSION
# ════════════════════════════════════════════════════════════════════════
class KotakSession:
    def __init__(self):
        self.client:                NeoAPI   = None
        self.logged_in:             bool     = False
        self.login_time:            datetime = None
        self._lock                           = threading.Lock()
        self._consecutive_failures: int     = 0

    def login(self) -> bool:
        """Full TOTP + MPIN login. Lock NOT held during network calls."""
        log.info("🔑 Logging into Kotak Neo...")
        client = NeoAPI(
            consumer_key=CFG["consumer_key"],
            environment=CFG["environment"],
            access_token=None,
            neo_fin_key=None,
        )
        totp = pyotp.TOTP(CFG["totp_secret"]).now()
        log.info(f"TOTP: {totp}")

        r1 = client.totp_login(mobile_number=CFG["mobile"], ucc=CFG["ucc"], totp=totp)
        if not r1 or not r1.get("data", {}).get("token"):
            raise RuntimeError(f"TOTP login failed: {r1}")
        log.info("TOTP ✓")

        r2 = client.totp_validate(mpin=CFG["mpin"])
        if not r2 or not r2.get("data", {}).get("token"):
            raise RuntimeError(f"MPIN failed: {r2}")
        log.info("MPIN ✓")

        with self._lock:
            self.client                = client
            self.logged_in             = True
            self.login_time            = datetime.now()
            self._consecutive_failures = 0
        log.info("✅ Login successful")
        return True

    def mark_failure(self):
        """Track API failures — auto re-login after 3 consecutive."""
        with self._lock:
            self._consecutive_failures += 1
            failures = self._consecutive_failures
        if failures >= 3:
            log.warning(f"⚠️  {failures} consecutive API failures — re-login")
            try:
                self.login()
            except Exception as e:
                log.error(f"Force re-login failed: {e}")

    def ensure_fresh(self):
        """Re-login if session >6 hours old or not set."""
        with self._lock:
            if not self.logged_in or self.client is None:
                need_login, age_h = True, 999.0
            else:
                age_h      = (datetime.now() - self.login_time).total_seconds() / 3600
                need_login = age_h > 6.0
        if need_login:
            log.info(f"🔄 Session refresh (age={age_h:.1f}h)")
            try:
                self.login()
            except Exception as e:
                log.error(f"Session refresh failed: {e}")

    def api(self) -> NeoAPI:
        """Return client. Skips session refresh if WS is actively delivering ticks."""
        # Avoid killing an active WS stream with a mid-session re-login.
        # Only call ensure_fresh if the stream is already dead.
        try:
            ws_alive = (time.time() - ws_manager._last_tick_ts) < 60
        except Exception:
            ws_alive = False
        if not ws_alive:
            self.ensure_fresh()
        return self.client


sess = KotakSession()


# ════════════════════════════════════════════════════════════════════════
# OPTION SYMBOL BUILDER
# ════════════════════════════════════════════════════════════════════════
def _next_expiry() -> str:
    """Return next expiry date as DDMMMYY e.g. '15APR25'."""
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


def build_option_symbol(option: str, underlying_px: float,
                        strike_mode: str = None) -> tuple:
    """
    Build option symbol string and return (symbol, strike, expiry).
    Example: ("NIFTY15APR2524000CE", 24000, "15APR25")

    ATM  = nearest strike (round half-up to step)
    OTM1 = 1 step above ATM for CE  / 1 step below for PE
    OTM2 = 2 steps from ATM
    ITM1 = 1 step below ATM for CE  / 1 step above for PE
    """
    mode   = strike_mode or CFG["strike_mode"]
    step   = CFG["strike_step"]
    atm    = int((underlying_px + step / 2) // step) * step  # floor-round, avoids banker rounding
    OFFSET = {"ATM": 0, "OTM1": 1, "OTM2": 2, "ITM1": -1, "ITM2": -2}
    dirn   = 1 if option == "CE" else -1
    strike = atm + OFFSET.get(mode, 0) * step * dirn
    expiry = _next_expiry()
    symbol = f"{CFG['index']}{expiry}{strike}{option}"
    return symbol, strike, expiry


# ════════════════════════════════════════════════════════════════════════
# TOKEN CACHE + LOOKUP
# ════════════════════════════════════════════════════════════════════════
_token_cache:      dict = {}    # "nfo_fo:SYMBOL" → numeric pSymbol string
_token_cache_date: date = None


def _expiry_ok(raw_exp: str, exp_ddmmmyy: str) -> bool:
    """
    Compare two expiry strings regardless of format.
    exp_ddmmmyy : our format  "15APR25"
    raw_exp     : API format  "15-APR-2025" | "15APR2025" | "15APR25" | "2025-04-15"

    Returns True if both represent the same calendar date.
    """
    if not raw_exp: return False
    norm = raw_exp.replace("-", "").replace("/", "").upper().strip()

    # Direct substring match first ("15APR25" in "15APR2025" is True)
    if exp_ddmmmyy in norm: return True

    # Parse both into date objects
    try:
        d1 = datetime.strptime(exp_ddmmmyy, "%d%b%y").date()
    except ValueError:
        return False

    for fmt in ("%d%b%Y", "%d%b%y", "%Y%m%d"):
        try:
            # trim to expected length
            d2 = datetime.strptime(norm[:len(fmt.replace("%Y","XXXX").replace("%d","XX").replace("%b","XXX").replace("%m","XX"))], fmt).date()
            if d1 == d2: return True
        except Exception:
            pass

    # "15APR2025" → parse day+mon+4-digit year
    m2 = re.match(r"(\d{1,2})([A-Z]{3})(\d{4})", norm)
    if m2:
        short = m2.group(1).zfill(2) + m2.group(2) + m2.group(3)[2:]  # "15APR25"
        try:
            if datetime.strptime(short, "%d%b%y").date() == d1:
                return True
        except ValueError:
            pass

    # ISO format "20250415"
    m3 = re.match(r"(\d{4})(\d{2})(\d{2})", norm)
    if m3:
        try:
            d2 = date(int(m3.group(1)), int(m3.group(2)), int(m3.group(3)))
            if d1 == d2: return True
        except ValueError:
            pass

    return False


def get_token(symbol: str, segment: str = NFO_SEG) -> str:
    """
    Resolve option symbol → numeric pSymbol token via Kotak Neo search_scrip.

    3-pass strategy:
      Pass 1: search_scrip(full_symbol)  — exact pTrdSymbol match
      Pass 2: search_scrip(index_name)   — broad, filter by strike+expiry+type
      Pass 3: search_scrip(index+strike) — narrower fallback

    Cache is invalidated at day start (options change daily).
    Returns "" if not found — caller falls back to symbol string.
    """
    global _token_cache, _token_cache_date

    today = date.today()
    if _token_cache_date != today:
        _token_cache.clear()
        _token_cache_date = today

    cache_key = f"{segment}:{symbol}"
    if cache_key in _token_cache:
        return _token_cache[cache_key]

    # Parse symbol: "NIFTY15APR2524000CE" → idx=NIFTY exp=15APR25 strike=24000 opt=CE
    m = re.match(r"^([A-Z]+?)(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$", symbol.upper())
    if not m:
        log.warning(f"Cannot parse option symbol: {symbol}")
        return ""
    _idx, _exp, _strike, _opt = m.groups()

    def _search(query: str) -> list:
        try:
            resp = sess.api().search_scrip(exchange_segment=segment, symbol=query)
            if isinstance(resp, list): return resp
            if isinstance(resp, dict):
                for k in ("data", "Data", "result", "message"):
                    v = resp.get(k)
                    if v: return v if isinstance(v, list) else [v]
        except Exception as e:
            log.debug(f"search_scrip({query!r}): {e}")
        return []

    def _tok(inst: dict) -> str:
        for f in ("pSymbol", "pTok", "tok", "symbol_token"):
            v = str(inst.get(f) or "").strip()
            if v and v not in ("0", "-1", "None", ""):
                return v
        return ""

    def _strike_match(inst: dict) -> bool:
        for f in ("dStrikePrice", "strikePrice", "strike"):
            raw = str(inst.get(f) or "").strip().rstrip(";")
            if raw:
                try:
                    return str(int(float(raw))) == _strike
                except ValueError:
                    pass
        return False

    def _try_match(items: list) -> str:
        for inst in items:
            if not isinstance(inst, dict): continue
            # Option type
            ot = (inst.get("pOptionType") or inst.get("optionType") or "").strip().upper()
            if ot and ot != _opt: continue
            # Strike
            if not _strike_match(inst): continue
            # Expiry
            raw_exp = str(inst.get("pExpiryDate") or inst.get("expiryDate") or "")
            if raw_exp and not _expiry_ok(raw_exp, _exp): continue
            tok = _tok(inst)
            if tok:
                return tok
        return ""

    try:
        # Pass 1: exact full-symbol search
        for inst in _search(symbol):
            if not isinstance(inst, dict): continue
            trd = (inst.get("pTrdSymbol") or inst.get("trdSym") or "").strip().upper()
            if trd == symbol.upper():
                tok = _tok(inst)
                if tok:
                    _token_cache[cache_key] = tok
                    log.info(f"✅ Token [P1] {symbol} → {tok}")
                    return tok

        # Pass 2: broad index search + field matching
        broad = _search(_idx) or _search(_idx.capitalize())
        log.debug(f"Token search({_idx}): {len(broad)} candidates")
        tok = _try_match(broad)
        if tok:
            _token_cache[cache_key] = tok
            log.info(f"✅ Token [P2] {symbol} → {tok}")
            return tok

        # Pass 3: narrow search with strike in query
        narrow = _search(f"{_idx}{_strike}")
        tok = _try_match(narrow)
        if tok:
            _token_cache[cache_key] = tok
            log.info(f"✅ Token [P3] {symbol} → {tok}")
            return tok

        log.warning(f"⚠️  Token not found for {symbol} — will use symbol string directly")

    except Exception as e:
        log.error(f"get_token({symbol}): {e}")
    return ""


# ════════════════════════════════════════════════════════════════════════
# WEBSOCKET  — PRIMARY price feed
# ════════════════════════════════════════════════════════════════════════
class WSManager:
    """
    Manages Kotak Neo WebSocket connection.  v7.1 — stable reconnect

    KEY DESIGN DECISIONS:
    ─────────────────────
    1. SINGLE client object: never replace sess.client during reconnect.
       The SDK keeps WS state inside the client.  Creating a new client
       breaks the WS handle and triggers "Connection is already closed".

    2. _reconnect_lock: only ONE reconnect at a time.
       Prevents the infinite loop: _on_close → reconnect → subscribe →
       _on_close → reconnect → ...

    3. Exponential back-off: 2s → 4s → 8s → 16s → 30s cap.
       After 5 failed subscribe attempts, do a full re-login.
       After 10 total failures, stop trying (prevent hammering the API).

    4. Watchdog only fires if BOTH: WS silent AND market is open.
       Avoids false reconnects at pre-open / after 15:30.

    5. Safe error stringification: SDK wraps some errors in objects
       whose __str__ returns None → we use _safe_str() everywhere.
    """

    MAX_RETRIES   = 10    # give up after this many consecutive failures
    RELOGIN_AFTER = 5     # full re-login after this many subscribe failures

    def __init__(self):
        self._lock            = threading.Lock()
        self._reconnect_lock  = threading.Lock()   # only ONE reconnect at a time
        self._subscriptions   = {}   # key → {"instrument_token":…, "exchange_segment":…}
        self._connected       = False
        self._last_tick_ts    = 0.0
        self._retry_count     = 0
        self._last_reconnect  = 0.0  # time.time() of last reconnect attempt

    # ── helpers ──────────────────────────────────────────────────────────
    @staticmethod
    def _safe_str(obj) -> str:
        """str() that never raises even if __str__ returns None (SDK bug)."""
        try:
            s = str(obj)
            return s if isinstance(s, str) else repr(obj)
        except Exception:
            return repr(obj)

    @property
    def is_alive(self) -> bool:
        """True if we received a WS tick within the last ws_fallback_secs seconds."""
        return (time.time() - self._last_tick_ts) < CFG["ws_fallback_secs"]

    # ── SDK callbacks ─────────────────────────────────────────────────────
    def _on_message(self, message):
        """Parse WS tick → update ltp_cache → feed candle_builder."""
        self._last_tick_ts = time.time()
        try:
            if isinstance(message, (str, bytes)):
                try:
                    data = json.loads(message)
                except Exception:
                    return
            else:
                data = message

            ticks = data if isinstance(data, list) else [data]
            for tick in ticks:
                if not isinstance(tick, dict):
                    continue
                ltp = _extract_ltp_from_tick(tick)
                if ltp <= 0:
                    continue

                tok_str = str(
                    tick.get("tk") or tick.get("token") or
                    tick.get("instrument_token") or ""
                )
                log.debug(f"WS tick: tok={tok_str!r} ltp={ltp}")

                if tok_str:
                    ltp_cache.set(tok_str, ltp)

                # Index tick → feed candle builder
                if ltp > 5000:
                    ltp_cache.set("INDEX", ltp)
                    candle_builder.tick(ltp, 0.0, datetime.now())

        except Exception as e:
            log.debug(f"WS on_message: {self._safe_str(e)}")

    def _on_open(self, message):
        """Called by SDK when WS connection established / restored."""
        with self._lock:
            self._connected   = True
            self._retry_count = 0
        log.info("✅ WebSocket connected")
        # Re-subscribe all known instruments (index + any open positions)
        with self._lock:
            subs = list(self._subscriptions.values())
        for tok_data in subs:
            is_idx = tok_data.get("isIndex", False)
            try:
                c = sess.client
                if c:
                    c.subscribe(
                        instrument_tokens=[{
                            "instrument_token": tok_data["instrument_token"],
                            "exchange_segment": tok_data["exchange_segment"],
                        }],
                        isIndex=is_idx,
                        isDepth=False,
                    )
            except Exception as e:
                log.debug(f"Re-subscribe {tok_data}: {self._safe_str(e)}")

    def _on_close(self, message):
        """Called by SDK when WS connection drops."""
        with self._lock:
            self._connected = False
        log.warning(f"WebSocket closed: {self._safe_str(message)}")
        # Do NOT reconnect here — the watchdog handles it with back-off.
        # Reconnecting inside _on_close causes the infinite loop.

    def _on_error(self, error):
        log.warning(f"WebSocket error: {self._safe_str(error)}")

    # ── internal subscribe ────────────────────────────────────────────────
    def _attach_callbacks_and_subscribe(self, tokens: list, is_index: bool):
        """
        Attach our callbacks to the current sess.client, then subscribe.
        MUST be called while NOT holding _reconnect_lock to avoid deadlock.
        """
        c = sess.client
        if c is None:
            raise RuntimeError("No client — not logged in")
        c.on_message = self._on_message
        c.on_error   = self._on_error
        c.on_close   = self._on_close
        c.on_open    = self._on_open
        c.subscribe(instrument_tokens=tokens, isIndex=is_index, isDepth=False)

    # ── public API ────────────────────────────────────────────────────────
    def subscribe_index(self):
        """Subscribe the configured index.  Called once at startup."""
        index_name = INDEX_NAME_MAP.get(CFG["index"])
        if not index_name:
            log.warning(f"No WS name for {CFG['index']}")
            return
        tok = {
            "instrument_token": index_name,
            "exchange_segment": NSE_SEG,
            "isIndex": True,
        }
        with self._lock:
            self._subscriptions["INDEX"] = tok
        log.info(f"📡 WS subscribing index: {index_name!r}")
        try:
            self._attach_callbacks_and_subscribe(
                [{"instrument_token": index_name, "exchange_segment": NSE_SEG}],
                is_index=True,
            )
        except Exception as e:
            log.warning(f"WS index subscribe: {self._safe_str(e)}")

    def subscribe_option(self, symbol: str, numeric_token: str):
        """Subscribe an option contract after trade entry."""
        if not numeric_token:
            log.debug(f"No token for {symbol} — WS option subscribe skipped")
            return
        tok = {
            "instrument_token": numeric_token,
            "exchange_segment": NFO_SEG,
            "isIndex": False,
        }
        with self._lock:
            self._subscriptions[numeric_token] = tok
        log.info(f"📡 WS option subscribe: {symbol} (tok={numeric_token})")
        try:
            self._attach_callbacks_and_subscribe(
                [{"instrument_token": numeric_token, "exchange_segment": NFO_SEG}],
                is_index=False,
            )
        except Exception as e:
            log.warning(f"WS option subscribe ({symbol}): {self._safe_str(e)}")

    def reconnect(self):
        """
        Reconnect with exponential back-off.  Thread-safe — only ONE
        reconnect runs at a time.  Does NOT create a new NeoAPI client
        unless the full re-login threshold is reached.
        """
        # One reconnect at a time
        acquired = self._reconnect_lock.acquire(blocking=False)
        if not acquired:
            log.debug("WS reconnect already in progress — skipping")
            return

        try:
            with self._lock:
                self._retry_count += 1
                attempt = self._retry_count

            if attempt > self.MAX_RETRIES:
                log.error(
                    f"WS: {attempt} consecutive failures — giving up. "
                    f"Check network / Kotak API status."
                )
                return

            # Exponential back-off: 2, 4, 8, 16, 30, 30, ...
            backoff = min(30, 2 ** attempt)
            log.info(f"🔄 WS reconnect attempt {attempt}/{self.MAX_RETRIES} (wait {backoff}s)")
            time.sleep(backoff)

            # Full re-login only every RELOGIN_AFTER failed subscribe attempts
            if attempt % self.RELOGIN_AFTER == 0:
                log.info(f"WS: {attempt} failures — doing full re-login")
                try:
                    sess.login()
                except Exception as e:
                    log.error(f"WS re-login failed: {self._safe_str(e)}")
                    return

            # Re-subscribe index (which also re-attaches all callbacks)
            try:
                self.subscribe_index()
                # Re-subscribe any open-position options
                with self._lock:
                    option_subs = {
                        k: v for k, v in self._subscriptions.items()
                        if k != "INDEX"
                    }
                for tok_str, tok_data in option_subs.items():
                    try:
                        self._attach_callbacks_and_subscribe(
                            [{"instrument_token": tok_data["instrument_token"],
                              "exchange_segment": tok_data["exchange_segment"]}],
                            is_index=False,
                        )
                    except Exception:
                        pass
                log.info(f"✅ WS reconnected (attempt {attempt})")
                with self._lock:
                    self._retry_count = 0   # reset on success

            except Exception as e:
                log.warning(f"WS re-subscribe failed (attempt {attempt}): {self._safe_str(e)}")

        finally:
            self._reconnect_lock.release()

    def start_watchdog(self):
        """
        Background thread: fires reconnect if WS is silent during market hours.
        Checks every 30s.  Will NOT fire:
          - outside 9:14 – 15:31 IST
          - on weekends
          - if a reconnect is already in progress
        """
        def _watch():
            time.sleep(90)   # generous startup grace period
            while True:
                try:
                    now    = datetime.now()
                    in_mkt = (
                        now.weekday() < 5 and
                        (now.hour > 9 or (now.hour == 9 and now.minute >= 14)) and
                        (now.hour < 15 or (now.hour == 15 and now.minute <= 31))
                    )
                    if in_mkt and not self.is_alive:
                        silence = time.time() - self._last_tick_ts
                        log.warning(f"⚠️  WS silent {silence:.0f}s — triggering reconnect")
                        self.reconnect()
                except Exception as e:
                    log.error(f"WSWatchdog: {self._safe_str(e)}")
                time.sleep(30)

        t = threading.Thread(target=_watch, daemon=True, name="WSWatchdog")
        t.start()


ws_manager = WSManager()


def _extract_ltp_from_tick(tick: dict) -> float:
    """Extract LTP from a Kotak Neo WS tick dict."""
    for key in ("lp", "ltP", "ltp", "LTP", "last_price", "last_traded_price", "c"):
        v = tick.get(key)
        if v is not None:
            try:
                f = float(v)
                if f > 0: return f
            except (TypeError, ValueError):
                pass
    return 0.0


# ════════════════════════════════════════════════════════════════════════
# REST QUOTE HELPERS
# ════════════════════════════════════════════════════════════════════════
def _parse_ltp_response(resp) -> float:
    """Parse any Kotak quotes() response shape → LTP float."""
    rows = []
    if isinstance(resp, list):
        rows = resp
    elif isinstance(resp, dict):
        for k in ("message", "data", "Data", "result"):
            v = resp.get(k)
            if v:
                rows = v if isinstance(v, list) else [v]
                break
        if not rows and "Error" not in str(resp) and "fault" not in str(resp):
            rows = [resp]

    for row in rows:
        if not isinstance(row, dict): continue
        for f in ("ltp", "ltP", "LTP", "last_traded_price",
                  "last_price", "lp", "lastPrice", "close"):
            v = row.get(f)
            if v is not None:
                try:
                    val = float(v)
                    if val > 0: return val
                except (TypeError, ValueError):
                    pass
    return 0.0


def get_index_ltp_rest() -> float:
    """Fetch NIFTY/BANKNIFTY index LTP via REST quotes()."""
    if CFG["paper"]: return 0.0
    index_name = INDEX_NAME_MAP.get(CFG["index"], "")
    if not index_name: return 0.0
    try:
        resp = sess.api().quotes(
            instrument_tokens=[{"instrument_token": index_name,
                                 "exchange_segment": NSE_SEG}],
            quote_type="ltp",
        )
        ltp = _parse_ltp_response(resp)
        if ltp > 0: log.debug(f"REST index LTP ₹{ltp:.1f}")
        return ltp
    except Exception as e:
        log.warning(f"REST index LTP error: {e}")
        sess.mark_failure()
        return 0.0


def get_option_ltp_rest(symbol: str) -> float:
    """
    Fetch option LTP via REST quotes().
    Uses numeric pSymbol token when available (bypasses symbol string validation).
    Logs full API response at WARNING when LTP=0 so you can debug.
    """
    if CFG["paper"]: return 0.0
    try:
        tok = get_token(symbol, NFO_SEG)
        candidates = [tok] if tok else []
        candidates.append(symbol)   # symbol string fallback

        for tok_val in candidates:
            if not tok_val: continue
            try:
                resp = sess.api().quotes(
                    instrument_tokens=[{"instrument_token": tok_val,
                                         "exchange_segment": NFO_SEG}],
                    quote_type="ltp",
                )
                ltp = _parse_ltp_response(resp)
                if ltp > 0:
                    log.debug(f"REST option LTP {symbol} ₹{ltp:.1f} [tok={tok_val}]")
                    return ltp
                # LTP=0 — log response for debugging
                log.warning(f"LTP=0 for {symbol} tok={tok_val!r}: {str(resp)[:250]}")
            except Exception as e:
                log.warning(f"option quotes({tok_val!r}): {e}")

    except Exception as e:
        log.error(f"get_option_ltp_rest({symbol}): {e}")
    return 0.0


def get_ltp_with_retry(symbol: str, retries: int = 3, delay: float = 1.0) -> float:
    """
    Get option LTP with retry. WS cache → REST, up to `retries` attempts.
    Returns 0.0 only if all attempts fail.
    """
    tok = get_token(symbol)
    for attempt in range(1, retries + 1):
        # Try WS cache first (freshest, zero latency)
        if tok:
            cached = ltp_cache.get(tok, max_age=5.0)
            if cached > 0:
                log.debug(f"LTP from WS cache {symbol} ₹{cached:.1f}")
                return cached
        # Fall back to REST
        ltp = get_option_ltp_rest(symbol)
        if ltp > 0:
            return ltp
        if attempt < retries:
            log.info(f"LTP=0 attempt {attempt}/{retries} for {symbol} — retrying in {delay}s")
            time.sleep(delay)
    return 0.0


# ════════════════════════════════════════════════════════════════════════
# BROKER  (order placement only — LTP now handled above)
# ════════════════════════════════════════════════════════════════════════
class Broker:
    def get_margin(self) -> float:
        if CFG["paper"]: return CFG["capital"]
        try:
            resp = sess.api().limits(segment="FO", exchange="NFO", product="MIS")
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
            resp = sess.api().place_order(
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
            resp = sess.api().modify_order(
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
            sess.api().cancel_order(order_id=order_id, isVerify=False)
            return True
        except Exception as e:
            log.error(f"cancel_order: {e}")
            return False


broker = Broker()


def _place_with_retry(symbol: str, qty: int, tx: str,
                      order_type: str = "MKT",
                      price: float = 0, trigger: float = 0,
                      retries: int = 3) -> str:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return broker.place_order(symbol, qty, tx=tx, order_type=order_type,
                                       price=price, trigger=trigger)
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
    option:       str
    entry_ltp:    float
    sl_prem:      float
    tgt_prem:     float
    lots:         int
    qty:          int
    entry_oid:    str
    sl_oid:       str
    ws_token:     str   = ""       # numeric WS token for this option
    peak_ltp:     float = 0.0
    pnl:          float = 0.0
    status:       str   = "OPEN"
    entry_time:   str   = ""
    exit_time:    str   = ""
    exit_ltp:     float = 0.0
    partial_done: bool  = False
    be_done:      bool  = False


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
            self.trades = 0; self.pnl = 0.0
            self.halted = False; self.candles = 0
            self._today = today

    def can_trade(self) -> tuple:
        self.reset_if_new_day()
        if self.halted: return False, "Trading HALTED"
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
    if opt_ltp <= 0: return CFG["max_lots"]
    try:
        margin = min(broker.get_margin(), CFG["capital"])
    except Exception:
        margin = CFG["capital"]
    usable  = margin * 0.80
    per_lot = opt_ltp * CFG["lot_size"]
    lots    = max(1, int(usable / per_lot))
    lots    = min(lots, CFG["max_lots"])
    log.info(f"Lots: margin=₹{margin:.0f} usable=₹{usable:.0f} per_lot=₹{per_lot:.0f} → {lots}L")
    return lots


# ════════════════════════════════════════════════════════════════════════
# SQUARE OFF
# ════════════════════════════════════════════════════════════════════════
def _get_exit_ltp(pos: Position) -> float:
    """Get exit LTP: WS cache → REST → fallback to entry."""
    if CFG["paper"]: return pos.entry_ltp
    # Try WS cache
    if pos.ws_token:
        cached = ltp_cache.get(pos.ws_token, max_age=10.0)
        if cached > 0: return cached
    # REST fallback
    ltp = get_option_ltp_rest(pos.symbol)
    return ltp if ltp > 0 else pos.entry_ltp


def square_off(symbol: str, reason: str = "MANUAL") -> dict:
    with state_lock:
        pos = state.positions.get(symbol)
    if not pos or pos.status != "OPEN":
        return {"error": f"No open position: {symbol}"}
    try:
        ltp = _get_exit_ltp(pos)
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
        notify_trade_exit(
            symbol=symbol, reason=reason, entry_ltp=pos.entry_ltp,
            exit_ltp=ltp, pnl=pnl, qty=pos.qty,
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
# TRADE EXECUTOR  — the most important function
# ════════════════════════════════════════════════════════════════════════
def execute_trade(result: SignalResult) -> bool:
    """
    Execute a trade from a signal.

    Flow:
      1. Guard checks (option type, time gate, day limits, duplicate)
      2. Build symbol (configured mode first, then fallback chain to ATM)
      3. get_ltp_with_retry(): WS cache → REST, 3 attempts, 1s apart
      4. Premium cap check
      5. Calculate lots / cost
      6. Place BUY order (with retry)
      7. Place SL-M order
      8. Subscribe option to WS for real-time monitoring
      9. Track position in state
    """
    option = "CE" if result.signal == "BUY_CE" else "PE"

    # ── Option type filter ────────────────────────────────────────────
    ot = CFG["option_type"]
    if ot == "CE Only" and option != "CE": return False
    if ot == "PE Only" and option != "PE": return False

    # ── Time gate 9:20 – 15:00 IST ───────────────────────────────────
    now     = datetime.now()
    bar_min = now.hour * 60 + now.minute
    if bar_min < 9 * 60 + 20:
        log.info("⏰ Before 9:20 — signal skipped"); return False
    if bar_min >= 15 * 60:
        log.info("⏰ After 15:00 — signal skipped"); return False

    # ── Day state ─────────────────────────────────────────────────────
    with state_lock:
        ok, reason = state.can_trade()
    if not ok:
        log.warning(f"🚫 {reason}"); return False

    # ── Build symbol + get LTP with fallback chain ────────────────────
    # Fallback: configured_mode → OTM1 → ATM
    MODES = [CFG["strike_mode"]]
    for m in ["OTM1", "ATM"]:
        if m not in MODES: MODES.append(m)

    symbol, opt_ltp, ws_token = "", 0.0, ""

    for mode in MODES:
        # get_validated_symbol: builds symbol AND cross-checks with Kotak
        # search_scrip to get the exact pTrdSymbol used for order placement.
        # Falls back to constructed symbol if search_scrip unavailable.
        sym, strike, expiry, tok = get_validated_symbol(option, result.close, mode)

        # Duplicate check (use validated symbol)
        with state_lock:
            if sym in state.positions and state.positions[sym].status == "OPEN":
                log.info(f"Already in {sym} — skip"); return False

        if CFG["paper"]:
            # Synthesise realistic premium from ATR
            atr       = result.atr if result.atr > 0 else 80.0
            mult      = {"ATM": 2.0, "OTM1": 1.3, "OTM2": 0.7, "ITM1": 3.5}.get(mode, 1.5)
            synth_ltp = round(atr * mult, 1)
            log.info(f"[PAPER] {sym} LTP=₹{synth_ltp} (mode={mode} K={strike} exp={expiry})")
            symbol, opt_ltp, ws_token = sym, synth_ltp, tok
            break

        log.info(f"📋 Trying {sym}  (spot={result.close:.0f} mode={mode} K={strike} exp={expiry})")
        ltp = get_ltp_with_retry(sym, retries=3, delay=1.0)

        if ltp > 0:
            symbol, opt_ltp, ws_token = sym, ltp, tok
            if mode != CFG["strike_mode"]:
                log.info(f"↳ Fell back to {mode} (configured={CFG['strike_mode']} returned 0)")
            break
        log.warning(f"LTP=0 for {sym} (mode={mode}) — trying next mode")

    if opt_ltp <= 0:
        log.error(
            f"❌ LTP=0 for all modes {MODES} — "
            f"check Kotak session / market hours / expiry date in .env"
        )
        return False

    # ── Premium cap ───────────────────────────────────────────────────
    if opt_ltp > CFG["max_premium"] and not CFG["paper"]:
        log.warning(f"Premium ₹{opt_ltp:.0f} > cap ₹{CFG['max_premium']:.0f} — trying OTM1")
        sym_otm1, _, _, tok_otm1 = get_validated_symbol(option, result.close, "OTM1")
        ltp_otm1 = get_ltp_with_retry(sym_otm1, retries=2)
        if 0 < ltp_otm1 <= CFG["max_premium"]:
            symbol   = sym_otm1
            opt_ltp  = ltp_otm1
            ws_token = tok_otm1
            log.info(f"↳ Using OTM1 {sym_otm1} ₹{opt_ltp:.1f} (within cap)")
        else:
            log.error(f"❌ Cannot find option within premium cap ₹{CFG['max_premium']:.0f}")
            return False

    # ── Position sizing ───────────────────────────────────────────────
    lots = calc_lots(opt_ltp)
    qty  = lots * CFG["lot_size"]
    cost = opt_ltp * qty

    if cost > CFG["capital"] * 0.90 and not CFG["paper"]:
        log.error(f"❌ Cost ₹{cost:.0f} > 90% capital ₹{CFG['capital']:.0f}"); return False

    # ── SL & Target (ATR-based) ───────────────────────────────────────
    atr_pct  = (result.atr / result.close) if result.close > 0 else 0.004
    sl_pct   = min(0.35, max(0.15, atr_pct * 1.5))
    sl_prem  = max(10.0, round(opt_ltp * (1.0 - sl_pct), 1))
    tgt_prem = round(opt_ltp * (1.0 + sl_pct * 2.5), 1)

    mode_lbl = "PAPER" if CFG["paper"] else "LIVE"
    log.info(
        f"\n{'='*62}\n"
        f"  [{mode_lbl}] {'BUY CALL (CE)' if option=='CE' else 'BUY PUT  (PE)'}\n"
        f"  Symbol  : {symbol}\n"
        f"  LTP     : ₹{opt_ltp:.1f}   Qty: {qty} ({lots}L)   Cost: ₹{cost:.0f}\n"
        f"  SL      : ₹{sl_prem:.1f}   Target: ₹{tgt_prem:.1f}\n"
        f"  Spot    : ₹{result.close:.1f}   Signal: {result.signal}\n"
        f"  Conf    : [{result.conf_str()}]  {result.confirmations}/8\n"
        f"  Reason  : {result.signal_reason}\n"
        f"{'='*62}"
    )

    # ── Entry order ───────────────────────────────────────────────────
    try:
        entry_oid = _place_with_retry(symbol, qty, tx="B", order_type="MKT")
    except Exception as e:
        log.error(f"Entry order FAILED: {e}"); return False

    # ── SL-M order ────────────────────────────────────────────────────
    sl_oid = ""
    try:
        sl_trigger = round(sl_prem * 0.995, 1)
        sl_oid = _place_with_retry(
            symbol, qty, tx="S", order_type="SL-M", trigger=sl_trigger)
        log.info(f"🛑 SL-M: {sl_oid} @ trig ₹{sl_trigger:.1f}")
    except Exception as e:
        log.warning(f"SL order failed — TrailMonitor will watch: {e}")

    # ── Subscribe option to WS for real-time monitoring ───────────────
    if ws_token and not CFG["paper"]:
        ws_manager.subscribe_option(symbol, ws_token)

    # ── Track position ────────────────────────────────────────────────
    pos = Position(
        symbol=symbol, option=option,
        entry_ltp=opt_ltp, sl_prem=sl_prem, tgt_prem=tgt_prem,
        lots=lots, qty=qty,
        entry_oid=entry_oid, sl_oid=sl_oid,
        ws_token=ws_token,
        peak_ltp=opt_ltp,
        entry_time=datetime.now().isoformat(),
    )

    with state_lock:
        state.positions[symbol] = pos
        state.trades += 1
        state.trade_log.append({
            "time": pos.entry_time, "symbol": symbol,
            "signal": result.signal, "option": option,
            "entry_ltp": opt_ltp, "sl_prem": sl_prem, "tgt_prem": tgt_prem,
            "lots": lots, "qty": qty, "cost": round(cost, 2),
            "entry_oid": entry_oid, "sl_oid": sl_oid,
            "mode": mode_lbl, "spot": result.close,
            "confs": result.confirmations, "reason": result.signal_reason,
        })

    log.info(
        f"🚀 [{mode_lbl}] TRADE OPEN: {symbol} | "
        f"{lots}L @ ₹{opt_ltp:.1f} | SL ₹{sl_prem:.1f} | Tgt ₹{tgt_prem:.1f}"
    )
    notify_trade_open(
        symbol=symbol, option=option, entry_ltp=opt_ltp,
        sl_prem=sl_prem, tgt_prem=tgt_prem, qty=qty, lots=lots,
        cost=cost, spot=result.close, confirmations=result.confirmations,
        reason=result.signal_reason, mode=mode_lbl,
    )
    return True


# ════════════════════════════════════════════════════════════════════════
# TRAILING SL MONITOR
# ════════════════════════════════════════════════════════════════════════
class TrailMonitor(threading.Thread):
    """
    Runs every 5s. For each open position:
      Stage 0 — SL hit → exit
      Stage 1 — Breakeven at 1:1 → shift SL to entry+0.5%
      Stage 2 — Partial exit at 2× entry (50% qty)
      Stage 3 — Full target at 2.5× entry
      Stage 4 — Trail: activate at +50%, 18% below peak
    LTP source: WS cache (primary) → REST (fallback)
    """
    def __init__(self, interval: int = 5):
        super().__init__(daemon=True, name="TrailMonitor")
        self.interval = interval

    def run(self):
        log.info(f"🔁 TrailMonitor started (every {self.interval}s)")
        while True:
            try: self._tick()
            except Exception as e: log.error(f"TrailMonitor: {e}")
            time.sleep(self.interval)

    def _get_ltp(self, pos: Position) -> float:
        """WS cache first, REST fallback."""
        if CFG["paper"]: return pos.entry_ltp * 1.05   # fake +5% for paper
        if pos.ws_token:
            cached = ltp_cache.get(pos.ws_token, max_age=10.0)
            if cached > 0: return cached
        return get_option_ltp_rest(pos.symbol)

    def _tick(self):
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
                ltp = self._get_ltp(pos)
                if ltp <= 0: continue
                self._manage(pos, sym, ltp)
            except Exception as e:
                log.warning(f"Position check ({sym}): {e}")

    def _manage(self, pos: Position, sym: str, ltp: float):
        if ltp > pos.peak_ltp:
            pos.peak_ltp = ltp

        # Stage 0: SL hit
        if ltp <= pos.sl_prem:
            log.info(f"🛑 SL HIT: {sym} ₹{ltp:.1f} <= ₹{pos.sl_prem:.1f}")
            square_off(sym, "SL_HIT"); return

        # Stage 1: Breakeven
        sl_range = pos.entry_ltp - pos.sl_prem
        be_trigger = pos.entry_ltp + sl_range
        if not pos.be_done and ltp >= be_trigger:
            new_be_sl = round(pos.entry_ltp * 1.005, 1)
            if new_be_sl > pos.sl_prem:
                old_sl = pos.sl_prem
                pos.sl_prem = new_be_sl
                pos.be_done = True
                log.info(f"🔒 BREAKEVEN {sym}: SL ₹{old_sl:.1f}→₹{new_be_sl:.1f}")
                if pos.sl_oid and not CFG["paper"]:
                    pos.sl_oid = broker.modify_order(
                        pos.sl_oid, sym, pos.qty,
                        new_trigger=round(new_be_sl * 0.995, 1),
                        new_price=new_be_sl,
                    )

        # Stage 2: Partial exit at 2×
        if not pos.partial_done and ltp >= pos.entry_ltp * 2.0:
            half_qty = pos.qty // 2
            if half_qty > 0:
                try:
                    _place_with_retry(sym, half_qty, tx="S", order_type="MKT")
                    pos.qty -= half_qty
                    pos.partial_done = True
                    partial_pnl = (ltp - pos.entry_ltp) * half_qty
                    with state_lock: state.pnl += partial_pnl
                    log.info(f"💰 PARTIAL EXIT {sym}: {half_qty}×₹{ltp:.1f} P&L ₹{partial_pnl:.0f}")
                except Exception as e:
                    log.warning(f"Partial exit failed: {e}")

        # Stage 3: Full target
        if ltp >= pos.tgt_prem:
            log.info(f"🎯 TARGET: {sym} ₹{ltp:.1f} >= ₹{pos.tgt_prem:.1f}")
            square_off(sym, "TGT_HIT"); return

        # Stage 4: Trailing SL at +50%, 18% below peak
        if ltp >= pos.entry_ltp * 1.5:
            new_sl = round(pos.peak_ltp * 0.82, 1)
            if new_sl > pos.sl_prem:
                old_sl = pos.sl_prem
                pos.sl_prem = new_sl
                log.info(f"📈 TRAIL SL {sym}: ₹{old_sl:.1f}→₹{new_sl:.1f} (peak=₹{pos.peak_ltp:.1f})")
                if pos.sl_oid and not CFG["paper"]:
                    pos.sl_oid = broker.modify_order(
                        pos.sl_oid, sym, pos.qty,
                        new_trigger=round(new_sl * 0.995, 1),
                        new_price=new_sl,
                    )


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
                        target=self.on_close, args=(closed,), daemon=True).start()
                self.cur = Candle(ts=ts, open=ltp, high=ltp, low=ltp,
                                  close=ltp, volume=vol)
            else:
                self.cur.high    = max(self.cur.high, ltp)
                self.cur.low     = min(self.cur.low,  ltp)
                self.cur.close   = ltp
                self.cur.volume += vol

    def flush(self):
        """Force-close in-progress candle (used at EOD)."""
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
                    target=self.on_close, args=(closed,), daemon=True).start()


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
# REST LTP POLLER  — FALLBACK feed (activates when WS is silent >30s)
# ════════════════════════════════════════════════════════════════════════
class LTPPoller(threading.Thread):
    """
    REST poll every 1s. ONLY feeds candle_builder when WS is silent.
    When WS is alive, this thread runs but candle_builder gets ticks from WS.
    This means LTPPoller is backup — it doesn't interfere with WS ticks.
    """
    def __init__(self, interval: float = 1.0):
        super().__init__(daemon=True, name="LTPPoller")
        self.interval     = interval
        self.errors       = 0
        self.last_ltp     = 0.0
        self._zero_streak = 0
        self._executor    = ThreadPoolExecutor(max_workers=1, thread_name_prefix="LTPCall")

    def _market_open(self) -> bool:
        now = datetime.now()
        return (
            now.weekday() < 5 and
            (now.hour > 9 or (now.hour == 9 and now.minute >= 14)) and
            now.hour < 16
        )

    def run(self):
        log.info(f"📡 LTPPoller started (REST fallback, every {self.interval}s)")
        while True:
            try:
                if not self._market_open():
                    self._zero_streak = 0
                    time.sleep(10); continue

                # If WS is alive, still poll but don't double-feed candle_builder
                ws_alive = ws_manager.is_alive

                try:
                    future = self._executor.submit(get_index_ltp_rest)
                    ltp    = future.result(timeout=10)
                except FuturesTimeoutError:
                    self.errors += 1
                    log.warning("REST LTP timeout — recycling executor (no re-login)")
                    try: self._executor.shutdown(wait=False)
                    except Exception: pass
                    self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="LTPCall")
                    time.sleep(2); continue

                if ltp > 0:
                    self.errors       = 0
                    self._zero_streak = 0
                    self.last_ltp     = ltp
                    ltp_cache.set("INDEX_REST", ltp)  # always update cache

                    # Only feed candle_builder if WS is dead (avoids double-ticks)
                    if not ws_alive:
                        log.debug(f"REST poll ₹{ltp:.1f} (WS silent, feeding candles)")
                        candle_builder.tick(ltp, 0.0, datetime.now())
                    else:
                        log.debug(f"REST poll ₹{ltp:.1f} (WS active, cache-only)")
                else:
                    self._zero_streak += 1
                    self.errors       += 1
                    if self._zero_streak >= 10:
                        # Only re-login if WS is also dead — REST returning 0
                        # with WS alive just means pre-open / no quote yet
                        if not ws_manager.is_alive:
                            log.warning(f"REST LTP=0 ×{self._zero_streak} AND WS dead — re-login")
                            try: sess.login()
                            except Exception as e: log.error(f"Re-login: {e}")
                        else:
                            log.debug(f"REST LTP=0 ×{self._zero_streak} (WS alive, skipping re-login)")
                        self._zero_streak = 0

            except Exception as e:
                self.errors += 1
                try:
                    err_str = str(e)
                    if not isinstance(err_str, str):
                        err_str = repr(e)
                except Exception:
                    err_str = f"<{type(e).__name__}>"
                log.warning(f"LTPPoller ({type(e).__name__}): {err_str}")
                if self.errors % 5 == 0:
                    try: sess.login()
                    except Exception: pass

            time.sleep(self.interval)


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

    # WS status indicator
    ws_ind = f"{GRN}WS✓{RST}" if ws_manager.is_alive else f"{RED}WS✗{RST}"

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
        f"  {ws_ind}"
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
# HISTORICAL WARMUP
# ════════════════════════════════════════════════════════════════════════
#
# DESIGN DECISION — two separate concerns:
#
#   WARMUP DATA  (indicators: EMA / ATR / VWAP / ORB)
#   ─────────────────────────────────────────────────
#   Source: yfinance  ^NSEI  (NIFTY 50) or  ^NSEBANK  (Bank Nifty)
#   Why:  yfinance pulls the official NSE index data — same source as
#         Kotak historical feed.  5-min bars are accurate to the tick.
#         No broker auth needed.  Always available pre-market.
#   Note: warmup data is ONLY used to seed EMA/ATR/VWAP/ORB.
#         It is NEVER used for entry prices or order placement.
#
#   TRADE SYMBOLS & LIVE LTP  (orders)
#   ───────────────────────────────────
#   Source: Kotak Neo API exclusively
#   • Exact pTrdSymbol  ← from get_validated_symbol() via search_scrip
#   • Live LTP          ← from WebSocket / REST quotes()
#   • All orders placed ← with symbol validated by Kotak scrip master
#
# This separation means: even if Kotak historical API doesn't exist on
# your SDK version, warmup succeeds and order symbols are always correct.

YF_TICKERS = {
    "NIFTY":      "^NSEI",
    "BANKNIFTY":  "^NSEBANK",
    "FINNIFTY":   "NIFTY_FIN_SERVICE.NS",
    "MIDCPNIFTY": "NIFTY_MIDCAP_100.NS",
}

# Map timeframe (minutes) to yfinance interval string
_YF_INTERVAL = {1: "1m", 3: "5m", 5: "5m", 10: "15m", 15: "15m",
                30: "30m", 60: "60m"}


def _warmup_yfinance(days: int) -> int:
    """
    Fetch NIFTY/BankNifty OHLCV from yfinance and feed into engine.

    yfinance '^NSEI' = official NSE NIFTY 50 index data.
    5-min bars available for last 60 days.  Accurate to < 1 tick for
    indicator calculation purposes.

    Returns number of bars loaded (0 on failure).
    """
    ticker = YF_TICKERS.get(CFG["index"])
    if not ticker:
        log.warning(f"No yfinance ticker for {CFG['index']}")
        return 0

    try:
        import yfinance as yf
        import pandas as _pd
        import warnings as _w
        _w.filterwarnings("ignore", category=FutureWarning)
        _w.filterwarnings("ignore", category=DeprecationWarning)
    except ImportError:
        log.warning("yfinance not installed — run: pip install yfinance")
        return 0

    yf_tf    = _YF_INTERVAL.get(CFG["timeframe"], "5m")
    # yfinance 1m/5m only goes back 60d; 60d is more than enough
    period   = f"{min(days, 59)}d"

    log.info(f"📥 yfinance {ticker}  {yf_tf}  {period}")
    try:
        df = yf.download(
            ticker,
            period=period,
            interval=yf_tf,
            auto_adjust=True,
            progress=False,
            multi_level_index=False,
        )
    except Exception as e:
        log.warning(f"yfinance download failed: {e}")
        return 0

    if df is None or df.empty:
        log.warning(f"yfinance returned empty DataFrame for {ticker}")
        return 0

    # Flatten MultiIndex columns if present (yfinance quirk)
    import pandas as _pd
    if isinstance(df.columns, _pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Normalise column names
    df.columns = [c.lower() for c in df.columns]
    for old, new in [("open","open"),("high","high"),("low","low"),
                     ("close","close"),("volume","volume")]:
        if old not in df.columns and new not in df.columns:
            log.warning(f"yfinance missing column: {old}")
            return 0

    # Convert timezone to IST (naive)
    if hasattr(df.index, "tz") and df.index.tz is not None:
        try:
            df.index = df.index.tz_convert("Asia/Kolkata").tz_localize(None)
        except Exception:
            df.index = df.index.tz_localize(None)

    # Filter to market hours 9:15 – 15:30 IST, weekdays only
    try:
        df = df.between_time("09:15", "15:30")
        df = df[df.index.dayofweek < 5]
    except Exception:
        pass

    df = df.dropna(subset=["close"])
    df = df[df["close"] > 0]

    count = 0
    for ts, row in df.iterrows():
        try:
            cl = float(row["close"])
            if cl <= 0:
                continue
            engine.update(
                timestamp = ts,
                open_     = float(row.get("open",  cl)),
                high      = float(row.get("high",  cl)),
                low       = float(row.get("low",   cl)),
                close     = cl,
                volume    = float(row.get("volume", 0)),
            )
            count += 1
        except Exception:
            continue

    log.info(f"✅ yfinance warmup: {count} bars  "
             f"{df.index[0].strftime('%d-%b %H:%M')} → "
             f"{df.index[-1].strftime('%d-%b %H:%M')}")
    return count


# ── get_validated_symbol: always use Kotak scrip master for order symbols ──
def get_validated_symbol(option: str, underlying_px: float,
                         mode: str = None) -> tuple:
    """
    Build a symbol string AND validate it against Kotak Neo search_scrip.

    Returns (exact_symbol, strike, expiry, numeric_token)
    where exact_symbol is the pTrdSymbol from Kotak scrip master.

    Falls back to constructed symbol if search_scrip is unavailable.
    This function is the SINGLE source of truth for order symbols.

    Why validation matters:
      Kotak uses pTrdSymbol format internally.  If our constructed string
      doesn't exactly match (e.g. wrong expiry date format, strike with
      decimal, index name variant), the order is rejected.  Fetching the
      actual pTrdSymbol from search_scrip eliminates this entirely.
    """
    sym, strike, expiry = build_option_symbol(option, underlying_px, mode)

    if CFG["paper"]:
        tok = get_token(sym)
        return sym, strike, expiry, tok

    # Try to get the exact pTrdSymbol from Kotak
    try:
        import re as _re
        m = _re.match(r"^([A-Z]+?)(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$", sym.upper())
        if not m:
            raise ValueError(f"Cannot parse {sym}")
        _idx, _exp, _strike, _opt = m.groups()

        resp = sess.client.search_scrip(exchange_segment=NFO_SEG, symbol=_idx)
        items = resp if isinstance(resp, list) else (resp or {}).get("data", [])

        for inst in (items or []):
            if not isinstance(inst, dict): continue
            # Match option type
            ot = (inst.get("pOptionType") or inst.get("optionType") or "").strip().upper()
            if ot != _opt: continue
            # Match strike
            raw_sk = str(inst.get("dStrikePrice") or "").rstrip(";").strip()
            try:
                if str(int(float(raw_sk))) != _strike: continue
            except Exception: continue
            # Match expiry
            raw_exp = str(inst.get("pExpiryDate") or "")
            if not _expiry_ok(raw_exp, _exp): continue
            # Found — get exact pTrdSymbol
            exact = (inst.get("pTrdSymbol") or inst.get("trdSym") or "").strip()
            tok   = str(inst.get("pSymbol") or inst.get("pTok") or "").strip()
            if exact:
                if exact != sym:
                    log.info(f"Symbol corrected: {sym!r} → {exact!r}")
                return exact, strike, expiry, tok
    except Exception as e:
        log.warning(f"Symbol validation fallback ({sym}): {e}")

    # Fallback: use constructed symbol as-is
    tok = get_token(sym)
    return sym, strike, expiry, tok


def warmup(days: int = 5):
    """
    Warm up indicator engine using Dhan historical candles API.
    POST https://api.dhan.co/v2/charts/intraday
    """
    needed = CFG["warmup_bars"]
    log.info(f"📚 Warmup: {CFG['index']} {CFG['timeframe']}min — need {needed} bars")

    from dhan_warmup import warmup_from_dhan
    count = warmup_from_dhan(engine, CFG["index"], CFG["timeframe"], days=days)

    bars = len(engine.df)
    rem  = max(0, needed - bars)
    if rem > 0:
        log.warning(
            f"⚠️  Partial warmup: {bars}/{needed} bars — "
            f"signals fire after {rem} more live candles (≈{rem * CFG['timeframe']} min)"
        )
    else:
        log.info(f"✅ Warmup complete — {bars} bars loaded")


# ════════════════════════════════════════════════════════════════════════
# MARKET HOURS GATE
# ════════════════════════════════════════════════════════════════════════
def wait_for_market():
    logged = False
    while True:
        try:
            now = datetime.now()
            if now.weekday() >= 5:
                if not logged: log.info("Weekend — waiting for Monday 9:15"); logged = True
                time.sleep(60); continue

            mo = now.replace(hour=9,  minute=15, second=0, microsecond=0)
            mc = now.replace(hour=15, minute=30, second=0, microsecond=0)

            if mo <= now <= mc:
                log.info("🟢 Market is open"); return
            if now > mc:
                if not logged: log.info("Market closed — waiting for tomorrow 9:15"); logged = True
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
    print(f"  Kotak Neo Options Auto-Trader  v7.0")
    print(f"  Mode         : {mode_lbl}")
    print(f"  Index        : {CFG['index']}")
    print(f"  Lot Size     : {CFG['lot_size']}")
    print(f"  Strike Step  : {CFG['strike_step']}")
    print(f"  Expiry       : {CFG['expiry_type']} {CFG['expiry_weekday']}")
    print(f"  Strike Mode  : {CFG['strike_mode']}")
    print(f"  Option Type  : {CFG['option_type']}  (AUTO | CE Only | PE Only)")
    print(f"  Timeframe    : {CFG['timeframe']} min")
    print(f"  Capital      : ₹{CFG['capital']:,.0f}")
    print(f"  Max Trades   : {CFG['max_trades']}/day")
    print(f"  Loss Limit   : {CFG['max_daily_loss']}% = ₹{CFG['capital']*CFG['max_daily_loss']/100:.0f}")
    print(f"  Max Premium  : ₹{CFG['max_premium']:.0f}/share")
    print(f"  Min Confs    : {CFG['min_conf']}/8")
    print(f"  Price Feed   : WebSocket PRIMARY + REST FALLBACK")
    print("=" * 65)

    # ── Validate credentials ─────────────────────────────────────────────
    REQUIRED = {
        "consumer_key":    "KOTAK_CONSUMER_KEY",
        "mobile":          "KOTAK_MOBILE",
        "ucc":             "KOTAK_UCC",
        "mpin":            "KOTAK_MPIN",
        "totp_secret":     "KOTAK_TOTP_SECRET",
    }
    missing = {env for cfg_k, env in REQUIRED.items() if not CFG[cfg_k]}
    if missing:
        log.error("=" * 60)
        log.error("STARTUP FAILED — Missing .env values:")
        for k in sorted(missing):
            log.error(f"  ✗  {k}")
        log.error(f"  .env: {_ENV_FILE}")
        log.error("=" * 60)
        sys.exit(1)

    log.info(f"✅ Credentials loaded from: {_ENV_FILE}")

    if not CFG["paper"]:
        print(f"  {RED}⚠️  LIVE MODE — REAL ORDERS WILL BE PLACED!{RST}")
        print("  Ctrl+C within 5s to abort...")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("  Aborted."); sys.exit(0)

    # 1. Login
    sess.login()
    notify_startup(CFG)

    # 1b. Diagnostics: log first search_scrip result so we can see token format
    log.info(f"🔍 Checking {CFG['index']} option token format...")
    try:
        _diag = sess.client.search_scrip(exchange_segment=NFO_SEG, symbol=CFG["index"])
        items = _diag if isinstance(_diag, list) else _diag.get("data", []) if isinstance(_diag, dict) else []
        if items and isinstance(items[0], dict):
            log.info(f"  Sample token keys: {list(items[0].keys())}")
            # Show a few option samples to see pExpiryDate format
            for item in items[:3]:
                log.info(
                    f"  pTrdSymbol={item.get('pTrdSymbol')!r}  "
                    f"pExpiryDate={item.get('pExpiryDate')!r}  "
                    f"pSymbol={item.get('pSymbol')}  "
                    f"pOptionType={item.get('pOptionType')!r}  "
                    f"dStrikePrice={item.get('dStrikePrice')}"
                )
    except Exception as _e:
        log.warning(f"Diagnostic search_scrip: {_e}")

    # 2. Warmup
    warmup(days=5)

    # 3. Wait for market
    wait_for_market()
    log.info("🟢 Market open — auto-trader v7.0 starting")

    # 4. Start WebSocket (PRIMARY feed) — must start before LTPPoller
    ws_manager.subscribe_index()
    ws_manager.start_watchdog()
    log.info("📡 WebSocket started (primary feed)")

    # 5. Start REST LTP Poller (FALLBACK — runs always, feeds candles only when WS silent)
    LTPPoller(interval=1.0).start()
    log.info("📊 REST LTPPoller started (fallback feed)")

    # 6. TrailMonitor
    TrailMonitor(interval=5).start()

    # 7. Telegram command listener
    TelegramCommandListener(
        get_state_fn=lambda: state,
        square_off_all_fn=square_off_all,
    ).start()

    # 7. Main loop
    log.info("✅ Running — Ctrl+C to stop")
    try:
        while True:
            time.sleep(30)
            # Only refresh session if WS has also been dead for >5 minutes.
            # Avoids the re-login loop that kills an active WS stream.
            ws_dead_secs = time.time() - ws_manager._last_tick_ts
            if not ws_manager.is_alive and ws_dead_secs > 300:
                try:
                    sess.ensure_fresh()
                except Exception as e:
                    log.warning(f"Session refresh: {e}")

            now = datetime.now()
            if now.weekday() < 5 and now.hour == 15 and 10 <= now.minute <= 14:
                candle_builder.flush()
                if state.open_positions():
                    log.info("⏰ 15:10 EOD backup — squaring off")
                    square_off_all("EOD")
                    time.sleep(120)

    except KeyboardInterrupt:
        log.info("\nCtrl+C — squaring off...")
        square_off_all("MANUAL")
        notify_daily_summary(state.trades, state.pnl, state.candles)
        print()
        print(f"  ── Session Summary ──────────────────────")
        print(f"  Trades   : {state.trades}")
        print(f"  P&L      : ₹{state.pnl:.2f}")
        print(f"  Candles  : {state.candles}")
        print(f"  Log      : kotak_live.log")
        print()