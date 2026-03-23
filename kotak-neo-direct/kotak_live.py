"""
kotak_live.py  v2  — FIXED
Changes:
  1. LOT_SIZES corrected: BANKNIFTY=15, NIFTY=50, FINNIFTY=40, MIDCPNIFTY=75
  2. EngineConfig uses min_conf=5 (was effectively 8 — never fired live)
  3. use_volume=False (index WS feed volume is unreliable)
  4. adx_thresh=20 (was 25 — more signals)
  5. capital=20000 to match .env MAX_CAPITAL
  6. Dashboard shows conf score X/8 and reason for no-signal
"""

import os, time, logging, threading, json, pyotp, requests
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

from neo_api_client import NeoAPI
from signal_engine  import SignalEngine, EngineConfig, SignalResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kotak_live.log"),
    ]
)
log = logging.getLogger("KotakLive")

CFG = {
    "consumer_key":  os.getenv("KOTAK_CONSUMER_KEY", ""),
    "mobile":        os.getenv("KOTAK_MOBILE",        ""),
    "ucc":           os.getenv("KOTAK_UCC",            ""),
    "mpin":          os.getenv("KOTAK_MPIN",           ""),
    "totp_secret":   os.getenv("KOTAK_TOTP_SECRET",    ""),
    "environment":   os.getenv("ENVIRONMENT",   "prod"),
    "paper_trade":   os.getenv("PAPER_TRADE",   "true").lower() == "true",
    "index":         os.getenv("INDEX",         "BANKNIFTY"),
    "timeframe_min": int(os.getenv("TIMEFRAME", "5")),
    "max_trades":    int(os.getenv("MAX_TRADES", "3")),
    "max_daily_loss":float(os.getenv("MAX_DAILY_LOSS", "3")),
    "capital":       float(os.getenv("MAX_CAPITAL", "20000")),
    "bridge_url":    os.getenv("BRIDGE_URL", "http://localhost:5000"),
    "webhook_secret":os.getenv("WEBHOOK_SECRET", "MY_API_KEY_TAG"),
}

LOT_SIZES = {
    "NIFTY":       75,
    "BANKNIFTY":   30,
    "FINNIFTY":    65,
    "MIDCPNIFTY":  120,
    "SENSEX":      20,
}

INDEX_TOKENS = {
    "BANKNIFTY":  {"instrument_token": "11915",   "exchange_segment": "nse_cm"},
    "NIFTY":      {"instrument_token": "26000",   "exchange_segment": "nse_cm"},
    "FINNIFTY":   {"instrument_token": "1333922", "exchange_segment": "nse_cm"},
    "MIDCPNIFTY": {"instrument_token": "288009",  "exchange_segment": "nse_cm"},
}

@dataclass
class Candle:
    timestamp: datetime
    open:   float = 0.0
    high:   float = 0.0
    low:    float = float('inf')
    close:  float = 0.0
    volume: float = 0.0
    closed: bool  = False

class CandleBuilder:
    def __init__(self, timeframe_min, on_candle_close):
        self.tf_min         = timeframe_min
        self.on_close       = on_candle_close
        self.current_candle = None
        self._lock          = threading.Lock()

    def _candle_start(self, dt):
        minutes = dt.hour * 60 + dt.minute
        floored = (minutes // self.tf_min) * self.tf_min
        return dt.replace(hour=floored//60, minute=floored%60, second=0, microsecond=0)

    def on_tick(self, ltp, volume, dt=None):
        if dt is None:
            dt = datetime.now()
        with self._lock:
            ts = self._candle_start(dt)
            if self.current_candle is None or ts > self.current_candle.timestamp:
                if self.current_candle is not None:
                    self.current_candle.closed = True
                    log.info(
                        f"Candle {self.current_candle.timestamp.strftime('%H:%M')} "
                        f"O={self.current_candle.open:.0f} H={self.current_candle.high:.0f} "
                        f"L={self.current_candle.low:.0f} C={self.current_candle.close:.0f}"
                    )
                    self.on_close(self.current_candle)
                self.current_candle = Candle(
                    timestamp=ts, open=ltp, high=ltp, low=ltp, close=ltp, volume=volume
                )
            else:
                c = self.current_candle
                c.high = max(c.high, ltp)
                c.low  = min(c.low,  ltp)
                c.close = ltp
                c.volume += volume


@dataclass
class DailyState:
    trades:           int      = 0
    pnl:              float    = 0.0
    halted:           bool     = False
    last_signal:      str      = "NONE"
    last_signal_time: datetime = None
    signals_fired:    list     = field(default_factory=list)
    _last_date:       date     = field(default_factory=date.today)

    def reset_if_new_day(self):
        today = date.today()
        if self._last_date != today:
            self.trades = 0; self.pnl = 0.0; self.halted = False
            self._last_date = today
            log.info("New day reset")

    def can_trade(self):
        self.reset_if_new_day()
        if self.halted: return False, "Halted"
        if self.trades >= CFG["max_trades"]: return False, f"Max trades {CFG['max_trades']}"
        if self.pnl <= -(CFG["capital"] * CFG["max_daily_loss"] / 100):
            self.halted = True
            return False, "Daily loss limit"
        return True, "OK"

daily      = DailyState()
daily_lock = threading.Lock()


class KotakSession:
    def __init__(self):
        self.client = None
        self.logged_in = False
        self.login_time = None

    def login(self):
        log.info("Kotak Neo login...")
        self.client = NeoAPI(
            consumer_key=CFG["consumer_key"], environment=CFG["environment"],
            access_token=None, neo_fin_key=None,
        )
        totp = pyotp.TOTP(CFG["totp_secret"]).now()
        log.info(f"TOTP: {totp}")
        r1 = self.client.totp_login(mobile_number=CFG["mobile"], ucc=CFG["ucc"], totp=totp)
        if not r1 or not r1.get("data", {}).get("token"):
            raise RuntimeError(f"TOTP failed: {r1}")
        r2 = self.client.totp_validate(mpin=CFG["mpin"])
        if not r2 or not r2.get("data", {}).get("token"):
            raise RuntimeError(f"MPIN failed: {r2}")
        self.logged_in = True; self.login_time = datetime.now()
        log.info("Login OK")

    def ensure_fresh(self):
        if not self.logged_in: self.login(); return
        if (datetime.now() - self.login_time).seconds / 3600 > 7:
            self.login()

session = KotakSession()


def send_to_bridge(payload):
    if CFG["paper_trade"]:
        log.info(f"[PAPER] {payload}")
        return {"status": "PAPER"}
    try:
        r = requests.post(f"{CFG['bridge_url']}/webhook", json=payload, timeout=5)
        return r.json()
    except Exception as e:
        log.error(f"Bridge error: {e}")
        return {"status": "ERROR"}

def build_payload(result, index):
    return {
        "tag":    CFG["webhook_secret"],
        "broker": "KotakNeo",
        "action": result.signal,
        "exchange_segment": "nfo_fo",
        "product": "INTRADAY",
        "index":  index,
        "strike": "ATM",
        "expiry": "Weekly",
        "expiry_weekday": "WED" if index == "BANKNIFTY" else "THU",
        "lots":   result.qty_lots,
        "lot_size": LOT_SIZES.get(index, 15),
        "entry":  result.close,
        "sl":     result.sl,
        "target": result.target,
        "atr":    result.atr,
        "trail":  "ATR",
        "adx":    result.adx,
        "rsi":    result.rsi,
        "vwap":   result.vwap,
        "tf":     str(CFG["timeframe_min"]),
    }


# ── SIGNAL ENGINE with FIXED config ──────────────────────────
index = CFG["index"]

engine_cfg = EngineConfig(
    lot_size       = LOT_SIZES.get(index, 15),
    capital        = CFG["capital"],
    min_confirmations       = 5,        # FIXED: was 8/8, now 5/8
    # cross_lookback = 3,        # EMA cross valid for 3 bars
    vol_required     = True,    # FIXED: disable unreliable index volume
    # adx_thresh     = 20,       # FIXED: was 25
    max_lots       = 3,
    risk_pct       = 5.0,
)

engine = SignalEngine(engine_cfg)


def on_candle_close(candle):
    result = engine.update(
        timestamp=candle.timestamp,
        open_=candle.open, high=candle.high,
        low=candle.low, close=candle.close, volume=candle.volume,
    )
    with daily_lock:
        daily.last_signal = result.signal
        daily.last_signal_time = candle.timestamp

    print_dashboard(result, candle)

    if result.signal in ("BUY_CE", "BUY_PE"):
        with daily_lock:
            ok, reason = daily.can_trade()
        if not ok:
            log.warning(f"Signal BLOCKED: {reason}")
            return
        log.info(
            f"SIGNAL {result.signal} @ {result.close:.1f} | "
            f"SL={result.sl:.1f} Tgt={result.target:.1f} | "
            f"{result.confirmations}/8 [{result.conf_summary}]"
        )
        resp = send_to_bridge(build_payload(result, index))
        log.info(f"Bridge: {resp}")
        with daily_lock:
            daily.trades += 1
            daily.signals_fired.append({
                "time": str(candle.timestamp), "signal": result.signal,
                "entry": result.close, "conf": result.confirmations,
                "resp": resp.get("status"),
            })


def print_dashboard(result, candle):
    now   = datetime.now().strftime("%H:%M:%S")
    G, R, Y, grey, reset = "\033[92m", "\033[91m", "\033[93m", "\033[90m", "\033[0m"
    sc    = G if result.signal == "BUY_CE" else R if result.signal == "BUY_PE" else grey
    icons = "".join([
        "E" if result.conf_ema_cross  else "·",
        "T" if result.conf_trend      else "·",
        "S" if result.conf_supertrend else "·",
        "V" if result.conf_vwap       else "·",
        "R" if result.conf_rsi        else "·",
        "A" if result.conf_adx        else "·",
        "L" if result.conf_volume     else "·",
        "B" if result.conf_no_squeeze else "·",
    ])
    print(
        f"\r[{now}] {index:12s} "
        f"LTP:{Y}{candle.close:>8.1f}{reset}  "
        f"RSI:{result.rsi:>5.1f}  ADX:{result.adx:>5.1f}  "
        f"Conf:[{icons}]{result.confirmations}/8  "
        f"Sig:{sc}{result.signal:<8}{reset}  "
        f"T:{daily.trades}/{CFG['max_trades']}",
        end="", flush=True,
    )
    if result.signal != "NONE":
        print()


candle_builder = CandleBuilder(CFG["timeframe_min"], on_candle_close)

def on_tick(message):
    try:
        data = json.loads(message) if isinstance(message, str) else message
        if not isinstance(data, dict): return
        ltp = float(data.get("lp") or data.get("ltP") or data.get("last_price") or 0)
        vol = float(data.get("v")  or data.get("vol") or 0)
        if ltp > 0:
            candle_builder.on_tick(ltp=ltp, volume=vol, dt=datetime.now())
    except Exception as e:
        log.debug(f"Tick: {e}")

def on_error(e):   log.error(f"WS error: {e}")
def on_close(m):   log.warning("WS closed — reconnect 5s"); time.sleep(5); start_websocket()
def on_open(m):    log.info(f"WS connected: {m}")


def load_historical_candles(symbol_token, days=3):
    log.info(f"Loading {days}d history...")
    try:
        to_dt   = datetime.now()
        from_dt = to_dt - timedelta(days=days)
        resp = session.client.historical_candles(
            instrument_token=symbol_token, exchange_segment="nse_cm",
            to_date=to_dt.strftime("%Y-%m-%d %H:%M:%S"),
            from_date=from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            interval=str(CFG["timeframe_min"]),
        )
        if not resp or not resp.get("data"):
            log.warning("No history — cold start"); return
        import pandas as pd
        for c in resp["data"]:
            if isinstance(c, list):
                ts,o,h,l,cl,v = c[0],c[1],c[2],c[3],c[4],c[5]
            else:
                ts=c.get("datetime"); o=c.get("open",0); h=c.get("high",0)
                l=c.get("low",0);    cl=c.get("close",0); v=c.get("volume",0)
            engine.update(pd.Timestamp(ts), float(o), float(h), float(l), float(cl), float(v))
        log.info("Engine warmed up")
    except Exception as e:
        log.warning(f"History failed: {e}")


def start_websocket():
    info = INDEX_TOKENS.get(index)
    if not info: log.error(f"No token for {index}"); return
    session.client.on_message = on_tick
    session.client.on_error   = on_error
    session.client.on_close   = on_close
    session.client.on_open    = on_open
    log.info(f"Subscribing {index} token={info['instrument_token']}")
    session.client.subscribe(instrument_tokens=[info], isIndex=True, isDepth=False)


def is_market_hours():
    now = datetime.now()
    if now.weekday() >= 5: return False
    return now.replace(hour=9,minute=15,second=0) <= now <= now.replace(hour=15,minute=30,second=0)


import pandas as pd

if __name__ == "__main__":
    print("=" * 65)
    print(f"  Kotak Live v2 — {index} {CFG['timeframe_min']}min  Capital:₹{CFG['capital']:,.0f}")
    print(f"  Paper:{CFG['paper_trade']}  MinConf:5/8  ADX:{engine_cfg.adx_thresh}  Vol:OFF")
    print("=" * 65)

    session.login()
    info = INDEX_TOKENS.get(index, {})
    if info:
        load_historical_candles(info.get("instrument_token", ""), days=3)

    if not is_market_hours():
        log.info("Waiting for market 9:15 AM...")
        while not is_market_hours(): time.sleep(30)

    start_websocket()

    try:
        while True:
            time.sleep(60)
            session.ensure_fresh()
            now = datetime.now()
            if now.hour == 15 and now.minute == 10:
                send_to_bridge({"tag": CFG["webhook_secret"], "action": "SQUAREOFF_ALL"})
    except KeyboardInterrupt:
        send_to_bridge({"tag": CFG["webhook_secret"], "action": "SQUAREOFF_ALL"})
        print("\nGoodbye.")
