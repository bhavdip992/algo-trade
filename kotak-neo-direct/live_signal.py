"""
live_signal.py
══════════════
Real-time signal generator using Kotak Neo WebSocket + Signal Engine v3.
Capital: Rs.20,000 | Generates CE/PE signals with entry/SL/target/trail.

Run:
  py -3.11 live_signal.py

Required .env:
  KOTAK_CONSUMER_KEY, KOTAK_MOBILE, KOTAK_UCC, KOTAK_MPIN, KOTAK_TOTP_SECRET
  INDEX=BANKNIFTY
  TIMEFRAME=5
  CAPITAL=20000
  PAPER_TRADE=true
"""

import os, sys, json, time, logging, threading, pyotp
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

from neo_api_client import NeoAPI
from signal_engine  import SignalEngine, EngineConfig, SignalResult

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("live_signal.log"),
    ]
)
log = logging.getLogger("LiveSignal")

# ── Config ─────────────────────────────────────────────────
CFG = {
    # Kotak credentials
    "consumer_key":      os.getenv("KOTAK_CONSUMER_KEY", ""),
    "mobile":            os.getenv("KOTAK_MOBILE",        ""),
    "ucc":               os.getenv("KOTAK_UCC",            ""),
    "mpin":              os.getenv("KOTAK_MPIN",           ""),
    "totp_secret":       os.getenv("KOTAK_TOTP_SECRET",    ""),
    # Server
    "environment":       os.getenv("ENVIRONMENT",     "prod"),
    "paper_trade":       os.getenv("PAPER_TRADE",     "true").lower() == "true",
    "bridge_url":        os.getenv("BRIDGE_URL",      "http://localhost:5000"),
    "webhook_secret":    os.getenv("WEBHOOK_SECRET",  "MY_API_KEY_TAG"),
    # Index
    "index":             os.getenv("INDEX",           "BANKNIFTY"),
    "tf_min":            int(os.getenv("TIMEFRAME",   "5")),
    # Capital & risk
    "capital":           float(os.getenv("MAX_CAPITAL",    "20000")),
    "max_trades":        int(os.getenv("MAX_TRADES",       "3")),
    "max_daily_loss":      float(os.getenv("MAX_DAILY_LOSS", "3")),
    "risk_pct":          float(os.getenv("RISK_PCT",       "2.0")),
    # Signal
    "signal_score":      int(os.getenv("SIGNAL_SCORE",     "5")),
    # Greedy trail settings (all from .env)
    "sl_atr_mult":       float(os.getenv("SL_ATR_MULT",       "1.2")),
    "breakeven_atr":     float(os.getenv("BREAKEVEN_ATR",     "0.8")),
    "trail_atr_mult":    float(os.getenv("TRAIL_ATR_MULT",    "1.0")),
    "greedy_atr":        float(os.getenv("GREEDY_ATR",        "2.0")),
    "greedy_trail":      float(os.getenv("GREEDY_TRAIL_MULT", "0.5")),
    "use_hard_target":   os.getenv("USE_HARD_TARGET", "false").lower() == "true",
    "tgt_rr":            float(os.getenv("TGT_RR",            "2.0")),
}

LOT_SIZES = {"NIFTY":65, "BANKNIFTY":30, "FINNIFTY":60, "MIDCPNIFTY":120, "SENSEX":20}

INDEX_TOKENS = {
    "BANKNIFTY":  {"instrument_token": "11915",   "exchange_segment": "nse_cm"},
    "NIFTY":      {"instrument_token": "26000",   "exchange_segment": "nse_cm"},
    "FINNIFTY":   {"instrument_token": "1333922", "exchange_segment": "nse_cm"},
    "MIDCPNIFTY": {"instrument_token": "288009",  "exchange_segment": "nse_cm"},
}

# ── Candle Builder ──────────────────────────────────────────
@dataclass
class Candle:
    timestamp: datetime
    open:   float = 0.0
    high:   float = 0.0
    low:    float = float('inf')
    close:  float = 0.0
    volume: float = 0.0

class CandleBuilder:
    def __init__(self, tf_min: int, on_close):
        self.tf_min   = tf_min
        self.on_close = on_close
        self.current  = None
        self._lock    = threading.Lock()

    def _floor(self, dt):
        mins = dt.hour * 60 + dt.minute
        f    = (mins // self.tf_min) * self.tf_min
        return dt.replace(hour=f//60, minute=f%60, second=0, microsecond=0)

    def tick(self, ltp: float, vol: float, dt: datetime = None):
        if dt is None: dt = datetime.now()
        with self._lock:
            ts = self._floor(dt)
            if self.current is None or ts > self.current.timestamp:
                if self.current is not None:
                    self.on_close(self.current)
                self.current = Candle(timestamp=ts, open=ltp, high=ltp, low=ltp, close=ltp, volume=vol)
            else:
                c = self.current
                c.high   = max(c.high, ltp)
                c.low    = min(c.low,  ltp)
                c.close  = ltp
                c.volume += vol

# ── Kotak Session ───────────────────────────────────────────
class KotakSession:
    def __init__(self):
        self.client    = None
        self.logged_in = False
        self.login_time= None

    def login(self):
        log.info("Logging in to Kotak Neo...")
        self.client = NeoAPI(
            consumer_key=CFG["consumer_key"],
            environment=CFG["environment"],
            access_token=None, neo_fin_key=None,
        )
        totp_obj = pyotp.TOTP(CFG["totp_secret"])
        r1 = None
        for offset in [0, -30, 30, -60, 60]:
            code = totp_obj.at(time.time() + offset)
            resp = self.client.totp_login(mobile_number=CFG["mobile"],
                                          ucc=CFG["ucc"], totp=code)
            if resp and resp.get("data", {}).get("token"):
                r1 = resp; break
        if not r1:
            raise RuntimeError("TOTP login failed — check secret and clock sync")
        r2 = self.client.totp_validate(mpin=CFG["mpin"])
        if not r2 or not r2.get("data", {}).get("token"):
            raise RuntimeError("MPIN validate failed")
        self.logged_in = True
        self.login_time = datetime.now()
        log.info("Kotak Neo login OK")

    def ensure_fresh(self):
        if not self.logged_in: self.login(); return
        if (datetime.now() - self.login_time).seconds / 3600 > 7:
            log.info("Refreshing session...")
            self.login()

    def load_history(self, token: str, days: int = 3):
        """Load historical candles to warm up indicators"""
        log.info(f"Loading {days}d history for warm-up...")
        try:
            to_dt   = datetime.now()
            from_dt = to_dt - timedelta(days=days)
            resp = self.client.historical_candles(
                instrument_token=token, exchange_segment="nse_cm",
                to_date=to_dt.strftime("%Y-%m-%d %H:%M:%S"),
                from_date=from_dt.strftime("%Y-%m-%d %H:%M:%S"),
                interval=str(CFG["tf_min"]),
            )
            if resp and resp.get("data"):
                return resp["data"]
        except Exception as e:
            log.warning(f"History load failed: {e} — starting cold")
        return []

session = KotakSession()

# ── Signal Engine ───────────────────────────────────────────
index = CFG["index"]
engine = SignalEngine(EngineConfig(
    capital            = CFG["capital"],
    risk_pct           = CFG["risk_pct"],
    max_trades_day     = CFG["max_trades"],
    max_daily_loss_pct = CFG["max_daily_loss"],
    index              = index,
    lot_size           = LOT_SIZES.get(index, 30),
    signal_threshold   = CFG["signal_score"],
    sl_atr_mult        = CFG["sl_atr_mult"],
    breakeven_atr      = CFG["breakeven_atr"],
    trail_atr_mult     = CFG["trail_atr_mult"],
    greedy_atr         = CFG["greedy_atr"],
    greedy_trail       = CFG["greedy_trail"],
    use_hard_target    = CFG["use_hard_target"],
    tgt_rr             = CFG["tgt_rr"],
    use_trail          = True,
))

# ── Signal State ────────────────────────────────────────────
last_signal     = "NONE"
last_signal_time= None
signal_count    = 0
signal_log      = []

# ── Dashboard ───────────────────────────────────────────────
COLORS = {
    "green":  "\033[92m",
    "red":    "\033[91m",
    "yellow": "\033[93m",
    "cyan":   "\033[96m",
    "gray":   "\033[90m",
    "orange": "\033[33m",
    "reset":  "\033[0m",
    "bold":   "\033[1m",
}

def clr(text, color): return f"{COLORS.get(color,'')}{text}{COLORS['reset']}"

def print_dashboard(result: SignalResult, candle: Candle):
    now  = datetime.now().strftime("%H:%M:%S")
    sig  = result.signal

    # Score bar
    filled = "█" * result.score + "░" * (10 - result.score)
    score_color = "green" if result.score >= 6 else "yellow" if result.score >= 4 else "gray"

    sig_color = "green" if sig == "BUY_CE" else "red" if sig == "BUY_PE" else "gray"

    # Regime color
    reg_color = "green" if result.regime == "TRENDING" else "yellow" if result.regime == "VOLATILE" else "gray"

    line = (
        f"\r{clr(now,'cyan')} "
        f"{clr(index,'bold'):14s} "
        f"LTP:{clr(f'{candle.close:>8.1f}','yellow')}  "
        f"RSI:{clr(f'{result.rsi:>5.1f}','cyan')}  "
        f"ADX:{clr(f'{result.adx:>5.1f}','cyan')}  "
        f"ATR:{clr(f'{result.atr:>6.1f}','gray')}  "
        f"Score:[{clr(filled, score_color)}] {result.score}/10  "
        f"Regime:{clr(result.regime[:7]+'  ', reg_color)}  "
        f"Signal:{clr(f'{sig:<8}', sig_color)}"
    )
    print(line, end="", flush=True)

    if sig != "NONE":
        print()  # newline on signal
        print_signal_box(result, candle)

def print_signal_box(result: SignalResult, candle: Candle):
    sig = result.signal
    c   = "green" if sig == "BUY_CE" else "red"
    opt = "CALL (CE)" if sig == "BUY_CE" else "PUT (PE)"
    direction = "↑ BULLISH" if sig == "BUY_CE" else "↓ BEARISH"

    print()
    print(clr("  ╔══════════════════════════════════════════╗", c))
    print(clr(f"  ║   {direction}  —  BUY {opt}               ║", c))
    print(clr("  ╠══════════════════════════════════════════╣", c))
    print(clr(f"  ║  Index    : {index:<30}║", c))
    print(clr(f"  ║  Time     : {str(candle.timestamp)[:19]:<30}║", c))
    print(clr(f"  ║  Entry    : {result.close:<30.2f}║", c))
    print(clr(f"  ║  Stop Loss: {result.sl:<30.2f}║", "red"))
    mode_lbl = "SOFT (greedy trail)" if not CFG["use_hard_target"] else "FIXED"
    print(clr(f"  ║  Target   : {result.target:<22.2f} [{mode_lbl}]║", "green"))
    print(clr(f"  ║  Trail SL : {result.trail_sl:<30.2f}║", "yellow"))
    print(clr("  ╠══════════════════════════════════════════╣", c))
    print(clr(f"  ║  Score    : {result.score}/10  [{result.score_detail}]" + " "*(28-len(result.score_detail)) + "║", c))
    print(clr(f"  ║  Strength : {result.strength:<30}║", c))
    print(clr(f"  ║  Regime   : {result.regime:<30}║", c))
    print(clr("  ╠══════════════════════════════════════════╣", c))
    print(clr(f"  ║  Capital  : Rs.{CFG['capital']:<27,.0f}║", "cyan"))
    print(clr(f"  ║  Risk     : Rs.{result.risk_amt:<27,.0f}║", "red"))
    print(clr(f"  ║  Reward   : Rs.{result.reward_amt:<27,.0f}║", "green"))
    print(clr(f"  ║  Qty      : {result.qty_lots} lot = {result.qty_units} units" + " "*20 + "║", "cyan"))
    print(clr(f"  ║  Strike   : {result.option_strike_hint[:30]:<30}║", "yellow"))
    print(clr("  ╚══════════════════════════════════════════╝", c))
    print()

# ── Candle Close Handler ────────────────────────────────────
def on_candle_close(candle: Candle):
    global last_signal, last_signal_time, signal_count

    result = engine.update(
        timestamp=candle.timestamp,
        open_=candle.open, high=candle.high,
        low=candle.low,    close=candle.close,
        volume=candle.volume,
    )

    last_signal      = result.signal
    last_signal_time = candle.timestamp

    print_dashboard(result, candle)

    if result.signal in ("BUY_CE", "BUY_PE") and result.score >= engine.cfg.signal_threshold:
        can, reason = engine.can_trade()
        if not can:
            log.warning(f"Signal blocked: {reason}")
            return

        signal_count += 1
        engine._daily_trades += 1

        entry = {
            "id":         signal_count,
            "time":       str(candle.timestamp),
            "index":      index,
            "signal":     result.signal,
            "entry":      result.close,
            "sl":         result.sl,
            "target":     result.target,
            "trail_sl":   result.trail_sl,
            "score":      result.score,
            "detail":     result.score_detail,
            "regime":     result.regime,
            "lots":       result.qty_lots,
            "risk_rs":    result.risk_amt,
            "reward_rs":  result.reward_amt,
            "atr":        result.atr,
            "rsi":        result.rsi,
            "adx":        result.adx,
            "hint":       result.option_strike_hint,
        }
        signal_log.append(entry)

        log.info(
            f"SIGNAL #{signal_count} | {result.signal} | "
            f"Entry={result.close} SL={result.sl} Tgt={result.target} | "
            f"Score={result.score}/10 [{result.score_detail}] | "
            f"Risk=Rs.{result.risk_amt:.0f} Reward=Rs.{result.reward_amt:.0f}"
        )

# ── Tick Handler ────────────────────────────────────────────
candle_builder = CandleBuilder(CFG["tf_min"], on_candle_close)

def on_tick(message):
    try:
        data = json.loads(message) if isinstance(message, str) else message
        ltp  = float(data.get("lp") or data.get("ltP") or data.get("last_price") or 0)
        vol  = float(data.get("v")  or data.get("vol") or data.get("volume") or 0)
        if ltp > 0:
            candle_builder.tick(ltp, vol)
    except Exception as e:
        log.debug(f"Tick error: {e}")

def on_error(e):   log.error(f"WS error: {e}")
def on_open(m):    log.info(f"WS connected: {m}")
def on_close(m):
    log.warning(f"WS closed — reconnecting in 5s...")
    time.sleep(5); start_ws()

def start_ws():
    tok = INDEX_TOKENS.get(index)
    if not tok: return
    session.client.on_message = on_tick
    session.client.on_error   = on_error
    session.client.on_open    = on_open
    session.client.on_close   = on_close
    session.client.subscribe(instrument_tokens=[tok], isIndex=True, isDepth=False)
    log.info(f"Subscribed to {index} live feed")

def is_market():
    now = datetime.now()
    if now.weekday() >= 5: return False
    return now.replace(hour=9,minute=15) <= now <= now.replace(hour=15,minute=30)

# ── Main ────────────────────────────────────────────────────
if __name__ == "__main__":
    print(clr("═" * 55, "cyan"))
    print(clr(f"  Kotak Neo Live Signal Engine v3", "bold"))
    print(clr(f"  Index      : {index}", "cyan"))
    print(clr(f"  Timeframe  : {CFG['tf_min']}-min candles", "cyan"))
    print(clr(f"  Capital    : Rs.{CFG['capital']:,.0f}", "green"))
    print(clr(f"  Max risk   : Rs.{CFG['capital']*0.02:.0f}/trade (2%)", "yellow"))
    print(clr(f"  Paper mode : {CFG['paper_trade']}", "yellow"))
    print(clr(f"  Score gate : 5/10 algorithms must agree", "cyan"))
    print(clr("═" * 55, "cyan"))
    print()
    print("  Algorithms: EMA | CROSS | Supertrend | VWAP | RSI")
    print("              ADX | BB | Heikin-Ashi | Divergence | Volume")
    print()

    # Login
    session.login()

    # Warm up with history
    tok = INDEX_TOKENS.get(index, {})
    history = session.load_history(tok.get("instrument_token", ""), days=3)
    import pandas as pd
    for c in history:
        try:
            if isinstance(c, list):
                ts = pd.Timestamp(c[0]); o=float(c[1]); h=float(c[2])
                l=float(c[3]);  cl=float(c[4]); v=float(c[5]) if len(c)>5 else 0
            else:
                ts = pd.Timestamp(c.get("datetime",""))
                o=float(c.get("open",0)); h=float(c.get("high",0))
                l=float(c.get("low",0));  cl=float(c.get("close",0))
                v=float(c.get("volume",0))
            engine.update(ts, o, h, l, cl, v)
        except Exception:
            pass
    log.info(f"Warmed up with {len(history)} historical bars")

    # Wait for market
    if not is_market():
        log.info("Outside market hours — waiting for 9:15 AM IST...")
        while not is_market():
            time.sleep(30)

    # Start live feed
    start_ws()

    # Keep alive
    try:
        while True:
            time.sleep(60)
            session.ensure_fresh()
            now = datetime.now()
            if now.hour == 15 and now.minute >= 10:
                log.info("15:10 — market closing soon, stopping new signals")
                engine.cfg.signal_threshold = 99  # block new signals
    except KeyboardInterrupt:
        print(f"\n\n  Session ended — {signal_count} signals generated")
        if signal_log:
            print(f"\n  Signal Summary:")
            for s in signal_log:
                print(f"  #{s['id']} {s['time'][:16]} {s['signal']:7s} "
                      f"Entry={s['entry']:.0f} SL={s['sl']:.0f} "
                      f"Tgt={s['target']:.0f} Score={s['score']}/10")
