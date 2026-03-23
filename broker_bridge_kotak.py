#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║   🇮🇳 INDIA OPTIONS AUTO-TRADER — Kotak Neo API Bridge v4.0 FIXED   ║
║   TradingView Webhook / kotak_live.py → Kotak Neo SDK v2             ║
║                                                                      ║
║   FIXES vs v3:                                                       ║
║     1. session_ok auto-set to True after login (was blocking trades) ║
║     2. LOT_SIZES unified: BANKNIFTY=30, NIFTY=75 (NSE 2024)         ║
║     3. Option premium cap: won't buy > ₹500 premium (protects 20k)  ║
║     4. Auto lot size calculation based on available capital          ║
║     5. Better option symbol fallback if ATM not found               ║
║     6. /webhook now also accepts SQUAREOFF_ALL action               ║
╚══════════════════════════════════════════════════════════════════════╝

INSTALL:
    pip install flask pyotp python-dotenv
    pip install "git+https://github.com/Kotak-Neo/Kotak-neo-api-v2.git@v2.0.1#egg=neo_api_client"

ENV (.env):
    KOTAK_CONSUMER_KEY=...
    KOTAK_MOBILE=+91XXXXXXXXXX
    KOTAK_UCC=XHHPN
    KOTAK_MPIN=525286
    KOTAK_TOTP_SECRET=...
    PAPER_TRADE=true
    WEBHOOK_SECRET=your_secret
    MAX_CAPITAL=20000
    MAX_DAILY_LOSS=3
    MAX_TRADES=3
"""

import os, json, logging, threading, time, pyotp
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

from neo_api_client import NeoAPI

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kotak_trader.log"),
    ]
)
log = logging.getLogger("KotakTrader")
app = Flask(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
CFG = {
    "consumer_key":    os.getenv("KOTAK_CONSUMER_KEY",  ""),
    "mobile":          os.getenv("KOTAK_MOBILE",         ""),
    "ucc":             os.getenv("KOTAK_UCC",             ""),
    "mpin":            os.getenv("KOTAK_MPIN",            ""),
    "totp_secret":     os.getenv("KOTAK_TOTP_SECRET",     ""),
    "environment":     os.getenv("ENVIRONMENT",    "prod"),
    "paper_trade":     os.getenv("PAPER_TRADE",    "true").lower() == "true",
    "webhook_secret":  os.getenv("WEBHOOK_SECRET", "MY_API_KEY_TAG"),
    "port":            int(os.getenv("PORT",        "5000")),
    "max_capital":     float(os.getenv("MAX_CAPITAL",    "20000")),
    "max_daily_loss":  float(os.getenv("MAX_DAILY_LOSS", "3")),    # %
    "max_trades":      int(os.getenv("MAX_TRADES",       "3")),
    # Maximum option premium to buy per lot (protect capital on 20k)
    "max_premium":     float(os.getenv("MAX_PREMIUM",    "500")),
}

# ── FIXED LOT SIZES (NSE F&O 2024 revision) ─────────────────
# Source: NSE circular Oct 2024
LOT_SIZES = {
    "NIFTY":       75,
    "BANKNIFTY":   30,
    "FINNIFTY":    65,
    "MIDCPNIFTY":  120,
    "SENSEX":      20,
}

# Kotak Neo segment / product
EXCHANGE_SEGMENT = "nfo_fo"
PRODUCT_CODE     = "INTRADAY"
VALIDITY         = "DAY"

# Strike step sizes for each index
STRIKE_STEPS = {
    "NIFTY":       50,
    "BANKNIFTY":   100,
    "FINNIFTY":    50,
    "MIDCPNIFTY":  25,
    "SENSEX":      100,
}


# ─────────────────────────────────────────────────────────────
# KOTAK NEO CLIENT — singleton with auto-login
# ─────────────────────────────────────────────────────────────
class KotakNeoClient:
    _instance   = None
    _lock       = threading.Lock()

    @classmethod
    def get(cls) -> "KotakNeoClient":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._client       = None
        self._logged_in    = False
        self._login_time   = None
        self._session_lock = threading.Lock()

    def _generate_totp(self) -> str:
        if not CFG["totp_secret"]:
            raise ValueError("KOTAK_TOTP_SECRET not set in .env")
        code = pyotp.TOTP(CFG["totp_secret"]).now()
        log.info(f"🔐 TOTP: {code}")
        return code

    def login(self) -> bool:
        """
        Full login: init → TOTP → MPIN.
        Returns True on success.
        FIX: now updates state.session_ok directly.
        """
        with self._session_lock:
            log.info("🔑 Logging into Kotak Neo...")
            self._client = NeoAPI(
                consumer_key=CFG["consumer_key"],
                environment=CFG["environment"],
                access_token=None,
                neo_fin_key=None,
            )
            # Step 1: TOTP
            totp = self._generate_totp()
            r1   = self._client.totp_login(
                mobile_number=CFG["mobile"],
                ucc=CFG["ucc"],
                totp=totp,
            )
            log.info(f"TOTP response: {r1}")
            if not r1 or not r1.get("data", {}).get("token"):
                raise RuntimeError(f"TOTP login failed: {r1}")

            # Step 2: MPIN
            r2 = self._client.totp_validate(mpin=CFG["mpin"])
            log.info(f"MPIN response: {r2}")
            if not r2 or not r2.get("data", {}).get("token"):
                raise RuntimeError(f"MPIN validation failed: {r2}")

            self._logged_in  = True
            self._login_time = datetime.now()

            # FIX: update global state here too
            with state_lock:
                state.session_ok = True

            log.info("✅ Kotak Neo login successful")
            return True

    def ensure_logged_in(self):
        """Re-login if session > 7 hours old."""
        if not self._logged_in or self._client is None:
            self.login()
            return
        if self._login_time:
            age_h = (datetime.now() - self._login_time).seconds / 3600
            if age_h > 7:
                log.info("🔄 Session near expiry — refreshing")
                self.login()

    @property
    def client(self) -> NeoAPI:
        self.ensure_logged_in()
        return self._client


neo_client = KotakNeoClient.get()


# ─────────────────────────────────────────────────────────────
# BROKER — all Kotak Neo interactions
# ─────────────────────────────────────────────────────────────
class KotakNeoBroker:
    _scrip_cache: dict = {}
    _cache_date:  date = None

    def _client(self) -> NeoAPI:
        return neo_client.client

    # ── Instrument token lookup ──────────────────────────────
    def get_scrip_token(self, trading_symbol: str) -> str:
        today = date.today()
        if self._cache_date != today:
            self._scrip_cache.clear()
            self._cache_date = today
        if trading_symbol in self._scrip_cache:
            return self._scrip_cache[trading_symbol]
        try:
            resp = self._client().search_scrip(
                exchange_segment=EXCHANGE_SEGMENT,
                symbol=trading_symbol,
            )
            if resp and resp.get("data"):
                for inst in resp["data"]:
                    if inst.get("trdSym") == trading_symbol or inst.get("sym") == trading_symbol:
                        token = inst.get("tok") or inst.get("pTok") or ""
                        self._scrip_cache[trading_symbol] = token
                        return token
                token = resp["data"][0].get("tok", "")
                self._scrip_cache[trading_symbol] = token
                return token
        except Exception as e:
            log.error(f"Scrip token lookup failed ({trading_symbol}): {e}")
        return ""

    # ── LTP ─────────────────────────────────────────────────
    def get_ltp(self, trading_symbol: str) -> float:
        try:
            token = self.get_scrip_token(trading_symbol)
            if not token:
                raise ValueError(f"No token for {trading_symbol}")
            resp = self._client().quotes(
                instrument_tokens=[{
                    "instrument_token": token,
                    "exchange_segment": EXCHANGE_SEGMENT,
                }],
                quote_type="ltp",
            )
            if resp and resp.get("data"):
                ltp = float(
                    resp["data"][0].get("ltp", 0) or
                    resp["data"][0].get("ltP", 0) or 0
                )
                log.debug(f"LTP {trading_symbol}: ₹{ltp}")
                return ltp
        except Exception as e:
            log.error(f"LTP fetch failed ({trading_symbol}): {e}")
        return 0.0

    # ── Funds / margin ───────────────────────────────────────
    def get_funds(self) -> dict:
        try:
            resp = self._client().limits(segment="FO", exchange="NFO", product="MIS")
            if resp and resp.get("data"):
                d = resp["data"]
                available = float(
                    d.get("net", 0) or
                    d.get("marginAvailable", 0) or
                    d.get("cashmarginavailable", 0) or 0
                )
                return {"available": available, "raw": d}
        except Exception as e:
            log.error(f"Funds fetch failed: {e}")
        return {"available": CFG["max_capital"]}

    # ── Place order ──────────────────────────────────────────
    def place_order(
        self,
        trading_symbol: str,
        qty: int,
        transaction_type: str = "B",
        order_type: str = "MKT",
        price: float = 0,
        trigger_price: float = 0,
        product: str = PRODUCT_CODE,
    ) -> str:
        if CFG["paper_trade"]:
            oid = f"PAPER_{int(time.time())}"
            log.info(f"[PAPER] {transaction_type} {qty}×{trading_symbol} @ {order_type} → {oid}")
            return oid
        try:
            resp = self._client().place_order(
                exchange_segment=EXCHANGE_SEGMENT,
                product=product,
                price=str(price) if price else "0",
                order_type=order_type,
                quantity=str(qty),
                validity=VALIDITY,
                trading_symbol=trading_symbol,
                transaction_type=transaction_type,
                amo="NO",
                disclosed_quantity="0",
                market_protection="0",
                pf="N",
                trigger_price=str(trigger_price) if trigger_price else "0",
                tag="OPTIONS_BOT",
            )
            log.info(f"Order response: {resp}")
            if resp and resp.get("data"):
                oid = str(resp["data"].get("nOrdNo") or resp["data"].get("orderId") or "")
                log.info(f"✅ Order placed: {oid} | {transaction_type} {qty}×{trading_symbol}")
                return oid
            raise RuntimeError(f"Order placement failed: {resp}")
        except Exception as e:
            log.error(f"Place order error: {e}")
            raise

    # ── Place SL order ───────────────────────────────────────
    def place_sl_order(
        self, trading_symbol: str, qty: int,
        trigger_price: float, price: float,
        transaction_type: str = "S",
    ) -> str:
        if CFG["paper_trade"]:
            oid = f"PAPER_SL_{int(time.time())}"
            log.info(f"[PAPER] SL {transaction_type} {qty}×{trading_symbol} trig={trigger_price:.1f} → {oid}")
            return oid
        try:
            resp = self._client().place_order(
                exchange_segment=EXCHANGE_SEGMENT,
                product=PRODUCT_CODE,
                price=str(round(price, 1)),
                order_type="SL",
                quantity=str(qty),
                validity=VALIDITY,
                trading_symbol=trading_symbol,
                transaction_type=transaction_type,
                amo="NO",
                disclosed_quantity="0",
                market_protection="0",
                pf="N",
                trigger_price=str(round(trigger_price, 1)),
                tag="OPTIONS_BOT_SL",
            )
            if resp and resp.get("data"):
                oid = str(resp["data"].get("nOrdNo") or resp["data"].get("orderId") or "")
                log.info(f"🛑 SL placed: {oid} @ trig={trigger_price:.1f}")
                return oid
            raise RuntimeError(f"SL order failed: {resp}")
        except Exception as e:
            log.error(f"SL order error: {e}")
            raise

    # ── Modify order ─────────────────────────────────────────
    def modify_order(
        self, order_id: str, trading_symbol: str, qty: int,
        new_trigger_price: float, new_price: float,
    ) -> str:
        if CFG["paper_trade"]:
            log.info(f"[PAPER] MODIFY {order_id} → trig={new_trigger_price:.1f}")
            return order_id
        try:
            resp = self._client().modify_order(
                order_id=order_id,
                price=str(round(new_price, 1)),
                quantity=str(qty),
                validity=VALIDITY,
                disclosed_quantity="0",
                trigger_price=str(round(new_trigger_price, 1)),
                order_type="SL",
                exchange_segment=EXCHANGE_SEGMENT,
                product=PRODUCT_CODE,
                trading_symbol=trading_symbol,
            )
            if resp and resp.get("data"):
                new_oid = str(resp["data"].get("nOrdNo") or order_id)
                log.info(f"✏️ Modified: {order_id} → trig={new_trigger_price:.1f}")
                return new_oid
        except Exception as e:
            log.error(f"Modify order error: {e}")
        return order_id

    # ── Cancel order ─────────────────────────────────────────
    def cancel_order(self, order_id: str) -> bool:
        if CFG["paper_trade"]:
            log.info(f"[PAPER] CANCEL {order_id}")
            return True
        try:
            self._client().cancel_order(order_id=order_id, isVerify=False)
            return True
        except Exception as e:
            log.error(f"Cancel error: {e}")
            return False

    # ── Positions ────────────────────────────────────────────
    def get_positions(self) -> list:
        try:
            resp = self._client().positions()
            if resp and resp.get("data"):
                return resp["data"]
        except Exception as e:
            log.error(f"Positions fetch failed: {e}")
        return []

    # ── WebSocket feed ───────────────────────────────────────
    def subscribe_ltp(self, tokens: list, on_tick_callback):
        try:
            c = self._client()
            c.on_message = on_tick_callback
            c.on_error   = lambda e: log.error(f"WS Error: {e}")
            c.on_close   = lambda m: log.warning(f"WS Closed: {m}")
            c.on_open    = lambda m: log.info(f"WS Connected: {m}")
            c.subscribe(instrument_tokens=tokens, isIndex=False, isDepth=False)
            log.info(f"WebSocket subscribed: {len(tokens)} tokens")
        except Exception as e:
            log.error(f"WebSocket subscribe failed: {e}")


broker = KotakNeoBroker()


# ─────────────────────────────────────────────────────────────
# TRADING STATE
# ─────────────────────────────────────────────────────────────
@dataclass
class OpenPosition:
    symbol:          str
    option:          str       # CE or PE
    index:           str
    entry_price:     float     # option premium at entry
    sl_underlying:   float     # underlying SL (from signal)
    target_underlying: float
    trail_mode:      str
    lots:            int
    qty:             int
    entry_oid:       str
    sl_oid:          str
    trail_sl_opt:    float     # trailing SL in option premium
    target_opt:      float     # target in option premium
    pnl:             float = 0.0
    status:          str   = "OPEN"
    entry_time:      str   = ""

@dataclass
class TradingState:
    daily_trades:   int   = 0
    daily_pnl:      float = 0.0
    open_positions: dict  = field(default_factory=dict)
    trade_log:      list  = field(default_factory=list)
    last_reset:     date  = field(default_factory=date.today)
    halted:         bool  = False
    session_ok:     bool  = False   # FIX: set True after login

    def reset_if_new_day(self):
        today = date.today()
        if today != self.last_reset:
            log.info("🗓 New trading day — resetting counters")
            self.daily_trades = 0
            self.daily_pnl    = 0.0
            self.halted       = False
            self.last_reset   = today

    def can_trade(self) -> tuple[bool, str]:
        self.reset_if_new_day()
        if self.halted:
            return False, "Trading halted"
        if not self.session_ok:
            return False, "Broker session not active — POST /login first"
        if self.daily_trades >= CFG["max_trades"]:
            return False, f"Max daily trades reached ({CFG['max_trades']})"
        max_loss = CFG["max_capital"] * CFG["max_daily_loss"] / 100
        if self.daily_pnl <= -max_loss:
            self.halted = True
            return False, f"Daily loss limit hit ₹{abs(self.daily_pnl):.0f}"
        return True, "OK"


state      = TradingState()
state_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────
# OPTION SYMBOL BUILDER
# ─────────────────────────────────────────────────────────────
def round_to_strike(price: float, index: str) -> int:
    step = STRIKE_STEPS.get(index, 50)
    return int(round(price / step) * step)

def get_next_expiry(index: str, expiry_type: str = "Weekly") -> str:
    """
    Returns expiry in Kotak Neo format: DDMMMYY (e.g. 26MAR25)
    BANKNIFTY: Wednesday  |  Others: Thursday
    """
    today      = date.today()
    exp_wday   = 2 if index == "BANKNIFTY" else 3   # Wed=2, Thu=3

    if expiry_type == "Monthly":
        month, year = today.month, today.year
        candidates  = [date(year, month, d)
                       for d in range(1, 32)
                       if d <= 31 and _valid_date(year, month, d)
                       and date(year, month, d).weekday() == exp_wday]
        expiry = candidates[-1] if candidates else today
    else:
        days_ahead = (exp_wday - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        expiry = today + timedelta(days=days_ahead)

    return expiry.strftime("%d%b%y").upper()   # e.g. 26MAR25

def _valid_date(y, m, d):
    try: date(y, m, d); return True
    except: return False

def build_option_symbol(
    index: str,
    strike_mode: str,
    expiry_type: str,
    option: str,
    underlying_ltp: float,
) -> str:
    """
    Build Kotak Neo NFO trading symbol.
    e.g. BANKNIFTY26MAR2551000CE
    """
    atm      = round_to_strike(underlying_ltp, index)
    step     = STRIKE_STEPS.get(index, 50)
    offsets  = {"ATM": 0, "OTM1": 1, "OTM2": 2, "ITM1": -1, "ITM2": -2}
    direction = 1 if option == "CE" else -1
    strike   = atm + offsets.get(strike_mode, 0) * step * direction
    expiry   = get_next_expiry(index, expiry_type)
    symbol   = f"{index}{expiry}{strike}{option}"
    log.info(f"Built symbol: {symbol} (underlying={underlying_ltp:.0f}, ATM={atm}, strike={strike})")
    return symbol


# ─────────────────────────────────────────────────────────────
# CAPITAL-AWARE LOT CALCULATOR
# ─────────────────────────────────────────────────────────────
def calculate_lots_for_capital(opt_ltp: float, lot_size: int) -> int:
    """
    For ₹20,000 capital:
      - Use ~80% of available capital for premium
      - Don't buy more than 1 lot if premium × lot_size > ₹8,000
    """
    available = min(FundManager.get_available(), CFG["max_capital"])
    usable    = available * 0.80   # keep 20% buffer

    cost_per_lot = opt_ltp * lot_size
    if cost_per_lot <= 0:
        return 1

    lots = max(1, int(usable / cost_per_lot))
    # Cap: never risk more than 50% in single trade
    max_lots = max(1, int(available * 0.50 / cost_per_lot))
    lots     = min(lots, max_lots, 3)   # absolute max 3 lots

    log.info(f"Lot calc: available=₹{available:.0f} usable=₹{usable:.0f} "
             f"cost/lot=₹{cost_per_lot:.0f} → {lots} lot(s)")
    return lots


# ─────────────────────────────────────────────────────────────
# FUND MANAGER
# ─────────────────────────────────────────────────────────────
class FundManager:
    @staticmethod
    def get_available() -> float:
        try:
            return min(float(broker.get_funds().get("available", 0)), CFG["max_capital"])
        except:
            return CFG["max_capital"]

    @staticmethod
    def can_afford(opt_ltp: float, lots: int, lot_size: int) -> tuple[bool, str]:
        # Option buy: you pay full premium (no margin needed beyond premium)
        cost = opt_ltp * lots * lot_size
        avail = FundManager.get_available()
        if cost > avail * 0.90:   # keep 10% buffer
            return False, f"Insufficient funds: need ₹{cost:.0f}, have ₹{avail:.0f}"
        return True, f"Funds OK: cost ₹{cost:.0f} of ₹{avail:.0f} available"


# ─────────────────────────────────────────────────────────────
# TRAILING SL MONITOR
# ─────────────────────────────────────────────────────────────
class TrailMonitor(threading.Thread):
    def __init__(self, check_interval: int = 5):
        super().__init__(daemon=True, name="TrailMonitor")
        self.check_interval = check_interval

    def run(self):
        log.info(f"🔁 Trail monitor started (interval={self.check_interval}s)")
        while True:
            try:
                self._tick()
            except Exception as e:
                log.error(f"TrailMonitor error: {e}")
            time.sleep(self.check_interval)

    def _tick(self):
        with state_lock:
            positions = list(state.open_positions.items())
        for symbol, pos in positions:
            if pos.status != "OPEN":
                continue
            try:
                ltp = broker.get_ltp(symbol)
                if ltp > 0:
                    self._evaluate(pos, ltp, symbol)
            except Exception as e:
                log.warning(f"Position check ({symbol}): {e}")

    def _evaluate(self, pos: OpenPosition, ltp: float, symbol: str):
        is_ce = pos.option == "CE"

        # Target hit
        if ltp >= pos.target_opt:
            log.info(f"🎯 TARGET HIT: {symbol} @ ₹{ltp:.1f} (target={pos.target_opt:.1f})")
            self._exit(pos, symbol, ltp, "TGT_HIT")
            return

        # SL hit
        if (is_ce and ltp <= pos.trail_sl_opt) or (not is_ce and ltp <= pos.trail_sl_opt):
            log.info(f"🛑 SL HIT: {symbol} @ ₹{ltp:.1f} (sl={pos.trail_sl_opt:.1f})")
            self._exit(pos, symbol, ltp, "SL_HIT")
            return

        # Trail SL upward when in profit (options only move one way in premium terms)
        if ltp > pos.entry_price:
            candidate = ltp * 0.85   # trail at 15% below current LTP
            if candidate > pos.trail_sl_opt:
                old = pos.trail_sl_opt
                pos.trail_sl_opt = candidate
                log.info(f"📈 Trail SL: {symbol} ₹{old:.1f} → ₹{candidate:.1f}")
                if pos.sl_oid:
                    new_oid = broker.modify_order(
                        order_id=pos.sl_oid,
                        trading_symbol=symbol,
                        qty=pos.qty,
                        new_trigger_price=round(candidate * 0.99, 1),
                        new_price=round(candidate, 1),
                    )
                    with state_lock:
                        pos.sl_oid = new_oid

    def _exit(self, pos: OpenPosition, symbol: str, exit_ltp: float, reason: str):
        try:
            oid = broker.place_order(
                trading_symbol=symbol, qty=pos.qty,
                transaction_type="S", order_type="MKT",
            )
            log.info(f"Exit order: {oid} ({reason})")
        except Exception as e:
            log.error(f"Exit order failed: {e}")
        pnl = (exit_ltp - pos.entry_price) * pos.qty
        with state_lock:
            pos.status = reason
            pos.pnl    = pnl
            state.daily_pnl += pnl
        emoji = "✅" if pnl >= 0 else "❌"
        log.info(f"{emoji} {reason} | {symbol} | entry={pos.entry_price:.1f} exit={exit_ltp:.1f} | P&L ₹{pnl:.0f}")


# ─────────────────────────────────────────────────────────────
# TRADE EXECUTOR
# ─────────────────────────────────────────────────────────────
def execute_trade(payload: dict) -> dict:
    # ── Handle square-off action ──────────────────────────
    if payload.get("action") in ("SQUAREOFF_ALL", "EXIT_ALL"):
        return _squareoff_all_internal()

    action     = payload.get("action", "")
    index      = payload.get("index", "BANKNIFTY").upper()
    strike_m   = payload.get("strike", "ATM")
    expiry     = payload.get("expiry", "Weekly")
    trail_mode = payload.get("trail", "ATR")

    # Determine CE or PE from action string
    if "CE" in action.upper():
        option = "CE"
    elif "PE" in action.upper():
        option = "PE"
    else:
        return {"status": "REJECTED", "reason": f"Cannot determine CE/PE from action: {action}"}

    lot_size = LOT_SIZES.get(index, 30)   # FIX: use correct lot sizes

    # ── Pre-trade checks ──────────────────────────────────
    ok, reason = state.can_trade()
    if not ok:
        return {"status": "REJECTED", "reason": reason}

    underlying_ltp = float(payload.get("entry", 0))
    if underlying_ltp <= 0:
        return {"status": "REJECTED", "reason": "Missing or zero entry price in webhook payload"}

    sl_price  = float(payload.get("sl", 0))
    tgt_price = float(payload.get("target", 0))

    # ── Build option symbol ───────────────────────────────
    symbol = build_option_symbol(index, strike_m, expiry, option, underlying_ltp)

    # ── Get option LTP ────────────────────────────────────
    opt_ltp = broker.get_ltp(symbol)
    if opt_ltp <= 0:
        if CFG["paper_trade"]:
            opt_ltp = 200.0   # paper trade fallback
            log.info(f"[PAPER] Using synthetic option LTP: ₹{opt_ltp}")
        else:
            return {"status": "ERROR", "reason": f"LTP=0 for {symbol}. Check symbol/market hours."}

    # ── Premium cap (protect 20k capital) ────────────────
    if opt_ltp > CFG["max_premium"] and not CFG["paper_trade"]:
        log.warning(f"Premium ₹{opt_ltp:.0f} > cap ₹{CFG['max_premium']:.0f} — trying OTM1")
        symbol  = build_option_symbol(index, "OTM1", expiry, option, underlying_ltp)
        opt_ltp = broker.get_ltp(symbol)
        if opt_ltp > CFG["max_premium"]:
            return {
                "status": "REJECTED",
                "reason": f"Premium ₹{opt_ltp:.0f} still exceeds cap ₹{CFG['max_premium']:.0f}. "
                          f"Increase MAX_PREMIUM or reduce capital usage."
            }

    # ── Auto lot sizing based on capital ─────────────────
    # FIX: calculate lots from available capital, not from payload
    lots = calculate_lots_for_capital(opt_ltp, lot_size)
    qty  = lots * lot_size

    # ── Fund check ────────────────────────────────────────
    ok, msg = FundManager.can_afford(opt_ltp, lots, lot_size)
    if not ok:
        return {"status": "REJECTED", "reason": msg}

    log.info(f"Fund check: {msg}")

    # ── Option SL / target (in option premium terms) ──────
    # Underlying move → option premium amplified ~3x (delta approx for ATM)
    if sl_price > 0:
        underlying_move_pct = abs(underlying_ltp - sl_price) / underlying_ltp
    else:
        underlying_move_pct = 0.006   # default 0.6% of underlying

    sl_opt  = max(5.0, round(opt_ltp * (1 - underlying_move_pct * 3), 1))
    tgt_opt = round(opt_ltp * (1 + underlying_move_pct * 3 * 2), 1)   # 2:1 RR

    # ── Place entry order ─────────────────────────────────
    try:
        entry_oid = broker.place_order(
            trading_symbol=symbol,
            qty=qty,
            transaction_type="B",
            order_type="MKT",
        )
    except Exception as e:
        return {"status": "ERROR", "reason": f"Entry order failed: {e}"}

    # ── Place SL order ────────────────────────────────────
    sl_oid = ""
    try:
        sl_oid = broker.place_sl_order(
            trading_symbol=symbol,
            qty=qty,
            trigger_price=round(sl_opt * 0.99, 1),
            price=sl_opt,
            transaction_type="S",
        )
    except Exception as e:
        log.warning(f"SL order failed (will monitor manually): {e}")

    # ── Track position ────────────────────────────────────
    pos = OpenPosition(
        symbol=symbol, option=option, index=index,
        entry_price=opt_ltp,
        sl_underlying=sl_price, target_underlying=tgt_price,
        trail_mode=trail_mode,
        lots=lots, qty=qty,
        entry_oid=entry_oid, sl_oid=sl_oid,
        trail_sl_opt=sl_opt, target_opt=tgt_opt,
        entry_time=datetime.now().isoformat(),
    )

    with state_lock:
        state.open_positions[symbol] = pos
        state.daily_trades += 1
        state.trade_log.append({
            "time":        pos.entry_time,
            "symbol":      symbol,
            "action":      action,
            "option":      option,
            "entry_opt":   opt_ltp,
            "sl_opt":      sl_opt,
            "target_opt":  tgt_opt,
            "lots":        lots,
            "qty":         qty,
            "entry_oid":   entry_oid,
            "sl_oid":      sl_oid,
            "underlying":  underlying_ltp,
            "capital_used": round(opt_ltp * qty, 2),
        })

    cost = round(opt_ltp * qty, 2)
    log.info(
        f"🚀 TRADE EXECUTED: {symbol} | {qty} qty ({lots}L) @ ₹{opt_ltp:.1f} | "
        f"Cost ₹{cost:.0f} | SL ₹{sl_opt:.1f} | Tgt ₹{tgt_opt:.1f}"
    )

    return {
        "status":         "EXECUTED",
        "symbol":         symbol,
        "option":         option,
        "lots":           lots,
        "qty":            qty,
        "entry_premium":  opt_ltp,
        "sl_premium":     sl_opt,
        "target_premium": tgt_opt,
        "capital_used":   cost,
        "entry_order_id": entry_oid,
        "sl_order_id":    sl_oid,
        "paper_trade":    CFG["paper_trade"],
    }


def _squareoff_all_internal() -> dict:
    """Square off all open positions — used by /squareoff_all and SQUAREOFF_ALL action."""
    results = []
    with state_lock:
        symbols = [s for s, p in state.open_positions.items() if p.status == "OPEN"]
    for sym in symbols:
        try:
            pos = state.open_positions[sym]
            ltp = broker.get_ltp(sym)
            oid = broker.place_order(trading_symbol=sym, qty=pos.qty,
                                     transaction_type="S", order_type="MKT")
            with state_lock:
                pos.status = "MANUAL_EXIT"
                pos.pnl    = (ltp - pos.entry_price) * pos.qty
                state.daily_pnl += pos.pnl
            results.append({"symbol": sym, "status": "OK", "pnl": round(pos.pnl, 2)})
        except Exception as e:
            results.append({"symbol": sym, "status": "ERROR", "error": str(e)})
    return {"squared_off": results}


# ─────────────────────────────────────────────────────────────
# FLASK ROUTES
# ─────────────────────────────────────────────────────────────

@app.route("/login", methods=["POST"])
def login():
    """Trigger Kotak Neo login. Automatically sets session_ok=True."""
    try:
        neo_client.login()   # FIX: login() now sets state.session_ok=True internally
        return jsonify({"status": "OK", "message": "Kotak Neo login successful", "session_ok": True})
    except Exception as e:
        log.error(f"Login failed: {e}")
        return jsonify({"status": "ERROR", "message": str(e)}), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    """Receive signal JSON from kotak_live.py or TradingView → execute trade."""
    try:
        payload = request.get_json(force=True)
        log.info(f"📩 Webhook received: {payload}")

        if payload.get("tag") != CFG["webhook_secret"]:
            return jsonify({"error": "Unauthorized — wrong webhook_secret"}), 403

        result = execute_trade(payload)
        status_code = 200 if result.get("status") in ("EXECUTED", "PAPER", "OK") else 400
        return jsonify(result), status_code

    except Exception as e:
        log.error(f"Webhook error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/status", methods=["GET"])
def status():
    with state_lock:
        state.reset_if_new_day()
        open_pos = [p for p in state.open_positions.values() if p.status == "OPEN"]
        return jsonify({
            "broker":          "Kotak Neo",
            "environment":     CFG["environment"],
            "paper_trade":     CFG["paper_trade"],
            "session_active":  state.session_ok,
            "daily_trades":    state.daily_trades,
            "max_trades":      CFG["max_trades"],
            "daily_pnl":       round(state.daily_pnl, 2),
            "max_daily_loss":  f"₹{CFG['max_capital'] * CFG['max_daily_loss'] / 100:.0f}",
            "available_funds": FundManager.get_available(),
            "open_positions":  len(open_pos),
            "halted":          state.halted,
            "max_capital":     CFG["max_capital"],
        })


@app.route("/positions", methods=["GET"])
def positions():
    with state_lock:
        result = []
        for sym, p in state.open_positions.items():
            try:
                live_ltp = broker.get_ltp(sym) if p.status == "OPEN" else p.entry_price
                live_pnl = (live_ltp - p.entry_price) * p.qty if p.status == "OPEN" else p.pnl
            except:
                live_ltp = p.entry_price
                live_pnl = p.pnl
            result.append({
                "symbol":     sym,
                "index":      p.index,
                "option":     p.option,
                "entry":      p.entry_price,
                "ltp":        live_ltp,
                "trail_sl":   p.trail_sl_opt,
                "target":     p.target_opt,
                "lots":       p.lots,
                "qty":        p.qty,
                "status":     p.status,
                "pnl":        round(live_pnl, 2),
                "entry_time": p.entry_time,
                "capital":    round(p.entry_price * p.qty, 2),
            })
        return jsonify(result)


@app.route("/squareoff/<symbol>", methods=["POST"])
def squareoff(symbol: str):
    with state_lock:
        pos = state.open_positions.get(symbol)
    if not pos or pos.status != "OPEN":
        return jsonify({"error": f"No open position for {symbol}"}), 404
    try:
        ltp = broker.get_ltp(symbol)
        oid = broker.place_order(trading_symbol=symbol, qty=pos.qty,
                                 transaction_type="S", order_type="MKT")
        with state_lock:
            pos.status = "MANUAL_EXIT"
            pos.pnl    = (ltp - pos.entry_price) * pos.qty
            state.daily_pnl += pos.pnl
        return jsonify({"status": "SQUARED_OFF", "symbol": symbol,
                        "pnl": round(pos.pnl, 2), "order_id": oid})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/squareoff_all", methods=["POST"])
def squareoff_all():
    return jsonify(_squareoff_all_internal())


@app.route("/halt", methods=["POST"])
def halt():
    with state_lock:
        state.halted = True
    log.warning("🚫 TRADING HALTED")
    return jsonify({"halted": True})


@app.route("/resume", methods=["POST"])
def resume():
    with state_lock:
        state.halted = False
    log.info("✅ Trading RESUMED")
    return jsonify({"halted": False})


@app.route("/log", methods=["GET"])
def trade_log():
    with state_lock:
        return jsonify(state.trade_log[-100:])


@app.route("/funds", methods=["GET"])
def funds():
    try:
        return jsonify(broker.get_funds())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/broker_positions", methods=["GET"])
def broker_positions():
    try:
        return jsonify(broker.get_positions())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running", "time": datetime.now().isoformat(),
                    "session_ok": state.session_ok, "paper_trade": CFG["paper_trade"]})


# ─────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 65)
    log.info("🇮🇳  Kotak Neo Options Auto-Trader v4.0 (FIXED)")
    log.info(f"    Environment : {CFG['environment'].upper()}")
    log.info(f"    Paper Mode  : {CFG['paper_trade']}")
    log.info(f"    Capital     : ₹{CFG['max_capital']:,.0f}")
    log.info(f"    Max Loss/Day: {CFG['max_daily_loss']}%  = ₹{CFG['max_capital'] * CFG['max_daily_loss'] / 100:,.0f}")
    log.info(f"    Max Trades  : {CFG['max_trades']} per day")
    log.info(f"    Max Premium : ₹{CFG['max_premium']:.0f} per option")
    log.info(f"    Port        : {CFG['port']}")
    log.info("=" * 65)

    if CFG["consumer_key"] and CFG["mobile"] and CFG["totp_secret"]:
        try:
            neo_client.login()
            # session_ok is set inside login() now
            log.info("✅ Auto-login successful — session_ok=True")
        except Exception as e:
            log.warning(f"Auto-login failed: {e}")
            log.warning("⚠️  POST /login to authenticate manually")
    else:
        log.warning("⚠️  Credentials not configured — set .env")

    TrailMonitor(check_interval=5).start()
    app.run(host="0.0.0.0", port=CFG["port"], debug=False)
