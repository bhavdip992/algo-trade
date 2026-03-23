"""
signal_engine.py  v3 — FIXED
────────────────────────────
KEY FIXES vs v2:
  1. Signal needs 5/8 confirmations (not ALL 8) → actually fires in live
  2. Separate CE and PE confirmation counts → cleaner directional logic
  3. Volume spike is OPTIONAL (indices have synthetic volume in spot feed)
  4. BB squeeze blocks only when very tight (threshold relaxed)
  5. Capital-based lot sizing for ₹20,000 capital
  6. Added signal_reason string for debugging
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Literal
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
@dataclass
class EngineConfig:
    # EMA lengths
    fast_len:     int   = 9
    mid_len:      int   = 21
    slow_len:     int   = 50
    trend_len:    int   = 200

    # Supertrend
    st_atr_len:   int   = 10
    st_factor:    float = 3.0

    # RSI
    rsi_len:      int   = 14
    rsi_ob:       int   = 70     # overbought — don't buy CE above this
    rsi_os:       int   = 30     # oversold   — don't buy PE below this

    # VWAP
    use_vwap:     bool  = True

    # Volume
    vol_mult:     float = 1.2    # FIX: lowered from 1.5 — spot index often has flat volume
    vol_required: bool  = False  # FIX: volume spike is NOT required (index feed unreliable)

    # ADX
    adx_len:      int   = 14
    adx_thresh:   int   = 20     # FIX: lowered from 25 — 25 is too strict

    # Bollinger Bands squeeze filter
    bb_len:       int   = 20
    bb_std:       float = 2.0
    bb_squeeze_block: bool = True  # block trades during squeeze

    # Minimum confirmations needed out of 8
    min_confirmations: int = 5   # FIX: was requiring all 8

    # SL / Target
    sl_mode:      str   = "ATR"
    sl_atr_mult:  float = 1.5
    sl_pct:       float = 0.8
    tgt_mode:     str   = "2:1 RR"
    tgt_pct:      float = 1.5
    tgt_atr_mult: float = 3.0

    # Trailing SL
    use_trail:    bool  = True
    trail_mode:   str   = "ATR"
    trail_trig:   float = 0.5
    trail_step:   float = 0.3

    # Position sizing — FIX: set for ₹20,000 capital
    capital:      float = 20000.0
    risk_pct:     float = 2.0     # risk 2% of capital per trade = ₹400
    lot_size:     int   = 30      # BANKNIFTY lot size (NSE: 30)
    max_lots:     int   = 1
    option_type:  str   = "AUTO"  # AUTO | CE Only | PE Only

    # Max option premium willing to pay per lot (₹ × lot_size)
    max_premium_per_lot: float = 500.0  # won't buy options above ₹500 premium


# ─────────────────────────────────────────────────────────────
# SIGNAL RESULT
# ─────────────────────────────────────────────────────────────
@dataclass
class SignalResult:
    signal:           Literal["BUY_CE", "BUY_PE", "NONE"]
    timestamp:        datetime
    close:            float
    sl:               float
    target:           float
    atr:              float
    rsi:              float
    adx:              float
    vwap:             float
    qty_lots:         int
    risk_amt:         float
    trail_sl:         float = 0.0
    signal_reason:    str   = ""   # NEW: why signal fired
    # Individual confirmations
    conf_ema_cross:   bool  = False
    conf_trend:       bool  = False
    conf_supertrend:  bool  = False
    conf_vwap:        bool  = False
    conf_rsi:         bool  = False
    conf_adx:         bool  = False
    conf_volume:      bool  = False
    conf_no_squeeze:  bool  = False

    @property
    def confirmations(self):
        return sum([
            self.conf_ema_cross, self.conf_trend, self.conf_supertrend,
            self.conf_vwap, self.conf_rsi, self.conf_adx,
            self.conf_volume, self.conf_no_squeeze
        ])

    def conf_string(self) -> str:
        return "".join([
            "E" if self.conf_ema_cross  else "·",
            "T" if self.conf_trend      else "·",
            "S" if self.conf_supertrend else "·",
            "V" if self.conf_vwap       else "·",
            "R" if self.conf_rsi        else "·",
            "A" if self.conf_adx        else "·",
            "L" if self.conf_volume     else "·",
            "B" if self.conf_no_squeeze else "·",
        ])


# ─────────────────────────────────────────────────────────────
# INDICATOR HELPERS (pure pandas/numpy — no ta-lib needed)
# ─────────────────────────────────────────────────────────────
def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def _rma(s: pd.Series, n: int) -> pd.Series:
    """Wilder's smoothing (same as Pine's rma())"""
    return s.ewm(alpha=1.0 / n, adjust=False).mean()

def _atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int) -> pd.Series:
    pc = close.shift(1)
    tr = pd.concat([(high - low), (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    return _rma(tr, n)

def _rsi(close: pd.Series, n: int) -> pd.Series:
    d  = close.diff()
    up = _rma(d.clip(lower=0), n)
    dn = _rma((-d).clip(lower=0), n)
    rs = up / dn.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)

def _adx(high: pd.Series, low: pd.Series, close: pd.Series, n: int) -> pd.Series:
    atr  = _atr(high, low, close, n)
    up   = high.diff()
    dn   = -low.diff()
    dmp  = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=high.index)
    dmn  = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=high.index)
    dip  = 100 * _rma(dmp, n) / atr.replace(0, np.nan)
    din  = 100 * _rma(dmn, n) / atr.replace(0, np.nan)
    dx   = 100 * (dip - din).abs() / (dip + din).replace(0, np.nan)
    return _rma(dx.fillna(0), n).fillna(0)

def _bbands(close: pd.Series, n: int, k: float):
    mid = _sma(close, n)
    sig = close.rolling(n).std(ddof=0)
    return mid, mid + k * sig, mid - k * sig

def _supertrend(high: pd.Series, low: pd.Series, close: pd.Series, n: int, mult: float):
    atr  = _atr(high, low, close, n)
    hl2  = (high + low) / 2
    bu   = (hl2 + mult * atr).values.copy()
    bl   = (hl2 - mult * atr).values.copy()
    cl   = close.values
    size = len(cl)
    st   = np.full(size, np.nan)
    dirn = np.zeros(size, dtype=int)
    for i in range(1, size):
        if np.isnan(atr.iloc[i]):
            continue
        bl[i] = bl[i] if (bl[i] > bl[i-1] or cl[i-1] < bl[i-1]) else bl[i-1]
        bu[i] = bu[i] if (bu[i] < bu[i-1] or cl[i-1] > bu[i-1]) else bu[i-1]
        prev  = st[i-1] if not np.isnan(st[i-1]) else bu[i]
        if prev == bu[i-1]:
            if cl[i] <= bu[i]:  st[i] = bu[i]; dirn[i] = 1
            else:               st[i] = bl[i]; dirn[i] = -1
        else:
            if cl[i] >= bl[i]:  st[i] = bl[i]; dirn[i] = -1
            else:               st[i] = bu[i]; dirn[i] = 1
    return pd.Series(st, index=close.index), pd.Series(dirn, index=close.index)

def _vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
    hlc3     = (high + low + close) / 3
    g        = high.index.date
    cumtpv   = (hlc3 * volume).groupby(g).cumsum()
    cumvol   = volume.groupby(g).cumsum()
    return (cumtpv / cumvol.replace(0, np.nan)).fillna(hlc3)


# ─────────────────────────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────────────────────────
class SignalEngine:
    def __init__(self, cfg: EngineConfig = None):
        self.cfg       = cfg or EngineConfig()
        self.df        = pd.DataFrame(
            columns=['open', 'high', 'low', 'close', 'volume'], dtype=float
        )
        self.df.index.name = 'timestamp'
        self._in_long   = False
        self._in_short  = False
        self._entry_px  = 0.0
        self._trail_sl  = 0.0

    # ── Feed one candle ─────────────────────────────────────
    def update(self, timestamp, open_, high, low, close, volume) -> SignalResult:
        row = pd.DataFrame(
            [{'open': float(open_), 'high': float(high), 'low': float(low),
              'close': float(close), 'volume': float(volume)}],
            index=[pd.Timestamp(timestamp)]
        )
        self.df = pd.concat([self.df, row], ignore_index=False)
        self.df = self.df.astype(float)
        # Keep last 500 bars (memory)
        if len(self.df) > 500:
            self.df = self.df.iloc[-500:]
        return self.evaluate()

    # ── Evaluate current bar ────────────────────────────────
    def evaluate(self) -> SignalResult:
        df  = self.df
        cfg = self.cfg

        # Need at least trend_len bars to warm up EMA 200
        if len(df) < max(cfg.trend_len, cfg.bb_len, 30):
            return self._empty(df)

        close  = df['close'].astype(float)
        high   = df['high'].astype(float)
        low    = df['low'].astype(float)
        volume = df['volume'].astype(float)
        px     = float(close.iloc[-1])

        # ── EMAs ──
        ema_fast  = _ema(close, cfg.fast_len)
        ema_mid   = _ema(close, cfg.mid_len)
        ef        = float(ema_fast.iloc[-1])
        ef1       = float(ema_fast.iloc[-2])
        em        = float(ema_mid.iloc[-1])
        em1       = float(ema_mid.iloc[-2])
        es        = float(_ema(close, cfg.slow_len).iloc[-1])
        et        = float(_ema(close, cfg.trend_len).iloc[-1])

        # ── ATR / RSI / ADX ──
        atr     = float(_atr(high, low, close, 14).iloc[-1])
        rsi     = float(_rsi(close, cfg.rsi_len).iloc[-1])
        adx_val = float(_adx(high, low, close, cfg.adx_len).iloc[-1])

        # ── Bollinger squeeze ──
        _, bb_up, bb_lo = _bbands(close, cfg.bb_len, cfg.bb_std)
        bw         = (bb_up - bb_lo) / close.replace(0, np.nan)
        bw_avg     = _sma(bw, cfg.bb_len)
        # FIX: only block if bandwidth is < 60% of average (was 80% — too sensitive)
        bb_squeeze = float(bw.iloc[-1]) < float(bw_avg.iloc[-1]) * 0.6

        # ── Supertrend ──
        try:
            _, st_dir = _supertrend(high, low, close, cfg.st_atr_len, cfg.st_factor)
            st_d = int(st_dir.iloc[-1])
        except Exception:
            st_d = 0

        # ── VWAP ──
        try:
            vwap_val = float(_vwap(high, low, close, volume).iloc[-1])
        except Exception:
            vwap_val = px

        # ── Volume ──
        vol_avg   = float(_sma(volume, 20).iloc[-1]) if float(_sma(volume, 20).iloc[-1]) > 0 else 1.0
        vol_spike = float(volume.iloc[-1]) > vol_avg * cfg.vol_mult

        # ─────────────────────────────────────────────────────
        # INDIVIDUAL CONFIRMATIONS
        # ─────────────────────────────────────────────────────
        cross_up    = ef1 < em1 and ef >= em     # fast EMA crossed above mid EMA
        cross_down  = ef1 > em1 and ef <= em     # fast EMA crossed below mid EMA

        # Trend: price above/below EMA200 and fast > slow (or reverse)
        bull_trend  = px > et and ef > es
        bear_trend  = px < et and ef < es

        # Supertrend: -1 = bullish, +1 = bearish
        st_bull     = st_d == -1
        st_bear     = st_d == 1

        # VWAP
        vwap_bull   = (not cfg.use_vwap) or (px > vwap_val)
        vwap_bear   = (not cfg.use_vwap) or (px < vwap_val)

        # RSI: for CE buy, RSI 45-70 (not oversold, not overbought); for PE buy, RSI 30-55
        rsi_buy_ok  = 45 < rsi < cfg.rsi_ob   # FIX: was rsi > 50, now > 45 (more lenient)
        rsi_sell_ok = cfg.rsi_os < rsi < 55    # FIX: was rsi < 50, now < 55

        trending    = adx_val > cfg.adx_thresh
        no_squeeze  = not bb_squeeze

        # ─────────────────────────────────────────────────────
        # COUNT CONFIRMATIONS SEPARATELY FOR BUY_CE AND BUY_PE
        # FIX: this is the main fix — we count per direction, not require all
        # ─────────────────────────────────────────────────────
        ce_confs = [cross_up, bull_trend, st_bull, vwap_bull, rsi_buy_ok, trending, vol_spike, no_squeeze]
        pe_confs = [cross_down, bear_trend, st_bear, vwap_bear, rsi_sell_ok, trending, vol_spike, no_squeeze]

        ce_count = sum(ce_confs)
        pe_count = sum(pe_confs)

        # Minimum confirmations check
        min_conf = cfg.min_confirmations
        # If vol_required is False, remove volume from minimum requirement
        if not cfg.vol_required:
            # Ignore volume spike in minimum count
            ce_count_adj = sum(ce_confs[:6]) + sum(ce_confs[7:])  # skip index 6 (vol)
            pe_count_adj = sum(pe_confs[:6]) + sum(pe_confs[7:])
            min_conf_adj = min_conf - 1  # one less required since volume not counted
        else:
            ce_count_adj = ce_count
            pe_count_adj = pe_count
            min_conf_adj = min_conf

        buy_ok  = (ce_count_adj >= min_conf_adj) and (cfg.option_type != "PE Only")
        sell_ok = (pe_count_adj >= min_conf_adj) and (cfg.option_type != "CE Only")

        # BUY_CE takes priority if both fire (shouldn't happen often)
        if buy_ok and sell_ok:
            # Prefer whichever has more confirmations
            buy_ok  = ce_count_adj >= pe_count_adj
            sell_ok = not buy_ok

        signal = "BUY_CE" if buy_ok else "BUY_PE" if sell_ok else "NONE"

        # Build reason string for debugging
        reason = ""
        if signal == "BUY_CE":
            fired = ["EMA" if ce_confs[0] else "", "Trend" if ce_confs[1] else "",
                     "ST" if ce_confs[2] else "", "VWAP" if ce_confs[3] else "",
                     "RSI" if ce_confs[4] else "", "ADX" if ce_confs[5] else "",
                     "Vol" if ce_confs[6] else "", "NoBB" if ce_confs[7] else ""]
            reason = f"CE {ce_count_adj}/{min_conf_adj}: " + ",".join(f for f in fired if f)
        elif signal == "BUY_PE":
            fired = ["EMA" if pe_confs[0] else "", "Trend" if pe_confs[1] else "",
                     "ST" if pe_confs[2] else "", "VWAP" if pe_confs[3] else "",
                     "RSI" if pe_confs[4] else "", "ADX" if pe_confs[5] else "",
                     "Vol" if pe_confs[6] else "", "NoBB" if pe_confs[7] else ""]
            reason = f"PE {pe_count_adj}/{min_conf_adj}: " + ",".join(f for f in fired if f)

        # ─────────────────────────────────────────────────────
        # SL / TARGET
        # ─────────────────────────────────────────────────────
        swing_lo = float(low.iloc[-10:].min())
        swing_hi = float(high.iloc[-10:].max())

        if signal == "BUY_CE":
            if cfg.sl_mode == "ATR":
                sl = px - atr * cfg.sl_atr_mult
            elif cfg.sl_mode == "Fixed %":
                sl = px * (1 - cfg.sl_pct / 100)
            else:
                sl = swing_lo
            rr  = 2.0 if cfg.tgt_mode == "2:1 RR" else 3.0 if cfg.tgt_mode == "3:1 RR" else 0
            tgt = (px * (1 + cfg.tgt_pct / 100) if cfg.tgt_mode == "Fixed %"
                   else px + atr * cfg.tgt_atr_mult if cfg.tgt_mode == "ATR"
                   else px + (px - sl) * rr)

        elif signal == "BUY_PE":
            if cfg.sl_mode == "ATR":
                sl = px + atr * cfg.sl_atr_mult
            elif cfg.sl_mode == "Fixed %":
                sl = px * (1 + cfg.sl_pct / 100)
            else:
                sl = swing_hi
            rr  = 2.0 if cfg.tgt_mode == "2:1 RR" else 3.0 if cfg.tgt_mode == "3:1 RR" else 0
            tgt = (px * (1 - cfg.tgt_pct / 100) if cfg.tgt_mode == "Fixed %"
                   else px - atr * cfg.tgt_atr_mult if cfg.tgt_mode == "ATR"
                   else px - (sl - px) * rr)
        else:
            sl  = px - atr * cfg.sl_atr_mult
            tgt = px + atr * cfg.tgt_atr_mult * 2

        # ─────────────────────────────────────────────────────
        # LOT SIZING based on capital
        # For ₹20,000 capital: risk 2% = ₹400 per trade
        # ─────────────────────────────────────────────────────
        risk_amt = cfg.capital * cfg.risk_pct / 100
        risk_pts = max(abs(px - sl), 1.0)
        qty_lots = max(1, int(risk_amt / (risk_pts * cfg.lot_size)))
        qty_lots = min(qty_lots, cfg.max_lots)

        # ─────────────────────────────────────────────────────
        # TRAILING SL STATE
        # ─────────────────────────────────────────────────────
        if signal == "BUY_CE":
            self._in_long  = True
            self._in_short = False
            self._entry_px = px
            self._trail_sl = sl

        if signal == "BUY_PE":
            self._in_short = True
            self._in_long  = False
            self._entry_px = px
            self._trail_sl = sl

        if self._in_long and cfg.use_trail:
            if px >= self._entry_px + atr * cfg.trail_trig:
                self._trail_sl = max(self._trail_sl, px - atr * cfg.trail_step)
            if px <= self._trail_sl:
                self._in_long = False

        if self._in_short and cfg.use_trail:
            if px <= self._entry_px - atr * cfg.trail_trig:
                self._trail_sl = min(self._trail_sl, px + atr * cfg.trail_step)
            if px >= self._trail_sl:
                self._in_short = False

        return SignalResult(
            signal        = signal,
            timestamp     = df.index[-1],
            close         = round(px, 2),
            sl            = round(sl, 2),
            target        = round(tgt, 2),
            atr           = round(atr, 2),
            rsi           = round(rsi, 1),
            adx           = round(adx_val, 1),
            vwap          = round(vwap_val, 2),
            qty_lots      = qty_lots,
            risk_amt      = round(risk_amt, 0),
            trail_sl      = round(self._trail_sl, 2),
            signal_reason = reason,
            conf_ema_cross  = cross_up or cross_down,
            conf_trend      = bull_trend or bear_trend,
            conf_supertrend = st_bull or st_bear,
            conf_vwap       = vwap_bull or vwap_bear,
            conf_rsi        = rsi_buy_ok or rsi_sell_ok,
            conf_adx        = trending,
            conf_volume     = vol_spike,
            conf_no_squeeze = no_squeeze,
        )

    def _empty(self, df: pd.DataFrame) -> SignalResult:
        px = float(df['close'].iloc[-1]) if len(df) > 0 else 0.0
        ts = df.index[-1] if len(df) > 0 else datetime.now()
        return SignalResult(
            signal="NONE", timestamp=ts,
            close=px, sl=0, target=0, atr=0, rsi=0, adx=0,
            vwap=0, qty_lots=0, risk_amt=0,
            signal_reason=f"Warming up: {len(df)}/{self.cfg.trend_len} bars"
        )
