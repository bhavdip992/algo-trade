"""
backtest.py
═══════════
Backtest the signal engine on historical Kotak Neo data.

Data sources (in order of preference):
  1. Kotak Neo historical API  (if logged in)
  2. CSV file you provide      (--csv path/to/file.csv)
  3. NSEpy / nsepython         (free, no login needed)

Output:
  - Terminal summary table
  - backtest_report.html  (full trade log + equity curve + metrics)

Usage:
  py -3.11 backtest.py                          # Kotak Neo data
  py -3.11 backtest.py --csv mydata.csv         # your own CSV
  py -3.11 backtest.py --days 60                # last 60 days
  py -3.11 backtest.py --index NIFTY --tf 15    # NIFTY 15-min
  py -3.11 backtest.py --optimize               # find best params

Install:
  py -3.11 -m pip install pandas numpy pyotp python-dotenv requests
"""

import os, sys, json, argparse, time, logging
import warnings

warnings.filterwarnings('ignore')
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field, asdict
from typing import List
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

# Import our signal engine
sys.path.insert(0, os.path.dirname(__file__))
from signal_engine import SignalEngine, EngineConfig, SignalResult

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("Backtest")

# ═════════════════════════════════════════════════════════════
# CONFIG
# ═════════════════════════════════════════════════════════════
LOT_SIZES = {
    "NIFTY": 65, "BANKNIFTY": 30, "FINNIFTY": 60, "MIDCPNIFTY": 120
}

# ═════════════════════════════════════════════════════════════
# TRADE RECORD
# ═════════════════════════════════════════════════════════════
@dataclass
class Trade:
    entry_time:   datetime
    exit_time:    datetime
    signal:       str        # BUY_CE | BUY_PE
    entry_price:  float
    exit_price:   float
    sl:           float
    target:       float
    qty:          int
    lots:         int
    exit_reason:  str        # TARGET | SL | TRAIL_SL | EOD | SIGNAL_FLIP
    pnl:          float
    pnl_pct:      float
    rsi_at_entry: float
    adx_at_entry: float
    atr_at_entry: float
    conf_count:   int
    duration_min: int

    @property
    def is_winner(self): return self.pnl > 0

# ═════════════════════════════════════════════════════════════
# DATA LOADERS
# ═════════════════════════════════════════════════════════════

def load_from_kotak(index: str, tf_min: int, days: int) -> pd.DataFrame:
    """
    Kotak Neo API has no historical data endpoint.
    Use nse_data.py which fetches real NSE data from multiple free sources.
    """
    try:
        from nse_data import get_historical_data
        return get_historical_data(index=index, tf_min=tf_min, days=days, source="auto")
    except ImportError:
        log.warning("nse_data.py not found in same folder — place nse_data.py next to backtest.py")
        return pd.DataFrame()
    except Exception as e:
        log.warning(f"nse_data fetch failed: {e}")
        return pd.DataFrame()
def load_from_csv(path: str) -> pd.DataFrame:
    """
    Load OHLCV from a CSV file.
    Expected columns: timestamp/datetime/date, open, high, low, close, volume
    """
    log.info(f"Loading CSV: {path}")
    df = pd.read_csv(path)

    # Find timestamp column
    for col in ["timestamp", "datetime", "date", "time", "Date", "Datetime"]:
        if col in df.columns:
            df["timestamp"] = pd.to_datetime(df[col])
            break
    else:
        df["timestamp"] = pd.to_datetime(df.iloc[:, 0])

    # Normalise column names to lowercase
    df.columns = [c.lower().strip() for c in df.columns]
    df = df.set_index("timestamp").sort_index()

    # Rename common variants
    rename_map = {
        "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume",
        "open price": "open", "high price": "high", "low price": "low",
        "close price": "close", "vol": "volume",
    }
    df = df.rename(columns=rename_map)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in CSV. Columns: {list(df.columns)}")
    if "volume" not in df.columns:
        df["volume"] = 1000000   # default volume if missing

    df = df[["open", "high", "low", "close", "volume"]].astype(float)
    log.info(f"Loaded {len(df)} candles from CSV ({df.index[0].date()} → {df.index[-1].date()})")
    return df


def load_from_nsepython(index: str, tf_min: int, days: int) -> pd.DataFrame:
    """Fallback: fetch from NSE directly using nsepython (no login needed)."""
    try:
        from nsepython import nse_eq, nsetools_live
        log.info("Trying nsepython for data...")
    except ImportError:
        pass

    # Build synthetic data for testing if all else fails
    log.warning("No data source available — generating synthetic test data")
    return _generate_synthetic_data(index, tf_min, days)


def _generate_synthetic_data(index: str, tf_min: int, days: int) -> pd.DataFrame:
    """
    Generate realistic synthetic OHLCV data for testing.
    Uses geometric Brownian motion with realistic volatility.
    """
    log.info("Generating synthetic data for testing (BANKNIFTY~50000, NIFTY~23000)...")
    start_price = 50000.0 if index == "BANKNIFTY" else 23000.0
    sessions_per_day = int((6 * 60 + 15) / tf_min)  # 9:15 to 15:30
    total_bars = days * sessions_per_day

    np.random.seed(42)
    dt    = tf_min / (252 * 375)
    sigma = 0.18  # annualised vol
    mu    = 0.08  # annual drift

    prices = [start_price]
    for _ in range(total_bars):
        ret = (mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * np.random.randn()
        prices.append(prices[-1] * np.exp(ret))

    rows = []
    base = datetime.now() - timedelta(days=days)
    bar_idx = 0
    for d in range(days):
        day = base + timedelta(days=d)
        if day.weekday() >= 5:
            continue
        session_start = day.replace(hour=9, minute=15, second=0, microsecond=0)
        for s in range(sessions_per_day):
            if bar_idx >= len(prices) - 1:
                break
            ts = session_start + timedelta(minutes=s * tf_min)
            o  = prices[bar_idx]
            c  = prices[bar_idx + 1]
            noise = abs(np.random.randn()) * o * sigma * np.sqrt(dt) * 2
            h  = max(o, c) + noise
            l  = min(o, c) - noise
            v  = np.random.randint(50000, 300000)
            rows.append({"timestamp": ts, "open": o, "high": h,
                         "low": l, "close": c, "volume": float(v)})
            bar_idx += 1

    df = pd.DataFrame(rows).set_index("timestamp")
    log.info(f"Generated {len(df)} synthetic candles")
    return df


# ═════════════════════════════════════════════════════════════
# BACKTEST ENGINE
# ═════════════════════════════════════════════════════════════
class Backtester:
    def __init__(self, df: pd.DataFrame, cfg: EngineConfig,
                 index: str = "NIFTY",
                 max_trades_per_day: int = 3,
                 initial_capital: float = 100000.0):
        self.df          = df
        self.cfg         = cfg
        self.index       = index
        self.max_tpd     = max_trades_per_day
        self.capital     = initial_capital
        self.lot_size    = LOT_SIZES.get(index, 30)
        self.trades: List[Trade] = []
        self.equity_curve: List[tuple] = []

    def run(self) -> List[Trade]:
        log.info(f"Running backtest on {len(self.df)} bars...")
        engine = SignalEngine(self.cfg)

        current_trade   = None
        daily_trades    = 0
        last_date       = None
        running_capital = self.capital

        for i, (ts, row) in enumerate(self.df.iterrows()):
            # Feed bar to engine
            result = engine.update(
                timestamp=ts,
                open_=row["open"], high=row["high"],
                low=row["low"],    close=row["close"],
                volume=row["volume"],
            )

            bar_date = ts.date()

            # Reset daily trade counter
            if bar_date != last_date:
                daily_trades = 0
                last_date    = bar_date

            px = float(row["close"])

            # ── Manage open trade ──────────────────────────
            if current_trade is not None:
                exit_reason = None
                exit_price  = px

                is_ce = current_trade["signal"] == "BUY_CE"

                # Target hit
                if is_ce and px >= current_trade["target"]:
                    exit_reason = "TARGET"
                    exit_price  = current_trade["target"]
                elif not is_ce and px <= current_trade["target"]:
                    exit_reason = "TARGET"
                    exit_price  = current_trade["target"]

                # SL hit
                elif is_ce and px <= current_trade["trail_sl"]:
                    exit_reason = "TRAIL_SL" if current_trade["trail_sl"] > current_trade["sl"] else "SL"
                    exit_price  = current_trade["trail_sl"]
                elif not is_ce and px >= current_trade["trail_sl"]:
                    exit_reason = "TRAIL_SL" if current_trade["trail_sl"] < current_trade["sl"] else "SL"
                    exit_price  = current_trade["trail_sl"]

                # EOD square-off (15:10)
                elif ts.hour == 15 and ts.minute >= 10:
                    exit_reason = "EOD"
                    exit_price  = px

                # Update trailing SL
                else:
                    atr = result.atr if result.atr > 0 else current_trade["atr"]
                    if is_ce:
                        trig = current_trade["entry"] + atr * self.cfg.trail_trig
                        if px >= trig:
                            new_sl = px - atr * self.cfg.trail_step
                            current_trade["trail_sl"] = max(current_trade["trail_sl"], new_sl)
                    else:
                        trig = current_trade["entry"] - atr * self.cfg.trail_trig
                        if px <= trig:
                            new_sl = px + atr * self.cfg.trail_step
                            current_trade["trail_sl"] = min(current_trade["trail_sl"], new_sl)

                if exit_reason:
                    pnl = (exit_price - current_trade["entry"]) * current_trade["qty"]
                    if not is_ce:
                        pnl = -pnl
                    pnl_pct = pnl / running_capital * 100
                    running_capital += pnl

                    dur = int((ts - current_trade["entry_time"]).total_seconds() / 60)

                    trade = Trade(
                        entry_time=current_trade["entry_time"],
                        exit_time=ts,
                        signal=current_trade["signal"],
                        entry_price=current_trade["entry"],
                        exit_price=exit_price,
                        sl=current_trade["sl"],
                        target=current_trade["target"],
                        qty=current_trade["qty"],
                        lots=current_trade["lots"],
                        exit_reason=exit_reason,
                        pnl=round(pnl, 2),
                        pnl_pct=round(pnl_pct, 3),
                        rsi_at_entry=current_trade["rsi"],
                        adx_at_entry=current_trade["adx"],
                        atr_at_entry=current_trade["atr"],
                        conf_count=current_trade["conf"],
                        duration_min=dur,
                    )
                    self.trades.append(trade)
                    self.equity_curve.append((ts, round(running_capital, 2)))
                    current_trade = None

            # ── Check for new signal ───────────────────────
            if (current_trade is None
                    and result.signal != "NONE"
                    and daily_trades < self.max_tpd
                    and result.atr > 0):

                qty  = result.qty_lots * self.lot_size
                current_trade = {
                    "signal":     result.signal,
                    "entry_time": ts,
                    "entry":      px,
                    "sl":         result.sl,
                    "target":     result.target,
                    "trail_sl":   result.sl,
                    "qty":        qty,
                    "lots":       result.qty_lots,
                    "atr":        result.atr,
                    "rsi":        result.rsi,
                    "adx":        result.adx,
                    "conf":       getattr(result, "score", getattr(result, "confirmations", 0)),
                }
                daily_trades += 1

        # Close any open trade at end of data
        if current_trade is not None:
            px  = float(self.df["close"].iloc[-1])
            pnl = (px - current_trade["entry"]) * current_trade["qty"]
            if current_trade["signal"] == "BUY_PE":
                pnl = -pnl
            running_capital += pnl
            dur = int((self.df.index[-1] - current_trade["entry_time"]).total_seconds() / 60)
            self.trades.append(Trade(
                entry_time=current_trade["entry_time"],
                exit_time=self.df.index[-1],
                signal=current_trade["signal"],
                entry_price=current_trade["entry"],
                exit_price=px,
                sl=current_trade["sl"],
                target=current_trade["target"],
                qty=current_trade["qty"],
                lots=current_trade["lots"],
                exit_reason="END_OF_DATA",
                pnl=round(pnl, 2),
                pnl_pct=round(pnl / self.capital * 100, 3),
                rsi_at_entry=current_trade["rsi"],
                adx_at_entry=current_trade["adx"],
                atr_at_entry=current_trade["atr"],
                conf_count=current_trade["conf"],
                duration_min=dur,
            ))

        log.info(f"Backtest complete — {len(self.trades)} trades")
        return self.trades


# ═════════════════════════════════════════════════════════════
# METRICS
# ═════════════════════════════════════════════════════════════
def compute_metrics(trades: List[Trade], initial_capital: float) -> dict:
    if not trades:
        print("\n  No trades generated. Reasons:")
        print("  1. Need 200+ bars to warm up EMA 200 — use --days 60 or more")
        print("  2. Signal conditions are strict (7 confirmations) — try synthetic data with --days 90")
        print("  3. If using CSV, check it has enough rows (at least 250 candles)")
        sys.exit(0)

    pnls      = [t.pnl for t in trades]
    winners   = [t for t in trades if t.is_winner]
    losers    = [t for t in trades if not t.is_winner]
    total_pnl = sum(pnls)

    win_rate  = len(winners) / len(trades) * 100
    avg_win   = sum(t.pnl for t in winners) / max(len(winners), 1)
    avg_loss  = sum(t.pnl for t in losers)  / max(len(losers),  1)
    rr_ratio  = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    # Max drawdown
    equity = initial_capital
    peak   = initial_capital
    max_dd = 0.0
    for t in trades:
        equity += t.pnl
        peak    = max(peak, equity)
        dd      = (peak - equity) / peak * 100
        max_dd  = max(max_dd, dd)

    # Profit factor
    gross_profit = sum(t.pnl for t in winners)
    gross_loss   = abs(sum(t.pnl for t in losers))
    profit_factor = gross_profit / max(gross_loss, 1)

    # Sharpe (simplified daily)
    daily_pnl = {}
    for t in trades:
        d = t.exit_time.date()
        daily_pnl[d] = daily_pnl.get(d, 0) + t.pnl
    dpnls = list(daily_pnl.values())
    sharpe = (np.mean(dpnls) / np.std(dpnls) * np.sqrt(252)) if len(dpnls) > 1 and np.std(dpnls) > 0 else 0

    # Breakdown by exit reason
    exit_counts = {}
    for t in trades:
        exit_counts[t.exit_reason] = exit_counts.get(t.exit_reason, 0) + 1

    # CE vs PE
    ce_trades  = [t for t in trades if t.signal == "BUY_CE"]
    pe_trades  = [t for t in trades if t.signal == "BUY_PE"]

    return {
        "total_trades":    len(trades),
        "winners":         len(winners),
        "losers":          len(losers),
        "win_rate":        round(win_rate, 1),
        "total_pnl":       round(total_pnl, 2),
        "total_pnl_pct":   round(total_pnl / initial_capital * 100, 2),
        "avg_win":         round(avg_win,  2),
        "avg_loss":        round(avg_loss, 2),
        "rr_ratio":        round(rr_ratio, 2),
        "profit_factor":   round(profit_factor, 2),
        "max_drawdown_pct":round(max_dd,  2),
        "sharpe_ratio":    round(sharpe,   2),
        "best_trade":      round(max(pnls), 2),
        "worst_trade":     round(min(pnls), 2),
        "avg_duration_min":round(sum(t.duration_min for t in trades) / len(trades), 1),
        "exit_breakdown":  exit_counts,
        "ce_trades":       len(ce_trades),
        "pe_trades":       len(pe_trades),
        "ce_win_rate":     round(len([t for t in ce_trades if t.is_winner]) / max(len(ce_trades), 1) * 100, 1),
        "pe_win_rate":     round(len([t for t in pe_trades if t.is_winner]) / max(len(pe_trades), 1) * 100, 1),
        "final_capital":   round(initial_capital + total_pnl, 2),
    }


# ═════════════════════════════════════════════════════════════
# PRINT REPORT TO TERMINAL
# ═════════════════════════════════════════════════════════════
def print_report(metrics: dict, trades: List[Trade]):
    sep = "─" * 52
    print(f"\n{'═'*52}")
    print(f"  BACKTEST RESULTS")
    print(f"{'═'*52}")
    print(f"  Total Trades    : {metrics['total_trades']}")
    print(f"  Winners         : {metrics['winners']}  ({metrics['win_rate']}%)")
    print(f"  Losers          : {metrics['losers']}")
    print(sep)
    print(f"  Total P&L       : Rs.{metrics['total_pnl']:>10,.2f}  ({metrics['total_pnl_pct']}%)")
    print(f"  Final Capital   : Rs.{metrics['final_capital']:>10,.2f}")
    print(f"  Max Drawdown    : {metrics['max_drawdown_pct']}%")
    print(f"  Profit Factor   : {metrics['profit_factor']}")
    print(f"  Sharpe Ratio    : {metrics['sharpe_ratio']}")
    print(sep)
    print(f"  Avg Win         : Rs.{metrics['avg_win']:>8,.2f}")
    print(f"  Avg Loss        : Rs.{metrics['avg_loss']:>8,.2f}")
    print(f"  Risk:Reward     : {metrics['rr_ratio']}")
    print(f"  Avg Duration    : {metrics['avg_duration_min']} min")
    print(sep)
    print(f"  CE trades       : {metrics['ce_trades']}  (Win: {metrics['ce_win_rate']}%)")
    print(f"  PE trades       : {metrics['pe_trades']}  (Win: {metrics['pe_win_rate']}%)")
    print(sep)
    print(f"  Exit reasons:")
    for reason, count in sorted(metrics["exit_breakdown"].items(), key=lambda x: -x[1]):
        print(f"    {reason:<16}: {count}")
    print(f"{'═'*52}\n")

    # Last 10 trades
    print("  Last 10 trades:")
    print(f"  {'Date':<12} {'Sig':<7} {'Entry':>7} {'Exit':>7} {'P&L':>8} {'Reason':<12}")
    print(f"  {'-'*12} {'-'*7} {'-'*7} {'-'*7} {'-'*8} {'-'*12}")
    for t in trades[-10:]:
        sign = "+" if t.pnl >= 0 else ""
        print(f"  {str(t.entry_time.date()):<12} {t.signal:<7} "
              f"{t.entry_price:>7.0f} {t.exit_price:>7.0f} "
              f"{sign}{t.pnl:>7.0f}  {t.exit_reason:<12}")
    print()


# ═════════════════════════════════════════════════════════════
# HTML REPORT
# ═════════════════════════════════════════════════════════════
def generate_html_report(trades: List[Trade], metrics: dict,
                          equity_curve: list, cfg: EngineConfig,
                          output_path: str = "backtest_report.html"):
    # Build equity curve data
    eq_labels = [str(ts.date()) for ts, _ in equity_curve[::max(1, len(equity_curve)//100)]]
    eq_values = [v for _, v in equity_curve[::max(1, len(equity_curve)//100)]]

    # Monthly P&L
    monthly: dict = {}
    for t in trades:
        key = t.exit_time.strftime("%Y-%m")
        monthly[key] = monthly.get(key, 0) + t.pnl
    monthly_labels = list(monthly.keys())
    monthly_values = [round(v, 2) for v in monthly.values()]
    monthly_colors = ["'#1D9E75'" if v >= 0 else "'#E24B4A'" for v in monthly_values]

    # Trade log rows
    trade_rows = ""
    for i, t in enumerate(reversed(trades), 1):
        color = "#1a3a2a" if t.is_winner else "#3a1a1a"
        sign  = "+" if t.pnl >= 0 else ""
        trade_rows += f"""
        <tr style="background:{color}">
            <td>{len(trades)-i+1}</td>
            <td>{t.entry_time.strftime('%d-%b %H:%M')}</td>
            <td>{t.exit_time.strftime('%d-%b %H:%M')}</td>
            <td><span class="badge {'ce' if t.signal=='BUY_CE' else 'pe'}">{t.signal}</span></td>
            <td>{t.entry_price:.0f}</td>
            <td>{t.exit_price:.0f}</td>
            <td>{t.sl:.0f}</td>
            <td>{t.target:.0f}</td>
            <td>{t.lots}</td>
            <td style="color:{'#4ade80' if t.is_winner else '#f87171'}">{sign}Rs.{t.pnl:,.0f}</td>
            <td>{t.exit_reason}</td>
            <td>{t.conf_count}/8</td>
            <td>{t.duration_min}m</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Backtest Report</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0f1117; color: #e2e8f0; font-family: 'Segoe UI', sans-serif; padding: 24px; }}
  h1 {{ color: #f97316; font-size: 22px; margin-bottom: 6px; }}
  h2 {{ color: #94a3b8; font-size: 14px; font-weight: 400; margin-bottom: 24px; }}
  h3 {{ color: #cbd5e1; font-size: 15px; margin: 24px 0 12px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 24px; }}
  .card {{ background: #1e2433; border: 1px solid #2d3748; border-radius: 10px; padding: 16px; }}
  .card .label {{ font-size: 11px; color: #64748b; text-transform: uppercase; letter-spacing: .5px; margin-bottom: 6px; }}
  .card .value {{ font-size: 22px; font-weight: 600; }}
  .green {{ color: #4ade80; }}
  .red {{ color: #f87171; }}
  .orange {{ color: #fb923c; }}
  .white {{ color: #f1f5f9; }}
  .chart-wrap {{ background: #1e2433; border: 1px solid #2d3748; border-radius: 10px; padding: 16px; margin-bottom: 24px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  th {{ background: #1e2433; color: #94a3b8; padding: 8px 10px; text-align: left; border-bottom: 1px solid #2d3748; position: sticky; top: 0; }}
  td {{ padding: 7px 10px; border-bottom: 1px solid #1a2030; }}
  .badge {{ padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
  .badge.ce {{ background: #052e16; color: #4ade80; }}
  .badge.pe {{ background: #450a0a; color: #f87171; }}
  .tbl-wrap {{ background: #1e2433; border: 1px solid #2d3748; border-radius: 10px; overflow: auto; max-height: 500px; }}
  .params {{ background: #1e2433; border: 1px solid #2d3748; border-radius: 10px; padding: 16px; margin-bottom: 24px; font-size: 12px; color: #94a3b8; line-height: 1.8; }}
</style>
</head>
<body>
<h1>Backtest Report — {metrics.get('total_trades',0)} Trades</h1>
<h2>Generated: {datetime.now().strftime('%d %b %Y %H:%M')} | Capital: Rs.{cfg.capital:,.0f}</h2>

<div class="grid">
  <div class="card"><div class="label">Total P&L</div>
    <div class="value {'green' if metrics['total_pnl']>=0 else 'red'}">
      Rs.{metrics['total_pnl']:,.0f}</div></div>
  <div class="card"><div class="label">Win Rate</div>
    <div class="value {'green' if metrics['win_rate']>=50 else 'red'}">{metrics['win_rate']}%</div></div>
  <div class="card"><div class="label">Profit Factor</div>
    <div class="value {'green' if metrics['profit_factor']>=1.5 else 'orange'}">{metrics['profit_factor']}</div></div>
  <div class="card"><div class="label">Max Drawdown</div>
    <div class="value red">{metrics['max_drawdown_pct']}%</div></div>
  <div class="card"><div class="label">Sharpe Ratio</div>
    <div class="value {'green' if metrics['sharpe_ratio']>=1 else 'orange'}">{metrics['sharpe_ratio']}</div></div>
  <div class="card"><div class="label">Risk:Reward</div>
    <div class="value orange">{metrics['rr_ratio']}</div></div>
  <div class="card"><div class="label">Total Trades</div>
    <div class="value white">{metrics['total_trades']}</div></div>
  <div class="card"><div class="label">Final Capital</div>
    <div class="value white">Rs.{metrics['final_capital']:,.0f}</div></div>
</div>

<div class="chart-wrap">
  <h3>Equity Curve</h3>
  <div style="height:260px"><canvas id="equityChart"></canvas></div>
</div>

<div class="chart-wrap">
  <h3>Monthly P&L</h3>
  <div style="height:200px"><canvas id="monthlyChart"></canvas></div>
</div>

<h3>All Trades</h3>
<div class="tbl-wrap">
<table>
  <thead><tr>
    <th>#</th><th>Entry</th><th>Exit</th><th>Signal</th>
    <th>Entry Px</th><th>Exit Px</th><th>SL</th><th>Target</th>
    <th>Lots</th><th>P&L</th><th>Exit Reason</th><th>Conf</th><th>Duration</th>
  </tr></thead>
  <tbody>{trade_rows}</tbody>
</table>
</div>

<div class="params" style="margin-top:24px">
  <b style="color:#cbd5e1">Strategy Parameters (v3 Engine):</b><br>
  EMA: {cfg.fast_len}/{cfg.mid_len}/{cfg.slow_len}/{cfg.trend_len} &nbsp;|&nbsp;
  Supertrend: {cfg.st_factor}×{cfg.st_atr_len} &nbsp;|&nbsp;
  RSI: {cfg.rsi_len} ({getattr(cfg,"rsi_os",30)}–{getattr(cfg,"rsi_ob",70)}) &nbsp;|&nbsp;
  ADX: {cfg.adx_thresh} &nbsp;|&nbsp;
  Vol: {cfg.vol_mult}× &nbsp;|&nbsp;
  SL: ATR {cfg.sl_atr_mult}× &nbsp;|&nbsp;
  Trail: {'GREEDY' if not getattr(cfg,'use_hard_target',False) else 'FIXED'} — Breakeven@{cfg.sl_pct}×ATR | Normal@{cfg.trail_step}×ATR | Tight@{cfg.trail_trig}×ATR (after {cfg.use_trail}×ATR profit) &nbsp;|&nbsp;
  Capital: Rs.{cfg.capital:,.0f} | Risk: {cfg.risk_pct}%
</div>

<script>
const eq_labels = {json.dumps(eq_labels)};
const eq_values = {json.dumps(eq_values)};
const mo_labels = {json.dumps(monthly_labels)};
const mo_values = {json.dumps(monthly_values)};
const mo_colors = [{','.join(monthly_colors)}];

new Chart(document.getElementById('equityChart'), {{
  type: 'line',
  data: {{ labels: eq_labels, datasets: [{{
    label: 'Equity', data: eq_values,
    borderColor: '#f97316', backgroundColor: 'rgba(249,115,22,0.08)',
    borderWidth: 2, pointRadius: 0, fill: true, tension: 0.3
  }}]}},
  options: {{ responsive:true, maintainAspectRatio:false,
    plugins:{{ legend:{{display:false}} }},
    scales:{{ x:{{ticks:{{color:'#64748b',maxTicksLimit:8}},grid:{{color:'#1e2d3d'}}}},
              y:{{ticks:{{color:'#64748b'}},grid:{{color:'#1e2d3d'}}}} }} }}
}});

new Chart(document.getElementById('monthlyChart'), {{
  type: 'bar',
  data: {{ labels: mo_labels, datasets: [{{
    label: 'Monthly P&L', data: mo_values, backgroundColor: mo_colors, borderRadius: 4
  }}]}},
  options: {{ responsive:true, maintainAspectRatio:false,
    plugins:{{ legend:{{display:false}} }},
    scales:{{ x:{{ticks:{{color:'#64748b'}},grid:{{color:'#1e2d3d'}}}},
              y:{{ticks:{{color:'#64748b'}},grid:{{color:'#1e2d3d'}}}} }} }}
}});
</script>
</body></html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    log.info(f"HTML report saved: {output_path}")


# ═════════════════════════════════════════════════════════════
# PARAMETER OPTIMIZER (grid search)
# ═════════════════════════════════════════════════════════════
def optimize(df: pd.DataFrame, index: str, capital: float):
    print("\nRunning parameter optimization (grid search)...")
    print("Testing combinations of key parameters...\n")

    best_score = -999
    best_cfg   = None
    best_met   = None
    results    = []

    grid = {
        "st_factor":   [2.5, 3.0, 3.5],
        "sl_atr_mult": [1.2, 1.5, 2.0],
        "tgt_mode":    ["2:1 RR", "3:1 RR"],
        "adx_thresh":  [20, 25, 30],
    }

    from itertools import product
    keys   = list(grid.keys())
    combos = list(product(*grid.values()))
    total  = len(combos)
    print(f"  Testing {total} combinations...\n")

    for i, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        cfg    = EngineConfig(
            capital=capital,
            lot_size=LOT_SIZES.get(index, 30),
            **params,
        )
        bt   = Backtester(df, cfg, index=index, initial_capital=capital)
        trds = bt.run()
        if len(trds) < 5:
            continue
        met   = compute_metrics(trds, capital)
        score = met["profit_factor"] * (met["win_rate"] / 100) * (1 - met["max_drawdown_pct"] / 100)

        results.append({**params, "trades": len(trds), "win_rate": met["win_rate"],
                        "pf": met["profit_factor"], "dd": met["max_drawdown_pct"],
                        "pnl": met["total_pnl"], "score": round(score, 4)})

        if score > best_score:
            best_score = score
            best_cfg   = cfg
            best_met   = met

        if (i + 1) % 10 == 0:
            print(f"  Progress: {i+1}/{total} | Best score: {best_score:.3f}", end="\r")

    print(f"\n  Optimization complete.\n")

    results_df = pd.DataFrame(results).sort_values("score", ascending=False)
    print("  Top 5 parameter combinations:")
    print(results_df.head(5).to_string(index=False))

    if best_met:
        print(f"\n  Best config: {asdict(best_cfg)}")
        print_report(best_met, [])


# ═════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest signal engine on Kotak Neo data")
    parser.add_argument("--csv",      type=str,   default="",            help="Path to OHLCV CSV file")
    parser.add_argument("--index",    type=str,   default="NIFTY",   help="Index name")
    parser.add_argument("--tf",       type=int,   default=5,             help="Timeframe in minutes")
    parser.add_argument("--days",     type=int,   default=30,            help="Historical days to fetch")
    parser.add_argument("--capital",  type=float, default=100000.0,      help="Starting capital")
    parser.add_argument("--max-tpd",  type=int,   default=3,             help="Max trades per day")
    parser.add_argument("--optimize", action="store_true",               help="Run parameter optimization")
    parser.add_argument("--output",   type=str,   default="backtest_report.html", help="HTML report path")
    args = parser.parse_args()

    # ── Load data ──
    df = pd.DataFrame()

    if args.csv:
        df = load_from_csv(args.csv)
    else:
        df = load_from_kotak(args.index, args.tf, args.days)

    if df.empty:
        log.warning("Using synthetic data — Kotak login or CSV not available")
        df = _generate_synthetic_data(args.index, args.tf, max(args.days, 90))

    # ── Config ──
    cfg = EngineConfig(
        capital          = args.capital,
        lot_size         = LOT_SIZES.get(args.index, 30),
        # breakeven_atr    = 0.8,
        sl_atr_mult      = 1.2,
        use_trail        = True,
    )

#     cfg = EngineConfig(
#     lot_size       = LOT_SIZES.get(args.index, 15),
#     capital        = args.capital,
#     min_confirmations       = 5,        # FIXED: was 8/8, now 5/8
#     # cross_lookback = 1,        # EMA cross valid for 3 bars
#     vol_required     = False,    # FIXED: disable unreliable index volume
#     adx_thresh     = 20,       # FIXED: was 25
#     max_lots       = 3,
#     risk_pct       = 5.0,
# )

    # ── Optimize or run single backtest ──
    if args.optimize:
        optimize(df, args.index, args.capital)
    else:
        bt      = Backtester(df, cfg, index=args.index,
                             max_trades_per_day=args.max_tpd,
                             initial_capital=args.capital)
        trades  = bt.run()
        metrics = compute_metrics(trades, args.capital)
        print_report(metrics, trades)
        generate_html_report(trades, metrics, bt.equity_curve, cfg, args.output)
        print(f"  HTML report: {args.output}")
        print(f"  Open it in your browser to see equity curve + full trade log\n")
