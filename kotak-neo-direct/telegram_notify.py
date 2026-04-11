"""
telegram_notify.py — Telegram bot notifications for Kotak Neo Auto-Trader

Setup:
  1. Message @BotFather on Telegram → /newbot → copy token
  2. Message @userinfobot on Telegram → copy your chat_id
  3. Add to .env:
       TELEGRAM_TOKEN=123456:ABC-your-token
       TELEGRAM_CHAT_ID=123456789

Commands your bot will respond to:
  /status   — current positions + day P&L
  /log      — last 30 lines of kotak_live.log
  /trades   — today's trade log
  /stop     — emergency square-off all (requires confirmation)
"""

import os
import threading
import requests
import logging
from datetime import datetime

log = logging.getLogger("TelegramBot")

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kotak_live.log")


def _token():   return os.getenv("TELEGRAM_TOKEN", "").strip()
def _chat_id(): return os.getenv("TELEGRAM_CHAT_ID", "").strip()
def _enabled(): return bool(_token() and _chat_id())


def _send(text: str, parse_mode: str = "HTML") -> bool:
    if not _enabled():
        return False
    tok     = _token()
    chat_id = _chat_id()
    try:
        url  = f"https://api.telegram.org/bot{tok}/sendMessage"
        resp = requests.post(url, json={
            "chat_id":    chat_id,
            "text":       text[:4096],
            "parse_mode": parse_mode,
        }, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")
        return False


def notify_startup(cfg: dict):
    mode = "🟡 PAPER" if cfg.get("paper") else "🔴 LIVE"
    _send(
        f"🚀 <b>Kotak Neo Trader Started</b>\n"
        f"Mode    : {mode}\n"
        f"Index   : {cfg.get('index')}\n"
        f"Capital : ₹{cfg.get('capital', 0):,.0f}\n"
        f"Max Trades: {cfg.get('max_trades')}/day\n"
        f"Timeframe : {cfg.get('timeframe')}min\n"
        f"Time    : {datetime.now().strftime('%d-%b-%Y %H:%M:%S')}"
    )


def notify_trade_open(symbol: str, option: str, entry_ltp: float,
                      sl_prem: float, tgt_prem: float,
                      qty: int, lots: int, cost: float,
                      spot: float, confirmations: int, reason: str,
                      mode: str = "LIVE"):
    emoji = "📈" if option == "CE" else "📉"
    _send(
        f"{emoji} <b>TRADE OPEN [{mode}]</b>\n"
        f"Symbol  : <code>{symbol}</code>\n"
        f"Type    : {'BUY CALL (CE)' if option == 'CE' else 'BUY PUT  (PE)'}\n"
        f"Entry   : ₹{entry_ltp:.1f}  ×{qty} ({lots}L)\n"
        f"Cost    : ₹{cost:.0f}\n"
        f"SL      : ₹{sl_prem:.1f}\n"
        f"Target  : ₹{tgt_prem:.1f}\n"
        f"Spot    : ₹{spot:.1f}\n"
        f"Confs   : {confirmations}/8\n"
        f"Reason  : {reason}\n"
        f"Time    : {datetime.now().strftime('%H:%M:%S')}"
    )


def notify_trade_exit(symbol: str, reason: str, entry_ltp: float,
                      exit_ltp: float, pnl: float, qty: int):
    emoji = "✅" if pnl >= 0 else "❌"
    _send(
        f"{emoji} <b>TRADE EXIT [{reason}]</b>\n"
        f"Symbol  : <code>{symbol}</code>\n"
        f"Entry   : ₹{entry_ltp:.1f}\n"
        f"Exit    : ₹{exit_ltp:.1f}\n"
        f"Qty     : {qty}\n"
        f"P&amp;L     : ₹{pnl:.0f}\n"
        f"Time    : {datetime.now().strftime('%H:%M:%S')}"
    )


def notify_sl_trail(symbol: str, old_sl: float, new_sl: float, peak: float):
    _send(
        f"📈 <b>TRAIL SL</b> <code>{symbol}</code>\n"
        f"SL : ₹{old_sl:.1f} → ₹{new_sl:.1f}\n"
        f"Peak: ₹{peak:.1f}"
    )


def notify_daily_summary(trades: int, pnl: float, candles: int):
    emoji = "✅" if pnl >= 0 else "❌"
    _send(
        f"{emoji} <b>Daily Summary</b>\n"
        f"Trades  : {trades}\n"
        f"P&amp;L     : ₹{pnl:.2f}\n"
        f"Candles : {candles}\n"
        f"Date    : {datetime.now().strftime('%d-%b-%Y')}"
    )


def notify_error(msg: str):
    _send(f"⚠️ <b>ERROR</b>\n{msg}\n{datetime.now().strftime('%H:%M:%S')}")


def notify_halted(reason: str):
    _send(f"🛑 <b>TRADING HALTED</b>\n{reason}\n{datetime.now().strftime('%H:%M:%S')}")


# ── Bot command listener ──────────────────────────────────────────────────────
class TelegramCommandListener(threading.Thread):
    """
    Polls Telegram for incoming commands every 3 seconds.
    Responds to /status /log /trades /stop
    """
    def __init__(self, get_state_fn, square_off_all_fn):
        super().__init__(daemon=True, name="TelegramBot")
        self.get_state      = get_state_fn       # callable → DayState
        self.square_off_all = square_off_all_fn  # callable(reason)
        self._offset        = 0
        self._stop_confirm  = {}                 # chat_id → timestamp

    def run(self):
        if not _enabled():
            log.info("Telegram bot disabled — set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in .env")
            return
        log.info("🤖 Telegram bot listening for commands")
        while True:
            try:
                self._poll()
            except Exception as e:
                log.warning(f"Telegram poll: {e}")
            threading.Event().wait(3)

    def _poll(self):
        url  = f"https://api.telegram.org/bot{_token()}/getUpdates"
        resp = requests.get(url, params={"offset": self._offset, "timeout": 2}, timeout=10)
        if resp.status_code != 200:
            return
        for update in resp.json().get("result", []):
            self._offset = update["update_id"] + 1
            msg = update.get("message", {})
            chat_id = str(msg.get("chat", {}).get("id", ""))
            text    = msg.get("text", "").strip().lower()

            # Only respond to configured chat
            if chat_id != str(TELEGRAM_CHAT_ID):
                continue

            if text == "/status":
                self._cmd_status()
            elif text == "/log":
                self._cmd_log()
            elif text == "/trades":
                self._cmd_trades()
            elif text == "/stop":
                self._cmd_stop(chat_id)
            elif text == "/stop confirm":
                self._cmd_stop_confirm(chat_id)
            elif text == "/help":
                self._cmd_help()

    def _cmd_status(self):
        state = self.get_state()
        open_pos = state.open_positions()
        lines = [
            f"📊 <b>Status</b>  {datetime.now().strftime('%H:%M:%S')}",
            f"Trades : {state.trades}",
            f"P&amp;L    : ₹{state.pnl:.2f}",
            f"Halted : {'🛑 YES' if state.halted else '✅ NO'}",
            f"Open   : {len(open_pos)} position(s)",
        ]
        for sym, pos in open_pos.items():
            lines.append(
                f"\n  <code>{sym}</code>\n"
                f"  Entry ₹{pos.entry_ltp:.1f}  SL ₹{pos.sl_prem:.1f}  "
                f"Tgt ₹{pos.tgt_prem:.1f}\n"
                f"  Peak ₹{pos.peak_ltp:.1f}"
            )
        _send("\n".join(lines))

    def _cmd_log(self):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            last30 = "".join(lines[-30:])
            _send(f"📋 <b>Last 30 log lines</b>\n<pre>{last30[-3500:]}</pre>")
        except Exception as e:
            _send(f"❌ Could not read log: {e}")

    def _cmd_trades(self):
        state = self.get_state()
        if not state.trade_log:
            _send("📭 No trades today.")
            return
        lines = [f"📒 <b>Today's Trades ({len(state.trade_log)})</b>"]
        for t in state.trade_log:
            pnl_str = ""
            for pos in state.positions.values():
                if pos.symbol == t["symbol"]:
                    pnl_str = f"  P&amp;L ₹{pos.pnl:.0f}" if pos.pnl else ""
                    break
            lines.append(
                f"\n<code>{t['symbol']}</code> {t['signal']}\n"
                f"  Entry ₹{t['entry_ltp']:.1f}  SL ₹{t['sl_prem']:.1f}  "
                f"Tgt ₹{t['tgt_prem']:.1f}\n"
                f"  Qty {t['qty']} ({t['lots']}L)  Cost ₹{t['cost']:.0f}"
                f"{pnl_str}\n"
                f"  {t['time'][:19]}"
            )
        _send("\n".join(lines))

    def _cmd_stop(self, chat_id: str):
        self._stop_confirm[chat_id] = datetime.now()
        _send(
            "⚠️ <b>EMERGENCY STOP</b>\n"
            "This will square off ALL open positions immediately.\n"
            "Reply <code>/stop confirm</code> within 30s to proceed."
        )

    def _cmd_stop_confirm(self, chat_id: str):
        ts = self._stop_confirm.get(chat_id)
        if not ts or (datetime.now() - ts).total_seconds() > 30:
            _send("❌ Confirmation expired. Send /stop again.")
            return
        self._stop_confirm.pop(chat_id, None)
        _send("🛑 Squaring off all positions...")
        try:
            results = self.square_off_all("TELEGRAM_STOP")
            total_pnl = sum(r.get("pnl", 0) for r in results if isinstance(r, dict))
            _send(
                f"✅ Square-off done\n"
                f"Positions closed: {len(results)}\n"
                f"Total P&amp;L: ₹{total_pnl:.0f}"
            )
        except Exception as e:
            _send(f"❌ Square-off failed: {e}")

    def _cmd_help(self):
        _send(
            "🤖 <b>Available Commands</b>\n"
            "/status — open positions + P&amp;L\n"
            "/log    — last 30 log lines\n"
            "/trades — today's trade log\n"
            "/stop   — emergency square-off all\n"
            "/help   — this message"
        )
