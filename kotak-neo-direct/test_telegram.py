"""
test_telegram.py — Test Telegram notifications without running the full trader
Usage: python test_telegram.py
"""

import os
import sys
from dotenv import load_dotenv

# Load .env from parent directory (same as kotak_live.py)
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

from telegram_notify import _send, _token, _chat_id, _enabled

def check_config():
    print(f"TELEGRAM_TOKEN   : {'✅ SET' if _token() else '❌ MISSING'} ({_token()[:10]}...)")
    print(f"TELEGRAM_CHAT_ID : {'✅ SET' if _chat_id() else '❌ MISSING'} ({_chat_id()})")
    print(f"Bot enabled      : {'✅ YES' if _enabled() else '❌ NO'}")
    if not _enabled():
        print("\n❌ Set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in your .env file first.")
        sys.exit(1)

    # Raw API test with full error output
    import requests
    print("\nTesting API connection...")
    try:
        url  = f"https://api.telegram.org/bot{_token()}/getMe"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get("ok"):
            print(f"  ✅ Bot connected: @{data['result']['username']}")
        else:
            print(f"  ❌ Bot error: {data}")
            sys.exit(1)
    except Exception as e:
        print(f"  ❌ Connection error: {e}")
        sys.exit(1)

    print("\nSending test ping...")
    import requests as req
    r = req.post(
        f"https://api.telegram.org/bot{_token()}/sendMessage",
        json={"chat_id": _chat_id(), "text": "ping"},
        timeout=10
    )
    print(f"  Status: {r.status_code}")
    print(f"  Response: {r.json()}")
    if not r.ok:
        sys.exit(1)
    print()

def test_startup():
    print("Sending startup message...")
    ok = _send(
        "🚀 <b>Kotak Neo Trader Started</b>\n"
        "Mode    : 🟡 PAPER\n"
        "Index   : NIFTY\n"
        "Capital : ₹1,00,000\n"
        "Max Trades: 3/day\n"
        "Timeframe : 5min\n"
        "Time    : TEST MESSAGE"
    )
    print(f"  {'✅ OK' if ok else '❌ FAILED'}")

def test_trade_open():
    print("Sending trade open message...")
    ok = _send(
        "📈 <b>TRADE OPEN [PAPER]</b>\n"
        "Symbol  : <code>NIFTY15APR2624000CE</code>\n"
        "Type    : BUY CALL (CE)\n"
        "Entry   : ₹150.0  ×65 (1L)\n"
        "Cost    : ₹9750\n"
        "SL      : ₹127.5\n"
        "Target  : ₹206.3\n"
        "Spot    : ₹23950.0\n"
        "Confs   : 6/8\n"
        "Reason  : EMA cross + Supertrend bullish\n"
        "Time    : 10:15:00"
    )
    print(f"  {'✅ OK' if ok else '❌ FAILED'}")

def test_trade_exit():
    print("Sending trade exit message...")
    ok = _send(
        "✅ <b>TRADE EXIT [TGT_HIT]</b>\n"
        "Symbol  : <code>NIFTY15APR2624000CE</code>\n"
        "Entry   : ₹150.0\n"
        "Exit    : ₹210.5\n"
        "Qty     : 65\n"
        "P&amp;L     : ₹3932\n"
        "Time    : 11:30:00"
    )
    print(f"  {'✅ OK' if ok else '❌ FAILED'}")

def test_sl_trail():
    print("Sending SL trail message...")
    ok = _send(
        "📈 <b>TRAIL SL</b> <code>NIFTY15APR2624000CE</code>\n"
        "SL : ₹127.5 → ₹164.0\n"
        "Peak: ₹200.0"
    )
    print(f"  {'✅ OK' if ok else '❌ FAILED'}")

def test_daily_summary():
    print("Sending daily summary message...")
    ok = _send(
        "✅ <b>Daily Summary</b>\n"
        "Trades  : 2\n"
        "P&amp;L     : ₹4250.00\n"
        "Candles : 78\n"
        "Date    : TEST"
    )
    print(f"  {'✅ OK' if ok else '❌ FAILED'}")

def test_error():
    print("Sending error message...")
    ok = _send("⚠️ <b>ERROR</b>\nTest error message\n10:00:00")
    print(f"  {'✅ OK' if ok else '❌ FAILED'}")

def test_status():
    print("Sending status message...")
    ok = _send(
        "📊 <b>Status</b>  10:30:00\n"
        "Trades : 1\n"
        "P&amp;L    : ₹3932.00\n"
        "Halted : ✅ NO\n"
        "Open   : 0 position(s)"
    )
    print(f"  {'✅ OK' if ok else '❌ FAILED'}")


if __name__ == "__main__":
    print("=" * 45)
    print("  Telegram Bot Test")
    print("=" * 45)
    check_config()

    test_startup()
    test_trade_open()
    test_trade_exit()
    test_sl_trail()
    test_daily_summary()
    test_error()
    test_status()

    print()
    print("✅ All test messages sent — check your Telegram!")
