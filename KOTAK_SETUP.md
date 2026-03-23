# 🇮🇳 Kotak Neo API Integration — Complete Setup Guide

## Files
| File | Purpose |
|------|---------|
| `broker_bridge_kotak.py` | Main Python server with full Kotak Neo integration |
| `.env.example` | Environment variable template |
| `india_options_trading.pine` | TradingView Pine Script v6 (unchanged) |

---

## Step 1 — Get Kotak Neo API Access

1. Login at **kotakneo.com** or the Neo mobile app
2. Go to **Invest tab → Trade API card**
3. Click **Create Application**
4. Copy the generated **Consumer Key (token)** → `KOTAK_CONSUMER_KEY` in `.env`
5. Your API access is instant — no approval wait needed
6. Zero brokerage on all API orders (only statutory charges apply)

---

## Step 2 — Set Up TOTP (One-Time Setup)

TOTP lets the bot auto-generate login codes without manual intervention.

1. Visit **kotaksecurities.com → Platform → Kotak Neo Trade API → Register for TOTP**
2. Select your account
3. A QR code appears (valid 5 minutes)
4. Open **Google Authenticator** or **Microsoft Authenticator** on your phone
5. Tap **+** → **Scan QR code**
6. You'll now receive 6-digit codes every 30 seconds

**To get the Base32 secret** (needed for `KOTAK_TOTP_SECRET`):
- Most authenticator apps don't show the secret after setup
- Use this method: instead of scanning QR, choose "Enter manually"
- The manual key shown is your Base32 secret
- OR: use a desktop authenticator like **Authy** which shows the secret

```python
# Test your TOTP secret works:
import pyotp
totp = pyotp.TOTP("YOUR_BASE32_SECRET")
print(totp.now())  # Should match your authenticator app
```

---

## Step 3 — Install & Configure

```bash
# Clone / download the files
cd your_project_folder

# Install dependencies
pip install flask pyotp python-dotenv
pip install "git+https://github.com/Kotak-Neo/Kotak-neo-api-v2.git@v2.0.1#egg=neo_api_client"

# Copy and fill in credentials
cp .env.example .env
nano .env   # Fill in all values

# Test paper trading first (PAPER_TRADE=true in .env)
python broker_bridge_kotak.py
```

---

## Step 4 — Daily Startup Routine

The server auto-logins at startup if credentials are configured.

```bash
# Start server
python broker_bridge_kotak.py

# Verify login worked:
curl http://localhost:5000/status

# If login failed, trigger manually:
curl -X POST http://localhost:5000/login
```

---

## Step 5 — Expose Webhook URL

### Local development (ngrok):
```bash
# Install: https://ngrok.com/download
ngrok http 5000
# Use the https URL in TradingView: https://abc123.ngrok.io/webhook
```

### Production (recommended):
Deploy to a VPS (DigitalOcean / AWS / Hetzner):
```bash
# Using systemd service for auto-restart
sudo nano /etc/systemd/system/kotak_trader.service

[Unit]
Description=Kotak Neo Options Trader
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/trader
ExecStart=/usr/bin/python3 broker_bridge_kotak.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target

sudo systemctl enable kotak_trader
sudo systemctl start kotak_trader
```

---

## Step 6 — TradingView Alert Setup

1. Open TradingView with BANKNIFTY or NIFTY chart (5-min timeframe)
2. Add the `india_options_trading.pine` indicator
3. Enable **Webhook** in indicator settings → enter your API tag
4. Right-click chart → **Add Alert**
5. Condition: `India Options Auto-Trader` → `📈 CE Buy Signal`
6. Actions: ✅ **Webhook URL** → `https://your-server.com/webhook`
7. Message: *(leave empty — Pine generates the JSON automatically)*
8. Expiry: **Open-ended**
9. Repeat: **Once per bar close**
10. Create the same for PE signal

**Sample webhook JSON sent by Pine Script:**
```json
{
  "broker": "Kotak Neo",
  "tag": "MY_API_KEY_TAG",
  "action": "BUY_CE",
  "index": "BANKNIFTY",
  "strike": "ATM",
  "expiry": "Weekly",
  "lots": 1,
  "lot_size": 15,
  "entry": 48520.5,
  "sl": 48200.0,
  "target": 48850.0,
  "trail": "ATR"
}
```

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/login` | POST | Trigger TOTP login (auto-runs on startup) |
| `/webhook` | POST | Receive TV signal → place order |
| `/status` | GET | Daily P&L, trades, session status |
| `/positions` | GET | Open positions with live P&L |
| `/broker_positions` | GET | Raw positions from Kotak Neo API |
| `/funds` | GET | Available margin from Kotak Neo |
| `/squareoff/<symbol>` | POST | Manually exit one position |
| `/squareoff_all` | POST | Emergency: exit all positions |
| `/halt` | POST | Stop new signals from executing |
| `/resume` | POST | Resume after halt |
| `/log` | GET | Last 100 trade records |
| `/health` | GET | Server heartbeat |

---

## Architecture

```
TradingView Chart (5-min BANKNIFTY)
        ↓ webhook JSON (HTTPS)
broker_bridge_kotak.py (Flask server)
        ↓ neo_api_client SDK
Kotak Neo API (NFO segment)
        ↓ placed orders
NSE F&O Exchange

Background thread (every 5s):
  broker_bridge_kotak.py
        ↓ get_ltp()
  Kotak Neo API
        ↓ price update
  Update trail SL → modify_order()
```

---

## Kotak Neo SDK Method Reference

```python
from neo_api_client import NeoAPI

client = NeoAPI(consumer_key="...", environment="prod")

# Auth
client.totp_login(mobile_number="+91...", ucc="AB1234", totp="123456")
client.totp_validate(mpin="123456")

# Market data
client.quotes(instrument_tokens=[{"instrument_token": "43245", "exchange_segment": "nfo_fo"}], quote_type="ltp")
client.search_scrip(exchange_segment="nfo_fo", symbol="BANKNIFTY05DEC2448000CE")

# Orders
client.place_order(exchange_segment="nfo_fo", product="INTRADAY", price="0",
                   order_type="MKT", quantity="15", validity="DAY",
                   trading_symbol="BANKNIFTY05DEC2448000CE", transaction_type="B",
                   amo="NO", disclosed_quantity="0", market_protection="0",
                   pf="N", trigger_price="0", tag="MY_BOT")

client.modify_order(order_id="...", price="...", quantity="...",
                    validity="DAY", trigger_price="...", order_type="SL",
                    exchange_segment="nfo_fo", product="INTRADAY",
                    trading_symbol="...")

client.cancel_order(order_id="...", isVerify=False)

# Account
client.positions()
client.limits(segment="FO", exchange="NFO", product="MIS")
client.order_history(order_id="...")
```

---

## Important Notes

- **Sessions** expire every ~8 hours — the server auto-refreshes at 7h mark
- **BANKNIFTY** expires on **Wednesday**, **NIFTY** on **Thursday**
- **Product code** `INTRADAY` = MIS (auto square-off at 3:20 PM by broker)
- **Max 10 orders/second** on Kotak Neo API
- Test with `PAPER_TRADE=true` for at least 30 days before going live
- Keep your `.env` file private — never commit to GitHub

---

## ⚠️ Disclaimer
This is for educational purposes only. Options trading involves substantial risk.
Not SEBI-registered financial advice. Always paper-trade first.
