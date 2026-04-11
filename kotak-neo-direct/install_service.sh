#!/bin/bash
set -e

SERVICE_SRC="/home/ubuntu/algo-trade/algo-trade/kotak-neo-direct/kotak_live.service"
DEST="/etc/systemd/system/kotak-live.service"

sudo cp "$SERVICE_SRC" "$DEST"
sudo systemctl daemon-reload
sudo systemctl enable kotak-live
sudo systemctl start kotak-live

echo ""
echo "✅ Service installed and started."
echo ""
echo "  sudo systemctl status kotak-live     # check status"
echo "  sudo journalctl -u kotak-live -f     # live logs"
echo "  sudo systemctl stop kotak-live       # stop"
echo "  sudo systemctl restart kotak-live    # restart"
