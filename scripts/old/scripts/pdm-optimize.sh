#!/usr/bin/env bash
set -euxo pipefail

echo "=== Updating system and installing tools ==="
sudo apt update
sudo apt install -y linux-cpupower irqbalance

echo "=== Setting CPU governor to performance and persisting via systemd ==="
# Create a tiny systemd unit that sets the governor each boot
sudo tee /etc/systemd/system/cpupower.service >/dev/null << 'EOF'
[Unit]
Description=Set CPU governor to performance
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/usr/bin/cpupower frequency-set -g performance

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now cpupower.service

echo "=== Apply governor immediately for this session too ==="
sudo cpupower frequency-set -g performance

echo "=== Kernel tuning to reduce swap lag ==="
sudo tee /etc/sysctl.d/99-pdm.conf >/dev/null <<'EOF'
vm.swappiness = 10
vm.vfs_cache_pressure = 50
vm.dirty_background_ratio = 5
vm.dirty_ratio = 15
EOF
sudo sysctl --system

echo "=== Enable IRQ balancing ==="
sudo systemctl enable --now irqbalance

echo "=== Optional: disable HDMI if headless ==="
if command -v vcgencmd >/dev/null; then
  vcgencmd display_power 0 || true
fi

echo "? Optimization complete! Reboot recommended: sudo reboot"
