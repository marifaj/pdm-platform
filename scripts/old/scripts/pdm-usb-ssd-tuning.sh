#!/usr/bin/env bash
set -euxo pipefail

echo "=== 0) Helpers: show devices so you can confirm IDs afterwards ==="
echo "# USB devices:"
lsusb || true
echo "# Disks:"
lsblk -o NAME,ROTA,SIZE,MODEL,TRAN || true
echo "# UAS driver in use?"
lsmod | grep -E "uas|usb_storage" || true

echo "=== 1) Disable autosuspend for common USB serial chips (avoid ESP32/sensor dropouts) ==="
# Add udev rule that sets power/control=on when these devices are added.
# You can add/remove lines as needed. Common IDs:
#  - CH340/CH341: 1a86:7523 or 1a86:55d4
#  - CP210x:      10c4:ea60
#  - FTDI:        0403:6001
sudo tee /etc/udev/rules.d/99-pdm-usb-nosuspend.rules >/dev/null <<'EOF'
# Keep USB serial adapters awake (no autosuspend)
ACTION=="add", SUBSYSTEM=="usb", ATTR{idVendor}=="1a86", ATTR{idProduct}=="7523", TEST=="power/control", RUN+="/bin/sh -c 'echo on > /sys$devpath/power/control'"
ACTION=="add", SUBSYSTEM=="usb", ATTR{idVendor}=="1a86", ATTR{idProduct}=="55d4", TEST=="power/control", RUN+="/bin/sh -c 'echo on > /sys$devpath/power/control'"
ACTION=="add", SUBSYSTEM=="usb", ATTR{idVendor}=="10c4", ATTR{idProduct}=="ea60", TEST=="power/control", RUN+="/bin/sh -c 'echo on > /sys$devpath/power/control'"
ACTION=="add", SUBSYSTEM=="usb", ATTR{idVendor}=="0403", ATTR{idProduct}=="6001", TEST=="power/control", RUN+="/bin/sh -c 'echo on > /sys$devpath/power/control'"
EOF

sudo udevadm control --reload
sudo udevadm trigger

echo "=== 2) Prefer SSD-friendly I/O scheduler + larger readahead for non-rotational disks ==="
# For any *newly plugged* non-rotational disk (rotational=0), set:
#  - mq-deadline scheduler (low jitter)
#  - readahead 4096 (helps sequential reads)
sudo tee /etc/udev/rules.d/60-ssd-performance.rules >/dev/null <<'EOF'
# Apply to non-rotational disks only (e.g., SSD/UASP)
ACTION=="add|change", KERNEL=="sd[a-z]", SUBSYSTEM=="block", ATTR{queue/rotational}=="0", RUN+="/bin/sh -c 'echo mq-deadline > /sys/block/%k/queue/scheduler || true'"
ACTION=="add|change", KERNEL=="sd[a-z]", SUBSYSTEM=="block", ATTR{queue/rotational}=="0", RUN+="/sbin/blockdev --setra 4096 /dev/%k"
EOF

sudo udevadm control --reload
sudo udevadm trigger

echo "=== 3) Enable periodic TRIM (keeps SSD fast; safer than continuous discard) ==="
sudo systemctl enable --now fstrim.timer

echo "=== 4) (Optional) Apply scheduler + readahead now on current disks ==="
for d in /sys/block/sd*; do
  if [ -f "$d/queue/rotational" ] && [ "$(cat "$d/queue/rotational")" = "0" ]; then
    echo mq-deadline | sudo tee "$d/queue/scheduler" >/dev/null || true
    sudo blockdev --setra 4096 "/dev/$(basename "$d")"
  fi
done

echo "=== 5) (Optional) Suggest fstab mount options (manual edit) ==="
echo "# If your SSD has an ext4 partition you mount at, consider in /etc/fstab:"
echo "# UUID=<your-uuid>  /mnt/data  ext4  noatime,nodiratime,commit=60  0  2"
echo "# (Use 'sudo blkid' to find UUID; create /mnt/data first)"
echo "# We avoid continuous 'discard' and rely on fstrim.timer (weekly)."

echo "✅ USB & SSD tuning applied. You can reboot, but it's not strictly required."
