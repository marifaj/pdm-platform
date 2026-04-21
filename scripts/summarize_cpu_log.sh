#!/bin/bash

LOG_FILE="$1"

if [ -z "$LOG_FILE" ]; then
  echo "Usage: $0 /path/to/cpu_log_file.log"
  exit 1
fi

if [ ! -f "$LOG_FILE" ]; then
  echo "File not found: $LOG_FILE"
  exit 1
fi

awk '
function get_service(cmd) {
  if (cmd ~ /inference/) return "inference"
  if (cmd ~ /ingestion/) return "ingestion"
  if (cmd ~ /storage/) return "storage"
  if (cmd ~ /event_processing/) return "event_processing"
  if (cmd ~ /notification/) return "notification"
  return ""
}

# Match only actual ps output rows:
# PID %CPU %MEM CMD...
$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9.]+$/ && $3 ~ /^[0-9.]+$/ {
  cmd = ""
  for (i = 4; i <= NF; i++) {
    cmd = cmd $i " "
  }

  svc = get_service(cmd)
  if (svc == "") next

  cpu = $2 + 0
  mem = $3 + 0

  cpu_sum[svc] += cpu
  mem_sum[svc] += mem
  count[svc]++

  if (!(svc in cpu_max) || cpu > cpu_max[svc]) cpu_max[svc] = cpu
  if (!(svc in mem_max) || mem > mem_max[svc]) mem_max[svc] = mem
}

END {
  printf "%-20s %-10s %-12s %-12s %-12s %-12s\n", "service", "samples", "avg_cpu", "peak_cpu", "avg_mem", "peak_mem"
  printf "%-20s %-10s %-12s %-12s %-12s %-12s\n", "--------------------", "----------", "------------", "------------", "------------", "------------"

  for (svc in count) {
    printf "%-20s %-10d %-12.2f %-12.2f %-12.2f %-12.2f\n",
      svc,
      count[svc],
      cpu_sum[svc] / count[svc],
      cpu_max[svc],
      mem_sum[svc] / count[svc],
      mem_max[svc]
  }
}
' "$LOG_FILE"