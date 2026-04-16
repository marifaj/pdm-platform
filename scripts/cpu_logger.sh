#!/bin/bash

RUN_NAME=${1:-run}
LOG_FILE=~/mva/logs/cpu_${RUN_NAME}_$(date +%Y%m%d_%H%M%S).log

echo "Starting CPU logger → $LOG_FILE"
echo "Run: $RUN_NAME"

while true; do
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  ps -eo pid,%cpu,%mem,cmd | grep -E "inference|ingestion|storage|event_processing|notification" | grep -v grep
  echo "----"
  sleep 2
done | tee "$LOG_FILE"