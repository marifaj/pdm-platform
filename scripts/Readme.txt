
chmod +x ~/mva/scripts/cpu_logger.sh

~/mva/scripts/cpu_logger.sh baseline
~/mva/scripts/cpu_logger.sh anomaly_test


chmod +x ~/mva/scripts/summarize_cpu_log.sh
~/mva/scripts/summarize_cpu_log.sh ~/mva/logs/cpu_100HzManualAnomaly_20260420_110108.log

~/mva/scripts/summarize_cpu_log.sh ~/mva/logs/cpu_baseline_YYYYMMDD_HHMMSS.log