#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

for campaign_dir in ./executed/2026-ICSA-PE/*/; do
    [[ -d "${campaign_dir}" ]] || continue
    # Only run if there are measurements present to merge.
    if compgen -G "${campaign_dir}"*/measurements.csv > /dev/null; then
        ./analyze_performance_energy_prepare_step2.py "${campaign_dir%/}"
    fi
done
