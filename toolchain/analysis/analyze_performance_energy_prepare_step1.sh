#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob
for campaign_dir in ./executed/2026-ICSA-PE/*/; do
    [[ -d "${campaign_dir}" ]] || continue
    for experiment_dir in "${campaign_dir}"*/; do
        [[ -d "${experiment_dir}" ]] || continue
        ./analyze_performance_energy_prepare_step1.py "${experiment_dir%/}" --latency-metric 100%
    done
done
