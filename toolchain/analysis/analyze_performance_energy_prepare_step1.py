#!/usr/bin/env python3
"""Prepare combined measurements.csv files for analyze_performance_energy."""

import argparse
import glob
import json
import os
from pathlib import Path

import pandas as pd


def generate_measurements_csv(experiment_dir, latency_metric='100%', output_filename='measurements.csv'):
    """Build and export aligned container and Locust metrics to CSV."""
    measurement_df = _build_measurements_dataframe(experiment_dir=experiment_dir, latency_metric=latency_metric)
    output_path = os.path.join(experiment_dir, output_filename)
    measurement_df.to_csv(output_path, index=False)
    return output_path


def _build_measurements_dataframe(experiment_dir, latency_metric):
    docker_pids_path = os.path.join(experiment_dir, 'docker_pids.json')
    locust_history_path = os.path.join(experiment_dir, 'result_stats_history.csv')
    cadvisor_container_path = os.path.join(experiment_dir, 'cadvisor_container.csv')
    perf_power_path = os.path.join(experiment_dir, 'perf_power.csv')

    with open(docker_pids_path) as docker_file:
        docker_pids = json.load(docker_file)

    locust_df = pd.read_csv(locust_history_path)
    locust_df = locust_df[locust_df['Name'] == 'Aggregated'].copy()
    locust_df['Date'] = pd.to_datetime(locust_df['Timestamp'], unit='s', utc=True).dt.tz_localize(None)

    metric_columns = ['Date', 'Requests/s', 'Failures/s', '50%', latency_metric]
    missing_columns = [col for col in metric_columns if col not in locust_df.columns]
    if missing_columns:
        raise ValueError(f'Missing columns in Locust history: {missing_columns}')

    locust_metrics = locust_df[metric_columns].sort_values('Date')
    if locust_metrics.empty:
        raise ValueError('No aggregated Locust metrics found to plot.')

    for column in ['Requests/s', 'Failures/s', '50%', latency_metric]:
        locust_metrics[column] = pd.to_numeric(locust_metrics[column], errors='coerce').fillna(0.0)

    container_frames = []
    start_time = locust_metrics['Date'].min()

    cadvisor_df = pd.DataFrame()
    if os.path.exists(cadvisor_container_path):
        cadvisor_df = pd.read_csv(
            cadvisor_container_path,
            usecols=["timestamp", "service", "memory_usage"],
        )
        cadvisor_df["timestamp"] = pd.to_numeric(cadvisor_df["timestamp"], errors="coerce")
        cadvisor_df["memory_usage"] = pd.to_numeric(cadvisor_df["memory_usage"], errors="coerce")
        cadvisor_df.dropna(subset=["timestamp", "service", "memory_usage"], inplace=True)
        if not cadvisor_df.empty:
            cadvisor_df["Date"] = pd.to_datetime(cadvisor_df["timestamp"], unit="s", utc=True).dt.tz_localize(None)

    perf_df = pd.DataFrame()
    if os.path.exists(perf_power_path):
        perf_df = pd.read_csv(perf_power_path)
        if {"timestamp_epoch", "dram_w"}.issubset(perf_df.columns):
            perf_df["timestamp_epoch"] = pd.to_numeric(perf_df["timestamp_epoch"], errors="coerce")
            perf_df["dram_w"] = pd.to_numeric(perf_df["dram_w"], errors="coerce")
            perf_df.dropna(subset=["timestamp_epoch", "dram_w"], inplace=True)
        else:
            perf_df = pd.DataFrame()

    for container in docker_pids.get('containers', []):
        container_id = container.get('container_id', '')
        container_name = container.get('container_name', '<unnamed>')
        prefix = container_id[:8]
        pattern = os.path.join(experiment_dir, f'pid_{prefix}_*-*.csv')
        csv_files = sorted(glob.glob(pattern))

        frames = []
        for csv_path in csv_files:
            frame = pd.read_csv(csv_path)
            if 'Date' not in frame:
                continue
            frame['Date'] = pd.to_datetime(frame['Date'])
            frame['CPU Utilization'] = pd.to_numeric(frame.get('CPU Utilization'), errors='coerce').fillna(0.0)
            frame['CPU Power'] = pd.to_numeric(frame.get('CPU Power'), errors='coerce').fillna(0.0)
            frames.append(frame[['Date', 'CPU Utilization', 'CPU Power']])

        if not frames:
            continue

        container_df = pd.concat(frames, ignore_index=True).groupby('Date', as_index=False)[['CPU Utilization', 'CPU Power']].sum()
        container_df['container_id'] = container_id
        container_df['container_name'] = container_name
        container_frames.append(container_df)
        start_time = min(start_time, container_df['Date'].min())

    if not container_frames:
        raise ValueError('No container CSV data found to plot.')

    locust_metrics['seconds_from_start'] = (locust_metrics['Date'] - start_time).dt.total_seconds()
    locust_export = (
        locust_metrics[['seconds_from_start', 'Requests/s', 'Failures/s', '50%', latency_metric]]
        .rename(columns={'Requests/s': 'requests_per_s', 'Failures/s': 'failures_per_s', '50%': 'latency_p50_ms', latency_metric: 'latency'})
        .sort_values('seconds_from_start')
        .reset_index(drop=True)
    )

    export_frames = []

    for container_df in container_frames:
        container_df['seconds_from_start'] = (container_df['Date'] - start_time).dt.total_seconds()
        container_name = container_df['container_name'].iloc[0]
        container_id = container_df['container_id'].iloc[0]

        container_plot_df = container_df.sort_values('seconds_from_start').reset_index(drop=True)
        date_series = container_plot_df['Date']
        if date_series.dt.tz is None:
            timestamp_series = date_series.dt.tz_localize('UTC')
        else:
            timestamp_series = date_series.dt.tz_convert('UTC')
        container_plot_df['timestamp'] = timestamp_series.dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        aligned = pd.merge_asof(
            container_plot_df.sort_values('seconds_from_start'),
            locust_export,
            on='seconds_from_start',
            direction='nearest',
        )
        # Align cadvisor memory usage if present.
        if not cadvisor_df.empty:
            cadvisor_container = cadvisor_df[cadvisor_df["service"] == container_name].copy()
            if not cadvisor_container.empty:
                cadvisor_container["seconds_from_start"] = (cadvisor_container["Date"] - start_time).dt.total_seconds()
                cadvisor_container.sort_values("seconds_from_start", inplace=True)
                aligned = pd.merge_asof(
                    aligned.sort_values("seconds_from_start"),
                    cadvisor_container[["seconds_from_start", "memory_usage"]],
                    on="seconds_from_start",
                    direction="nearest",
                )
        # Align DRAM power if available.
        if not perf_df.empty:
            perf_copy = perf_df.copy()
            # Align to experiment start_time (seconds).
            start_ts = start_time.timestamp()
            perf_copy["seconds_from_start"] = perf_copy["timestamp_epoch"] - start_ts
            perf_copy.sort_values("seconds_from_start", inplace=True)
            aligned = pd.merge_asof(
                aligned.sort_values("seconds_from_start"),
                perf_copy[["seconds_from_start", "dram_w"]],
                on="seconds_from_start",
                direction="nearest",
            )
        if "memory_usage" not in aligned.columns:
            aligned["memory_usage"] = pd.NA
        if "dram_w" not in aligned.columns:
            aligned["dram_w"] = pd.NA
        aligned['container_name'] = container_name
        aligned['container_id'] = container_id
        aligned = aligned.rename(columns={'CPU Utilization': 'cpu_utilization', 'CPU Power': 'cpu_power'})
        aligned = aligned[
            [
                'timestamp',
                'cpu_utilization',
                'cpu_power',
                'dram_w',
                'memory_usage',
                'container_id',
                'container_name',
                'seconds_from_start',
                'requests_per_s',
                'failures_per_s',
                'latency_p50_ms',
                'latency',
            ]
        ]
        export_frames.append(aligned)

    measurement_df = pd.concat(export_frames, ignore_index=True)
    return measurement_df


def _parse_args():
    parser = argparse.ArgumentParser(description="Prepare measurements.csv for analyze_performance_energy.ipynb")
    parser.add_argument('experiment_dir', help='Path to the experiment directory')
    parser.add_argument('--latency-metric', default='100%', help='Latency column to align from Locust history (default: 100%)')
    parser.add_argument('--output', default='measurements.csv', help='Filename for the generated CSV (default: measurements.csv)')
    return parser.parse_args()


def main():
    args = _parse_args()
    experiment_path = os.path.abspath(args.experiment_dir)
    output_path = generate_measurements_csv(experiment_dir=experiment_path, latency_metric=args.latency_metric, output_filename=args.output)
    print(f"Saved combined measurements to {output_path}")


if __name__ == '__main__':
    main()
