#!/usr/bin/env python3
"""Merge all per-experiment measurements.csv files into a single dataset."""

import argparse
import glob
import os
import sys

import pandas as pd


def _parse_args():
    parser = argparse.ArgumentParser(description="Merge multiple measurements.csv files into one dataset.")
    parser.add_argument("root", help="Directory containing experiment sub-folders")
    parser.add_argument("--pattern", default="*/measurements.csv", help="Glob pattern relative to root for locating CSV files (default: */measurements.csv)")
    parser.add_argument("--output", default=None, help="Output filename (relative to root). Defaults to the name of the root directory.")
    if len(sys.argv) == 1:
        parser.print_help()
        parser.exit()
    return parser.parse_args()


def main():
    args = _parse_args()
    root_dir = args.root
    pattern = args.pattern
    output_file = args.output
    if not os.path.isdir(root_dir):
        raise NotADirectoryError(f"Root directory {root_dir!r} does not exist")
    csv_paths = sorted(glob.glob(os.path.join(root_dir, pattern)))
    if not csv_paths:
        raise FileNotFoundError(f"No measurements files found under {root_dir} matching pattern {pattern!r}")
    frames = []
    for run_id, csv_path in enumerate(csv_paths, start=1):
        df = pd.read_csv(csv_path)
        df["run_id"] = run_id
        df["experiment_name"] = os.path.basename(os.path.dirname(csv_path))
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    if output_file is None:
        output_file = f"{os.path.basename(os.path.normpath(root_dir))}.csv"
    output_path = os.path.join(root_dir, output_file)
    combined.to_csv(output_path, index=False)
    print(f"Merged {len(csv_paths)} files into {output_path}")


if __name__ == "__main__":
    main()
