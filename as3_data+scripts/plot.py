#!/usr/bin/env python3
"""
Load `as3_results.csv` into a pandas DataFrame and plot Time (ms) vs Number of Records
for each operation. Saves a PNG named `as3_times.png` by default.

Usage:
	python plot.py --csv /path/to/as3_results.csv --out as3_times.png

The script groups by 'Operation Name' and plots a line for each operation.
If the CSV contains both indexed and non-indexed runs (Index column), it will
distinguish them by line style.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

try:
	import pandas as pd
	import matplotlib.pyplot as plt
except Exception as e:  # pragma: no cover - friendly runtime message
	print("Missing dependency:", e)
	print("Please install requirements: pandas matplotlib")
	sys.exit(2)

# seaborn is optional; if available we'll use it for a nicer default style
try:
	import seaborn as sns  # type: ignore
except Exception:
	sns = None


def load_results(csv_path: str) -> pd.DataFrame:
	"""Load CSV and normalise column types.

	Expects columns: Operation Name, Index, Number of Records, Time (ms)
	"""
	df = pd.read_csv(csv_path)

	# Normalize column names (strip)
	df.columns = [c.strip() for c in df.columns]

	# Ensure expected columns exist
	expected = ["Operation Name", "Index", "Number of Records", "Time (ms)"]
	for col in expected:
		if col not in df.columns:
			raise KeyError(f"Expected column '{col}' not found in CSV")

	# Convert types
	# Index may be boolean-like strings: 'True'/'False'
	df["Index"] = df["Index"].astype(str).str.strip().map(lambda v: v == "True")
	df["Number of Records"] = pd.to_numeric(df["Number of Records"], errors="coerce").astype("Int64")
	df["Time (ms)"] = pd.to_numeric(df["Time (ms)"], errors="coerce")

	# Drop rows with missing important data
	df = df.dropna(subset=["Operation Name", "Number of Records", "Time (ms)"])

	return df


def plot_times(df: pd.DataFrame, out_path: Optional[str] = "as3_times.png") -> None:
	"""Create and save a plot of Time (ms) vs Number of Records for each operation.

	- Hue: Operation Name
	- Style: Index (True/False)
	- Uses seaborn lineplot with markers
	"""
	# If seaborn available, use it for style; otherwise fall back to matplotlib
	if sns is not None:
		sns.set(style="whitegrid")

	plt.figure(figsize=(12, 7))

	# Sort values to make lines sensible
	df = df.sort_values(["Operation Name", "Index", "Number of Records"])

	# We'll map each Operation Name to a base color and draw two styles for Index True/False
	ops = list(df["Operation Name"].unique())
	color_cycle = plt.rcParams.get("axes.prop_cycle").by_key().get("color", [])
	color_map = {op: color_cycle[i % len(color_cycle)] for i, op in enumerate(ops)}

	linestyles = {False: "-", True: "--"}
	markers = {False: "o", True: "s"}

	ax = plt.gca()

	for (op_name, idx), group in df.groupby(["Operation Name", "Index"]):
		x = group["Number of Records"].astype(int).values
		y = group["Time (ms)"].values
		label = f"{op_name}{' (idx)' if idx else ''}"
		ax.plot(x, y, label=label, color=color_map.get(op_name), linestyle=linestyles.get(idx, "-"), marker=markers.get(idx, None))

	ax.set_title("Operation Time vs Number of Records")
	ax.set_xlabel("Number of Records")
	ax.set_ylabel("Time (ms)")

	ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left")
	plt.tight_layout()
	plt.savefig(out_path, dpi=150)
	print(f"Saved plot to {out_path}")


def main(argv: Optional[list[str]] = None) -> int:
	p = argparse.ArgumentParser(description="Plot as3_results.csv timings")
	p.add_argument("--csv", default=os.path.join(os.path.dirname(__file__), "as3_results.csv"), help="Path to as3_results.csv")
	p.add_argument("--out", default="as3_times.png", help="Output PNG path")
	args = p.parse_args(argv)

	if not os.path.exists(args.csv):
		print(f"CSV file not found: {args.csv}")
		return 2

	df = load_results(args.csv)
	plot_times(df, out_path=args.out)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
