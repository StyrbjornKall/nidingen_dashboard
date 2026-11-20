"""
analyze_histogram_bins.py — Measurement resolution and histogram bins per species
=================================================================================

Weights and wing lengths are recorded with different precision depending on the
species: small birds to 0.1 g, big ones in whole grams (sometimes 5 or 10 g),
and wing length to 1 mm (sometimes 2). A histogram whose bin width is not a
whole number of those steps shows false spikes (some bins hold two recorded
values, their neighbours one). This script finds, for every species and for
weight and wing length separately:

  min_gap       smallest positive difference between two distinct recorded values
  resolution    the coarsest step that at least 97 % of the birds lie on
                (with at least 30 birds; fewer birds keep the finest step)
  grid_share    share of birds on that step
  lo, hi        the range shown in the histogram (0.5–99.5 % quantiles)
  n_steps       number of resolution steps in that range
  step_multiple whole steps per bin, chosen so the bin count is closest to 50
  bin_width     step_multiple × resolution
  n_bins        resulting number of bins

and writes them to data/metadata/histogram_bins.csv. ``build_aggregates.py``
reads that file to build ``agg_morph_hist``. Edit a row by hand to override the
bin width for a species; rows missing from the file are derived from the data.

Whole year, all years, new ringings (``ringon``) only; the species grid is shared
by the seasons so Vår, Höst and Helår use the same bin edges.

Usage
-----
    uv run python app/src/preprocess_data/analyze_histogram_bins.py
    uv run python app/src/preprocess_data/analyze_histogram_bins.py --db PATH --out PATH
    uv run python app/src/build_aggregates.py        # afterwards, to use the new bins
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import build_aggregates as ba  # noqa: E402

_DEFAULT_DB = ba._PROJECT_DIR / "data" / "bird_ringing_0016.duckdb"


def summarise(plan: list[dict]) -> None:
    for variable, unit in (("weight", "g"), ("wing", "mm")):
        rows = [r for r in plan if r["variable"] == variable]
        print(f"\n{variable}: {len(rows)} species")
        by_res = Counter(r["resolution"] for r in rows)
        print("  resolution (" + unit + ") → species: " + ", ".join(f"{k:g}: {v}" for k, v in sorted(by_res.items())))
        by_bins = Counter(min(r["n_bins"] // 10 * 10, 60) for r in rows)
        print("  bins: " + ", ".join(f"{k}{'+' if k == 60 else '–' + str(k + 9)}: {v}" for k, v in sorted(by_bins.items())))
        coarse = sorted((r for r in rows if r["resolution"] > ba.HIST_UNIT[variable]), key=lambda r: -r["n"])[:8]
        for r in coarse:
            print(f"    coarse: {r['species']:<6} {r['swedish_name'] or '':<22} n={r['n']:>6}  step {r['resolution']:g} {unit}, "
                  f"{r['grid_share']:.0%} on the step")
        sparse = [r for r in rows if r["n_bins"] < 20 and r["n"] >= 100]
        for r in sparse[:5]:
            print(f"    narrow range: {r['species']:<6} n={r['n']:>6}  only {r['n_steps']} steps → {r['n_bins']} bins")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=_DEFAULT_DB)
    parser.add_argument("--out", type=Path, default=ba.DEFAULT_HIST_BINS_CSV)
    args = parser.parse_args()
    if not args.db.exists():
        sys.exit(f"Database not found: {args.db}")
    con = duckdb.connect(str(args.db), read_only=True)
    try:
        plan = ba.derive_hist_plan(con)
    finally:
        con.close()
    ba.write_hist_plan(args.out, plan)
    print(f"Wrote {len(plan)} rows to {args.out}")
    summarise(plan)


if __name__ == "__main__":
    main()
