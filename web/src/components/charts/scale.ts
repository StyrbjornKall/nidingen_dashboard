/** Scales shared by the charts (React + SVG, d3 helpers; no Plotly). */
import { scaleLinear } from "d3-scale";

/** A nice y scale from 0, about four intervals, never empty. */
export function yAxis(maxValue: number, intervals = 4): { top: number; ticks: number[] } {
  const scale = scaleLinear().domain([0, Math.max(1, maxValue)]).nice(intervals);
  return { top: scale.domain()[1] ?? 1, ticks: scale.ticks(intervals) };
}

/** The largest value to draw: the stacked sum for bars, the largest single value for lines. */
export function maxValue(series: readonly (readonly number[])[], stacked: boolean): number {
  const len = series[0]?.length ?? 0;
  let max = 0;
  for (let i = 0; i < len; i++) {
    let sum = 0;
    for (const s of series) {
      const v = s[i] ?? 0;
      sum += v;
      if (!stacked) max = Math.max(max, v);
    }
    if (stacked) max = Math.max(max, sum);
  }
  return max;
}

/** Runs of consecutive gap-year buckets as [first, last] index pairs. */
export function gapRuns(points: readonly { is_gap_year: boolean }[]): [number, number][] {
  const runs: [number, number][] = [];
  points.forEach((b, i) => {
    if (!b.is_gap_year) return;
    const last = runs[runs.length - 1];
    if (last && last[1] === i - 1) last[1] = i;
    else runs.push([i, i]);
  });
  return runs;
}
