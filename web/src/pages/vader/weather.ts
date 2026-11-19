/**
 * Pure helpers for Väder: the variables and their colours, URL choices,
 * line and bar paths with gaps for missing values, wind-rose geometry and
 * the compass names.
 */
import { color, heatRamp, speciesPalette } from "../../lib/tokens";

export type VarKey = "temp" | "vind" | "rikt" | "tryck" | "regn" | "sikt";

export const VARS: { key: VarKey; label: string; color: string }[] = [
  { key: "temp", label: "Temperatur", color: color.trend },
  { key: "vind", label: "Vindstyrka", color: speciesPalette[2] },
  { key: "rikt", label: "Vindriktning", color: color.ink },
  { key: "tryck", label: "Lufttryck", color: color.chart_worst_year },
  { key: "regn", label: "Nederbörd", color: heatRamp[1] },
  { key: "sikt", label: "Sikt", color: color.chart_mean },
];

export const DEFAULT_VARS: readonly VarKey[] = ["temp", "vind", "rikt", "tryck"];

/** The variables in `?visa=temp,regn`, in the fixed order of the chips; no parameter means the defaults. */
export function parseVars(raw: string | null): VarKey[] {
  if (raw === null) return [...DEFAULT_VARS];
  const wanted = new Set(raw.split(",").map((s) => s.trim()));
  return VARS.filter((v) => wanted.has(v.key)).map((v) => v.key);
}

/** The value for `?visa=`: null when it equals the defaults, "-" when nothing is chosen. */
export function serializeVars(vars: readonly VarKey[]): string | null {
  const same = vars.length === DEFAULT_VARS.length && DEFAULT_VARS.every((k) => vars.includes(k));
  return same ? null : vars.length ? VARS.filter((v) => vars.includes(v.key)).map((v) => v.key).join(",") : "-";
}

export const COMPASS = ["N", "NO", "Ö", "SO", "S", "SV", "V", "NV"] as const;

/** Compass name for a wind direction in degrees (the direction the wind blows from). */
export function dirName(deg: number): string {
  return COMPASS[Math.round((((deg % 360) + 360) % 360) / 45) % 8]!;
}

/**
 * Rotation of the wind arrow. The glyph points down (south); wind *from* 0°
 * (north) blows towards the south, so the arrow turns by the direction itself.
 */
export const arrowRotation = (dir: number) => ((dir % 360) + 360) % 360;

/** Every n-th day gets an arrow, so arrows are at least `minGap` pixels apart. */
export function arrowStep(days: number, width: number, minGap = 16): number {
  if (width <= 0) return 1;
  return Math.max(1, Math.ceil(days / Math.max(1, Math.floor(width / minGap))));
}

/** First day of each month inside the dates ("2025-07-01"…), as {index, month 1–12}. */
export function monthStarts(dates: readonly string[]): { index: number; month: number }[] {
  return dates.flatMap((d, i) => (d.slice(8, 10) === "01" ? [{ index: i, month: Number(d.slice(5, 7)) }] : []));
}

/** Index of the day with the most birds (the first on a tie), or −1 when no bird was caught. */
export function bestIndex(catches: readonly number[]): number {
  let best = -1;
  catches.forEach((c, i) => {
    if (c > 0 && (best < 0 || c > catches[best]!)) best = i;
  });
  return best;
}

type Num = number | null | undefined;

/** Runs of consecutive indices that have a value. */
export function valueRuns(values: readonly Num[]): number[][] {
  const runs: number[][] = [];
  let cur: number[] = [];
  values.forEach((v, i) => {
    if (v == null) {
      if (cur.length) runs.push(cur);
      cur = [];
    } else cur.push(i);
  });
  if (cur.length) runs.push(cur);
  return runs;
}

/** A line over the days, broken where a value is missing. X and Y are in drawing units. */
export function linePath(values: readonly Num[], x: (i: number) => number, y: (v: number) => number): string {
  return valueRuns(values)
    .map((run) => `M${run.map((i) => `${x(i).toFixed(1)} ${y(values[i]!).toFixed(1)}`).join("L")}`)
    .join("");
}

/** The filled area under the same line, one closed shape per run. */
export function areaPath(values: readonly Num[], x: (i: number) => number, y: (v: number) => number, base: number): string {
  return valueRuns(values)
    .map((run) => {
      const pts = run.map((i) => `${x(i).toFixed(1)} ${y(values[i]!).toFixed(1)}`);
      return `M${x(run[0]!).toFixed(1)} ${base}L${pts.join("L")}L${x(run[run.length - 1]!).toFixed(1)} ${base}Z`;
    })
    .join("");
}

/** Bars from the baseline up; zero and missing values leave no bar. `w` is the bar width in drawing units. */
export function barsPath(values: readonly Num[], x: (i: number) => number, y: (v: number) => number, base: number, w: number): string {
  return values
    .map((v, i) => (v != null && v > 0 ? `M${(x(i) - w / 2).toFixed(1)} ${y(v).toFixed(1)}h${w.toFixed(2)}V${base}h${(-w).toFixed(2)}Z` : ""))
    .join("");
}

export interface Range {
  min: number;
  max: number;
}

/** Smallest and largest present value, or null when there is none. */
export function rangeOf(values: readonly Num[]): Range | null {
  const present = values.filter((v): v is number => v != null);
  return present.length ? { min: Math.min(...present), max: Math.max(...present) } : null;
}

/** A top for a track that starts at 0: the maximum rounded up to a multiple of `step`, at least `floor`. */
export function niceTop(max: number, step: number, floor: number): number {
  return Math.max(floor, Math.ceil(max / step) * step);
}

/** The compass sector with the highest mean catch among those with at least `minDays` days; −1 if none. */
export function topSector(rose: readonly { mean_catch: number | null; days: number }[], minDays = 3): number {
  let top = -1;
  rose.forEach((s, i) => {
    if (s.mean_catch != null && s.days >= minDays && (top < 0 || s.mean_catch > rose[top]!.mean_catch!)) top = i;
  });
  return top;
}

/** The wedge of one compass sector on a 400 × 400 rose (sector 0 = north, clockwise), radius `r`. */
export function wedgePath(sector: number, r: number): string {
  const a0 = ((sector * 45 - 20) * Math.PI) / 180;
  const a1 = ((sector * 45 + 20) * Math.PI) / 180;
  const pt = (a: number) => `${(200 + r * Math.sin(a)).toFixed(1)} ${(200 - r * Math.cos(a)).toFixed(1)}`;
  return `M200 200L${pt(a0)}A${r.toFixed(1)} ${r.toFixed(1)} 0 0 1 ${pt(a1)}Z`;
}

/** Radius of a wedge for a mean catch: square-root scale from 18 to 160. */
export function wedgeRadius(mean: number, maxMean: number): number {
  return 18 + Math.sqrt(Math.max(0, mean) / Math.max(1e-9, maxMean)) * 142;
}
