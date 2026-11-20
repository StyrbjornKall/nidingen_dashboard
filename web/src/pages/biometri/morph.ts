/**
 * Pure helpers for Biometri: log axes derived from the data, bubble
 * sizes, the order and colours of raw age and sex codes, and bin labels.
 */
import { scaleLinear } from "d3-scale";
import { rampColor } from "../../lib/color";
import { formatNumber } from "../../lib/format";

export interface LogAxis {
  lo: number;
  hi: number;
  ticks: number[];
  /** Position of a value, 0 (lo) to 1 (hi). */
  frac: (v: number) => number;
}

const TICK_SETS = [[1, 1.5, 2, 3, 5, 7], [1, 2, 5], [1, 3], [1]] as const;
const MAX_TICKS = 8;

/** 1-2-5 style ticks inside [lo, hi]: the finest set that gives at most 8. */
export function niceLogTicks(lo: number, hi: number): number[] {
  // Zoomed in far, the range is too narrow for 1-2-5 steps: plain linear ticks read better.
  if (hi / lo < 3) return scaleLinear().domain([lo, hi]).ticks(6);
  const k0 = Math.floor(Math.log10(lo));
  const k1 = Math.ceil(Math.log10(hi));
  let best: number[] = [];
  for (const set of TICK_SETS) {
    const ticks: number[] = [];
    for (let k = k0; k <= k1; k++) {
      for (const m of set) {
        const v = Number((m * 10 ** k).toPrecision(12));
        if (v >= lo && v <= hi) ticks.push(v);
      }
    }
    best = ticks;
    if (ticks.length <= MAX_TICKS) break;
  }
  return best;
}

/** A log axis that fits the data with a margin (a share of the log range). */
export function logAxis(min: number, max: number, margin = 0.06): LogAxis {
  const a = Math.log(min);
  const b = Math.log(Math.max(max, min * 1.0001));
  const lo = Math.exp(a - (b - a) * margin);
  const hi = Math.exp(b + (b - a) * margin);
  const span = Math.log(hi) - Math.log(lo);
  return { lo, hi, ticks: niceLogTicks(lo, hi), frac: (v) => (Math.log(v) - Math.log(lo)) / span };
}

/** Bubble radius from a count on a log scale between the smallest and largest species. */
export function bubbleRadius(n: number, nMin: number, nMax: number, rMin: number, rMax: number): number {
  if (nMax <= nMin) return (rMin + rMax) / 2;
  const t = (Math.log10(Math.max(n, nMin)) - Math.log10(nMin)) / (Math.log10(nMax) - Math.log10(nMin));
  return rMin + Math.min(1, t) * (rMax - rMin);
}

/** Round counts (powers of ten) that fit the data, for the size legend. */
export function sizeLegendValues(nMax: number): number[] {
  const all = [100, 1000, 10000, 100000].filter((v) => v <= nMax);
  return all.slice(-3);
}

/** Youngest first: 00, 0+, 10, 1+, 20, 2+ … codes that are not digit + 0/+ go last. */
export function ageSortKey(code: string): number {
  const m = /^(\d)([0+])$/.exec(code);
  return m ? Number(m[1]) * 2 + (m[2] === "+" ? 1 : 0) : 1000;
}

export function sortAgeCodes(codes: readonly string[]): string[] {
  return [...codes].sort((a, b) => ageSortKey(a) - ageSortKey(b) || a.localeCompare(b));
}

/** Male, female, other raw codes, then missing. */
export function sortSexCodes(codes: readonly (string | null)[]): (string | null)[] {
  const rank = (c: string | null) => (c === "M" ? 0 : c === "F" ? 1 : c === null ? 3 : 2);
  return [...codes].sort((a, b) => rank(a) - rank(b) || String(a).localeCompare(String(b)));
}

/** One colour per age code present, spread over the design's green ramp (dark = young). */
export function ageColors(codes: readonly string[], ramp: readonly string[]): Map<string, string> {
  const sorted = sortAgeCodes(codes);
  return new Map(sorted.map((c, i) => [c, rampColor(ramp, sorted.length === 1 ? 0 : i / (sorted.length - 1))]));
}

/** "Hane", "Hona", "Okänd", otherwise the raw code. */
export function sexLabel(code: string | null): string {
  return code === "M" ? "Hane" : code === "F" ? "Hona" : code === null ? "Okänd" : code;
}

/** "10", "2+" … or "Okänd" for a missing age. */
export function ageLabel(code: string | null): string {
  return code === null ? "Okänd" : code;
}

/** "74 %" for a share 0–1. */
export function sharePercent(share: number): string {
  return `${formatNumber(Math.round(share * 100))} %`;
}

/**
 * The label of a histogram bin. Bins that are one measurement step wide
 * (1 mm, 0.1 g) read as that value; wider bins as a range.
 */
export function binLabel(low: number, high: number, unit: "g" | "mm"): string {
  const w = high - low;
  const step = unit === "mm" ? 1 : 0.1;
  const mid = (low + high) / 2;
  if (Math.abs(w - step) < 1e-6) return `${formatNumber(mid, unit === "mm" ? 0 : 1)} ${unit}`;
  const d = unit === "mm" ? (w < 1 ? 1 : 0) : w < 1 ? 2 : 1;
  return `${formatNumber(low, d)}–${formatNumber(high, d)} ${unit}`;
}

/** Where a value sits in [lo, hi] as a percentage, clamped to 0–100. */
export function percentAlong(v: number, lo: number, hi: number): number {
  return hi > lo ? Math.max(0, Math.min(100, ((v - lo) / (hi - lo)) * 100)) : 0;
}

/** Index of the bin that contains `x` (a fraction 0–1 across the bins' span); -1 if none. */
export function binAt(bins: readonly { low: number; high: number }[], x: number): number {
  if (!bins.length) return -1;
  const lo = bins[0]!.low;
  const hi = bins[bins.length - 1]!.high;
  const v = lo + Math.max(0, Math.min(1, x)) * (hi - lo);
  const i = bins.findIndex((b) => v >= b.low && v < b.high);
  return i >= 0 ? i : bins.length - 1;
}

/** "våren (januari–juni)", "hela året": the season as a phrase, from the API's months. */
export function seasonPhrase(season: { id: string; months: number[] }, monthName: (m: number) => string): string {
  const first = season.months[0]!;
  const last = season.months[season.months.length - 1]!;
  if (season.id === "helar") return "hela året";
  const word = season.id === "var" ? "våren" : season.id === "host" ? "hösten" : season.id;
  return `${word} (${monthName(first)}–${monthName(last)})`;
}


/** Tick labels with just enough decimals to tell the ticks apart ("5", "10" but "70,5", "71"). */
export function formatTicks(ticks: readonly number[]): string[] {
  let d = 0;
  while (d < 3 && ticks.some((t) => Math.abs(t * 10 ** d - Math.round(t * 10 ** d)) > 1e-6)) d++;
  return ticks.map((t) => formatNumber(t, d));
}

/**
 * The visible part of the plot as fractions of the full axes (0 = left or
 * bottom, 1 = right or top). Zoom is linked: both axes always show the same
 * share, so the shape of the cloud is kept.
 */
export interface Win {
  x0: number;
  x1: number;
  y0: number;
  y1: number;
}

export const FULL_WIN: Win = { x0: 0, x1: 1, y0: 0, y1: 1 };
export const MAX_ZOOM = 60;

/** How many times larger than the whole plot the current view is magnified. */
export const zoomLevel = (w: Win) => 1 / (w.x1 - w.x0);

/**
 * Zoom by `factor` (> 1 in, < 1 out) keeping the point at (ax, ay), given as
 * fractions of the *window* (ay from the bottom), where it is.
 */
export function zoomWin(w: Win, factor: number, ax = 0.5, ay = 0.5): Win {
  const size = Math.min(1, Math.max(1 / MAX_ZOOM, (w.x1 - w.x0) / factor));
  const px = w.x0 + ax * (w.x1 - w.x0);
  const py = w.y0 + ay * (w.y1 - w.y0);
  const x0 = Math.min(1 - size, Math.max(0, px - ax * size));
  const y0 = Math.min(1 - size, Math.max(0, py - ay * size));
  return { x0, x1: x0 + size, y0, y1: y0 + size };
}

/** Move the view by (dx, dy) window-widths (dy up), staying inside the plot. */
export function panWin(w: Win, dx: number, dy: number): Win {
  const size = w.x1 - w.x0;
  const x0 = Math.min(1 - size, Math.max(0, w.x0 + dx * size));
  const y0 = Math.min(1 - size, Math.max(0, w.y0 + dy * size));
  return { x0, x1: x0 + size, y0, y1: y0 + size };
}

/** A log axis restricted to a range of fractions, so ticks and positions fit the zoomed view. */
export function windowAxis(axis: LogAxis, f0: number, f1: number): LogAxis {
  const a = Math.log(axis.lo);
  const span = Math.log(axis.hi) - a;
  const lo = Math.exp(a + f0 * span);
  const hi = Math.exp(a + f1 * span);
  const inner = Math.log(hi) - Math.log(lo);
  return { lo, hi, ticks: niceLogTicks(lo, hi), frac: (v) => (Math.log(v) - Math.log(lo)) / inner };
}

/** The inputs to a regression line's confidence band (all from the API). */
export interface FitStats {
  n: number;
  slope: number;
  intercept: number;
  x_mean: number;
  sxx: number;
  residual_se: number;
  t_crit: number;
}

/**
 * The fitted line and its 95 % confidence band for the mean response:
 * ŷ(x) ± t · s · √(1/n + (x − x̄)² / Sxx), with the exact t quantile for df = n − 2.
 */
export function fitBand(fit: FitStats, x0: number, x1: number, steps = 24): { x: number; y: number; lo: number; hi: number }[] {
  const out: { x: number; y: number; lo: number; hi: number }[] = [];
  for (let i = 0; i <= steps; i++) {
    const x = x0 + ((x1 - x0) * i) / steps;
    const y = fit.intercept + fit.slope * x;
    const half = fit.t_crit * fit.residual_se * Math.sqrt(1 / fit.n + (x - fit.x_mean) ** 2 / fit.sxx);
    out.push({ x, y, lo: y - half, hi: y + half });
  }
  return out;
}
