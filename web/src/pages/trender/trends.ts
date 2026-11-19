/** Pure helpers for Trender: captions, forest-plot axis and trend text. */
import { formatNumber, formatTrend } from "../../lib/format";
import { yearRuns } from "../../lib/years";

/** "+8,2", "−8,5" (one decimal under 10, as everywhere on the site). */
export const formatSlope = (v: number) => formatTrend(v, true);

/** "95 % KI −13,9 till −3,1 fåglar/år". */
export function ciText(lo: number, hi: number): string {
  return `95 % KI ${formatSlope(lo)} till ${formatSlope(hi)} fåglar/år`;
}

/**
 * The caption under the trend charts, generated from the period, the gap
 * years and the current year (never hard-coded, design/README.md):
 * "Trenderna bygger på hela säsonger 1980–1989 samt 2005–2025. 1990–2004
 * ingår inte: stationen var bara delvis bemannad dessa år. 2026 pågår och
 * ingår inte."
 */
export function trendYearsCaption(
  period: { year_from: number; year_to: number },
  gap: { year_from: number; year_to: number; reason: string },
  currentYear: number,
): string {
  const years: number[] = [];
  const left: number[] = [];
  for (let y = period.year_from; y <= period.year_to; y++) (y >= gap.year_from && y <= gap.year_to ? left : years).push(y);
  let text = `Trenderna bygger på hela säsonger ${yearRuns(years)}.`;
  if (left.length) {
    const reason = gap.reason.charAt(0).toLowerCase() + gap.reason.slice(1);
    text += ` ${yearRuns(left)} ingår inte: ${reason}`;
  }
  if (currentYear > period.year_to) text += ` ${currentYear} pågår och ingår inte.`;
  return text;
}

/** 1, 2 or 5 times a power of ten, at or above x/1.5-ish (design's `nice`). */
export function nice(x: number): number {
  if (!(x > 0)) return 1;
  const e = Math.pow(10, Math.floor(Math.log10(x)));
  const f = x / e;
  return (f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10) * e;
}

/**
 * Half-width of the symmetric forest-plot axis: covers every interval
 * shown, rounded to a readable number (design).
 */
export function forestExtent(intervals: readonly { ci_low: number; ci_high: number }[]): number {
  let m = 0;
  for (const r of intervals) m = Math.max(m, Math.abs(r.ci_low), Math.abs(r.ci_high));
  if (m === 0) return 1;
  const e = nice(m / 2) * 2;
  return e < m ? e * 1.5 : e;
}

/** Five ticks from −extent to +extent, labelled with sign. */
export function forestTicks(extent: number): { value: number; label: string }[] {
  return [-1, -0.5, 0, 0.5, 1].map((k) => {
    const v = k * extent;
    const label = k === 0 ? "0" : `${v > 0 ? "+" : ""}${formatNumber(v, extent < 4 ? 1 : 0)}`;
    return { value: v, label };
  });
}

/** Position in percent on a symmetric axis, clamped to the plot. */
export function forestX(v: number, extent: number): number {
  return Math.max(0, Math.min(100, ((v + extent) / (2 * extent)) * 100));
}
