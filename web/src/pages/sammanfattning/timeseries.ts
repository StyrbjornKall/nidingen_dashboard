/**
 * Pure helpers for "Ringmärkta över tid" on Sammanfattning: bucket labels,
 * x-axis ticks and the default chart type. Kept free of React so they can
 * be unit tested. Scales and gap runs are in components/charts/scale.ts.
 */
import type { Schemas } from "../../lib/api";
import { formatWeek, monthName } from "../../lib/format";

export type Grain = Schemas["TimeSeries"]["grain"];
export type Bucket = Schemas["Bucket"];

export const GRAIN_NOUN: Record<Grain, string> = { day: "dag", week: "vecka", month: "månad", year: "år" };

/** "2025-04-19" -> {month: 4, day: 19}, without going through Date (no time-zone surprises). */
function monthDay(iso: string): { month: number; day: number } {
  const [, m = 1, d = 1] = iso.split("-").map(Number);
  return { month: m, day: d };
}

/** Readout label for one bucket: "1987", "maj 1987", "v.12 · 19–25 mar 2025", "19 apr 2025". */
export function bucketLabel(b: Bucket, grain: Grain): string {
  if (grain === "year") return String(b.year);
  if (grain === "month") return `${monthName(b.index)} ${b.year}`;
  const s = monthDay(b.start);
  if (grain === "day") return `${s.day} ${monthName(s.month)} ${b.year}`;
  const e = monthDay(b.end);
  return `${formatWeek({ week: b.index, start_month: s.month, start_day: s.day, end_month: e.month, end_day: e.day })} ${b.year}`;
}

export interface AxisLabel {
  /** Bucket index the label is centred on. */
  i: number;
  label: string;
}

/**
 * X-axis labels, as in the design: for several years, a year label every
 * 1, 2 or 5 years (by how many years are shown); within a single year,
 * month names, or every fourth week. `maxLabels` thins them further on
 * narrow charts so they never overlap.
 */
export function axisLabels(buckets: readonly Bucket[], grain: Grain, maxLabels = Infinity): AxisLabel[] {
  const first = buckets[0];
  const last = buckets[buckets.length - 1];
  if (!first || !last) return [];
  const y0 = first.year;
  const nYears = last.year - y0 + 1;
  const step = nYears > 20 ? 5 : nYears > 8 ? 2 : 1;
  const yearTick = (y: number) => (y - y0) % step === 0;

  const out: AxisLabel[] = [];
  buckets.forEach((b, i) => {
    let label = "";
    if (grain === "year") label = yearTick(b.year) ? String(b.year) : "";
    else if (grain === "month") label = nYears === 1 ? monthName(b.index) : b.index === 1 && yearTick(b.year) ? String(b.year) : "";
    else if (grain === "week") label = nYears === 1 ? ((b.index - 1) % 4 === 0 ? `v.${b.index}` : "") : b.index === 1 && yearTick(b.year) ? String(b.year) : "";
    else if (monthDay(b.start).day === 1) label = monthName(monthDay(b.start).month);
    if (label) out.push({ i, label });
  });

  if (out.length <= maxLabels) return out;
  const every = Math.ceil(out.length / Math.max(1, maxLabels));
  return out.filter((_, k) => k % every === 0);
}

/** Bar for day and week would be hairlines, so those default to a line chart (design). */
export function defaultChartType(grain: Grain): "bar" | "line" {
  return grain === "day" || grain === "week" ? "line" : "bar";
}
