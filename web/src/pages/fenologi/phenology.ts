/**
 * Pure helpers for Fenologi: 7-day weeks as days of the year, month ticks,
 * the heat-map colour ramp and the year choices of the migration calendar.
 * Days of year are on a non-leap calendar, as in the API.
 */
import { monthName } from "../../lib/format";
import { rampColor } from "../../lib/color";
import { heatRamp } from "../../lib/tokens";

/** Week 1 = 1–7 January; week 52 runs to 31 December (8 days). */
export const weekStart = (w: number) => (w - 1) * 7 + 1;
export const weekEnd = (w: number) => (w >= 52 ? 365 : w * 7);

/** Last week of vår: the season of a week is that of its 4th day (API). */
export const LAST_SPRING_WEEK = 26;

const MONTH_START = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];

/** Month starts inside [d0, d1] (not in the last 4 days), as {doy, label}. */
export function monthTicks(d0: number, d1: number): { doy: number; label: string }[] {
  return MONTH_START.flatMap((doy, i) => (doy >= d0 && doy < d1 - 4 ? [{ doy, label: monthName(i + 1) }] : []));
}

/** 0–1 on the three-stop heat ramp from tokens.json (design's `ramp`). */
export const heatColor = (t: number): string => rampColor(heatRamp, t);

/** Years offered by the migration calendar: newest first, gap years left out. */
export function calendarYears(first: number, last: number, gap: { year_from: number; year_to: number }): number[] {
  const out: number[] = [];
  for (let y = last; y >= first; y--) if (y < gap.year_from || y > gap.year_to) out.push(y);
  return out;
}

/** Index range [a, b] into a 52-week array for weeks w0..w1. */
export function weekSlice<T>(values: readonly T[], w0: number, w1: number): T[] {
  return values.slice(w0 - 1, w1);
}
