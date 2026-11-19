/**
 * Swedish formatting (HANDOFF.md §5.2): decimal comma, the minus sign "−",
 * a (non-breaking) space as thousands separator, and dates like "19 apr".
 */

const MONTHS = ["jan", "feb", "mar", "apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov", "dec"] as const;
const MONTHS_LONG = [
  "januari", "februari", "mars", "april", "maj", "juni",
  "juli", "augusti", "september", "oktober", "november", "december",
] as const;

const intFormat = new Intl.NumberFormat("sv-SE", { maximumFractionDigits: 0 });
const decimalFormats = new Map<number, Intl.NumberFormat>();

function decimalFormat(decimals: number): Intl.NumberFormat {
  let f = decimalFormats.get(decimals);
  if (!f) {
    f = new Intl.NumberFormat("sv-SE", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
    decimalFormats.set(decimals, f);
  }
  return f;
}

/** Normalise to the Unicode minus sign, whatever the runtime's ICU does. */
const minus = (s: string) => s.replace(/-/g, "−");

/** 396301 -> "396 301", -12 -> "−12". */
export function formatInt(n: number): string {
  return minus(intFormat.format(Math.round(n)));
}

/** 3.14159, 1 -> "3,1". */
export function formatNumber(n: number, decimals = 0): string {
  return minus(decimals === 0 ? intFormat.format(n) : decimalFormat(decimals).format(n));
}

/**
 * Trend and other small-or-large values: one decimal under 10, whole numbers
 * from 10 (HANDOFF.md §5.2). `signed` adds "+" to positive values.
 */
export function formatTrend(n: number, signed = false): string {
  const decimals = Math.abs(n) < 10 ? 1 : 0;
  const text = formatNumber(n, decimals);
  return signed && n > 0 ? `+${text}` : text;
}

/** 395777 -> "~396 000": rounded to thousands for headline figures; small numbers stay exact. */
export function formatApprox(n: number): string {
  return n < 1000 ? formatInt(n) : `~${formatInt(Math.round(n / 1000) * 1000)}`;
}

/** 0.1234 -> "12,3 %". */
export function formatPercent(share: number, decimals = 1): string {
  return `${formatNumber(share * 100, decimals)} %`;
}

function toDate(value: string | Date): Date {
  if (value instanceof Date) return value;
  // "2025-04-19" is a calendar date: read it as local, not UTC midnight.
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  return m ? new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3])) : new Date(value);
}

/** "2025-04-19" -> "19 apr", or "19 apr 2025" with the year. */
export function formatDate(value: string | Date, withYear = false): string {
  const d = toDate(value);
  const text = `${d.getDate()} ${MONTHS[d.getMonth()]}`;
  return withYear ? `${text} ${d.getFullYear()}` : text;
}

export function monthName(month: number, long = false): string {
  return (long ? MONTHS_LONG : MONTHS)[month - 1] ?? "";
}

/** Day of year on a non-leap calendar (as in the API) -> {month, day}. */
export function doyToMonthDay(doy: number): { month: number; day: number } {
  const d = new Date(2001, 0, Math.round(doy));
  return { month: d.getMonth() + 1, day: d.getDate() };
}

/** 109.4 -> "19 apr". */
export function formatDoy(doy: number): string {
  const { month, day } = doyToMonthDay(doy);
  return `${day} ${monthName(month)}`;
}

/**
 * A 7-day week counted from 1 January (not an ISO week): "v.12 · 19–25 mar",
 * or "v.9 · 26 feb–4 mar" across a month boundary.
 */
export function formatWeek(week: { week: number; start_month: number; start_day: number; end_month: number; end_day: number }): string {
  const range =
    week.start_month === week.end_month
      ? `${week.start_day}–${week.end_day} ${monthName(week.end_month)}`
      : `${week.start_day} ${monthName(week.start_month)}–${week.end_day} ${monthName(week.end_month)}`;
  return `v.${week.week} · ${range}`;
}

/** "2026-11-16T05:00:00" (UTC) -> "16 nov 2026, 06:00" in Swedish time. */
export function formatDateTime(utc: string): string {
  const iso = /[zZ]|[+-]\d{2}:\d{2}$/.test(utc) ? utc : `${utc}Z`;
  const parts = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "Europe/Stockholm",
    year: "numeric", month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false,
  }).formatToParts(new Date(iso));
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? "";
  return `${Number(get("day"))} ${monthName(Number(get("month")))} ${get("year")}, ${get("hour")}:${get("minute")}`;
}
