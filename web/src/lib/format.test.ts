import { describe, expect, it } from "vitest";
import {
  formatApprox, formatDate, formatDateTime, formatDoy, formatInt, formatNumber, formatPercent, formatTrend, formatWeek,
} from "./format";

const NBSP = " ";

describe("Swedish number format", () => {
  it("uses a space as thousands separator", () => {
    expect(formatInt(396301).replace(NBSP, " ")).toBe("396 301");
  });
  it("uses the minus sign, not a hyphen", () => {
    expect(formatInt(-12)).toBe("−12");
    expect(formatNumber(-3.25, 1)).toBe("−3,3");
  });
  it("uses a decimal comma", () => {
    expect(formatNumber(3.14159, 2)).toBe("3,14");
  });
  it("shows trends with one decimal under 10, whole numbers from 10", () => {
    expect(formatTrend(4.26)).toBe("4,3");
    expect(formatTrend(-0.04)).toBe("−0,0");
    expect(formatTrend(13.4)).toBe("13");
    expect(formatTrend(-65.2)).toBe("−65");
    expect(formatTrend(2, true)).toBe("+2,0");
    expect(formatTrend(-2, true)).toBe("−2,0");
  });
  it("formats shares as percent", () => {
    expect(formatPercent(0.1234)).toBe(`12,3${NBSP}%`);
  });
});

describe("dates", () => {
  it("formats calendar dates without time zone shifts", () => {
    expect(formatDate("2025-04-19")).toBe("19 apr");
    expect(formatDate("2025-05-01", true)).toBe("1 maj 2025");
  });
  it("turns a non-leap day of year into a date", () => {
    expect(formatDoy(109)).toBe("19 apr");
    expect(formatDoy(60)).toBe("1 mar");
    expect(formatDoy(365)).toBe("31 dec");
  });
  it("labels 7-day weeks with their dates", () => {
    expect(formatWeek({ week: 12, start_month: 3, start_day: 19, end_month: 3, end_day: 25 })).toBe("v.12 · 19–25 mar");
    expect(formatWeek({ week: 9, start_month: 2, start_day: 26, end_month: 3, end_day: 4 })).toBe("v.9 · 26 feb–4 mar");
  });
  it("shows UTC build times in Swedish time", () => {
    expect(formatDateTime("2026-11-16T05:00:00")).toBe("16 nov 2026, 06:00");   // CET = UTC+1
    expect(formatDateTime("2026-07-01T22:30:00Z")).toBe("2 jul 2026, 00:30");    // CEST = UTC+2
  });
});

describe("formatApprox", () => {
  it("rounds headline figures to thousands with a tilde, keeping small numbers exact", () => {
    expect(formatApprox(395777)).toBe("~396\u00a0000");
    expect(formatApprox(54292)).toBe("~54\u00a0000");
    expect(formatApprox(6223)).toBe("~6\u00a0000");
    expect(formatApprox(999)).toBe("999");
    expect(formatApprox(0)).toBe("0");
  });
});
