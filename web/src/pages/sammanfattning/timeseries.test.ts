import { describe, expect, it } from "vitest";
import { axisLabels, bucketLabel, gapRuns, maxValue, yAxis, type Bucket } from "./timeseries";

const b = (year: number, index: number, start: string, end = start, gap = false): Bucket => ({
  year, index, start, end, is_gap_year: gap,
});

const years = (from: number, to: number, gap?: [number, number]) =>
  Array.from({ length: to - from + 1 }, (_, k) => {
    const y = from + k;
    return b(y, y, `${y}-01-01`, `${y}-12-31`, gap ? y >= gap[0] && y <= gap[1] : false);
  });

describe("bucketLabel", () => {
  it("labels each grain in Swedish", () => {
    expect(bucketLabel(b(1987, 1987, "1987-01-01", "1987-12-31"), "year")).toBe("1987");
    expect(bucketLabel(b(1987, 5, "1987-05-01", "1987-05-31"), "month")).toBe("maj 1987");
    expect(bucketLabel(b(2025, 109, "2025-04-19"), "day")).toBe("19 apr 2025");
  });

  it("shows the dates of a 7-day week, not an ISO week", () => {
    expect(bucketLabel(b(2025, 12, "2025-03-19", "2025-03-25"), "week")).toBe("v.12 · 19–25 mar 2025");
    expect(bucketLabel(b(2025, 9, "2025-02-26", "2025-03-04"), "week")).toBe("v.9 · 26 feb–4 mar 2025");
  });
});

describe("axisLabels", () => {
  it("labels every fifth year over more than 20 years", () => {
    const labels = axisLabels(years(1980, 2025), "year").map((l) => l.label);
    expect(labels).toEqual(["1980", "1985", "1990", "1995", "2000", "2005", "2010", "2015", "2020", "2025"]);
  });

  it("labels every second year over 9–20 years and every year below", () => {
    expect(axisLabels(years(2016, 2025), "year").map((l) => l.label)).toEqual(["2016", "2018", "2020", "2022", "2024"]);
    expect(axisLabels(years(2020, 2025), "year")).toHaveLength(6);
  });

  it("uses month names within a single year", () => {
    const months = Array.from({ length: 12 }, (_, k) => b(2025, k + 1, `2025-${String(k + 1).padStart(2, "0")}-01`));
    expect(axisLabels(months, "month").map((l) => l.label)).toEqual(
      ["jan", "feb", "mar", "apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov", "dec"],
    );
  });

  it("marks every fourth week within a single year", () => {
    const weeks = Array.from({ length: 52 }, (_, k) => b(2025, k + 1, "2025-01-01"));
    const labels = axisLabels(weeks, "week");
    expect(labels.slice(0, 3)).toEqual([{ i: 0, label: "v.1" }, { i: 4, label: "v.5" }, { i: 8, label: "v.9" }]);
    expect(labels).toHaveLength(13);
  });

  it("thins labels to fit a narrow chart", () => {
    expect(axisLabels(years(1980, 2025), "year", 4).map((l) => l.label)).toEqual(["1980", "1995", "2010", "2025"]);
  });
});

describe("y axis and maxima", () => {
  it("rounds the top up to a nice value", () => {
    expect(yAxis(16418)).toEqual({ top: 20000, ticks: [0, 5000, 10000, 15000, 20000] });
    expect(yAxis(0).top).toBe(1);
  });

  it("stacks bars and not lines", () => {
    const series = [[1, 5, 2], [4, 1, 2]];
    expect(maxValue(series, true)).toBe(6);
    expect(maxValue(series, false)).toBe(5);
    expect(maxValue([], true)).toBe(0);
  });
});

describe("gapRuns", () => {
  it("finds the partly staffed years as one run", () => {
    const bs = years(1985, 2010, [1990, 2004]);
    expect(gapRuns(bs)).toEqual([[5, 19]]);
    expect(gapRuns(years(2016, 2025))).toEqual([]);
  });
});
