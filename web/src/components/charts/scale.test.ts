import { describe, expect, it } from "vitest";
import { gapRuns, maxValue, yAxis } from "./scale";

const years = (from: number, to: number, gap?: [number, number]) =>
  Array.from({ length: to - from + 1 }, (_, k) => ({ is_gap_year: gap ? from + k >= gap[0] && from + k <= gap[1] : false }));

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
