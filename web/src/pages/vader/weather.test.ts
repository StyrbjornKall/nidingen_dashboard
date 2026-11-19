import { describe, expect, it } from "vitest";
import {
  areaPath, arrowRotation, arrowStep, barsPath, bestIndex, dirName, linePath, monthStarts, niceTop, parseVars, rangeOf, serializeVars,
  topSector, valueRuns, wedgePath, wedgeRadius,
} from "./weather";

describe("chosen variables", () => {
  it("defaults to the design's four, keeps the chips' order and ignores unknown names", () => {
    expect(parseVars(null)).toEqual(["temp", "vind", "rikt", "tryck"]);
    expect(parseVars("sikt,temp,nonsense")).toEqual(["temp", "sikt"]);
    expect(parseVars("-")).toEqual([]);
  });

  it("writes nothing to the URL for the defaults", () => {
    expect(serializeVars(["tryck", "temp", "rikt", "vind"])).toBeNull();
    expect(serializeVars(["temp", "regn"])).toBe("temp,regn");
    expect(serializeVars([])).toBe("-");
    expect(parseVars(serializeVars([]))).toEqual([]);
  });
});

describe("wind", () => {
  it("names the eight sectors, wrapping round north", () => {
    expect(dirName(0)).toBe("N");
    expect(dirName(350)).toBe("N");
    expect(dirName(90)).toBe("Ö");
    expect(dirName(225)).toBe("SV");
    expect(dirName(-45)).toBe("NV");
  });

  it("turns the arrow to where the wind blows: wind from the east points west", () => {
    // The glyph points down. From 0° (north) the wind blows south: no turn. From 90° (east) it blows west.
    expect(arrowRotation(0)).toBe(0);
    expect(arrowRotation(90)).toBe(90);
    expect(arrowRotation(-90)).toBe(270);
  });

  it("thins the arrows to fit the width", () => {
    expect(arrowStep(184, 800)).toBe(4);
    expect(arrowStep(60, 800)).toBe(2);
    expect(arrowStep(60, 2000)).toBe(1);
    expect(arrowStep(184, 0)).toBe(1);
  });
});

describe("days", () => {
  it("finds month starts and the best day", () => {
    expect(monthStarts(["2025-06-29", "2025-06-30", "2025-07-01", "2025-07-02", "2025-08-01"])).toEqual([
      { index: 2, month: 7 }, { index: 4, month: 8 },
    ]);
    expect(bestIndex([0, 5, 9, 9, 2])).toBe(2);
    expect(bestIndex([0, 0])).toBe(-1);
  });
});

describe("paths", () => {
  const x = (i: number) => i * 10;
  const y = (v: number) => 100 - v;

  it("breaks a line where a value is missing", () => {
    expect(valueRuns([1, 2, null, 3, undefined, 4, 5])).toEqual([[0, 1], [3], [5, 6]]);
    expect(linePath([10, 20, null, 30], x, y)).toBe("M0.0 90.0L10.0 80.0M30.0 70.0");
    expect(linePath([null, null], x, y)).toBe("");
  });

  it("closes an area to the baseline, one shape per run", () => {
    expect(areaPath([10, 20, null, 30], x, y, 100)).toBe("M0.0 100L0.0 90.0L10.0 80.0L10.0 100ZM30.0 100L30.0 70.0L30.0 100Z");
  });

  it("draws bars only for values above zero", () => {
    expect(barsPath([0, 40, null], x, y, 100, 6)).toBe("M7.0 60.0h6.00V100h-6.00Z");
  });

  it("finds ranges and round tops", () => {
    expect(rangeOf([null, 3, -2, 8])).toEqual({ min: -2, max: 8 });
    expect(rangeOf([null])).toBeNull();
    expect(niceTop(13.2, 4, 12)).toBe(16);
    expect(niceTop(2, 4, 12)).toBe(12);
  });
});

describe("wind rose", () => {
  const rose = [
    { mean_catch: 90, days: 1 }, // one day only: too thin to call the best direction
    { mean_catch: 40, days: 6 },
    { mean_catch: 55, days: 3 },
    { mean_catch: null, days: 0 },
  ];

  it("picks the best sector among those with enough days", () => {
    expect(topSector(rose)).toBe(2);
    expect(topSector(rose, 1)).toBe(0);
    expect(topSector([{ mean_catch: null, days: 0 }])).toBe(-1);
  });

  it("draws a wedge from the centre, north up and clockwise", () => {
    expect(wedgePath(0, 100)).toBe("M200 200L165.8 106.0A100.0 100.0 0 0 1 234.2 106.0Z");
    expect(wedgePath(2, 100).startsWith("M200 200L")).toBe(true);
  });

  it("scales the radius by the square root of the mean", () => {
    expect(wedgeRadius(0, 100)).toBe(18);
    expect(wedgeRadius(100, 100)).toBe(160);
    expect(wedgeRadius(25, 100)).toBeCloseTo(89, 5);
  });
});
