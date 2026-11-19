import { describe, expect, it } from "vitest";
import { ageColors, ageSortKey, binAt, binLabel, bubbleRadius, logAxis, niceLogTicks, percentAlong, seasonPhrase, sizeLegendValues, sortAgeCodes, sortSexCodes } from "./morph";

describe("log axes", () => {
  it("picks at most eight ticks, the finest set that fits", () => {
    expect(niceLogTicks(45, 450)).toEqual([50, 70, 100, 150, 200, 300]);
    expect(niceLogTicks(4.4, 1150)).toEqual([5, 10, 20, 50, 100, 200, 500, 1000]);
  });

  it("fits the data with a margin and places values by their logarithm", () => {
    const a = logAxis(10, 1000, 0);
    expect(a.lo).toBeCloseTo(10);
    expect(a.hi).toBeCloseTo(1000);
    expect(a.frac(100)).toBeCloseTo(0.5);
    const padded = logAxis(10, 1000);
    expect(padded.lo).toBeLessThan(10);
    expect(padded.hi).toBeGreaterThan(1000);
  });

  it("survives a single value", () => {
    const a = logAxis(20, 20);
    expect(Number.isFinite(a.frac(20))).toBe(true);
  });
});

describe("bubbles", () => {
  it("grows with the logarithm of the count, within its limits", () => {
    expect(bubbleRadius(10, 10, 10000, 4, 20)).toBe(4);
    expect(bubbleRadius(10000, 10, 10000, 4, 20)).toBe(20);
    expect(bubbleRadius(316.2278, 10, 10000, 4, 20)).toBeCloseTo(12, 1);
    expect(bubbleRadius(1, 10, 10000, 4, 20)).toBe(4); // below the smallest species
  });

  it("offers up to three round sizes that exist in the data", () => {
    expect(sizeLegendValues(62360)).toEqual([100, 1000, 10000]);
    expect(sizeLegendValues(500)).toEqual([100]);
  });
});

describe("raw age and sex codes", () => {
  it("orders age codes youngest first, odd codes last", () => {
    expect(ageSortKey("10")).toBeLessThan(ageSortKey("1+"));
    expect(sortAgeCodes(["3+", "10", "--", "2+", "20", "00", "1+"])).toEqual(["00", "10", "1+", "20", "2+", "3+", "--"]);
  });

  it("gives each code a colour along the ramp, first dark and last light", () => {
    const c = ageColors(["20", "10", "2+"], ["#000000", "#ffffff"]);
    expect(c.get("10")).toBe("rgb(0,0,0)");
    expect(c.get("2+")).toBe("rgb(255,255,255)");
    expect(c.get("20")).toBe("rgb(128,128,128)");
  });

  it("puts men, women, other codes and unknown in that order", () => {
    expect(sortSexCodes([null, "N", "F", "M", "G"])).toEqual(["M", "F", "G", "N", null]);
  });
});

describe("histogram labels", () => {
  it("reads a one-step bin as its value and a wide bin as a range", () => {
    expect(binLabel(59.5, 60.5, "mm")).toBe("60 mm");
    expect(binLabel(12.95, 13.05, "g")).toBe("13,0 g");
    expect(binLabel(12.9, 13.34, "g")).toBe("12,90–13,34 g");
    expect(binLabel(70, 72.5, "mm")).toBe("70–73 mm");
  });

  it("finds the bin under a position and keeps the median inside the box", () => {
    const bins = [{ low: 0, high: 1 }, { low: 1, high: 2 }, { low: 2, high: 3 }, { low: 3, high: 4 }];
    expect(binAt(bins, 0)).toBe(0);
    expect(binAt(bins, 0.6)).toBe(2);
    expect(binAt(bins, 1)).toBe(3);
    expect(percentAlong(5, 0, 10)).toBe(50);
    expect(percentAlong(-3, 0, 10)).toBe(0);
    expect(percentAlong(30, 0, 10)).toBe(100);
  });
});

describe("season phrase", () => {
  it("names the months from the API", () => {
    const name = (m: number) => ["", "januari", "februari", "mars", "april", "maj", "juni", "juli", "augusti", "september", "oktober", "november", "december"][m]!;
    expect(seasonPhrase({ id: "var", months: [1, 2, 3, 4, 5, 6] }, name)).toBe("våren (januari–juni)");
    expect(seasonPhrase({ id: "host", months: [7, 8, 9, 10, 11, 12] }, name)).toBe("hösten (juli–december)");
    expect(seasonPhrase({ id: "helar", months: [1, 12] }, name)).toBe("hela året");
  });
});
