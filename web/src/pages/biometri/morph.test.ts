import { describe, expect, it } from "vitest";
import { fitBand, FULL_WIN, formatTicks, panWin, windowAxis, zoomLevel, zoomWin, ageColors, ageSortKey, binAt, binLabel, bubbleRadius, logAxis, niceLogTicks, percentAlong, seasonPhrase, sizeLegendValues, sortAgeCodes, sortSexCodes } from "./morph";

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

describe("zooming the bubble chart", () => {
  it("keeps the point under the pointer where it is", () => {
    const w = zoomWin(FULL_WIN, 2, 0.25, 0.75); // zoom in on the upper left quarter
    expect(w.x1 - w.x0).toBeCloseTo(0.5);
    // The anchor was at x = 0.25, y = 0.75 of the plot and stays at the same place in the window.
    expect(w.x0 + 0.25 * 0.5).toBeCloseTo(0.25);
    expect(w.y0 + 0.75 * 0.5).toBeCloseTo(0.75);
    expect(zoomLevel(w)).toBeCloseTo(2);
  });

  it("stays inside the plot and within its limits", () => {
    const corner = zoomWin(FULL_WIN, 4, 0, 0);
    expect(corner).toEqual({ x0: 0, x1: 0.25, y0: 0, y1: 0.25 });
    const far = zoomWin(FULL_WIN, 1e6);
    expect(zoomLevel(far)).toBeCloseTo(60);
    expect(zoomWin(far, 1 / 1e6)).toEqual(FULL_WIN); // zooming out ends at the whole plot
    const edge = zoomWin(zoomWin(FULL_WIN, 4, 1, 1), 2, 1, 1);
    expect(edge.x1).toBeCloseTo(1);
    expect(edge.y1).toBeCloseTo(1);
  });

  it("pans in window widths and stops at the edges", () => {
    const w = zoomWin(FULL_WIN, 4, 0.5, 0.5); // 0.375 .. 0.625
    const moved = panWin(w, 1, 0);
    expect(moved.x0).toBeCloseTo(0.625);
    expect(panWin(w, 100, -100)).toEqual({ x0: 0.75, x1: 1, y0: 0, y1: 0.25 });
    expect(panWin(FULL_WIN, 1, 1)).toEqual(FULL_WIN); // nothing to pan when the whole plot shows
  });

  it("re-fits the ticks to what is in view", () => {
    const axis = logAxis(10, 1000, 0);
    const half = windowAxis(axis, 0, 0.5); // 10 .. 100
    expect(half.lo).toBeCloseTo(10);
    expect(half.hi).toBeCloseTo(100);
    expect(half.frac(Math.sqrt(10 * 100))).toBeCloseTo(0.5);
    // Zoomed far in, the ticks are plain linear steps.
    const tight = windowAxis(axis, 0.5, 0.52); // about 100 .. 109.6
    expect(tight.ticks.length).toBeGreaterThanOrEqual(3);
    expect(tight.ticks.every((t) => t >= tight.lo && t <= tight.hi)).toBe(true);
  });

  it("writes tick labels with the decimals they need", () => {
    expect(formatTicks([5, 10, 20, 50])).toEqual(["5", "10", "20", "50"]);
    expect(formatTicks([70, 70.5, 71])).toEqual(["70,0", "70,5", "71,0"]);
    expect(formatTicks([0.25, 0.5])).toEqual(["0,25", "0,50"]);
  });
});

describe("fitBand", () => {
  const fit = { n: 11, slope: 2, intercept: 1, x_mean: 5, sxx: 110, residual_se: 3, t_crit: 2.262 };
  it("is narrowest at the mean of x and symmetric around the line", () => {
    const band = fitBand(fit, 0, 10, 10);
    const mid = band[5]!;
    expect(mid.x).toBe(5);
    expect(mid.y).toBeCloseTo(11);
    // half width at x̄ = t · s / √n
    expect(mid.hi - mid.y).toBeCloseTo((2.262 * 3) / Math.sqrt(11), 9);
    expect(mid.y - mid.lo).toBeCloseTo(mid.hi - mid.y, 9);
    expect(band[0]!.hi - band[0]!.lo).toBeGreaterThan(mid.hi - mid.lo);
  });
  it("uses the line y = intercept + slope · x", () => {
    expect(fitBand(fit, 0, 10, 2).map((p) => p.y)).toEqual([1, 11, 21]);
  });
});
