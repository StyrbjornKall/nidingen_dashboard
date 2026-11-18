import { describe, expect, it } from "vitest";
import { ciText, forestExtent, forestTicks, forestX, formatSlope, nice, trendYearsCaption, yearRuns } from "./trends";

const GAP = { year_from: 1990, year_to: 2004, reason: "Stationen var bara delvis bemannad dessa år." };

describe("trend text", () => {
  it("formats slopes with sign, decimal comma and the minus sign", () => {
    expect(formatSlope(8.24)).toBe("+8,2");
    expect(formatSlope(-8.5)).toBe("−8,5");
    expect(formatSlope(-23.7)).toBe("−24");
    expect(ciText(-13.94, -3.1)).toBe("95 % KI −14 till −3,1 fåglar/år");
  });
});

describe("yearRuns", () => {
  it("joins runs of years in Swedish", () => {
    expect(yearRuns([1980, 1981, 1982, 1990])).toBe("1980–1982 samt 1990");
    expect(yearRuns([2016, 2017])).toBe("2016–2017");
    expect(yearRuns([1, 3, 5])).toBe("1, 3 samt 5");
    expect(yearRuns([])).toBe("");
  });
});

describe("trendYearsCaption", () => {
  it("names the years used and left out, from config and data", () => {
    expect(trendYearsCaption({ year_from: 1980, year_to: 2025 }, GAP, 2026)).toBe(
      "Trenderna bygger på hela säsonger 1980–1989 samt 2005–2025. 1990–2004 ingår inte: stationen var bara delvis bemannad dessa år. 2026 pågår och ingår inte.",
    );
  });

  it("leaves out the gap note when the period has no gap years", () => {
    expect(trendYearsCaption({ year_from: 2016, year_to: 2025 }, GAP, 2025)).toBe("Trenderna bygger på hela säsonger 2016–2025.");
  });
});

describe("forest plot axis", () => {
  it("rounds the extent so every interval fits", () => {
    expect(nice(7)).toBe(10);
    expect(nice(0.23)).toBe(0.2);
    expect(forestExtent([{ ci_low: -17.1, ci_high: 13.6 }])).toBe(20);
    expect(forestExtent([{ ci_low: -81.8, ci_high: 59 }])).toBe(100);
    expect(forestExtent([{ ci_low: -2.3, ci_high: 1 }])).toBe(3);
    expect(forestExtent([])).toBe(1);
    for (const m of [0.3, 1.7, 3.2, 6.9, 13, 44, 99]) expect(forestExtent([{ ci_low: -m, ci_high: 0 }])).toBeGreaterThanOrEqual(m);
  });

  it("labels five symmetric ticks", () => {
    expect(forestTicks(20).map((t) => t.label)).toEqual(["−20", "−10", "0", "+10", "+20"]);
    expect(forestTicks(3).map((t) => t.label)).toEqual(["−3,0", "−1,5", "0", "+1,5", "+3,0"]);
  });

  it("places values on the axis and clamps outliers", () => {
    expect(forestX(0, 20)).toBe(50);
    expect(forestX(-20, 20)).toBe(0);
    expect(forestX(40, 20)).toBe(100);
  });
});
