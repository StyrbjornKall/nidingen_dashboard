import { describe, expect, it } from "vitest";
import { pickableYears } from "../../lib/years";
import { heatColor, monthTicks, weekEnd, weekSlice, weekStart } from "./phenology";

describe("weeks as days", () => {
  it("counts 7-day weeks from 1 January, week 52 to the year's end", () => {
    expect([weekStart(1), weekEnd(1)]).toEqual([1, 7]);
    expect([weekStart(12), weekEnd(12)]).toEqual([78, 84]); // 19–25 March
    expect([weekStart(52), weekEnd(52)]).toEqual([358, 365]);
  });

  it("puts month ticks inside the window", () => {
    expect(monthTicks(64, 329).map((t) => t.label)).toEqual(["apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov"]);
    expect(monthTicks(64, 329)[0]).toEqual({ doy: 91, label: "apr" });
  });

  it("slices weeks out of a 52-week list", () => {
    const weeks = Array.from({ length: 52 }, (_, i) => i + 1);
    expect(weekSlice(weeks, 10, 12)).toEqual([10, 11, 12]);
  });
});

describe("heatColor", () => {
  it("runs through the three ramp stops", () => {
    expect(heatColor(0)).toBe("rgb(239,244,237)");
    expect(heatColor(0.5)).toBe("rgb(127,180,150)");
    expect(heatColor(1)).toBe("rgb(35,79,60)");
    expect(heatColor(2)).toBe("rgb(35,79,60)");
  });
});

describe("pickableYears", () => {
  it("leaves out the partly staffed years", () => {
    const years = pickableYears(1985, 2008, { year_from: 1990, year_to: 2004 });
    expect(years.slice(0, 4)).toEqual([2008, 2007, 2006, 2005]);
    expect(years).not.toContain(1990);
    expect(years.at(-1)).toBe(1985);
  });
});
