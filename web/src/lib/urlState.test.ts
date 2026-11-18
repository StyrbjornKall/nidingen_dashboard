import { describe, expect, it } from "vitest";
import { toQueryString } from "./api";
import { buildSearch, parseSpeciesParam, sharedSearch } from "./urlState";

describe("species in the URL", () => {
  it("normalises, de-duplicates and caps the list", () => {
    expect(parseSpeciesParam("röhak, LÖSÅN,,röhak", 6)).toEqual(["RÖHAK", "LÖSÅN"]);
    expect(parseSpeciesParam("A,B,C,D,E,F,G", 6)).toHaveLength(6);
    expect(parseSpeciesParam(null, 6)).toEqual([]);
  });
  it("drops unknown codes once the species list is known", () => {
    expect(parseSpeciesParam("RÖHAK,FOO", 6, new Set(["RÖHAK"]))).toEqual(["RÖHAK"]);
  });
  it("keeps commas readable when writing", () => {
    const p = new URLSearchParams();
    p.set("arter", "RÖHAK,LÖSÅN");
    p.set("period", "r20");
    expect(buildSearch(p)).toBe("?arter=R%C3%96HAK,L%C3%96S%C3%85N&period=r20");
    expect(new URLSearchParams(buildSearch(p)).get("arter")).toBe("RÖHAK,LÖSÅN");
  });
  it("carries only the shared keys between pages", () => {
    expect(sharedSearch("?arter=R%C3%96HAK&year=2020&sasong=var")).toBe("?arter=R%C3%96HAK&sasong=var");
    expect(sharedSearch("")).toBe("");
  });
});

describe("API query strings", () => {
  it("joins lists with commas and skips empty values", () => {
    expect(toQueryString({ species: ["RÖHAK", "LÖSÅN"], year: 2025, period: undefined, x: null, y: [] }))
      .toBe("species=R%C3%96HAK,L%C3%96S%C3%85N&year=2025");
  });
});
