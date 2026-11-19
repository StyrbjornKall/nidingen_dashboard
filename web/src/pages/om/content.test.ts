import { describe, expect, it } from "vitest";
import { faq, glossary, materialSentence, monthSpan, WEEKS_ANSWER, type Facts } from "./content";

const facts: Facts = {
  nNew: 395777,
  nRecap: 54292,
  nRecoveries: 6223,
  nSpecies: 210,
  firstYear: 1980,
  currentYear: 2026,
  completeThrough: 2025,
  maxCompare: 6,
  gap: { yearFrom: 1991, yearTo: 2004, reason: "Stationen var bara delvis bemannad dessa år." },
  seasons: [{ id: "var", months: [1, 2, 3, 4, 5, 6] }, { id: "host", months: [7, 8, 9, 10, 11, 12] }],
  weatherNote: "Väderuppgifterna för en dag gäller bara kl. 03–12 svensk tid.",
  ringingMaxDate: "2026-05-09",
  builtAtUtc: "2026-11-19T09:30:00",
};

const ids = (f: Facts) => faq(f).map((q) => q.id);
const get = (f: Facts, id: string) => faq(f).find((q) => q.id === id)!;

describe("generated sentences", () => {
  it("names the months of a season", () => {
    expect(monthSpan([1, 2, 3, 4, 5, 6])).toBe("januari–juni");
    expect(monthSpan([7, 8, 9, 10, 11, 12])).toBe("juli–december");
  });

  it("rounds the material to thousands, from the data", () => {
    expect(materialSentence(facts)).toBe(
      "Materialet omfattar omkring 396 000 nyringmärkta fåglar, 54 000 kontroller på ön och 6 000 återfynd från hela världen, tillsammans med väderdata från SMHI. " +
        "Siffrorna uppdateras när en ny säsong har förts in i stationens databas.",
    );
  });

  it("takes the season boundaries from the config, not from the text", () => {
    const season = glossary(facts).find((t) => t.term === "Säsong")!;
    expect(season.text).toContain("Vår är januari–juni och höst är juli–december.");
    const moved = glossary({ ...facts, seasons: [{ id: "var", months: [1, 2, 3, 4, 5] }, { id: "host", months: [6, 7, 8, 9, 10, 11, 12] }] });
    expect(moved.find((t) => t.term === "Säsong")!.text).toContain("Vår är januari–maj och höst är juni–december.");
  });
});

describe("the questions", () => {
  it("covers the decided topics: weeks, weather window and the partly staffed years", () => {
    expect(ids(facts)).toEqual(expect.arrayContaining(["veckor", "timmar", "delvis"]));
    expect(get(facts, "veckor").a).toBe(WEEKS_ANSWER);
    expect(WEEKS_ANSWER).toContain("Vecka 12 på webbplatsen är alltså alltid 19–25 mars");
    expect(get(facts, "timmar").a).toBe(facts.weatherNote); // one source: the config note
  });

  it("builds the gap-year question from the config", () => {
    expect(get(facts, "delvis").q).toBe("Varför saknas 1991–2004 i trenderna?");
    expect(get(facts, "delvis").a).toContain("Stationen var bara delvis bemannad dessa år.");
    expect(get({ ...facts, gap: { yearFrom: 1990, yearTo: 2003, reason: "X." } }, "delvis").q).toBe("Varför saknas 1990–2003 i trenderna?");
  });

  it("explains the running year only while there is one", () => {
    expect(get(facts, "innevarande").q).toBe("Varför är 2026 inte med i trenderna?");
    expect(get(facts, "innevarande").a).toContain("till och med 9 maj 2026");
    expect(ids({ ...facts, currentYear: 2025 })).not.toContain("innevarande");
  });

  it("states the compare limit and the species count from the data", () => {
    expect(get(facts, "jamfor").a).toContain("upp till 6 arter");
    expect(get({ ...facts, maxCompare: 4 }, "jamfor").a).toContain("upp till 4 arter");
    expect(get(facts, "arter").a).toContain("210 olika artkoder");
  });

  it("has a unique id for every question", () => {
    expect(new Set(ids(facts)).size).toBe(ids(facts).length);
  });
});
