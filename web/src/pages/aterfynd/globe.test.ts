import { describe, expect, it } from "vitest";
import { titleCase, clamp, dragView, findSentence, flyView, kmText, lonDelta, nearestIndex, placeText, sameFind, wrapLon, zoomBy, type Find } from "./globe";

describe("longitudes", () => {
  it("wraps to [−180, 180) and takes the short way round", () => {
    expect(wrapLon(190)).toBe(-170);
    expect(wrapLon(-190)).toBe(170);
    expect(lonDelta(170, -170)).toBe(20);
    expect(lonDelta(-170, 170)).toBe(-20);
    expect(lonDelta(10, 40)).toBe(30);
  });
});

describe("the view", () => {
  const home = { lon: 0, lat: 0, zoom: 1 };

  it("flies the short way and ends on the target, with latitude kept in range", () => {
    expect(flyView({ lon: 170, lat: 0, zoom: 2 }, { lon: -170, lat: 10 }, 0.5).lon).toBeCloseTo(-180, 5);
    const end = flyView(home, { lon: 12, lat: 80 }, 1);
    expect(end).toEqual({ lon: 12, lat: 60, zoom: 1 });
    expect(flyView(home, { lon: 12, lat: 30 }, 0)).toEqual(home);
  });

  it("turns slower when zoomed in, and stops at the poles", () => {
    const a = dragView(home, 100, 0, 1);
    const b = dragView({ ...home, zoom: 2 }, 100, 0, 1);
    expect(a.lon).toBeCloseTo(-32);
    expect(b.lon).toBeCloseTo(-16);
    expect(dragView(home, 0, 100000, 1).lat).toBe(75);
    expect(dragView(home, 0, -100000, 1).lat).toBe(-75);
  });

  it("keeps the zoom inside its limits", () => {
    expect(zoomBy(home, 1000).zoom).toBe(22);
    expect(zoomBy(home, 1.4 ** 9).zoom).toBeCloseTo(20.6, 0); // nine steps in from the start stay in range
    expect(zoomBy(home, 0.01).zoom).toBe(0.8);
    expect(clamp(5, 0, 3)).toBe(3);
  });
});

describe("picking a point", () => {
  const items = [{ x: 10, y: 10, i: 0 }, { x: 14, y: 10, i: 1 }, { x: 14, y: 10, i: 2 }, { x: 100, y: 100, i: 3 }];

  it("takes the nearest within range, the topmost on a tie, or none", () => {
    expect(nearestIndex(items, 11, 10, 8)).toBe(0);
    expect(nearestIndex(items, 13, 10, 8)).toBe(2);
    expect(nearestIndex(items, 50, 50, 8)).toBe(-1);
    expect(nearestIndex([], 0, 0, 8)).toBe(-1);
  });
});

describe("place names", () => {
  it("title-cases names written in capitals, keeping small words and hyphens", () => {
    expect(titleCase("SWAKOPMUND")).toBe("Swakopmund");
    expect(titleCase("ILHA DE ENU")).toBe("Ilha de Enu");
    expect(titleCase("CENTRAL REGION")).toBe("Central Region");
    expect(titleCase("BORÅS-ÖSTRA")).toBe("Borås-Östra");
    expect(titleCase("ST. JOHN'S")).toBe("St. John's");
    expect(titleCase("DE PANNE")).toBe("De Panne");
  });

  it("leaves names that already have lower case, and digits, alone", () => {
    expect(titleCase("Skagen")).toBe("Skagen");
    expect(titleCase("Guinea-Bissau")).toBe("Guinea-Bissau");
    expect(titleCase("55")).toBe("55");
    expect(titleCase("")).toBe("");
  });
});

describe("texts", () => {
  const out: Find = { direction: "out", species: "RÖHAK", find_date: "1996-03-02", ring_date: "1995-10-01", days: 153, lat: 40, lon: -3, place: "Madrid", country: "Spanien", distance_km: 2400 };

  it("names a place from what is known", () => {
    expect(placeText({ place: "Skagen", country: "Danmark" })).toBe("Skagen, Danmark");
    expect(placeText({ place: "SWAKOPMUND", country: "Namibia" })).toBe("Swakopmund, Namibia");
    expect(placeText({ place: "", country: "Norge" })).toBe("Norge");
    expect(placeText({ place: null, country: null })).toBe("okänd plats");
    expect(kmText(2400)).toBe("2 400 km");
    expect(kmText(null)).toBe("okänt avstånd");
  });

  it("describes outbound and inbound finds", () => {
    expect(findSentence(out, "Rödhake")).toEqual({
      kicker: "Märkt på Nidingen",
      title: "Rödhake — 2 400 km",
      text: "Hittad vid Madrid, Spanien 2 mar 1996, 153 dagar efter märkningen.",
    });
    const inbound: Find = { ...out, direction: "in", place: "Lista", country: "Norge", ring_date: "1989-08-22", find_date: "1989-09-01", distance_km: 350 };
    expect(findSentence(inbound, "Bergfink").text).toBe("Märkt vid Lista, Norge 22 aug 1989, fångad på Nidingen 1 sep 1989.");
  });

  it("recognises copies of the same find", () => {
    expect(sameFind(out, { ...out })).toBe(true);
    expect(sameFind(out, { ...out, lat: 41 })).toBe(false);
  });
});
