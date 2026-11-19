/**
 * Pure helpers for the Återfynd globe: the view (centre and zoom), dragging,
 * the fly-to animation, picking the point under the pointer, and the texts.
 */
import { formatDate, formatInt } from "../../lib/format";

/** Nidingen, as [lon, lat] (the station is at about 57.30 °N, 11.90 °E). */
export const NIDINGEN: [number, number] = [11.9, 57.3];

export interface View {
  /** The longitude and latitude at the centre of the globe. */
  lon: number;
  lat: number;
  zoom: number;
}

export const MIN_ZOOM = 0.8;
/** Far enough to see single harbours: about nine zoom steps (×1.4) from the start. */
export const MAX_ZOOM = 22;
const MAX_DRAG_LAT = 75;
const MAX_FLY_LAT = 60;

export const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v));

/** Longitude wrapped to [−180, 180). */
export const wrapLon = (lon: number) => ((((lon + 180) % 360) + 360) % 360) - 180;

/** The shortest signed way round from one longitude to another, in degrees. */
export const lonDelta = (from: number, to: number) => ((((to - from + 180) % 360) + 360) % 360) - 180;

export const easeInOutCubic = (t: number) => (t < 0.5 ? 4 * t * t * t : 1 - (-2 * t + 2) ** 3 / 2);

/** The view `t` (0–1) of the way from `from` to a target centre, taking the short way round. */
export function flyView(from: View, to: { lon: number; lat: number; zoom?: number }, t: number): View {
  const e = easeInOutCubic(clamp(t, 0, 1));
  const targetLat = clamp(to.lat, -MAX_FLY_LAT, MAX_FLY_LAT);
  return {
    lon: wrapLon(from.lon + lonDelta(from.lon, to.lon) * e),
    lat: from.lat + (targetLat - from.lat) * e,
    zoom: from.zoom + ((to.zoom ?? from.zoom) - from.zoom) * e,
  };
}

/**
 * The view after dragging `dx`, `dy` pixels from `start`. Dragging right turns
 * the globe east-to-west under the finger, dragging down shows more north.
 * `k` is viewBox units per pixel (600 / on-screen width); a zoomed globe turns slower.
 */
export function dragView(start: View, dx: number, dy: number, k: number): View {
  const f = (0.32 * k) / start.zoom;
  return { ...start, lon: wrapLon(start.lon - dx * f), lat: clamp(start.lat + dy * f, -MAX_DRAG_LAT, MAX_DRAG_LAT) };
}

export const zoomBy = (view: View, factor: number): View => ({ ...view, zoom: clamp(view.zoom * factor, MIN_ZOOM, MAX_ZOOM) });

/** The point nearest to (px, py) within `maxDist` pixels; later items win a tie (they are drawn on top). */
export function nearestIndex(items: readonly { x: number; y: number; i: number }[], px: number, py: number, maxDist: number): number {
  let best = -1;
  let bestD = maxDist * maxDist;
  for (const it of items) {
    const d = (it.x - px) ** 2 + (it.y - py) ** 2;
    if (d <= bestD) {
      best = it.i;
      bestD = d;
    }
  }
  return best;
}

export interface Find {
  direction: "out" | "in";
  species: string;
  ring_date?: string | null;
  find_date: string;
  days?: number | null;
  lat: number;
  lon: number;
  place?: string | null;
  country?: string | null;
  distance_km?: number | null;
}

const SMALL_WORDS = new Set(["de", "da", "do", "dos", "das", "du", "la", "le", "von", "van", "der", "den", "af", "av", "och", "y", "del"]);

/**
 * "ILHA DE ENU" -> "Ilha de Enu". Only text written entirely in capitals is
 * changed; names that already have lower-case letters are left as they are.
 */
export function titleCase(text: string): string {
  if (text !== text.toLocaleUpperCase("sv") || text === text.toLocaleLowerCase("sv")) return text;
  let first = true;
  return text
    .toLocaleLowerCase("sv")
    .replace(/[\p{L}][\p{L}']*/gu, (word) => {
      const small = !first && SMALL_WORDS.has(word);
      first = false;
      return small ? word : word.charAt(0).toLocaleUpperCase("sv") + word.slice(1);
    });
}

/** "Skagen, Danmark", "Danmark", or "okänd plats". */
export function placeText(p: Pick<Find, "place" | "country">): string {
  const parts = [p.place, p.country].filter((x): x is string => !!x && x.trim() !== "").map(titleCase);
  return parts.length ? parts.join(", ") : "okänd plats";
}

export const kmText = (km: number | null | undefined) => (km == null ? "okänt avstånd" : `${formatInt(km)} km`);

/** The dark panel next to the globe: what is known about one find. */
export function findSentence(p: Find, name: string): { kicker: string; title: string; text: string } {
  const where = placeText(p);
  if (p.direction === "out") {
    const after = p.days == null ? "" : `, ${formatInt(p.days)} dagar efter märkningen`;
    return {
      kicker: "Märkt på Nidingen",
      title: `${name} — ${kmText(p.distance_km)}`,
      text: `Hittad vid ${where} ${formatDate(p.find_date, true)}${after}.`,
    };
  }
  const ringed = p.ring_date ? ` ${formatDate(p.ring_date, true)}` : "";
  return {
    kicker: "Märkt utomlands",
    title: `${name} — ${kmText(p.distance_km)}`,
    text: `Märkt vid ${where}${ringed}, fångad på Nidingen ${formatDate(p.find_date, true)}.`,
  };
}

/** True when two finds are the same record (the longest list holds copies). */
export const sameFind = (a: Find, b: Find) =>
  a.direction === b.direction && a.species === b.species && a.find_date === b.find_date && a.lat === b.lat && a.lon === b.lon && a.distance_km === b.distance_km;
