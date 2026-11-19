/**
 * State that lives in the URL so links can be shared (HANDOFF.md §1, §7):
 *   arter   selected species, in selection order (max 6): ?arter=RÖHAK,LÖSÅN
 *   period  period preset id from /api/meta:               ?period=r20
 *   sasong  season: var, host or helar:                    ?sasong=var
 *
 * The values are written with commas and Swedish letters kept readable
 * (encodeURIComponent per code, joined with ",").
 */
import { useCallback, useMemo } from "react";
import { useLocation, useNavigate } from "react-router";
import { speciesPalette } from "./tokens";

export const SHARED_KEYS = ["arter", "period", "sasong"] as const;
export type SharedKey = (typeof SHARED_KEYS)[number];
export type SeasonId = "var" | "host" | "helar";

export const ALL_SPECIES = "ALLA";

/** Parse "röhak,LÖSÅN,,röhak" -> ["RÖHAK", "LÖSÅN"]; unknown codes dropped when `known` is given. */
export function parseSpeciesParam(raw: string | null, max: number, known?: ReadonlySet<string>): string[] {
  const out: string[] = [];
  for (const part of (raw ?? "").split(",")) {
    const code = part.trim().toUpperCase();
    if (!code || out.includes(code)) continue;
    if (known && !known.has(code)) continue;
    out.push(code);
    if (out.length >= max) break;
  }
  return out;
}

/** Build "?arter=R%C3%96HAK,L%C3%96S%C3%85N&period=r20" with readable commas. */
export function buildSearch(params: URLSearchParams): string {
  const parts: string[] = [];
  params.forEach((value, key) => {
    if (value === "") return;
    const encoded = key === "arter" || key === "visa"
      ? value.split(",").map((v) => encodeURIComponent(v)).join(",")
      : encodeURIComponent(value);
    parts.push(`${encodeURIComponent(key)}=${encoded}`);
  });
  return parts.length ? `?${parts.join("&")}` : "";
}

/** Keep only the shared keys, for links between pages. */
export function sharedSearch(search: string): string {
  const current = new URLSearchParams(search);
  const kept = new URLSearchParams();
  for (const key of SHARED_KEYS) {
    const v = current.get(key);
    if (v) kept.set(key, v);
  }
  return buildSearch(kept);
}

/** Read and write one URL parameter, replacing the history entry. */
export function useSearchParam(key: string): [string | null, (value: string | null) => void] {
  const location = useLocation();
  const navigate = useNavigate();
  const value = new URLSearchParams(location.search).get(key);
  const setValue = useCallback(
    (next: string | null) => {
      // Read the address bar, not the render's `location`: two changes in quick
      // succession (several controls) must not overwrite each other.
      const live = typeof window !== "undefined" && window.location.pathname === location.pathname ? window.location.search : location.search;
      const params = new URLSearchParams(live);
      if (next === null || next === "") params.delete(key);
      else params.set(key, next);
      navigate({ pathname: location.pathname, search: buildSearch(params), hash: location.hash }, { replace: true });
    },
    [key, location.pathname, location.search, location.hash, navigate],
  );
  return [value, setValue];
}

export interface SpeciesSelection {
  selected: string[];
  /** Colour of a selected species (by selection order), or undefined. */
  colorOf: (code: string) => string | undefined;
  toggle: (code: string) => void;
  set: (codes: string[]) => void;
  max: number;
}

/**
 * The shared species selection. `defaults` is used when the URL has none;
 * `fallback` replaces an emptied selection (each page sets its own, as in
 * the design: Sammanfattning falls back to ALLA, the others to RÖHAK).
 */
export function useSpeciesSelection(opts: {
  defaults: readonly string[];
  fallback: readonly string[];
  max: number;
  known?: ReadonlySet<string>;
}): SpeciesSelection {
  const { defaults, fallback, max, known } = opts;
  const [raw, setRaw] = useSearchParam("arter");
  const selected = useMemo(() => {
    const fromUrl = parseSpeciesParam(raw, max, known);
    if (fromUrl.length) return fromUrl;
    return parseSpeciesParam(defaults.join(","), max, known);
  }, [raw, max, known, defaults]);

  const set = useCallback(
    (codes: string[]) => {
      const next = codes.length ? codes.slice(0, max) : [...fallback];
      setRaw(next.join(","));
    },
    [fallback, max, setRaw],
  );
  const toggle = useCallback(
    (code: string) => {
      if (selected.includes(code)) set(selected.filter((c) => c !== code));
      else if (selected.length < max) set([...selected, code]);
    },
    [selected, max, set],
  );
  const colorOf = useCallback(
    (code: string) => {
      const i = selected.indexOf(code);
      return i >= 0 ? speciesPalette[i % speciesPalette.length] : undefined;
    },
    [selected],
  );
  return { selected, colorOf, toggle, set, max };
}

/** A choice from a fixed list, stored in the URL; invalid values fall back to the default. */
export function useChoiceParam<T extends string>(key: SharedKey | string, allowed: readonly T[], fallback: T): [T, (v: T) => void] {
  const [raw, setRaw] = useSearchParam(key);
  const value = (allowed as readonly string[]).includes(raw ?? "") ? (raw as T) : fallback;
  const set = useCallback((v: T) => setRaw(v === fallback ? null : v), [fallback, setRaw]);
  return [value, set];
}
