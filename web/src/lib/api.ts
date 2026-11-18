/**
 * Typed client for the FastAPI backend. Types are generated from the API's
 * OpenAPI schema (`npm run gen:api`), so they follow app/api/schemas.py.
 */
import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import type { components } from "./api-types";

export type Schemas = components["schemas"];
export type Meta = Schemas["Meta"];
export type SpeciesInfo = Schemas["SpeciesInfo"];
export type PeriodInfo = Schemas["PeriodInfo"];
export type Verdict = Schemas["TrendRow"]["verdict"];

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export type QueryParams = Record<string, string | number | boolean | readonly string[] | null | undefined>;

/** `{species: ["RÖHAK","LÖSÅN"], year: 2025}` -> `species=R%C3%96HAK,L%C3%96S%C3%85N&year=2025`. */
export function toQueryString(params: QueryParams = {}): string {
  const parts: string[] = [];
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null) continue;
    if (Array.isArray(value)) {
      if (value.length === 0) continue;
      parts.push(`${key}=${value.map((v) => encodeURIComponent(v)).join(",")}`);
    } else {
      parts.push(`${key}=${encodeURIComponent(String(value))}`);
    }
  }
  return parts.join("&");
}

export async function getJson<T>(path: string, params?: QueryParams, signal?: AbortSignal): Promise<T> {
  const qs = toQueryString(params);
  const res = await fetch(qs ? `${path}?${qs}` : path, { signal, headers: { Accept: "application/json" } });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = (await res.json()) as { detail?: unknown };
      if (typeof body.detail === "string") detail = body.detail;
    } catch {
      /* not JSON */
    }
    throw new ApiError(res.status, detail);
  }
  return (await res.json()) as T;
}

/**
 * Fetch an API endpoint with TanStack Query. The data only changes when the
 * database is rebuilt, so results never go stale within a visit.
 */
export function useApi<T>(path: string, params?: QueryParams, options: { enabled?: boolean } = {}): UseQueryResult<T> {
  return useQuery({
    queryKey: [path, toQueryString(params)],
    queryFn: ({ signal }) => getJson<T>(path, params, signal),
    staleTime: Infinity,
    enabled: options.enabled ?? true,
  });
}

export function useMeta(): UseQueryResult<Meta> {
  return useApi<Meta>("/api/meta");
}
