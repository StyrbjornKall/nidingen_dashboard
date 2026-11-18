import { useMemo } from "react";
import type { Schemas } from "../../lib/api";
import { color } from "../../lib/tokens";
import type { TrendPlotSeries } from "./TrendPlot";

export type Trends = Schemas["Trends"];

/** Years of the period, gap years and one dense count list per species. */
export function useTrendSeries(data: Trends | undefined, colorOf: (c: string) => string | undefined) {
  return useMemo(() => {
    if (!data) return { years: [] as number[], gapYears: new Set<number>(), series: [] as TrendPlotSeries[] };
    const years: number[] = [];
    for (let y = data.period.year_from; y <= data.period.year_to; y++) years.push(y);
    const gapYears = new Set<number>();
    const series = data.species.map((s) => {
      const byYear = new Map(s.years.map((p) => [p.year, p.n]));
      s.years.forEach((p) => p.is_gap_year && gapYears.add(p.year));
      return {
        code: s.species,
        color: colorOf(s.species) ?? color.chart_mean,
        values: years.map((y) => byYear.get(y) ?? 0),
        fit: s.fit ?? null,
      };
    });
    return { years, gapYears, series };
  }, [data, colorOf]);
}
