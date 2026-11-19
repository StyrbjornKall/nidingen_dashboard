import { useMemo } from "react";
import { chartStyles, Crosshair, GapBands, PlotSvg, Scrubber, XLabels, YTicks } from "../../components/charts/Plot";
import { gapRuns, yAxis } from "../../components/charts/scale";
import type { Schemas } from "../../lib/api";
import { formatInt } from "../../lib/format";

type Fit = NonNullable<Schemas["TrendSeries"]["fit"]>;

export interface TrendPlotSeries {
  code: string;
  color: string;
  /** One count per year in `years`. */
  values: readonly number[];
  fit: Fit | null;
}

export interface TrendPlotProps {
  years: readonly number[];
  gapYears: ReadonlySet<number>;
  series: readonly TrendPlotSeries[];
  /** "lines": raw counts as thin lines, band and fit in the species colour
   *  (comparison chart). "bars": one species, bars in its colour, band and
   *  fit in ink (species cards). */
  mode: "lines" | "bars";
  /** Plot height in px (labels come on top of that). */
  height: number;
  className?: string;
  /** Interactive reading; leave out for static small multiples. */
  hover?: number;
  onHover?: (i: number) => void;
  valueText?: string;
  animationKey?: string;
}

const W = 1000;
const H = 300;

/** Year labels every 1, 2 or 5 years, as on Sammanfattning. */
function yearLabels(years: readonly number[]) {
  const n = years.length;
  const step = n > 20 ? 5 : n > 8 ? 2 : 1;
  const y0 = years[0] ?? 0;
  return years.flatMap((y, i) => ((y - y0) % step === 0 ? [{ key: y, label: String(y), left: ((i + 0.5) / n) * 100 }] : []));
}

/** Only the first and last year, for the small species cards. */
function endLabels(years: readonly number[]) {
  const n = years.length;
  const first = years[0];
  const last = years[n - 1];
  if (first === undefined || last === undefined) return [];
  return [
    { key: first, label: String(first), left: (0.5 / n) * 100 },
    { key: last, label: String(last), left: ((n - 0.5) / n) * 100 },
  ];
}

/**
 * Counts per year with the fitted trend and its 95 % band (from the API,
 * gap years already left out of the fit). Gap years are hatched; the band
 * and line span the whole period.
 */
export function TrendPlot({ years, gapYears, series, mode, height, className, hover = -1, onHover, valueText = "", animationKey }: TrendPlotProps) {
  const n = years.length;
  const bw = W / n;
  const index = useMemo(() => new Map(years.map((y, i) => [y, i])), [years]);

  const y = useMemo(() => {
    let top = 0;
    for (const s of series) {
      for (const v of s.values) top = Math.max(top, v);
      for (const b of s.fit?.band ?? []) top = Math.max(top, b.hi);
    }
    return yAxis(top * (mode === "lines" ? 1.04 : 1), mode === "lines" ? 4 : 2);
  }, [series, mode]);

  const Y = (v: number) => Math.max(0, Math.min(H, H - (v / y.top) * H));
  const X = (year: number) => ((index.get(year) ?? 0) + 0.5) * bw;
  const runs = useMemo(() => gapRuns(years.map((yr) => ({ is_gap_year: gapYears.has(yr) }))), [years, gapYears]);

  const drawn = series.map((s) => {
    const band = s.fit?.band ?? [];
    const first = band[0];
    const last = band[band.length - 1];
    return {
      ...s,
      band: band.length
        ? `M${band.map((b) => `${X(b.year).toFixed(1)} ${Y(b.hi).toFixed(1)}`).join("L")}L${[...band].reverse().map((b) => `${X(b.year).toFixed(1)} ${Y(b.lo).toFixed(1)}`).join("L")}Z`
        : "",
      fitLine: first && last ? `M${X(first.year).toFixed(1)} ${Y(first.fit).toFixed(1)}L${X(last.year).toFixed(1)} ${Y(last.fit).toFixed(1)}` : "",
      raw: `M${s.values.map((v, i) => `${((i + 0.5) * bw).toFixed(1)} ${Y(v).toFixed(1)}`).join("L")}`,
    };
  });

  const barW = bw * 0.66;
  const bars = (values: readonly number[], gap: boolean) =>
    values
      .map((v, i) => {
        const yr = years[i] ?? 0;
        if (v <= 0 || gapYears.has(yr) !== gap) return "";
        return `M${((i + 0.5) * bw - barW / 2).toFixed(1)} ${Y(v).toFixed(1)}h${barW.toFixed(1)}V${H}h-${barW.toFixed(1)}Z`;
      })
      .join("");

  return (
    <div className={className}>
      <div className={chartStyles.plot} style={{ height }}>
        <GapBands runs={runs} n={n} label={mode === "lines" ? "delvis bemannad" : null} />
        <YTicks ticks={y.ticks} top={y.top} format={formatInt} strongZero={mode === "lines"} small={mode === "bars"} />
        <PlotSvg width={W} height={H} animationKey={animationKey}>
          {drawn.map((s) =>
            mode === "lines" ? (
              <g key={s.code}>
                <path d={s.band} fill={s.color} fillOpacity={0.12} />
                <path d={s.raw} fill="none" stroke={s.color} strokeOpacity={0.45} strokeWidth={1.4} strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
                <path d={s.fitLine} fill="none" stroke={s.color} strokeWidth={3} strokeLinecap="round" vectorEffect="non-scaling-stroke" />
              </g>
            ) : (
              <g key={s.code}>
                <path d={bars(s.values, false)} fill={s.color} fillOpacity={0.55} />
                <path d={bars(s.values, true)} fill={s.color} fillOpacity={0.25} />
                <path d={s.band} fill="var(--color-ink)" fillOpacity={0.12} />
                <path d={s.fitLine} fill="none" stroke="var(--color-ink)" strokeWidth={2.2} strokeLinecap="round" vectorEffect="non-scaling-stroke" />
              </g>
            ),
          )}
        </PlotSvg>
        {onHover && hover >= 0 && hover < n && <Crosshair left={((hover + 0.5) / n) * 100} />}
        {onHover && <Scrubber n={n} hover={hover} onHover={onHover} valueText={valueText} />}
      </div>
      <XLabels labels={mode === "lines" ? yearLabels(years) : endLabels(years)} />
    </div>
  );
}
