import { area, line } from "d3-shape";
import { useMemo } from "react";
import { chartStyles, Crosshair, GapBands, PlotSvg, Scrubber, useWidth, XLabels, YTicks } from "../../components/charts/Plot";
import { gapRuns, maxValue, yAxis } from "../../components/charts/scale";
import { formatInt } from "../../lib/format";
import styles from "./Sammanfattning.module.css";
import { axisLabels, type Bucket, type Grain } from "./timeseries";

export interface ChartSeries {
  code: string;
  name: string;
  color: string;
  values: readonly number[];
  /** Recaptures, drawn as a dashed line when given. */
  recap?: readonly number[];
}

export interface TimeSeriesChartProps {
  buckets: readonly Bucket[];
  grain: Grain;
  type: "bar" | "line";
  series: readonly ChartSeries[];
  /** Index of the bucket being read, or −1. */
  hover: number;
  onHover: (i: number) => void;
  /** Spoken value of the keyboard cursor (the readout text). */
  valueText: string;
  /** Changes whenever the chart should replay its entry animation. */
  animationKey: string;
}

const W = 1000;
const H = 300;

/** How far PageUp/PageDown jump: a year of buckets, or five years. */
const PAGE_STEP: Record<Grain, number> = { day: 30, week: 52, month: 12, year: 5 };

/**
 * Counts over time for up to six species: stacked bars or lines, gap years
 * shaded, a crosshair that follows the pointer or the arrow keys. The SVG
 * stretches to the box (viewBox 1000 × 300, no aspect ratio) and the axis
 * labels are HTML, so the text never scales.
 */
export function TimeSeriesChart({ buckets, grain, type, series, hover, onHover, valueText, animationKey }: TimeSeriesChartProps) {
  const n = buckets.length;
  const [plotRef, width] = useWidth<HTMLDivElement>();
  const stacked = type === "bar";

  const y = useMemo(() => {
    const all = series.flatMap((s) => (s.recap ? [s.recap] : []));
    const top = Math.max(maxValue(series.map((s) => s.values), stacked), maxValue(all, false));
    return yAxis(top);
  }, [series, stacked]);

  const bw = W / n;
  const x = (i: number) => (i + 0.5) * bw;

  const paths = useMemo(() => {
    const Yf = (v: number) => H - (v / y.top) * H;
    if (stacked) {
      const gap = bw > 4 ? bw * 0.24 : 0;
      const w = (bw - gap).toFixed(2);
      const base = new Array<number>(n).fill(0);
      return series.map((s) => {
        let normal = "";
        let partly = "";
        for (let i = 0; i < n; i++) {
          const v = s.values[i] ?? 0;
          const b0 = base[i] ?? 0;
          if (v > 0) {
            const d = `M${(i * bw + gap / 2).toFixed(2)} ${Yf(b0 + v).toFixed(2)}h${w}V${Yf(b0).toFixed(2)}h-${w}Z`;
            if (buckets[i]?.is_gap_year) partly += d;
            else normal += d;
          }
          base[i] = b0 + v;
        }
        return { code: s.code, color: s.color, normal, partly, line: "", area: "" };
      });
    }
    const ln = line<number>().x((_, i) => (i + 0.5) * bw).y(Yf);
    const ar = area<number>().x((_, i) => (i + 0.5) * bw).y0(H).y1(Yf);
    return series.map((s) => ({ code: s.code, color: s.color, normal: "", partly: "", line: ln(s.values as number[]) ?? "", area: ar(s.values as number[]) ?? "" }));
  }, [series, stacked, buckets, n, bw, y.top]);

  const recapPaths = useMemo(() => {
    const ln = line<number>().x((_, i) => (i + 0.5) * bw).y((v) => H - (v / y.top) * H);
    return series.filter((s) => s.recap).map((s) => ({ code: s.code, color: s.color, d: ln(s.recap as number[]) ?? "" }));
  }, [series, bw, y.top]);

  const labels = useMemo(
    () =>
      axisLabels(buckets, grain, width ? Math.max(2, Math.floor(width / 56)) : Infinity).map((l) => ({
        key: l.i, label: l.label, left: ((l.i + 0.5) / n) * 100,
      })),
    [buckets, grain, width, n],
  );
  const gaps = useMemo(() => gapRuns(buckets), [buckets]);

  return (
    <div className={styles.chart}>
      <div className={`${chartStyles.plot} ${styles.plot}`} ref={plotRef}>
        <GapBands runs={gaps} n={n} />
        <YTicks ticks={y.ticks} top={y.top} format={formatInt} />
        <PlotSvg width={W} height={H} animationKey={animationKey}>
          {paths.map((p) =>
            stacked ? (
              <g key={p.code} fill={p.color}>
                <path d={p.normal} />
                <path d={p.partly} fillOpacity={0.45} />
              </g>
            ) : (
              <g key={p.code}>
                <path d={p.area} fill={p.color} fillOpacity={0.08} />
                <path d={p.line} fill="none" stroke={p.color} strokeWidth={2} strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
              </g>
            ),
          )}
          {recapPaths.map((p) => (
            <path
              key={`r-${p.code}`}
              d={p.d}
              fill="none"
              stroke={p.color}
              strokeWidth={1.75}
              strokeDasharray="5 4"
              strokeLinejoin="round"
              vectorEffect="non-scaling-stroke"
            />
          ))}
        </PlotSvg>
        {hover >= 0 && hover < n && <Crosshair left={(x(hover) / W) * 100} />}
        <Scrubber n={n} hover={hover} onHover={onHover} valueText={valueText} pageStep={PAGE_STEP[grain]} />
      </div>
      <XLabels labels={labels} />
    </div>
  );
}
