import { area, line } from "d3-shape";
import { useEffect, useMemo, useRef, useState, type KeyboardEvent, type PointerEvent } from "react";
import { formatInt } from "../../lib/format";
import styles from "./Sammanfattning.module.css";
import { axisLabels, gapRuns, maxValue, yAxis, type Bucket, type Grain } from "./timeseries";

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

/** Width of an element, kept up to date (0 where ResizeObserver is missing). */
function useWidth<T extends HTMLElement>() {
  const ref = useRef<T>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    setWidth(el.clientWidth);
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(([entry]) => entry && setWidth(entry.contentRect.width));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, width] as const;
}

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

  const labels = useMemo(() => axisLabels(buckets, grain, width ? Math.max(2, Math.floor(width / 56)) : Infinity), [buckets, grain, width]);
  const gaps = useMemo(() => gapRuns(buckets), [buckets]);

  const pick = (e: PointerEvent<HTMLDivElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    const i = Math.max(0, Math.min(n - 1, Math.floor(((e.clientX - r.left) / r.width) * n)));
    if (i !== hover) onHover(i);
  };

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const from = hover < 0 ? (e.key === "ArrowLeft" ? n : -1) : hover;
    const moves: Record<string, number> = {
      ArrowRight: from + 1, ArrowLeft: from - 1, ArrowUp: from + 1, ArrowDown: from - 1,
      PageUp: from + PAGE_STEP[grain], PageDown: from - PAGE_STEP[grain], Home: 0, End: n - 1,
    };
    if (!(e.key in moves)) return;
    e.preventDefault();
    onHover(Math.max(0, Math.min(n - 1, moves[e.key] ?? from)));
  };

  return (
    <div className={styles.chart}>
      <div className={styles.plot} ref={plotRef}>
        {gaps.map(([a, b]) => (
          <div
            key={a}
            className={styles.gapBand}
            style={{ left: `${(a / n) * 100}%`, width: `${((b - a + 1) / n) * 100}%` }}
            aria-hidden="true"
          >
            {(b - a + 1) / n > 0.12 && <span className={styles.gapLabel}>delvis bemannad</span>}
          </div>
        ))}
        {y.ticks.map((t) => (
          <div key={t} className={styles.tick} style={{ top: `${100 - (t / y.top) * 100}%` }} aria-hidden="true">
            <span className={`mono ${styles.tickLabel}`}>{formatInt(t)}</span>
          </div>
        ))}
        <svg
          key={animationKey}
          className={styles.wipe}
          viewBox={`0 0 ${W} ${H}`}
          preserveAspectRatio="none"
          aria-hidden="true"
        >
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
        </svg>
        {hover >= 0 && hover < n && <div className={styles.crosshair} style={{ left: `${(x(hover) / W) * 100}%` }} aria-hidden="true" />}
        <div
          className={styles.hit}
          role="slider"
          tabIndex={0}
          aria-label="Läs av diagrammet"
          aria-valuemin={0}
          aria-valuemax={Math.max(0, n - 1)}
          aria-valuenow={Math.max(0, hover)}
          aria-valuetext={valueText}
          onPointerMove={pick}
          onPointerDown={pick}
          onPointerLeave={(e) => e.pointerType === "mouse" && onHover(-1)}
          onKeyDown={onKey}
          onBlur={() => onHover(-1)}
        />
      </div>
      <div className={styles.xAxis} aria-hidden="true">
        {labels.map((l) => (
          <span key={l.i} className={`mono ${styles.xLabel}`} style={{ left: `${(x(l.i) / W) * 100}%` }}>
            {l.label}
          </span>
        ))}
      </div>
    </div>
  );
}
