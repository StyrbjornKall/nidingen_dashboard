/**
 * Building blocks for the React + SVG charts. A plot is a relative box
 * whose SVG stretches to fill it (fixed viewBox, no aspect ratio) while
 * every label is HTML positioned in percent, so text never scales.
 */
import { useEffect, useRef, useState, type KeyboardEvent, type PointerEvent, type ReactNode } from "react";
import { createPortal } from "react-dom";
import styles from "./charts.module.css";

/** Width of an element, kept up to date (0 where ResizeObserver is missing). */
export function useWidth<T extends HTMLElement>() {
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

/**
 * Horizontal grid lines with their labels in the left gutter. The gutter
 * width is the CSS variable --chart-gutter (default 52px), set by the
 * wrapper that pads the plot.
 */
export function YTicks({
  ticks, top, format, strongZero = false, small = false,
}: {
  ticks: readonly number[];
  top: number;
  format: (v: number) => string;
  strongZero?: boolean;
  small?: boolean;
}) {
  return (
    <>
      {ticks.map((t) => (
        <div
          key={t}
          className={`${styles.tick} ${strongZero && t === 0 ? styles.zero : ""}`}
          style={{ top: `${100 - (t / top) * 100}%` }}
          aria-hidden="true"
        >
          <span className={`mono ${styles.tickLabel} ${small ? styles.small : ""}`}>
            {format(t)}
          </span>
        </div>
      ))}
    </>
  );
}

/** Labels under the plot; `left` is in percent of the plot width. */
export function XLabels({ labels }: { labels: readonly { key: string | number; left: number; label: string }[] }) {
  return (
    <div className={styles.xAxis} aria-hidden="true">
      {labels.map((l) => (
        <span key={l.key} className={`mono ${styles.xLabel}`} style={{ left: `${l.left}%` }}>
          {l.label}
        </span>
      ))}
    </div>
  );
}

/**
 * Hatched bands over runs of partly staffed years (index pairs from
 * `gapRuns`), labelled when wide enough. HTML, so the stripes don't stretch.
 */
export function GapBands({ runs, n, label = "delvis bemannad" }: { runs: readonly [number, number][]; n: number; label?: string | null }) {
  return (
    <>
      {runs.map(([a, b]) => (
        <div
          key={a}
          className={styles.gapBand}
          style={{ left: `${(a / n) * 100}%`, width: `${((b - a + 1) / n) * 100}%` }}
          aria-hidden="true"
        >
          {label && (b - a + 1) / n > 0.12 && <span className={styles.gapLabel}>{label}</span>}
        </div>
      ))}
    </>
  );
}

/** A vertical line at `left` percent. */
export function Crosshair({ left }: { left: number }) {
  return <div className={styles.crosshair} style={{ left: `${left}%` }} aria-hidden="true" />;
}

/**
 * The invisible layer that reads the chart: follows the pointer (mouse,
 * or a finger dragging sideways) and the keyboard (arrows, Home/End,
 * PageUp/PageDown), as an ARIA slider whose value text is the readout.
 */
export function Scrubber({
  n, hover, onHover, valueText, pageStep = 5, label = "Läs av diagrammet", onSelect, onDrag,
}: {
  n: number;
  /** Index being read, or −1. */
  hover: number;
  onHover: (i: number) => void;
  valueText: string;
  pageStep?: number;
  label?: string;
  /** Click and drag across the chart: called with the first and last index of the dragged range. */
  onSelect?: (first: number, last: number) => void;
  /** The range while it is being dragged (for drawing it), null when the drag ends. */
  onDrag?: (range: [number, number] | null) => void;
}) {
  const from = useRef<number | null>(null);
  const indexAt = (e: PointerEvent<HTMLDivElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    return Math.max(0, Math.min(n - 1, Math.floor(((e.clientX - r.left) / r.width) * n)));
  };
  const pick = (e: PointerEvent<HTMLDivElement>) => {
    const i = indexAt(e);
    if (i !== hover) onHover(i);
    if (from.current !== null) onDrag?.([Math.min(from.current, i), Math.max(from.current, i)]);
  };
  const start = (e: PointerEvent<HTMLDivElement>) => {
    pick(e);
    if (!onSelect || e.button !== 0) return;
    from.current = indexAt(e);
    e.currentTarget.setPointerCapture?.(e.pointerId);
  };
  const end = (e: PointerEvent<HTMLDivElement>) => {
    const a = from.current;
    if (a === null) return;
    from.current = null;
    onDrag?.(null);
    const b = indexAt(e);
    if (a !== b) onSelect?.(Math.min(a, b), Math.max(a, b));
  };
  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const from = hover < 0 ? (e.key === "ArrowLeft" || e.key === "ArrowDown" ? n : -1) : hover;
    const moves: Record<string, number> = {
      ArrowRight: from + 1, ArrowLeft: from - 1, ArrowUp: from + 1, ArrowDown: from - 1,
      PageUp: from + pageStep, PageDown: from - pageStep, Home: 0, End: n - 1,
    };
    if (!(e.key in moves)) return;
    e.preventDefault();
    onHover(Math.max(0, Math.min(n - 1, moves[e.key] ?? from)));
  };
  return (
    <div
      className={styles.hit}
      role="slider"
      tabIndex={0}
      aria-label={label}
      aria-valuemin={0}
      aria-valuemax={Math.max(0, n - 1)}
      aria-valuenow={Math.max(0, hover)}
      aria-valuetext={valueText}
      onPointerMove={pick}
      onPointerDown={start}
      onPointerUp={end}
      onPointerCancel={() => {
        from.current = null;
        onDrag?.(null);
      }}
      onPointerLeave={(e) => e.pointerType === "mouse" && from.current === null && onHover(-1)}
      onKeyDown={onKey}
      onBlur={() => onHover(-1)}
    />
  );
}

/** The stretched SVG layer; remounts (and replays its wipe) when `animationKey` changes. */
export function PlotSvg({ width, height, animationKey, children }: { width: number; height: number; animationKey?: string; children: ReactNode }) {
  return (
    <svg key={animationKey} className={`${styles.svg} ${styles.wipe}`} viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true">
      {children}
    </svg>
  );
}

export { styles as chartStyles };

/** What a tooltip needs: where the pointer is (viewport pixels) and what to say. */
export type TipState = { x: number; y: number; lines: ReactNode } | null;

/**
 * A small floating readout that follows the pointer. Fixed to the viewport
 * and rendered in `body`, so scrolling cards and animated rows never clip it.
 * It flips to the left of the pointer near the right edge.
 */
export function Tooltip({ tip }: { tip: TipState }) {
  if (!tip || typeof document === "undefined") return null;
  const flip = tip.x > window.innerWidth - 240;
  return createPortal(
    <div
      className={styles.tip}
      role="presentation"
      style={{ left: tip.x, top: tip.y, transform: `translate(${flip ? "calc(-100% - 14px)" : "14px"}, -110%)` }}
    >
      {tip.lines}
    </div>,
    document.body,
  );
}
