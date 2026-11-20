import { useEffect, useRef, useState, type KeyboardEvent, type PointerEvent as ReactPointerEvent } from "react";
import { ChartCard, type CardQuery } from "../../components/ChartCard";
import type { Schemas } from "../../lib/api";
import { formatInt, formatNumber } from "../../lib/format";
import styles from "./Biometri.module.css";
import {
  bubbleRadius, FULL_WIN, formatTicks, logAxis, panWin, sizeLegendValues, windowAxis, zoomLevel, zoomWin, type LogAxis, type Win,
} from "./morph";

type Mean = Schemas["MorphMean"];

/** Plot geometry in viewBox units. Narrow screens get a taller, smaller plot. */
const WIDE = { w: 640, h: 420, x0: 56, x1: 630, y0: 10, y1: 370, rMin: 4, rMax: 21 };
const NARROW = { w: 360, h: 440, x0: 60, x1: 350, y0: 10, y1: 390, rMin: 3, rMax: 14 };

interface Hover {
  name: string;
  color: string | undefined;
  weight: number;
  wing: number;
  n: number;
}

/**
 * "Vikt mot vinglängd, alla arter": every species' mean weight against its
 * mean wing length on log axes that fit the data, bubble area by the number
 * of birds ringed since 1980 (log scale). Selected species are coloured,
 * the rest grey; a click on a bubble adds or removes the species. The plot
 * zooms and pans (buttons, Ctrl + wheel, pinch, drag, keys) because bubbles
 * of species with similar measures sit on top of each other.
 */
export function BubbleCard({
  query, season, selected, colorOf, nameOf, onToggle, max,
}: {
  query: CardQuery<Schemas["MorphMeans"]>;
  /** The season as a phrase for the caption. */
  season: string;
  selected: readonly string[];
  colorOf: (c: string) => string | undefined;
  nameOf: (c: string) => string;
  onToggle: (c: string) => void;
  max: number;
}) {
  const [box, width] = useElementWidth();
  const [hover, setHover] = useState<Hover | null>(null);
  const g = width > 0 && width < 520 ? NARROW : WIDE;

  return (
    <ChartCard
      title="Vikt mot vinglängd, alla arter"
      subtitle={
        <>
          Varje bubbla är en arts medelvärde för fåglar som ringmärkts under {season}. Storleken visar hur många som ringmärkts sedan 1980
          (tiologaritmisk skala, log₁₀). Grått = övriga arter. Klicka på en bubbla för att lägga till eller ta bort arten. Ligger bubblor över varandra kan du zooma in.
        </>
      }
      actions={<Readout hover={hover} fallback={query.data ? firstSelected(query.data.rows, selected, colorOf) : null} />}
      query={query}
      isEmpty={(d) => d.rows.length === 0}
      emptyMessage="Inga arter har tillräckligt många vägda och mätta fåglar under den här säsongen."
      footnote={
        query.data &&
        `Arter med färre än ${query.data.min_n} vägda och mätta fåglar under säsongen saknas. Axlarna är logaritmiska (tiologaritmen, log₁₀: lika långt mellan 1 och 10 som mellan 10 och 100) och anpassade efter arterna i diagrammet. Siffrorna på axlarna är de verkliga värdena.`
      }
    >
      {(data) => {
        const rows = data.rows;
        const xs = logAxis(Math.min(...rows.map((r) => r.wing_mean)), Math.max(...rows.map((r) => r.wing_mean)));
        const ys = logAxis(Math.min(...rows.map((r) => r.weight_mean)), Math.max(...rows.map((r) => r.weight_mean)));
        const nMin = Math.min(...rows.map((r) => r.n_total_since_1980));
        const nMax = Math.max(...rows.map((r) => r.n_total_since_1980));
        const radius = (n: number) => bubbleRadius(n, nMin, nMax, g.rMin, g.rMax);
        const missing = selected.filter((c) => !rows.some((r) => r.species === c) && c !== "ALLA");
        const legend = sizeLegendValues(nMax);

        return (
          <div className={styles.bubbleBody}>
            <BubblePlot
              boxRef={box}
              resetKey={season}
              rows={rows}
              xs={xs}
              ys={ys}
              g={g}
              radius={radius}
              selected={selected}
              colorOf={colorOf}
              nameOf={nameOf}
              onToggle={onToggle}
              max={max}
              setHover={setHover}
            />

            <div className={styles.sizeLegend}>
              <span>Ringmärkta sedan 1980:</span>
              {legend.map((v) => (
                <span key={v} className={styles.sizeItem}>
                  <span
                    className={styles.sizeCircle}
                    style={{ width: Math.round(2 * radius(v) * (width > 0 ? width / g.w : 1)) }}
                    aria-hidden="true"
                  />
                  {formatInt(v)}
                </span>
              ))}
            </div>
            {missing.length > 0 && (
              <p className={styles.missing}>
                Saknas i diagrammet (för få vägda och mätta fåglar under säsongen): {missing.map(nameOf).join(", ")}.
              </p>
            )}
          </div>
        );
      }}
    </ChartCard>
  );
}

type Geometry = typeof WIDE;

const ZOOM_STEP = 1.6;
/** Pointer movement (pixels) before a press becomes a drag rather than a click. */
const DRAG_SLOP = 4;

/**
 * The plot with its zoomable view. The view is a window onto the full axes
 * (`Win`, shared by both axes); bubbles keep their size when zooming, so
 * ones that overlap come apart. Everything is clipped to the plot area.
 */
function BubblePlot({
  boxRef, resetKey, rows, xs, ys, g, radius, selected, colorOf, nameOf, onToggle, max, setHover,
}: {
  boxRef: (el: HTMLDivElement | null) => void;
  resetKey: string;
  rows: readonly Mean[];
  xs: LogAxis;
  ys: LogAxis;
  g: Geometry;
  radius: (n: number) => number;
  selected: readonly string[];
  colorOf: (c: string) => string | undefined;
  nameOf: (c: string) => string;
  onToggle: (c: string) => void;
  max: number;
  setHover: (h: Hover | null) => void;
}) {
  const [win, setWin] = useState<Win>(FULL_WIN);
  const [dragging, setDragging] = useState(false);
  const clip = useRef<HTMLDivElement>(null);
  const pointers = useRef(new Map<number, { x: number; y: number }>());
  const gesture = useRef<{ start: { x: number; y: number }; win: Win; moved: boolean; pinch?: number } | null>(null);
  const winRef = useRef(win);
  winRef.current = win;

  // A new season has other axes: start from the whole plot.
  useEffect(() => setWin(FULL_WIN), [resetKey]);

  const level = zoomLevel(win);
  const zoomed = level > 1.001;
  const full = selected.length >= max;
  const xv = windowAxis(xs, win.x0, win.x1);
  const yv = windowAxis(ys, win.y0, win.y1);
  const plotW = g.x1 - g.x0;
  const plotH = g.y1 - g.y0;

  // Position inside the clip, in percent of the plot area.
  const fx = (frac: number) => frac;
  const fy = (frac: number) => 1 - frac;
  const bubbles = rows
    .map((r) => {
      const x = fx(xv.frac(r.wing_mean));
      const y = fy(yv.frac(r.weight_mean));
      const rad = radius(r.n_total_since_1980);
      return { r, code: r.species, x, y, rad, color: colorOf(r.species) };
    })
    // Leave out what is entirely outside the view.
    .filter((b) => b.x >= -b.rad / plotW && b.x <= 1 + b.rad / plotW && b.y >= -b.rad / plotH && b.y <= 1 + b.rad / plotH);
  const others = bubbles.filter((b) => b.color === undefined);
  const chosen = bubbles.filter((b) => b.color !== undefined).sort((a, b) => selected.indexOf(a.code) - selected.indexOf(b.code));

  const zoomAt = (factor: number, ax = 0.5, ay = 0.5) => setWin((w) => zoomWin(w, factor, ax, ay));
  const anchor = (clientX: number, clientY: number) => {
    const r = clip.current!.getBoundingClientRect();
    return { ax: (clientX - r.left) / r.width, ay: 1 - (clientY - r.top) / r.height };
  };

  // Ctrl/Cmd + wheel (and a trackpad pinch, which arrives as Ctrl + wheel) zooms; a plain wheel still scrolls the page.
  useEffect(() => {
    const el = clip.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      if (!e.ctrlKey && !e.metaKey) return;
      e.preventDefault();
      const r = el.getBoundingClientRect();
      setWin((w) => zoomWin(w, Math.exp(-e.deltaY * 0.003), (e.clientX - r.left) / r.width, 1 - (e.clientY - r.top) / r.height));
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, []);

  const spread = () => {
    const [a, b] = [...pointers.current.values()];
    return a && b ? Math.hypot(a.x - b.x, a.y - b.y) : 0;
  };

  const onDown = (e: ReactPointerEvent<HTMLDivElement>) => {
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (pointers.current.size === 2) {
      gesture.current = { start: { x: e.clientX, y: e.clientY }, win: winRef.current, moved: true, pinch: spread() };
      setDragging(true);
    } else if (pointers.current.size === 1) {
      gesture.current = { start: { x: e.clientX, y: e.clientY }, win: winRef.current, moved: false };
    }
  };

  const onMove = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (!pointers.current.has(e.pointerId)) return;
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });
    const gs = gesture.current;
    if (!gs) return;
    const r = clip.current!.getBoundingClientRect();
    if (pointers.current.size >= 2 && gs.pinch) {
      const [a, b] = [...pointers.current.values()];
      const now = spread();
      if (a && b && now > 0) {
        const mid = anchor((a.x + b.x) / 2, (a.y + b.y) / 2);
        const factor = now / gs.pinch;
        gs.pinch = now;
        setWin((w) => zoomWin(w, factor, mid.ax, mid.ay));
      }
      return;
    }
    const dx = e.clientX - gs.start.x;
    const dy = e.clientY - gs.start.y;
    if (!gs.moved && Math.hypot(dx, dy) > DRAG_SLOP) {
      // Only a view that can move starts a drag; otherwise the press stays a click.
      if (!zoomed) return;
      gs.moved = true;
      setDragging(true);
      setHover(null);
      clip.current!.setPointerCapture(e.pointerId);
    }
    if (gs.moved) setWin(panWin(gs.win, -dx / r.width, dy / r.height));
  };

  const onUp = (e: ReactPointerEvent<HTMLDivElement>) => {
    pointers.current.delete(e.pointerId);
    if (pointers.current.size === 0) {
      gesture.current = null;
      setDragging(false);
    } else if (pointers.current.size === 1 && gesture.current) {
      // Back to one finger after a pinch: carry on dragging from here.
      const [p] = [...pointers.current.values()];
      gesture.current = { start: { x: p!.x, y: p!.y }, win: winRef.current, moved: true };
    }
  };

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const step = 0.25;
    if (e.key === "+" || e.key === "=") zoomAt(ZOOM_STEP);
    else if (e.key === "-" || e.key === "_") zoomAt(1 / ZOOM_STEP);
    else if (e.key === "0" || e.key === "Escape" || e.key === "Home") setWin(FULL_WIN);
    else if (zoomed && e.key === "ArrowLeft") setWin((w) => panWin(w, -step, 0));
    else if (zoomed && e.key === "ArrowRight") setWin((w) => panWin(w, step, 0));
    else if (zoomed && e.key === "ArrowUp") setWin((w) => panWin(w, 0, step));
    else if (zoomed && e.key === "ArrowDown") setWin((w) => panWin(w, 0, -step));
    else return;
    e.preventDefault();
  };

  const percentX = (frac: number) => `${(g.x0 + frac * plotW) / g.w * 100}%`;
  const percentY = (frac: number) => `${(g.y0 + (1 - frac) * plotH) / g.h * 100}%`;
  const xLabels = formatTicks(xv.ticks);
  const yLabels = formatTicks(yv.ticks);

  return (
    <>
      <div ref={boxRef} className={styles.bubblePlot} style={{ aspectRatio: `${g.w} / ${g.h}` }}>
        <svg className={styles.bubbleSvg} viewBox={`0 0 ${g.w} ${g.h}`} aria-hidden="true">
          <rect x={g.x0} y={g.y0} width={plotW} height={plotH} className={styles.plotBg} />
          <path
            className={styles.gridPath}
            d={[
              ...xv.ticks.map((v) => `M${(g.x0 + xv.frac(v) * plotW).toFixed(1)} ${g.y0}V${g.y1}`),
              ...yv.ticks.map((v) => `M${g.x0} ${(g.y0 + (1 - yv.frac(v)) * plotH).toFixed(1)}H${g.x1}`),
            ].join("")}
          />
        </svg>
        {xv.ticks.map((v, i) => (
          <span key={`x${v}`} className={`mono ${styles.xTick}`} style={{ left: percentX(xv.frac(v)), top: `${((g.y1 + 8) / g.h) * 100}%` }} aria-hidden="true">
            {xLabels[i]}
          </span>
        ))}
        {yv.ticks.map((v, i) => (
          <span key={`y${v}`} className={`mono ${styles.yTick}`} style={{ width: `${((g.x0 - 6) / g.w) * 100}%`, top: percentY(yv.frac(v)) }} aria-hidden="true">
            {yLabels[i]}
          </span>
        ))}
        <span className={styles.xTitle} style={{ left: `${((g.x0 + g.x1) / 2 / g.w) * 100}%` }} aria-hidden="true">Vinglängd (mm, log₁₀-skala)</span>
        <span className={styles.yTitle} style={{ top: `${((g.y0 + g.y1) / 2 / g.h) * 100}%` }} aria-hidden="true">Vikt (g, log₁₀-skala)</span>

        <div
          ref={clip}
          className={styles.clip}
          data-zoomed={zoomed}
          data-dragging={dragging}
          style={{ left: `${(g.x0 / g.w) * 100}%`, top: `${(g.y0 / g.h) * 100}%`, width: `${(plotW / g.w) * 100}%`, height: `${(plotH / g.h) * 100}%` }}
          role="group"
          aria-label={`Arternas medelvikt och medelvinglängd. Plus och minus zoomar${zoomed ? ", piltangenterna flyttar vyn" : ""}.`}
          tabIndex={0}
          onPointerDown={onDown}
          onPointerMove={onMove}
          onPointerUp={onUp}
          onPointerCancel={onUp}
          onPointerLeave={() => setHover(null)}
          onKeyDown={onKey}
          onDoubleClick={(e) => {
            if (e.target !== e.currentTarget) return;
            const a = anchor(e.clientX, e.clientY);
            zoomAt(ZOOM_STEP * 1.5, a.ax, a.ay);
          }}
        >
          {[...others, ...chosen].map((b) => {
            const on = b.color !== undefined;
            const blocked = !on && full;
            const name = nameOf(b.code);
            return (
              <button
                key={b.code}
                type="button"
                className={styles.bubble}
                data-on={on}
                aria-pressed={on}
                aria-disabled={blocked}
                aria-label={`${name}${on ? ", vald" : blocked ? `, högst ${max} arter kan jämföras` : ", lägg till i jämförelsen"}`}
                title={blocked ? `Högst ${max} arter kan jämföras. Ta bort en art först.` : undefined}
                // Keyboard users pick species in the species picker; only chosen ones stay in the tab order.
                tabIndex={on ? 0 : -1}
                onClick={() => !blocked && onToggle(b.code)}
                onPointerEnter={() => setHover(toHover(b.r, name, b.color))}
                onFocus={() => setHover(toHover(b.r, name, b.color))}
                onBlur={() => setHover(null)}
                style={{
                  left: `${b.x * 100}%`,
                  top: `${b.y * 100}%`,
                  width: `${((2 * b.rad) / plotW) * 100}%`,
                  background: on ? b.color : undefined,
                }}
              />
            );
          })}
          {chosen.map((b) => (
            <span
              key={b.code}
              className={styles.bubbleLabel}
              style={{ left: `${b.x * 100}%`, top: `calc(${b.y * 100}% - ${(((b.rad + 2) / plotH) * 100).toFixed(2)}%)`, color: b.color }}
            >
              {nameOf(b.code)}
            </span>
          ))}
        </div>
      </div>

      <div className={styles.zoomBar}>
        <button type="button" className={styles.zoomButton} aria-label="Zooma in" onClick={() => zoomAt(ZOOM_STEP)} disabled={level >= 59.9}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true"><path d="M8 3v10M3 8h10" /></svg>
        </button>
        <button type="button" className={styles.zoomButton} aria-label="Zooma ut" onClick={() => zoomAt(1 / ZOOM_STEP)} disabled={!zoomed}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true"><path d="M3 8h10" /></svg>
        </button>
        <button type="button" className={styles.zoomButton} onClick={() => setWin(FULL_WIN)} disabled={!zoomed}>
          Visa alla
        </button>
        <span className={`mono ${styles.zoomLevel}`} aria-live="polite">{zoomed ? `${formatNumber(level, level < 10 ? 1 : 0)}×` : "1×"}</span>
        <span className={styles.zoomHint}>Zooma med knapparna, Ctrl + rullning eller nyp. Dra för att flytta vyn.</span>
      </div>
    </>
  );
}

function toHover(r: Mean, name: string, color: string | undefined): Hover {
  return { name, color, weight: r.weight_mean, wing: r.wing_mean, n: r.n_total_since_1980 };
}

function firstSelected(rows: readonly Mean[], selected: readonly string[], colorOf: (c: string) => string | undefined): Hover | null {
  for (const code of selected) {
    const r = rows.find((x) => x.species === code);
    if (r) return toHover(r, r.name, colorOf(code));
  }
  return null;
}

function Readout({ hover, fallback }: { hover: Hover | null; fallback: Hover | null }) {
  const h = hover ?? fallback;
  return (
    <div className={styles.readout} aria-live="polite">
      {h ? (
        <>
          <span className={styles.readoutName}>
            <span className={styles.dot} style={{ background: h.color ?? "var(--color-chart-mean)" }} aria-hidden="true" />
            {h.name}
          </span>
          <span className="mono">{formatNumber(h.weight, 1)} g · {formatNumber(h.wing, 1)} mm</span>
          <span className="mono">{formatInt(h.n)} ringmärkta sedan 1980</span>
        </>
      ) : (
        <span className={styles.readoutName}>Håll över en bubbla</span>
      )}
    </div>
  );
}

/**
 * Width of an element that may mount later than its component (the plot only
 * exists once the data has loaded), so a callback ref feeds the observer.
 */
function useElementWidth() {
  const [el, setEl] = useState<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    if (!el) return;
    setWidth(el.clientWidth);
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(([entry]) => entry && setWidth(entry.contentRect.width));
    ro.observe(el);
    return () => ro.disconnect();
  }, [el]);
  return [setEl, width] as const;
}
