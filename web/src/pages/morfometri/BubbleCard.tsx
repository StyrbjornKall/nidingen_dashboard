import { useEffect, useState } from "react";
import { ChartCard, type CardQuery } from "../../components/ChartCard";
import type { Schemas } from "../../lib/api";
import { formatInt, formatNumber } from "../../lib/format";
import styles from "./Morfometri.module.css";
import { bubbleRadius, logAxis, sizeLegendValues } from "./morph";

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
 * the rest grey; a click on a bubble adds or removes the species.
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
  const full = selected.length >= max;

  return (
    <ChartCard
      title="Vikt mot vinglängd, alla arter"
      subtitle={
        <>
          Varje bubbla är en arts medelvärde för fåglar som ringmärkts under {season}. Storleken visar hur många som ringmärkts sedan 1980
          (logaritmisk skala). Grått = övriga arter. Klicka på en bubbla för att lägga till eller ta bort arten.
        </>
      }
      actions={<Readout hover={hover} fallback={query.data ? firstSelected(query.data.rows, selected, colorOf) : null} />}
      query={query}
      isEmpty={(d) => d.rows.length === 0}
      emptyMessage="Inga arter har tillräckligt många vägda och mätta fåglar under den här säsongen."
      footnote={
        query.data &&
        `Arter med färre än ${query.data.min_n} vägda och mätta fåglar under säsongen saknas. Axlarna är logaritmiska och anpassade efter arterna i diagrammet.`
      }
    >
      {(data) => {
        const rows = data.rows;
        const xs = logAxis(Math.min(...rows.map((r) => r.wing_mean)), Math.max(...rows.map((r) => r.wing_mean)));
        const ys = logAxis(Math.min(...rows.map((r) => r.weight_mean)), Math.max(...rows.map((r) => r.weight_mean)));
        const nMin = Math.min(...rows.map((r) => r.n_total_since_1980));
        const nMax = Math.max(...rows.map((r) => r.n_total_since_1980));
        const px = (f: number) => g.x0 + f * (g.x1 - g.x0);
        const py = (f: number) => g.y1 - f * (g.y1 - g.y0);
        const radius = (n: number) => bubbleRadius(n, nMin, nMax, g.rMin, g.rMax);

        const bubble = (r: Mean) => ({
          r,
          code: r.species,
          x: (px(xs.frac(r.wing_mean)) / g.w) * 100,
          y: (py(ys.frac(r.weight_mean)) / g.h) * 100,
          rad: radius(r.n_total_since_1980),
          color: colorOf(r.species),
        });
        const all = rows.map(bubble);
        // Big bubbles first so small ones stay reachable; selected ones on top in order.
        const others = all.filter((b) => b.color === undefined);
        const chosen = all.filter((b) => b.color !== undefined).sort((a, b) => selected.indexOf(a.code) - selected.indexOf(b.code));
        const missing = selected.filter((c) => !rows.some((r) => r.species === c) && c !== "ALLA");
        const legend = sizeLegendValues(nMax);

        return (
          <div className={styles.bubbleBody}>
            <div ref={box} className={styles.bubblePlot} style={{ aspectRatio: `${g.w} / ${g.h}` }}>
              <svg className={styles.bubbleSvg} viewBox={`0 0 ${g.w} ${g.h}`} aria-hidden="true">
                <rect x={g.x0} y={g.y0} width={g.x1 - g.x0} height={g.y1 - g.y0} className={styles.plotBg} />
                <path
                  className={styles.gridPath}
                  d={[
                    ...xs.ticks.map((v) => `M${px(xs.frac(v)).toFixed(1)} ${g.y0}V${g.y1}`),
                    ...ys.ticks.map((v) => `M${g.x0} ${py(ys.frac(v)).toFixed(1)}H${g.x1}`),
                  ].join("")}
                />
              </svg>
              {xs.ticks.map((v) => (
                <span key={`x${v}`} className={`mono ${styles.xTick}`} style={{ left: `${(px(xs.frac(v)) / g.w) * 100}%`, top: `${((g.y1 + 8) / g.h) * 100}%` }} aria-hidden="true">
                  {formatNumber(v)}
                </span>
              ))}
              {ys.ticks.map((v) => (
                <span key={`y${v}`} className={`mono ${styles.yTick}`} style={{ width: `${(g.x0 - 6) / g.w * 100}%`, top: `${(py(ys.frac(v)) / g.h) * 100}%` }} aria-hidden="true">
                  {formatNumber(v)}
                </span>
              ))}
              <span className={styles.xTitle} style={{ left: `${((g.x0 + g.x1) / 2 / g.w) * 100}%` }} aria-hidden="true">Vinglängd (mm, log-skala)</span>
              <span className={styles.yTitle} style={{ top: `${((g.y0 + g.y1) / 2 / g.h) * 100}%` }} aria-hidden="true">Vikt (g, log-skala)</span>

              <div role="group" aria-label="Arternas medelvikt och medelvinglängd" onPointerLeave={() => setHover(null)}>
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
                        left: `${b.x}%`,
                        top: `${b.y}%`,
                        width: `${((2 * b.rad) / g.w) * 100}%`,
                        background: on ? b.color : undefined,
                      }}
                    />
                  );
                })}
                {chosen.map((b) => (
                  <span
                    key={b.code}
                    className={styles.bubbleLabel}
                    style={{ left: `${b.x}%`, top: `${b.y - ((b.rad + 2) / g.h) * 100}%`, color: b.color }}
                  >
                    {nameOf(b.code)}
                  </span>
                ))}
              </div>
            </div>

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
