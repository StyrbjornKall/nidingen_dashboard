import { useMemo, useState, type PointerEvent } from "react";
import { scaleLinear } from "d3-scale";
import { ChartCard, type CardQuery } from "../../components/ChartCard";
import { Tooltip, useWidth, type TipState } from "../../components/charts/Plot";
import type { Schemas } from "../../lib/api";
import { formatInt, formatNumber } from "../../lib/format";
import { color } from "../../lib/tokens";
import styles from "./Biometri.module.css";
import { binAt, binLabel, fitBand, formatTicks, percentAlong } from "./morph";

type Measure = Schemas["Measure"];
type ScatterSpecies = Schemas["ScatterSpecies"];

/** "Fördelningar": a histogram of weight and of wing length per selected species, each on its own scale. */
export function DistributionsCard({
  query, scatter, colorOf, nameOf, season,
}: {
  query: CardQuery<Schemas["MorphDetails"]>;
  scatter: Schemas["MorphScatter"] | undefined;
  colorOf: (c: string) => string | undefined;
  nameOf: (c: string) => string;
  season: string;
}) {
  return (
    <ChartCard
      title="Fördelningar"
      subtitle="Varje art på sin egen skala. Den mörka linjen är medianen. Till höger: vikt mot vinglängd, med trendlinje och 95 procents konfidensintervall."
      query={query}
      skeleton="rows"
      isEmpty={(d) => d.species.length === 0}
      emptyMessage="Välj minst en art i artväljaren. Mått jämförs per art, så ”Alla arter” visas inte här."
      footnote={`Fåglar som ringmärkts under ${season}, alla år sedan 1980. Spannet täcker de mittersta 99 procenten av fåglarna (0,5–99,5 %). Stapelbredden är alltid ett helt antal mätsteg för just den arten (till exempel 0,1 g för små fåglar och 1 g för silltrut, 1 mm för vinge), vald så att det blir ungefär 50 staplar. Toppar vid hela och halva gram är verkliga: vikter avrundades ofta så i början av serien. Spridningsdiagrammet visar högst 5 000 slumpvis valda fåglar per art, men trendlinjen bygger på alla.`}
    >
      {(data) => (
        <div className={styles.scroll}>
          <div className={styles.histGrid}>
            <span />
            <span className={styles.colHead}>Vikt (g)</span>
            <span className={styles.colHead}>Vinglängd (mm)</span>
            <span className={styles.colHead}>Vikt mot vinglängd</span>
            {data.species.map((s) => (
              <Row
                key={s.species}
                name={nameOf(s.species)}
                color={colorOf(s.species) ?? color.chart_mean}
                weight={s.weight}
                wing={s.wing}
                scatter={scatter?.species.find((p) => p.species === s.species)}
                scatterLoading={scatter === undefined}
              />
            ))}
          </div>
        </div>
      )}
    </ChartCard>
  );
}

function Row({
  name, color: c, weight, wing, scatter, scatterLoading,
}: {
  name: string; color: string; weight: Measure | null | undefined; wing: Measure | null | undefined;
  scatter: ScatterSpecies | undefined; scatterLoading: boolean;
}) {
  return (
    <>
      <span className={styles.rowName}>
        <span className={styles.dot} style={{ background: c }} aria-hidden="true" />
        {name}
      </span>
      <Histogram name={name} what="vikt" unit="g" m={weight} color={c} />
      <Histogram name={name} what="vinglängd" unit="mm" m={wing} color={c} />
      <div className={styles.scatterWrap}>
        {scatter ? <Scatter name={name} s={scatter} color={c} /> : <div className={styles.noMeasure}>{scatterLoading ? "Laddar …" : "Inga mätningar"}</div>}
      </div>
    </>
  );
}

function Histogram({ name, what, unit, m, color: c }: { name: string; what: string; unit: "g" | "mm"; m: Measure | null | undefined; color: string }) {
  const [at, setAt] = useState(-1);
  const [tip, setTip] = useState<TipState>(null);
  if (!m || m.bins.length === 0) return <div className={styles.noMeasure}>Inga {what}suppgifter för den här säsongen</div>;

  const lo = m.bins[0]!.low;
  const hi = m.bins[m.bins.length - 1]!.high;
  const top = Math.max(1, ...m.bins.map((b) => b.n));
  const x = (v: number) => ((v - lo) / (hi - lo)) * 100;
  const bar = (b: (typeof m.bins)[number]) => {
    const w = x(b.high) - x(b.low);
    return { x: x(b.low) + w * 0.1, w: w * 0.8, h: (b.n / top) * 38 };
  };
  const med = percentAlong(m.median, lo, hi);

  const move = (e: PointerEvent<HTMLDivElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    const i = binAt(m.bins, (e.clientX - r.left) / r.width);
    const b = m.bins[i]!;
    setAt(i);
    setTip({
      x: e.clientX,
      y: e.clientY,
      lines: (
        <>
          <strong>{name}</strong>
          <br />
          {binLabel(b.low, b.high, unit)}
          <br />
          <span className="mono">{formatInt(b.n)} fåglar</span>
        </>
      ),
    });
  };
  const leave = () => {
    setAt(-1);
    setTip(null);
  };

  return (
    <div>
      <div
        className={styles.hist}
        role="img"
        aria-label={`${name}, ${what}: median ${formatNumber(m.median, 1)} ${unit}, mittersta hälften ${formatNumber(m.q25, 1)}–${formatNumber(m.q75, 1)} ${unit}, ${formatInt(m.n)} fåglar`}
        onPointerMove={move}
        onPointerLeave={leave}
      >
        <svg viewBox="0 0 100 40" preserveAspectRatio="none" className={styles.histSvg} aria-hidden="true">
          {m.bins.map((b, i) => {
            const r = bar(b);
            return <rect key={i} x={r.x} y={40 - r.h} width={r.w} height={r.h} fill={c} fillOpacity={i === at ? 1 : 0.78} />;
          })}
        </svg>
        <span className={styles.medLine} style={{ left: `${med}%` }} aria-hidden="true" />
        <span className={`mono ${styles.medLabel} ${med > 78 ? styles.medLabelLeft : ""}`} style={{ left: `${med}%` }} aria-hidden="true">
          {formatNumber(m.median, 1)} {unit}
        </span>
      </div>
      <div className={`mono ${styles.histAxis}`} aria-hidden="true">
        <span>{formatNumber(lo, unit === "g" ? 1 : 0)}</span>
        <span>{formatNumber(hi, unit === "g" ? 1 : 0)}</span>
      </div>
      <Tooltip tip={tip} />
    </div>
  );
}


const SCATTER_H = 130;
const PAD = { l: 34, r: 6, t: 6, b: 18 };

/** Weight (y) against wing length (x), linear axes: sampled points, the OLS line and its 95 % band. */
function Scatter({ name, s, color: c }: { name: string; s: ScatterSpecies; color: string }) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const w = Math.max(width, 120);

  const geo = useMemo(() => {
    if (s.points.length < 3 || !s.fit) return null;
    const xs = s.points.map((p) => p[0]!);
    const ys = s.points.map((p) => p[1]!);
    const x0 = Math.min(...xs);
    const x1 = Math.max(...xs);
    const band = x1 > x0 ? fitBand(s.fit, x0, x1) : [];
    const yLo = Math.min(...ys, ...band.map((b) => b.lo));
    const yHi = Math.max(...ys, ...band.map((b) => b.hi));
    const x = scaleLinear().domain([x0, x1]).nice(4).range([PAD.l, w - PAD.r]);
    const y = scaleLinear().domain([yLo, yHi]).nice(4).range([SCATTER_H - PAD.b, PAD.t]);
    return { x, y, band };
  }, [s, w]);

  if (!geo || !s.fit) return <div className={styles.noMeasure}>För få mätningar</div>;
  const { x, y, band } = geo;
  const dots = s.points.map((p) => `M${x(p[0]!).toFixed(1)} ${y(p[1]!).toFixed(1)}h1.6v1.6h-1.6z`).join("");
  const line = band.map((b, i) => `${i ? "L" : "M"}${x(b.x).toFixed(1)} ${y(b.y).toFixed(1)}`).join("");
  const area =
    band.map((b, i) => `${i ? "L" : "M"}${x(b.x).toFixed(1)} ${y(b.hi).toFixed(1)}`).join("") +
    [...band].reverse().map((b) => `L${x(b.x).toFixed(1)} ${y(b.lo).toFixed(1)}`).join("") + "Z";
  const xt = x.ticks(4);
  const yt = y.ticks(4);
  const xl = formatTicks(xt);
  const yl = formatTicks(yt);
  const shown = s.n_shown < s.n_total ? `${formatInt(s.n_shown)} av ${formatInt(s.n_total)} fåglar visas` : `${formatInt(s.n_total)} fåglar`;

  return (
    <div ref={ref}>
      <svg
        width={w}
        height={SCATTER_H}
        role="img"
        aria-label={`${name}, vikt mot vinglängd: ${shown}, trendlinje ${formatNumber(s.fit.slope, 2)} g per mm`}
      >
        {yt.map((t, i) => (
          <g key={t}>
            <line x1={PAD.l} x2={w - PAD.r} y1={y(t)} y2={y(t)} stroke="var(--color-line)" strokeWidth={1} />
            <text x={PAD.l - 4} y={y(t)} textAnchor="end" dominantBaseline="middle" fontSize={10} fill="var(--color-text-muted)">{yl[i]}</text>
          </g>
        ))}
        {xt.map((t, i) => (
          <text key={t} x={x(t)} y={SCATTER_H - 4} textAnchor="middle" fontSize={10} fill="var(--color-text-muted)">{xl[i]}</text>
        ))}
        <path d={dots} fill={c} fillOpacity={0.35} />
        <path d={area} fill={c} fillOpacity={0.25} />
        <path d={line} fill="none" stroke={c} strokeWidth={2} />
      </svg>
      <div className={`mono ${styles.histAxis}`}>
        <span>{shown}</span>
        <span>{formatNumber(s.fit.slope, 2)} g/mm</span>
      </div>
    </div>
  );
}
