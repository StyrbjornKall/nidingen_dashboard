import { useState, type PointerEvent } from "react";
import { ChartCard, type CardQuery } from "../../components/ChartCard";
import { Tooltip, type TipState } from "../../components/charts/Plot";
import type { Schemas } from "../../lib/api";
import { formatInt, formatNumber } from "../../lib/format";
import { color } from "../../lib/tokens";
import styles from "./Morfometri.module.css";
import { binAt, binLabel, percentAlong } from "./morph";

type Measure = Schemas["Measure"];

/** "Fördelningar": a histogram of weight and of wing length per selected species, each on its own scale. */
export function DistributionsCard({
  query, colorOf, nameOf, season,
}: {
  query: CardQuery<Schemas["MorphDetails"]>;
  colorOf: (c: string) => string | undefined;
  nameOf: (c: string) => string;
  season: string;
}) {
  return (
    <ChartCard
      title="Fördelningar"
      subtitle="Varje art på sin egen skala. Den mörka linjen är medianen."
      query={query}
      skeleton="rows"
      isEmpty={(d) => d.species.length === 0}
      emptyMessage="Välj minst en art i artväljaren. Mått jämförs per art, så ”Alla arter” visas inte här."
      footnote={`Fåglar som ringmärkts under ${season}, alla år sedan 1980. Spannet täcker de mittersta 99 procenten av fåglarna (0,5–99,5 %). Det är högst 50 staplar, färre när måtten bara anges i hela steg (1 mm, 0,1 g).`}
    >
      {(data) => (
        <div className={styles.scroll}>
          <div className={styles.histGrid}>
            <span />
            <span className={styles.colHead}>Vikt (g)</span>
            <span className={styles.colHead}>Vinglängd (mm)</span>
            {data.species.map((s) => (
              <Row key={s.species} name={nameOf(s.species)} color={colorOf(s.species) ?? color.chart_mean} weight={s.weight} wing={s.wing} />
            ))}
          </div>
        </div>
      )}
    </ChartCard>
  );
}

function Row({ name, color: c, weight, wing }: { name: string; color: string; weight: Measure | null | undefined; wing: Measure | null | undefined }) {
  return (
    <>
      <span className={styles.rowName}>
        <span className={styles.dot} style={{ background: c }} aria-hidden="true" />
        {name}
      </span>
      <Histogram name={name} what="vikt" unit="g" m={weight} color={c} />
      <Histogram name={name} what="vinglängd" unit="mm" m={wing} color={c} />
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
