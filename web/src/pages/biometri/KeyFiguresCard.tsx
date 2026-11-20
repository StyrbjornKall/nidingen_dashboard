import { ChartCard, type CardQuery } from "../../components/ChartCard";
import type { Schemas } from "../../lib/api";
import { formatInt, formatNumber } from "../../lib/format";
import { color } from "../../lib/tokens";
import styles from "./Biometri.module.css";

type Measure = Schemas["Measure"];

/** "Nyckeltal": median and middle half of weight and wing, and the count, per selected species. */
export function KeyFiguresCard({
  query, colorOf, nameOf, note,
}: {
  query: CardQuery<Schemas["MorphDetails"]>;
  colorOf: (c: string) => string | undefined;
  nameOf: (c: string) => string;
  note?: string;
}) {
  return (
    <ChartCard
      title="Nyckeltal"
      query={query}
      skeleton="rows"
      isEmpty={(d) => d.species.length === 0}
      emptyMessage="Välj minst en art i artväljaren. Mått jämförs per art, så ”Alla arter” visas inte här."
      footnote={
        <>
          Små siffror under medianen: mittersta hälften av fåglarna (kvartilavstånd). {note}
        </>
      }
    >
      {(data) => (
        <div className={styles.tiles}>
          {data.species.map((s) => (
            <div key={s.species} className={styles.tile}>
              <span className={styles.tileName}>
                <span className={styles.dot} style={{ background: colorOf(s.species) ?? color.chart_mean }} aria-hidden="true" />
                {nameOf(s.species)}
              </span>
              <div className={styles.tileGrid}>
                <Figure label="Vikt, median" m={s.weight} unit="g" />
                <Figure label="Vinge, median" m={s.wing} unit="mm" />
                <div>
                  <div className={styles.figLabel}>Ringmärkta</div>
                  <div className={`mono ${styles.figValue}`}>{formatInt(s.n_total_since_1980)}</div>
                  <div className={`mono ${styles.figSub}`}>sedan 1980</div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </ChartCard>
  );
}

function Figure({ label, m, unit }: { label: string; m: Measure | null | undefined; unit: string }) {
  return (
    <div>
      <div className={styles.figLabel}>{label}</div>
      <div className={`mono ${styles.figValue}`}>{m ? `${formatNumber(m.median, 1)} ${unit}` : "–"}</div>
      <div className={`mono ${styles.figSub}`}>{m ? `${formatNumber(m.q25, 1)}–${formatNumber(m.q75, 1)}` : "inga mått"}</div>
    </div>
  );
}
