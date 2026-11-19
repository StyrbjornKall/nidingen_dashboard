import { useState } from "react";
import { ChartCard } from "../../components/ChartCard";
import { SegmentedControl } from "../../components/SegmentedControl";
import { useApi, type PeriodInfo, type Schemas } from "../../lib/api";
import { formatInt, formatPercent } from "../../lib/format";
import { ALL_SPECIES } from "../../lib/urlState";
import { periodParams, yearSpan } from "../shared/selection";
import styles from "./Sammanfattning.module.css";

/** Shown at first; "Visa alla" lists every species with new ringings. */
export const TOP_LIMIT = 100;
const ALL_LIMIT = 500;

type Mode = "antal" | "andel";

export interface TopSpeciesCardProps {
  period: PeriodInfo;
  selected: readonly string[];
  colorOf: (code: string) => string | undefined;
  onToggle: (code: string) => void;
  max: number;
}

/** "Flest ringmärkta arter": ranked by new ringings, selected species first. */
export function TopSpeciesCard({ period, selected, colorOf, onToggle, max }: TopSpeciesCardProps) {
  const [mode, setMode] = useState<Mode>("antal");
  const [showAll, setShowAll] = useState(false);
  const picked = selected.filter((c) => c !== ALL_SPECIES);
  const query = useApi<Schemas["TopSpecies"]>(
    "/api/summary/top-species",
    { ...periodParams(period), species: picked, limit: showAll ? ALL_LIMIT : TOP_LIMIT },
    { keepPrevious: true },
  );
  const full = selected.length >= max;

  return (
    <ChartCard
      title="Flest ringmärkta arter"
      subtitle={
        <>
          Summa {yearSpan(period)}.{picked.length > 0 && " Valda arter ligger överst."} Klicka på en art för att lägga till
          eller ta bort den i jämförelsen.
        </>
      }
      query={query}
      skeleton="rows"
      isEmpty={(d) => d.rows.length === 0}
      emptyMessage={`Inga fåglar ringmärktes ${yearSpan(period)}.`}
      actions={
        <SegmentedControl
          label="Visa som"
          value={mode}
          onChange={setMode}
          options={[
            { value: "antal", label: "Antal" },
            { value: "andel", label: "Andel av alla" },
          ]}
        />
      }
    >
      {(data) => {
        const top = Math.max(1, ...data.rows.map((r) => r.n));
        const listed = data.rows.filter((r) => !r.selected).length;
        return (
          <div className={styles.topBody}>
            <ol className={styles.topList}>
              {data.rows.map((r) => {
                const c = colorOf(r.species);
                const on = c !== undefined;
                const disabled = !on && full;
                return (
                  <li key={r.species}>
                    <button
                      type="button"
                      className={styles.row}
                      aria-pressed={on}
                      disabled={disabled}
                      title={disabled ? `Högst ${max} arter kan jämföras. Ta bort en art först.` : undefined}
                      onClick={() => onToggle(r.species)}
                    >
                      <span className={`mono ${styles.rank}`}>{r.rank}</span>
                      <span className={styles.rowName} style={{ fontWeight: on ? 700 : 500 }}>
                        <span className={styles.dot} style={{ background: c ?? "transparent" }} aria-hidden="true" />
                        <span className={styles.rowNameText}>{r.name}</span>
                      </span>
                      <span className={styles.track} aria-hidden="true">
                        <span
                          className={styles.hbar}
                          style={{ width: `${(r.n / top) * 100}%`, background: c ?? "var(--bar-other)" }}
                        />
                      </span>
                      <span className={`mono ${styles.rowValue}`}>
                        {mode === "antal" ? formatInt(r.n) : formatPercent(r.share)}
                      </span>
                    </button>
                  </li>
                );
              })}
            </ol>
            <div className={styles.topFoot}>
              <p className={styles.topCount}>
                {showAll || listed >= data.n_species - picked.length
                  ? `Alla ${formatInt(data.n_species)} arter som ringmärktes ${yearSpan(period)}.`
                  : `Visar topp ${formatInt(TOP_LIMIT)} av ${formatInt(data.n_species)} arter.`}
              </p>
              {data.n_species > TOP_LIMIT && (
                <button type="button" className={styles.more} onClick={() => setShowAll((v) => !v)}>
                  {showAll ? `Visa bara topp ${TOP_LIMIT}` : `Visa alla ${formatInt(data.n_species)} arter`}
                </button>
              )}
            </div>
          </div>
        );
      }}
    </ChartCard>
  );
}
