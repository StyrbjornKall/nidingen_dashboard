import { useState } from "react";
import { ChartCard } from "../../components/ChartCard";
import { verdictLabel } from "../../components/TrendBadge";
import { useApi, type PeriodInfo, type Schemas } from "../../lib/api";
import { formatInt } from "../../lib/format";
import type { SeasonId } from "../../lib/urlState";
import styles from "./Trender.module.css";
import { ciText, forestExtent, forestTicks, forestX, formatSlope } from "./trends";

/** Species with fewer new ringings than this in the period and season are hidden at first. */
export const MIN_TOTAL = 100;

export function OverviewCard({
  season, period, selected, colorOf, onToggle, max, caption,
}: {
  season: SeasonId;
  period: PeriodInfo;
  selected: readonly string[];
  colorOf: (code: string) => string | undefined;
  onToggle: (code: string) => void;
  max: number;
  caption: string | undefined;
}) {
  const [showFew, setShowFew] = useState(false);
  const query = useApi<Schemas["TrendOverview"]>(
    "/api/trends/overview",
    { season, period: period.id, species: selected, min_total: showFew ? 0 : MIN_TOTAL },
    { keepPrevious: true },
  );
  const full = selected.length >= max;

  return (
    <ChartCard
      title="Alla arter"
      subtitle="Punkten är den skattade förändringen i antal ringmärkta fåglar per år, strecket dess 95 % konfidensintervall. Ihålig punkt betyder att intervallet korsar noll, alltså ingen säker trend. Klicka på en art för att lägga till den i jämförelsen. Valda arter ligger överst, därefter sorteras övriga efter förändring."
      query={query}
      skeleton="rows"
      isEmpty={(d) => d.rows.length === 0}
      emptyMessage="Inga arter har tillräckligt många fåglar under perioden och säsongen."
      footnote={caption}
    >
      {(data) => {
        const extent = forestExtent(data.rows);
        const ticks = forestTicks(extent);
        const zero = forestX(0, extent);
        return (
          <div className={styles.forestBody}>
            <div className={styles.forest} role="list">
              <div className={`${styles.forestRow} ${styles.forestHead}`} aria-hidden="true">
                <span />
                <div>
                  <div className={styles.forestTitle}>Förändring i antal ringmärkta, fåglar per år</div>
                  <div className={styles.forestTicks}>
                    {ticks.map((t, k) => (
                      <span
                        key={t.value}
                        className={`mono ${k % 2 === 1 ? styles.halfTick : ""}`}
                        style={{ left: `${forestX(t.value, extent)}%` }}
                      >
                        {t.label}
                      </span>
                    ))}
                  </div>
                </div>
                <span className={styles.forestTitle} style={{ textAlign: "right" }}>
                  Trend<span className={styles.unitHead}> fåglar/år</span>
                </span>
              </div>
              {data.rows.map((r) => {
                const c = colorOf(r.species);
                const on = c !== undefined;
                const ink = c ?? "var(--color-chart-mean)";
                const sure = r.verdict !== "none";
                const disabled = !on && full;
                const lo = forestX(r.ci_low, extent);
                const hi = forestX(r.ci_high, extent);
                return (
                  <div key={r.species} role="listitem">
                    <button
                      type="button"
                      className={`${styles.forestRow} ${styles.forestButton}`}
                      aria-pressed={on}
                      disabled={disabled}
                      title={disabled ? `Högst ${max} arter kan jämföras. Ta bort en art först.` : undefined}
                      onClick={() => onToggle(r.species)}
                    >
                      <span className={styles.forestName} style={{ fontWeight: on ? 700 : 500 }}>
                        <span
                          className={styles.forestDot}
                          style={{ background: c ?? "transparent", boxShadow: `inset 0 0 0 1px ${c ?? "#9AA89F"}` }}
                          aria-hidden="true"
                        />
                        <span className={styles.forestNameText}>{r.name}</span>
                      </span>
                      <span className={styles.forestCell} aria-hidden="true">
                        <span className={styles.forestZero} style={{ left: `${zero}%` }} />
                        <span className={styles.whisker} style={{ left: `${lo}%`, width: `${hi - lo}%`, background: ink }} />
                        <span
                          className={styles.point}
                          style={{ left: `${forestX(r.slope, extent)}%`, borderColor: ink, background: sure ? ink : "var(--color-surface)" }}
                        />
                      </span>
                      <span className={`mono ${styles.forestValue}`}>
                        {formatSlope(r.slope)}
                        <span className={styles.unit}> fåglar/år</span>
                      </span>
                      <span className="visually-hidden">
                        , {ciText(r.ci_low, r.ci_high)}, {verdictLabel(r.verdict).toLowerCase()}, {formatInt(r.total)} fåglar under perioden
                      </span>
                    </button>
                  </div>
                );
              })}
            </div>
            <div className={styles.forestFoot}>
              <p className={styles.ci}>
                {showFew
                  ? `Alla ${formatInt(data.rows.length)} arter som ringmärkts under perioden och säsongen.`
                  : `${formatInt(data.rows.length)} arter med minst ${MIN_TOTAL} ringmärkta fåglar under perioden och säsongen${selected.length ? ", plus valda arter" : ""}. Arter med färre fåglar ger mycket osäkra trender.`}
              </p>
              <button type="button" className={styles.more} onClick={() => setShowFew((v) => !v)}>
                {showFew ? `Visa bara arter med minst ${MIN_TOTAL} fåglar` : "Visa alla arter"}
              </button>
            </div>
          </div>
        );
      }}
    </ChartCard>
  );
}
