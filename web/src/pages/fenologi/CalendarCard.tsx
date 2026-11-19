import { useId, useState } from "react";
import { Tooltip, type TipState } from "../../components/charts/Plot";
import { ChartCard } from "../../components/ChartCard";
import { SegmentedControl } from "../../components/SegmentedControl";
import { useApi, type Meta, type Schemas } from "../../lib/api";
import { formatDate, formatInt, formatNumber, formatWeek } from "../../lib/format";
import { heatRamp } from "../../lib/tokens";
import { yearRuns } from "../../lib/years";
import styles from "./Fenologi.module.css";
import { calendarYears, heatColor, LAST_SPRING_WEEK, monthTicks, weekEnd, weekSlice, weekStart } from "./phenology";

type Top = "5" | "10" | "15" | "all";
type Scale = "rel" | "abs";

/**
 * Flyttningskalendern: weekly catch per species as a heat map, with its own
 * year, top-N and colour-scale controls (not the species picker's period).
 */
export function CalendarCard({ meta, selected, colorOf }: { meta: Meta; selected: readonly string[]; colorOf: (c: string) => string | undefined }) {
  const [year, setYear] = useState("avg");
  const [top, setTop] = useState<Top>("10");
  const [scale, setScale] = useState<Scale>("rel");
  const yearId = useId();
  const [tip, setTip] = useState<TipState>(null);
  const query = useApi<Schemas["Calendar"]>(
    "/api/phenology/calendar",
    { year, top, species: selected },
    { keepPrevious: true },
  );
  const { week_from: w0, week_to: w1 } = meta.active_weeks;
  const years = calendarYears(meta.first_year, meta.current_year, meta.gap_years);
  const d0 = weekStart(w0);
  const span = weekEnd(w1) - d0 + 1;

  return (
    <ChartCard
      title="Flyttningskalendern"
      badge="Påverkas inte av artväljaren"
      subtitle="Hela fågelsamhällets år, vecka för vecka. Färgen normaliseras per art och säsong, vår och höst var för sig, så att båda flyttningarna syns. Arter du jämför ligger överst, markerade med en prick, därefter sorteras övriga efter när de passerar."
      query={query}
      skeleton="rows"
      isEmpty={(d) => d.rows.length === 0}
      emptyMessage="Inga fåglar ringmärktes det året."
      actions={
        <>
          <label htmlFor={yearId} className={styles.selectLabel}>
            År
          </label>
          <select id={yearId} className={styles.select} value={year} onChange={(e) => setYear(e.target.value)}>
            <option value="avg">Snitt alla år</option>
            {years.map((y) => (
              <option key={y} value={String(y)}>
                {y}
              </option>
            ))}
          </select>
          <SegmentedControl
            label="Antal arter"
            value={top}
            onChange={setTop}
            options={[
              { value: "5", label: "Topp 5" },
              { value: "10", label: "Topp 10" },
              { value: "15", label: "Topp 15" },
              { value: "all", label: "Alla arter" },
            ]}
          />
          <SegmentedControl
            label="Färgskala"
            value={scale}
            onChange={setScale}
            options={[
              { value: "rel", label: "Per säsong" },
              { value: "abs", label: "Antal" },
            ]}
          />
        </>
      }
      footnote={
        query.data &&
        (query.data.mode === "avg"
          ? `Snitt per år för ${yearRuns(query.data.reference_years)}; ${meta.gap_years.year_from}–${meta.gap_years.year_to} ingår inte. Veckorna är sjudagarsperioder från 1 januari, inte kalenderveckor.`
          : query.data.year === meta.current_year
            ? `${query.data.year} pågår; data till och med ${formatDate(meta.build.ringing_max_date)}. Veckorna är sjudagarsperioder från 1 januari.`
            : "Veckorna är sjudagarsperioder från 1 januari, inte kalenderveckor.")
      }
    >
      {(data) => {
        const weeks = weekSlice(data.weeks, w0, w1);
        const nSelected = data.rows.filter((r) => r.selected).length;
        return (
          <div className={styles.calendar}>
            <div className={styles.calendarInner}>
              {data.rows.map((r, ri) => {
                const values = weekSlice(r.values, w0, w1);
                const scaled = weekSlice(r.scaled, w0, w1);
                const peak = values.indexOf(Math.max(...values));
                const peakWeek = weeks[peak];
                const c = colorOf(r.species);
                return (
                  <div
                    key={r.species}
                    className={`${styles.calRow} ${ri === nSelected - 1 && nSelected < data.rows.length ? styles.calSplit : ""}`}
                  >
                    <span className={styles.calName} style={{ fontWeight: r.selected ? 700 : 500 }}>
                      <span className={styles.calDot} style={{ background: c ?? "transparent" }} aria-hidden="true" />
                      <span className={styles.ellipsis}>{r.name}</span>
                      {peakWeek && values[peak]! > 0 && (
                        <span className="visually-hidden">, flest fåglar {formatWeek(peakWeek)}</span>
                      )}
                    </span>
                    <div className={styles.heat} style={{ gridTemplateColumns: `repeat(${weeks.length}, minmax(0, 1fr))` }} aria-hidden="true">
                      {weeks.map((w, i) => {
                        const v = values[i] ?? 0;
                        const t = scale === "rel" ? (scaled[i] ?? 0) : data.max_value > 0 ? Math.sqrt(v / data.max_value) : 0;
                        return (
                          <span
                            key={w.week}
                            className={`${styles.cell} ${w.week === LAST_SPRING_WEEK + 1 ? styles.seasonStart : ""}`}
                            style={{ background: heatColor(t) }}
                            onPointerMove={(e) =>
                              setTip({
                                x: e.clientX,
                                y: e.clientY,
                                lines: (
                                  <>
                                    <strong>{r.name}</strong>
                                    <br />
                                    {formatWeek(w)}
                                    <br />
                                    <span className="mono">
                                      {formatNumber(v, data.mode === "avg" && v < 10 ? 1 : 0)} {data.mode === "avg" ? "i snitt" : "ringmärkta"}
                                    </span>
                                  </>
                                ),
                              })
                            }
                            onPointerLeave={() => setTip(null)}
                          />
                        );
                      })}
                    </div>
                    <span className={`mono ${styles.calTotal}`}>{formatInt(r.total)}</span>
                  </div>
                );
              })}
              <div className={styles.calRow} aria-hidden="true">
                <span />
                <div className={styles.monthAxis}>
                  {monthTicks(d0, d0 + span - 1).map((m) => (
                    <span key={m.doy} className="mono" style={{ left: `${((m.doy - d0) / span) * 100}%` }}>
                      {m.label}
                    </span>
                  ))}
                </div>
                <span className={`mono ${styles.calTotalHead}`}>{data.mode === "avg" ? "per år" : "totalt"}</span>
              </div>
            </div>
            <Tooltip tip={tip} />
            <div className={styles.rampLegend}>
              <span>{scale === "rel" ? "Få" : "0"}</span>
              <span className={styles.ramp} style={{ background: `linear-gradient(90deg, ${heatRamp.join(", ")})` }} aria-hidden="true" />
              <span>{scale === "rel" ? "Toppvecka per säsong" : `${formatNumber(data.max_value, 0)} per vecka`}</span>
            </div>
          </div>
        );
      }}
    </ChartCard>
  );
}
