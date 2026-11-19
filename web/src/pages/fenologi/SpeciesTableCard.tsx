import { useState, type PointerEvent } from "react";
import { ChartCard } from "../../components/ChartCard";
import { Tooltip, type TipState } from "../../components/charts/Plot";
import { verdictLabel } from "../../components/TrendBadge";
import { useApi, type PeriodInfo, type Schemas } from "../../lib/api";
import { formatDoy, formatInt, formatTrend } from "../../lib/format";
import { yearRuns } from "../../lib/years";
import styles from "./Fenologi.module.css";

/** Rows shown before "Visa alla". */
const FIRST_ROWS = 10;

type Row = Schemas["TableRow"];

const TREND_COLOR = { up: "var(--color-badge-up-fg)", down: "var(--color-badge-down-fg)", none: "var(--color-text-secondary)" } as const;

/** "Arterna": latest year, recent average, trend, medians and a sparkline per species. */
export function SpeciesTableCard({
  period, selected, colorOf, onToggle, max,
}: {
  period: PeriodInfo;
  selected: readonly string[];
  colorOf: (c: string) => string | undefined;
  onToggle: (c: string) => void;
  max: number;
}) {
  const [all, setAll] = useState(false);
  const query = useApi<Schemas["PhenologyTable"]>("/api/phenology/table", { period: period.id, species: selected }, { keepPrevious: true });
  const full = selected.length >= max;

  return (
    <ChartCard
      title="Arterna"
      subtitle="Klicka på en art för att lägga till den i jämförelsen. Valda arter ligger överst, därefter sorteras övriga efter antal ringmärkta under perioden."
      query={query}
      skeleton="rows"
      isEmpty={(d) => d.rows.length === 0}
      footnote={query.data && tableNote(query.data)}
    >
      {(data) => {
        const rows = all ? data.rows : data.rows.slice(0, Math.max(FIRST_ROWS, data.rows.filter((r) => r.selected).length));
        return (
          <div className={styles.tableBody}>
            <div className={styles.tableScroll}>
              <table className={styles.table}>
                <thead>
                  <tr>
                    <th scope="col">Art</th>
                    <th scope="col" className={styles.num}>{data.latest_year}</th>
                    <th scope="col" className={styles.num}>Snitt {yearRuns(data.avg_years)}</th>
                    <th scope="col" className={styles.num}>Trend, fåglar/år</th>
                    <th scope="col" className={styles.num}>Median vår</th>
                    <th scope="col" className={styles.num}>Median höst</th>
                    <th scope="col">{data.period.label}</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r) => {
                    const c = colorOf(r.species);
                    const on = c !== undefined;
                    const disabled = !on && full;
                    return (
                      <tr key={r.species} data-selected={on}>
                        <th scope="row">
                          <button
                            type="button"
                            className={styles.rowButton}
                            aria-pressed={on}
                            disabled={disabled}
                            title={disabled ? `Högst ${max} arter kan jämföras. Ta bort en art först.` : undefined}
                            onClick={() => onToggle(r.species)}
                          >
                            <span className={styles.calDot} style={{ background: c ?? "transparent", boxShadow: `inset 0 0 0 1px ${c ?? "#9AA89F"}` }} aria-hidden="true" />
                            <span className={styles.rowNames}>
                              <span className={styles.rowName}>{r.name}</span>
                              {r.name_sci && <span className={styles.rowSci}>{r.name_sci}</span>}
                            </span>
                          </button>
                        </th>
                        <td className={`mono ${styles.num}`}>{formatInt(r.latest_n)}</td>
                        <td className={`mono ${styles.num} ${styles.muted}`}>{r.avg_n == null ? "–" : formatInt(r.avg_n)}</td>
                        <td className={`mono ${styles.num}`} style={{ color: TREND_COLOR[r.verdict], fontWeight: 500 }}>
                          {r.trend_slope == null ? "–" : formatTrend(r.trend_slope, true)}
                          {r.trend_slope != null && <span className="visually-hidden">, {verdictLabel(r.verdict).toLowerCase()}</span>}
                        </td>
                        <td className={`mono ${styles.num}`}>{median(r, "var")}</td>
                        <td className={`mono ${styles.num}`}>{median(r, "host")}</td>
                        <td>
                          <Spark values={r.spark} years={data.spark_years} gap={data.spark_gap} color={c ?? "var(--color-chart-mean)"} />
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {data.rows.length > FIRST_ROWS && (
              <button type="button" className={styles.more} onClick={() => setAll((v) => !v)}>
                {all ? "Visa färre" : `Visa alla ${formatInt(data.rows.length)} arter`}
              </button>
            )}
          </div>
        );
      }}
    </ChartCard>
  );
}

function tableNote(d: Schemas["PhenologyTable"]): string {
  const gapYears = d.spark_years.filter((_, i) => d.spark_gap[i]);
  const without = gapYears.length ? `, utan ${yearRuns(gapYears)}` : "";
  return (
    `Snitt: medel per år ${yearRuns(d.avg_years)}. ` +
    `Trend: förändring i antal ringmärkta per år under ${d.period.label} (helår${without}), färgad när den är säker (95 % konfidensintervall). ` +
    "Median: medel av årens mediandagar."
  );
}

function median(r: Row, season: "var" | "host"): string {
  if (r.single_season && r.single_season_season !== season) return "–";
  const doy = season === "var" ? r.median_var_doy : r.median_host_doy;
  return doy == null ? "–" : formatDoy(doy);
}

/** Counts per year; partly staffed years drawn faint. Hover reads one year. */
function Spark({
  values, years, gap, color: c,
}: {
  values: readonly number[];
  years: readonly number[];
  gap: readonly boolean[];
  color: string;
}) {
  const [at, setAt] = useState(-1);
  const [tip, setTip] = useState<TipState>(null);
  const max = Math.max(1, ...values);
  const n = values.length;
  const xPct = (i: number) => (i / Math.max(1, n - 1)) * 100;
  const pt = (v: number, i: number) => `${xPct(i).toFixed(1)},${(30 - (v / max) * 28).toFixed(1)}`;
  // Split into runs of staffed / partly staffed years, sharing the joining point.
  const runs: { gap: boolean; pts: string[] }[] = [];
  values.forEach((v, i) => {
    const g = gap[i] ?? false;
    const last = runs[runs.length - 1];
    if (last && last.gap === g) last.pts.push(pt(v, i));
    else runs.push({ gap: g, pts: last ? [last.pts[last.pts.length - 1]!, pt(v, i)] : [pt(v, i)] });
  });
  const move = (e: PointerEvent<HTMLDivElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    const i = Math.max(0, Math.min(n - 1, Math.round(((e.clientX - r.left) / r.width) * (n - 1))));
    setAt(i);
    setTip({
      x: e.clientX,
      y: e.clientY,
      lines: (
        <>
          <strong>{years[i]}</strong>: <span className="mono">{formatInt(values[i] ?? 0)}</span>
          {gap[i] && (
            <>
              <br />
              delvis bemannad
            </>
          )}
        </>
      ),
    });
  };
  const leave = () => {
    setAt(-1);
    setTip(null);
  };
  return (
    <div className={styles.sparkBox} onPointerMove={move} onPointerLeave={leave}>
      <svg className={styles.spark} viewBox="0 0 100 30" preserveAspectRatio="none" aria-hidden="true">
        {runs.map((r, k) => (
          <polyline key={k} points={r.pts.join(" ")} fill="none" stroke={c} strokeOpacity={r.gap ? 0.3 : 1} strokeWidth={1.6} vectorEffect="non-scaling-stroke" />
        ))}
      </svg>
      {at >= 0 && (
        <span
          className={styles.sparkDot}
          style={{ left: `${xPct(at)}%`, top: `${(1 - ((values[at] ?? 0) / max) * 28 / 30) * 100}%`, background: c }}
          aria-hidden="true"
        />
      )}
      <Tooltip tip={tip} />
    </div>
  );
}
