import { useState, type CSSProperties } from "react";
import { ChartCard } from "../../components/ChartCard";
import { Crosshair, Scrubber } from "../../components/charts/Plot";
import { yAxis } from "../../components/charts/scale";
import { useApi, type Meta, type PeriodInfo, type Schemas } from "../../lib/api";
import { formatDoy, formatInt, formatNumber } from "../../lib/format";
import { color } from "../../lib/tokens";
import { yearRuns } from "../../lib/years";
import styles from "./Fenologi.module.css";
import { monthTicks, weekEnd, weekStart } from "./phenology";

type Season = "var" | "host";
type RidgeSeason = NonNullable<Schemas["RidgeSpecies"]["var"]>;

const SEASON_WORD: Record<Season, string> = { var: "våren", host: "hösten" };

/**
 * "När passerar arterna i detalj?": mean daily catch per season for each
 * selected species, raw counts on its own y axis (shared by its two
 * seasons), the latest year dashed and the mean median passage day.
 */
export function RidgesCard({
  meta, period, species, colorOf, nameOf,
}: {
  meta: Meta;
  period: PeriodInfo;
  species: readonly string[];
  colorOf: (c: string) => string | undefined;
  nameOf: (c: string) => string;
}) {
  const query = useApi<Schemas["Ridges"]>("/api/phenology/ridges", { species, period: period.id }, { keepPrevious: true });
  // Days drawn per season: the season's days (from the API) cut to the
  // station's active weeks, so the panels meet without a gap.
  const sd = query.data?.season_days;
  const win: Record<Season, [number, number]> = {
    var: [Math.max(sd?.var?.doy_from ?? 1, weekStart(meta.active_weeks.week_from)), sd?.var?.doy_to ?? 181],
    host: [sd?.host?.doy_from ?? 182, Math.min(sd?.host?.doy_to ?? 365, weekEnd(meta.active_weeks.week_to))],
  };
  // Panel widths follow the number of days in each season (CSS var, so
  // the phone layout can drop the label column).
  const cols = { "--panels": `${win.var[1] - win.var[0] + 1}fr ${win.host[1] - win.host[0] + 1}fr` } as CSSProperties;

  return (
    <ChartCard
      title="När passerar arterna i detalj?"
      subtitle={
        query.data ? (
          <>
            Säsongens fångst per dag, snitt för {yearRuns(query.data.reference_years)}. Det lodräta strecket är mediandagen
            för flytt, samma median som i tabellen nedan. Flytten är uppdelad i två säsonger: vår (januari–juni) och höst
            (juli–december). Arter som nästan bara ringmärks under en säsong har en enda median. Diagrammen visar råa antal
            ringmärkta per dag. Varje art har sin egen y-axel, gemensam för vår och höst, så höjderna går inte att jämföra
            mellan arter.
          </>
        ) : undefined
      }
      actions={
        query.data && (
          <ul className={styles.ridgeLegend}>
            <li>
              <span className={styles.swArea} aria-hidden="true" />
              Snitt
            </li>
            <li>
              <span className={styles.swDashed} aria-hidden="true" />
              {query.data.latest_year}
            </li>
            <li>
              <span className={styles.swMedian} aria-hidden="true" />
              Median
            </li>
          </ul>
        )
      }
      query={query}
      footnote={`En median räknas bara för år med minst ${meta.median_min_birds} ringmärkta fåglar av arten under säsongen.`}
    >
      {(data) => (
        <div className={styles.ridgesScroll}>
          <div className={styles.ridges}>
            <div className={styles.ridgeRow} style={cols}>
              <span className={styles.labelCell} />
              <span />
              <span className={styles.seasonHead}>Vår</span>
              <span className={styles.seasonHead}>Höst</span>
            </div>
            {data.species.map((s, k) => {
              const c = colorOf(s.species) ?? color.chart_mean;
              const y = yAxis(Math.max(1, s.y_max) * 1.05, 2);
              return (
                <div key={s.species} className={`${styles.ridgeRow} ${styles.ridge}`} style={{ ...cols, animationDelay: `${k * 90}ms` }}>
                  <div className={styles.ridgeLabel}>
                    <div className={styles.ridgeName}>
                      <span className={styles.calDot} style={{ background: c }} aria-hidden="true" />
                      {nameOf(s.species)}
                    </div>
                    {(["var", "host"] as const).map((se) => (
                      <div key={se} className={`mono ${styles.ridgeMedian}`}>
                        median {se === "var" ? "vår" : "höst"}: {medianText(s[se])}
                      </div>
                    ))}
                  </div>
                  <div className={styles.ridgeAxis} aria-hidden="true">
                    {y.ticks.map((t) => (
                      <span key={t} className="mono" style={{ top: `${100 - (t / y.top) * 100}%` }}>
                        {formatInt(t)}
                      </span>
                    ))}
                  </div>
                  {(["var", "host"] as const).map((se) => (
                    <RidgePanel key={se} season={se} name={nameOf(s.species)} data={s[se]} window={win[se]} top={y.top} ticks={y.ticks} color={c} latestYear={data.latest_year} />
                  ))}
                </div>
              );
            })}
            <div className={styles.ridgeRow} style={cols} aria-hidden="true">
              <span className={styles.labelCell} />
              <span />
              {(["var", "host"] as const).map((se) => {
                const [d0, d1] = win[se];
                return (
                  <div key={se} className={styles.monthAxis}>
                    {monthTicks(d0, d1).map((m) => (
                      <span key={m.doy} className="mono" style={{ left: `${((m.doy - d0) / (d1 - d0 + 1)) * 100}%` }}>
                        {m.label}
                      </span>
                    ))}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </ChartCard>
  );
}

function medianText(s: RidgeSeason | null | undefined): string {
  if (!s) return "–";
  return s.median_doy == null ? "för få fåglar" : formatDoy(s.median_doy);
}

function RidgePanel({
  season, name, data, window: [d0, d1], top, ticks, color: c, latestYear,
}: {
  season: Season;
  name: string;
  data: RidgeSeason | null | undefined;
  window: [number, number];
  top: number;
  ticks: number[];
  color: string;
  latestYear: number;
}) {
  const [hover, setHover] = useState(-1);
  const grid = ticks.filter((t) => t > 0);
  if (!data) {
    return (
      <div className={styles.panel}>
        <div className={styles.panelEmpty}>Ringmärks nästan bara på {season === "var" ? "hösten" : "våren"}</div>
      </div>
    );
  }
  const n = d1 - d0 + 1;
  const days = Array.from({ length: n }, (_, i) => d0 + i);
  const meanAt = (doy: number) => data.mean[doy - data.doy_from] ?? 0;
  const latestAt = (doy: number) => data.latest[doy - data.doy_from] ?? null;
  const X = (i: number) => (((i + 0.5) / n) * 100).toFixed(2);
  const Y = (v: number) => (100 - (v / top) * 100).toFixed(2);
  const area = `M0 100L${days.map((d, i) => `${X(i)} ${Y(meanAt(d))}`).join("L")}L100 100Z`;
  // The running year stops at the last ringing date.
  const latestPts = days.flatMap((d, i) => {
    const v = latestAt(d);
    return v === null ? [] : [`${X(i)} ${Y(v)}`];
  });
  const latest = latestPts.length ? `M${latestPts.join("L")}` : "";
  const med = data.median_doy == null ? null : Math.max(0, Math.min(100, ((data.median_doy - d0 + 0.5) / n) * 100));

  const hd = hover >= 0 && hover < n ? d0 + hover : -1;
  const readout =
    hd > 0
      ? [
          formatDoy(hd),
          `snitt ${formatNumber(meanAt(hd), meanAt(hd) < 1 ? 2 : meanAt(hd) < 10 ? 1 : 0)}`,
          latestAt(hd) !== null ? `${latestYear}: ${formatInt(latestAt(hd)!)}` : null,
        ]
          .filter(Boolean)
          .join(" · ")
      : "";
  const hoverLeft = hover >= 0 ? ((hover + 0.5) / n) * 100 : 0;

  return (
    <div className={styles.panel} data-reading={hd > 0}>
      {grid.map((t) => (
        <div key={t} className={styles.panelGrid} style={{ top: `${100 - (t / top) * 100}%` }} />
      ))}
      <svg className={styles.panelSvg} viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true">
        <path d={area} fill={c} fillOpacity={0.32} stroke={c} strokeWidth={2} vectorEffect="non-scaling-stroke" />
        {latest && <path d={latest} fill="none" stroke="var(--color-ink)" strokeWidth={1.6} strokeDasharray="4 3" vectorEffect="non-scaling-stroke" />}
      </svg>
      {med !== null && (
        <>
          <span className={styles.medianLine} style={{ left: `${med}%` }} />
          <span className={`mono ${styles.medianLabel} ${med > 80 ? styles.medianLabelLeft : ""}`} style={{ left: `${med}%` }}>
            {formatDoy(data.median_doy!)}
          </span>
        </>
      )}
      {hd > 0 && (
        <>
          <Crosshair left={hoverLeft} />
          <span className={`mono ${styles.ridgeReadout} ${hoverLeft > 55 ? styles.ridgeReadoutLeft : ""}`} style={{ left: `${hoverLeft}%` }}>
            {readout}
          </span>
        </>
      )}
      <Scrubber
        n={n}
        hover={hover}
        onHover={setHover}
        valueText={readout || `Median ${medianText(data)}`}
        pageStep={7}
        label={`${name}, ${SEASON_WORD[season]}: läs av dag för dag`}
      />
    </div>
  );
}
