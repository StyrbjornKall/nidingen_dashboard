import { useState } from "react";
import { ChartCard, type CardQuery } from "../../components/ChartCard";
import { TrendBadge } from "../../components/TrendBadge";
import { formatInt } from "../../lib/format";
import styles from "./Trender.module.css";
import { TrendPlot } from "./TrendPlot";
import { useTrendSeries, type Trends } from "./trendSeries";
import { ciText, formatSlope } from "./trends";

export function ComparisonCard({
  query, colorOf, nameOf, caption,
}: {
  query: CardQuery<Trends>;
  colorOf: (code: string) => string | undefined;
  nameOf: (code: string) => string;
  caption: string | undefined;
}) {
  const [hover, setHover] = useState(-1);
  const { years, gapYears, series } = useTrendSeries(query.data, colorOf);
  const year = hover >= 0 ? years[hover] : undefined;
  const readout =
    year !== undefined
      ? `${year}${gapYears.has(year) ? " (delvis bemannad)" : ""} · ${series.map((s) => `${nameOf(s.code)} ${formatInt(s.values[hover] ?? 0)}`).join(" · ")}`
      : "Håll muspekaren över diagrammet";

  return (
    <ChartCard
      title="Antal ringmärkta per år, valda arter"
      subtitle="Faktiskt antal fåglar per år, alla arter på samma skala. Den heldragna linjen är den anpassade trenden i fåglar per år och bandet visar hur säker den är (95 % konfidensintervall)."
      actions={<span className={`mono ${styles.readout}`}>{query.data ? readout : ""}</span>}
      query={query}
      isEmpty={(d) => d.species.every((s) => s.years.every((p) => p.n === 0))}
      emptyMessage="Inga fåglar av de valda arterna ringmärktes under perioden och säsongen."
      footnote={caption}
    >
      {(data) => (
        <div className={styles.comparison}>
          <TrendPlot
            className={styles.bigPlot}
            height={360}
            years={years}
            gapYears={gapYears}
            series={series}
            mode="lines"
            hover={hover}
            onHover={setHover}
            valueText={readout}
            animationKey={`${data.season}|${data.period.id}|${series.map((s) => s.code).join(",")}`}
          />
          <ul className={styles.legend}>
            {series.map((s) => (
              <li key={s.code} className={styles.legendItem}>
                <span className={styles.legendName}>
                  <span className={styles.legendLine} style={{ background: s.color }} aria-hidden="true" />
                  {nameOf(s.code)}
                </span>
                {s.fit ? (
                  <>
                    <span className={styles.legendVerdict}>
                      <TrendBadge verdict={s.fit.verdict} />
                      <span className="mono">{formatSlope(s.fit.slope)} fåglar per år</span>
                    </span>
                    <span className={`mono ${styles.ci}`}>{ciText(s.fit.ci_low, s.fit.ci_high)}</span>
                  </>
                ) : (
                  <span className={styles.ci}>Inga fåglar under perioden, ingen trend.</span>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}
    </ChartCard>
  );
}
