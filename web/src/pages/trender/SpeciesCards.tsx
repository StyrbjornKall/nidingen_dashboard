import { TrendBadge } from "../../components/TrendBadge";
import type { SpeciesInfo } from "../../lib/api";
import styles from "./Trender.module.css";
import { useTrendSeries, type Trends } from "./trendSeries";
import { TrendPlot } from "./TrendPlot";
import { ciText, formatSlope } from "./trends";

/** "Art för art": one small card per selected species, each on its own scale. */
export function SpeciesCards({
  data, colorOf, infoOf,
}: {
  data: Trends | undefined;
  colorOf: (code: string) => string | undefined;
  infoOf: (code: string) => SpeciesInfo | undefined;
}) {
  const { years, gapYears, series } = useTrendSeries(data, colorOf);
  if (!data) return null;
  return (
    <div className={styles.cards}>
      {series.map((s) => {
        const info = infoOf(s.code);
        return (
          <article key={s.code} className={styles.card} style={{ borderTopColor: s.color }}>
            <div className={styles.cardHead}>
              <h3 className={styles.cardTitle}>{info?.name ?? s.code}</h3>
              {info?.name_sci && <span className={styles.sci}>{info.name_sci}</span>}
            </div>
            <div>
              <div className={styles.cardSub}>
                <span>Antal ringmärkta per år</span>
                {s.fit && <span className="mono">{formatSlope(s.fit.slope)} fåglar/år</span>}
              </div>
              <TrendPlot
                className={styles.miniPlot}
                height={130}
                years={years}
                gapYears={gapYears}
                series={[s]}
                mode="bars"
                animationKey={`${data.season}|${data.period.id}|${s.code}`}
              />
            </div>
            {s.fit ? (
              <div className={styles.cardFoot}>
                <TrendBadge verdict={s.fit.verdict} prefix="Antal: " />
                <span className={`mono ${styles.ci}`}>{ciText(s.fit.ci_low, s.fit.ci_high)}</span>
              </div>
            ) : (
              <p className={styles.ci}>Inga fåglar under perioden, ingen trend.</p>
            )}
          </article>
        );
      })}
    </div>
  );
}
