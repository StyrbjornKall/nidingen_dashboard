import { ChartCard, type CardQuery } from "../../components/ChartCard";
import type { Schemas } from "../../lib/api";
import { formatDate, formatInt, formatNumber } from "../../lib/format";
import styles from "./Vader.module.css";
import { dirName } from "./weather";

type Weather = Schemas["Weather"];

/** "Säsongens bästa fångstdagar": the six days with the most birds and their weather. */
export function BestDaysCard({ query }: { query: CardQuery<Weather> }) {
  return (
    <ChartCard
      title="Säsongens bästa fångstdagar"
      query={query}
      skeleton="rows"
      isEmpty={(d) => d.best_days.length === 0}
      emptyMessage="Inga fåglar ringmärktes i den här säsongen."
      footnote="Väder som medel eller summa för kl. 03–12 samma dag."
    >
      {(data) => (
        <div className={styles.scrollX}>
          <table className={styles.best}>
            <thead>
              <tr>
                <th scope="col">Datum</th>
                <th scope="col" className={styles.right}>Fångst</th>
                <th scope="col" className={styles.right}>Vind, m/s</th>
                <th scope="col" className={styles.right}>Temp</th>
                <th scope="col" className={styles.right}>Tryck, hPa</th>
              </tr>
            </thead>
            <tbody>
              {data.best_days.map((d) => (
                <tr key={d.date}>
                  <th scope="row">{formatDate(d.date, true)}</th>
                  <td className={`mono ${styles.right}`}>{formatInt(d.catch)}</td>
                  <td className={`mono ${styles.right}`}>{d.wind_speed == null ? "–" : `${d.wind_dir == null ? "" : `${dirName(d.wind_dir)} `}${formatNumber(d.wind_speed, 1)}`}</td>
                  <td className={`mono ${styles.right}`}>{d.temp_mean == null ? "–" : `${formatNumber(d.temp_mean, 1)}°`}</td>
                  <td className={`mono ${styles.right}`}>{d.pressure == null ? "–" : formatInt(Math.round(d.pressure))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </ChartCard>
  );
}
