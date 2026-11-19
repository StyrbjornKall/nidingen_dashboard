import { useState } from "react";
import { ChartCard, type CardQuery } from "../../components/ChartCard";
import { Tooltip, type TipState } from "../../components/charts/Plot";
import type { Schemas } from "../../lib/api";
import { formatInt, formatNumber } from "../../lib/format";
import { color, heatRamp } from "../../lib/tokens";
import styles from "./Vader.module.css";
import { COMPASS, topSector, wedgePath, wedgeRadius } from "./weather";

type Weather = Schemas["Weather"];

/** Fewest days a direction needs to be called the best one. */
export const MIN_DAYS = 3;
const GRID = [55, 110, 160];

/**
 * "Fångst per vindriktning": the mean catch per day, grouped by where the
 * wind came from, over the days the station was staffed. Wedges are on a
 * square-root scale; the best direction is coloured.
 */
export function RoseCard({ query, speciesName }: { query: CardQuery<Weather>; speciesName: string }) {
  const [tip, setTip] = useState<TipState>(null);
  return (
    <ChartCard
      title="Fångst per vindriktning"
      query={query}
      isEmpty={(d) => d.wind_rose.every((s) => s.days === 0)}
      emptyMessage="Inga dagar med både fångst och vindriktning i den här säsongen."
      footnote={`Medelvärde över de dagar stationen märkte eller kontrollerade fåglar och vindriktningen är känd. Vindriktningen är dagens medelvind kl. 03–12. En riktning med färre än ${MIN_DAYS} dagar kan inte bli den bästa. Bågarna växer med kvadratroten av medelvärdet.`}
    >
      {(data) => {
        const rose = data.wind_rose;
        const top = topSector(rose, MIN_DAYS);
        const max = Math.max(1e-9, ...rose.map((s) => s.mean_catch ?? 0));
        const insight = top >= 0
          ? `Flest ${speciesName === "Alla arter" ? "fåglar" : speciesName.toLowerCase()} per dag vid vind från ${rose[top]!.label} (${formatNumber(rose[top]!.mean_catch!, 1)} i snitt, ${formatInt(rose[top]!.days)} dagar).`
          : `Ingen riktning har minst ${MIN_DAYS} dagar att jämföra.`;
        return (
          <div className={styles.rose}>
            <p className={styles.roseLead}>Genomsnittlig fångst per dag, grupperat efter varifrån det blåste. {insight}</p>
            <div className={styles.roseBox}>
              <svg viewBox="0 0 400 400" role="img" aria-label="Vindros med genomsnittlig fångst per vindriktning" className={styles.roseSvg}>
                <path
                  d={GRID.map((r) => `M${200 - r} 200a${r} ${r} 0 1 0 ${2 * r} 0a${r} ${r} 0 1 0 ${-2 * r} 0`).join("")}
                  fill="none"
                  stroke={color.grid}
                  strokeWidth={1}
                />
                {rose.map((s, i) =>
                  s.mean_catch == null || s.mean_catch <= 0 ? null : (
                    <path
                      key={s.label}
                      d={wedgePath(i, wedgeRadius(s.mean_catch, max))}
                      fill={i === top ? color.trend : heatRamp[1]}
                      fillOpacity={s.days >= MIN_DAYS ? 1 : 0.45}
                      stroke={color.surface}
                      strokeWidth={2}
                      onPointerMove={(e) =>
                        setTip({
                          x: e.clientX,
                          y: e.clientY,
                          lines: (
                            <>
                              <strong>Vind från {s.label}</strong>
                              <br />
                              <span className="mono">{formatNumber(s.mean_catch!, 1)} per dag · {formatInt(s.days)} dagar</span>
                            </>
                          ),
                        })
                      }
                      onPointerLeave={() => setTip(null)}
                    />
                  ),
                )}
              </svg>
              {COMPASS.map((label, i) => {
                const a = (i * 45 * Math.PI) / 180;
                return (
                  <span key={label} className={styles.roseLabel} style={{ left: `${(200 + 182 * Math.sin(a)) / 4}%`, top: `${(200 - 182 * Math.cos(a)) / 4}%` }} aria-hidden="true">
                    {label}
                  </span>
                );
              })}
            </div>
            <ul className="visually-hidden">
              {rose.map((s) => (
                <li key={s.label}>
                  Vind från {s.label}: {s.mean_catch == null ? "inga dagar" : `${formatNumber(s.mean_catch, 1)} per dag, ${formatInt(s.days)} dagar`}
                </li>
              ))}
            </ul>
            <Tooltip tip={tip} />
          </div>
        );
      }}
    </ChartCard>
  );
}
