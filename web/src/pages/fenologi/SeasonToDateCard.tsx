import { useState } from "react";
import { ChartCard } from "../../components/ChartCard";
import { chartStyles, Crosshair, PlotSvg, Scrubber, XLabels, YTicks } from "../../components/charts/Plot";
import { yAxis } from "../../components/charts/scale";
import { useApi, type Meta, type Schemas } from "../../lib/api";
import { formatDoy, formatInt } from "../../lib/format";
import { color } from "../../lib/tokens";
import { yearRuns } from "../../lib/years";
import styles from "./Fenologi.module.css";
import { monthTicks, weekEnd, weekStart } from "./phenology";

const W = 1000;
const H = 300;

/**
 * "Säsongen {år} mot alla tidigare år": the cumulative count of new
 * ringings this year against the mean, ±1 SD band, top and worst year of
 * the earlier complete years (gap years left out), for one species at a time.
 */
export function SeasonToDateCard({
  meta, species, colorOf, nameOf,
}: {
  meta: Meta;
  species: readonly string[];
  colorOf: (c: string) => string | undefined;
  nameOf: (c: string) => string;
}) {
  const [picked, setPicked] = useState<string | null>(null);
  const code = picked && species.includes(picked) ? picked : species[0];
  const [hover, setHover] = useState(-1);
  const query = useApi<Schemas["SeasonToDate"]>("/api/phenology/season-to-date", { species: code }, { enabled: !!code, keepPrevious: true });
  const year = meta.current_year;

  return (
    <ChartCard
      title={`Säsongen ${year} mot alla tidigare år`}
      subtitle="Hur många fåglar som ringmärkts hittills under året, räknat upp dag för dag, jämfört med tidigare år vid samma datum."
      query={query}
      footnote={
        query.data &&
        `Medel, standardavvikelse, toppår och sämsta år räknas på hela säsonger ${yearRuns(query.data.reference_years)}; ${meta.gap_years.year_from}–${meta.gap_years.year_to} ingår inte.`
      }
    >
      {(data) => {
        const d0 = weekStart(meta.active_weeks.week_from);
        const d1 = weekEnd(meta.active_weeks.week_to);
        const n = d1 - d0 + 1;
        const days = Array.from({ length: n }, (_, i) => d0 + i);
        const at = (list: readonly (number | null)[], doy: number) => list[doy - 1];
        const top = Math.max(
          1,
          ...days.map((d) => Math.max(at(data.sd_high, d) ?? 0, at(data.top?.cumulative ?? [], d) ?? 0, at(data.current, d) ?? 0)),
        );
        const y = yAxis(top * 1.04);
        const X = (i: number) => ((i + 0.5) / n) * W;
        const Y = (v: number) => H - (v / y.top) * H;
        const path = (list: readonly (number | null)[]) => {
          const pts = days.flatMap((d, i) => {
            const v = at(list, d);
            return v == null ? [] : [`${X(i).toFixed(1)} ${Y(v).toFixed(1)}`];
          });
          return pts.length ? `M${pts.join("L")}` : "";
        };
        const band = `M${days.map((d, i) => `${X(i).toFixed(1)} ${Y(at(data.sd_high, d) ?? 0).toFixed(1)}`).join("L")}L${[...days]
          .reverse()
          .map((d) => `${X(d - d0).toFixed(1)} ${Y(at(data.sd_low, d) ?? 0).toFixed(1)}`)
          .join("L")}Z`;

        // A running year ends in a blinking dot at its latest reported value.
        const live = data.year === year;
        const through = data.data_through_doy;
        const latestValue = at(data.current, through);
        const showDot = live && latestValue != null && through >= d0 && through <= d1;
        const dotLeft = ((through - d0 + 0.5) / n) * 100;
        const done = !live;
        const s = data.summary;
        const up = s.diff >= 0;
        const name = nameOf(data.species);
        const refText = yearRuns(data.reference_years);

        const hd = hover >= 0 && hover < n ? d0 + hover : -1;
        const readout =
          hd > 0
            ? [
                formatDoy(hd),
                at(data.current, hd) != null ? `${data.year}: ${formatInt(at(data.current, hd)!)}` : null,
                `medel ${formatInt(at(data.mean, hd) ?? 0)}`,
                data.top ? `toppår ${data.top.year}: ${formatInt(at(data.top.cumulative, hd) ?? 0)}` : null,
                data.worst ? `sämsta år ${data.worst.year}: ${formatInt(at(data.worst.cumulative, hd) ?? 0)}` : null,
              ]
                .filter(Boolean)
                .join(" · ")
            : "Håll muspekaren över diagrammet för att läsa av ett datum";

        return (
          <div className={styles.std}>
            {species.length > 1 && (
              <div className={styles.tabs} role="group" aria-label="Art">
                {species.map((c) => (
                  <button key={c} type="button" className={styles.tab} aria-pressed={c === code} onClick={() => setPicked(c)}>
                    <span className={styles.calDot} style={{ background: colorOf(c) ?? color.chart_mean }} aria-hidden="true" />
                    {nameOf(c)}
                  </button>
                ))}
              </div>
            )}

            <div className={styles.stdSummary}>
              <span className={`mono ${styles.stdNow}`}>{formatInt(s.current_total)}</span>
              <span className={styles.stdNowLabel}>
                {name}, ringmärkta {data.year}
                {done ? " (avslutad)" : ` till och med ${formatDoy(through)}`}
              </span>
              <span className={styles.stdDiff} data-up={up}>
                {up ? "+" : "−"}
                {formatInt(Math.abs(s.diff))} fåglar mot ett normalt år{done ? "" : " vid samma datum"}
              </span>
              <dl className={styles.stdTiles}>
                <div>
                  <dt style={{ color: "var(--color-badge-up-fg)" }}>Toppår</dt>
                  <dd className="mono">{data.top?.year ?? "–"}</dd>
                  <dd className="mono">{data.top ? `${formatInt(data.top.total)} fåglar` : ""}</dd>
                </div>
                <div>
                  <dt style={{ color: "#5E4C8A" }}>Sämsta år</dt>
                  <dd className="mono">{data.worst?.year ?? "–"}</dd>
                  <dd className="mono">{data.worst ? `${formatInt(data.worst.total)} fåglar` : ""}</dd>
                </div>
                <div>
                  <dt>Standardavvikelse</dt>
                  <dd className="mono">{s.sd_annual_total == null ? "–" : `±${formatInt(s.sd_annual_total)}`}</dd>
                  <dd className="mono">fåglar per år</dd>
                </div>
              </dl>
            </div>

            <ul className={styles.stdLegend}>
              <li><span className={styles.swCurrent} aria-hidden="true" />{data.year}</li>
              <li><span className={styles.swMean} aria-hidden="true" />Medel {refText}</li>
              <li><span className={styles.swBand} aria-hidden="true" />± 1 standardavvikelse</li>
              <li><span className={styles.swTop} aria-hidden="true" />Toppår</li>
              <li><span className={styles.swWorst} aria-hidden="true" />Sämsta år</li>
            </ul>

            <p className={`mono ${styles.stdReadout}`}>{readout}</p>

            <div className={styles.stdPlot}>
              <div className={chartStyles.plot} style={{ height: 320 }}>
                <YTicks ticks={y.ticks} top={y.top} format={formatInt} />
                <PlotSvg width={W} height={H} animationKey={data.species}>
                  <path d={band} fill={color.chart_band_fill} />
                  {data.top && <path d={path(data.top.cumulative)} fill="none" stroke={color.chart_top_year} strokeWidth={2} strokeLinecap="round" vectorEffect="non-scaling-stroke" />}
                  {data.worst && <path d={path(data.worst.cumulative)} fill="none" stroke={color.chart_worst_year} strokeWidth={2} strokeDasharray="1.5 3" strokeLinecap="round" vectorEffect="non-scaling-stroke" />}
                  <path d={path(data.mean)} fill="none" stroke={color.chart_mean} strokeWidth={2} strokeDasharray="5 4" vectorEffect="non-scaling-stroke" />
                  <path d={path(data.current)} fill="none" stroke={color.chart_current_year} strokeWidth={3} strokeLinecap="round" vectorEffect="non-scaling-stroke" />
                </PlotSvg>
                {showDot && (
                  <>
                    <span className={styles.liveDot} style={{ left: `${dotLeft}%`, top: `${(Y(latestValue) / H) * 100}%` }} aria-hidden="true" />
                    <span
                      className={`mono ${styles.liveLabel} ${dotLeft > 82 ? styles.liveLabelLeft : ""}`}
                      style={{ left: `${dotLeft}%`, top: `${(Y(latestValue) / H) * 100}%` }}
                    >
                      {formatInt(latestValue)}
                    </span>
                  </>
                )}
                {hover >= 0 && <Crosshair left={((hover + 0.5) / n) * 100} />}
                <Scrubber n={n} hover={hover} onHover={setHover} valueText={readout} pageStep={7} />
              </div>
              <XLabels labels={monthTicks(d0, d1).map((m) => ({ key: m.doy, label: m.label, left: ((m.doy - d0) / n) * 100 }))} />
            </div>
          </div>
        );
      }}
    </ChartCard>
  );
}
