import { Fragment, useState } from "react";
import { ChartCard, type CardQuery } from "../../components/ChartCard";
import { chartStyles, Crosshair, PlotSvg, Scrubber, useWidth } from "../../components/charts/Plot";
import type { Meta, Schemas } from "../../lib/api";
import { formatDate, formatInt, formatNumber, monthName } from "../../lib/format";
import { color } from "../../lib/tokens";
import styles from "./Vader.module.css";
import {
  areaPath, arrowRotation, arrowStep, barsPath, bestIndex, dirName, linePath, monthStarts, niceTop, rangeOf, VARS, type VarKey,
} from "./weather";

type Day = Schemas["WeatherDay"];
type Weather = Schemas["Weather"];

const W = 1000;
const H = 100;
/** Space left of the plots: the track label column plus the gap. */
const LABEL_COL = 128 + 14;

interface Track {
  key: "fangst" | VarKey;
  label: string;
  color: string;
  height: number;
  kind: "bars" | "line" | "area" | "arrows";
  values: (number | null)[];
  lo: number;
  hi: number;
  /** Text for a day, and the range line under the label. */
  value: (i: number) => string;
  range: string;
  /** True for days whose value comes from Vinga A. */
  vinga?: boolean[];
}

const f1 = (v: number | null | undefined, unit: string) => (v == null ? "–" : `${formatNumber(v, 1)} ${unit}`);
const f0 = (v: number | null | undefined, unit: string) => (v == null ? "–" : `${formatInt(Math.round(v))} ${unit}`);

/** Builds the tracks for the chosen variables: the catch first, then the weather in the chips' order. */
function buildTracks(days: readonly Day[], chosen: readonly VarKey[], hours: { from: number; to: number }): Track[] {
  const col = (k: VarKey) => VARS.find((v) => v.key === k)!.color;
  const label = (k: VarKey) => VARS.find((v) => v.key === k)!.label;
  const pick = (f: (d: Day) => number | null | undefined) => days.map((d) => f(d) ?? null);
  const catches = days.map((d) => d.catch);
  const maxCatch = Math.max(1, ...catches);

  const out: Track[] = [{
    key: "fangst", label: "Fångst", color: color.data, height: 110, kind: "bars", values: catches, lo: 0, hi: maxCatch,
    value: (i) => `${formatInt(catches[i] ?? 0)} st`, range: `max ${formatInt(Math.max(0, ...catches))}`,
  }];
  const range = (vals: (number | null)[], fmt: (v: number) => string) => {
    const r = rangeOf(vals);
    return r ? `${fmt(r.min)} till ${fmt(r.max)}` : "inga värden";
  };

  for (const key of chosen) {
    if (key === "temp") {
      const v = pick((d) => d.temp_mean);
      const r = rangeOf(v);
      out.push({ key, label: label(key), color: col(key), height: 80, kind: "line", values: v, lo: (r?.min ?? 0) - 1, hi: (r?.max ?? 1) + 1,
        value: (i) => f1(v[i], "°C"), range: range(v, (x) => formatNumber(x, 1)) });
    } else if (key === "vind") {
      const v = pick((d) => d.wind_speed);
      const hi = niceTop(rangeOf(v)?.max ?? 0, 4, 12);
      out.push({ key, label: label(key), color: col(key), height: 80, kind: "area", values: v, lo: 0, hi,
        value: (i) => f1(v[i], "m/s"), range: `0–${hi} m/s` });
    } else if (key === "rikt") {
      const v = pick((d) => d.wind_dir);
      out.push({ key, label: label(key), color: col(key), height: 34, kind: "arrows", values: v, lo: 0, hi: 360,
        value: (i) => (v[i] == null ? "–" : `${dirName(v[i]!)} (${Math.round(v[i]!)}°)`), range: "pil = vart det blåser" });
    } else if (key === "tryck") {
      const v = pick((d) => d.pressure);
      const r = rangeOf(v);
      out.push({ key, label: label(key), color: col(key), height: 80, kind: "line", values: v, lo: (r?.min ?? 0) - 2, hi: (r?.max ?? 1) + 2,
        value: (i) => f0(v[i], "hPa"), range: range(v, (x) => formatInt(Math.round(x))), vinga: days.map((d) => d.pressure_from_vinga) });
    } else if (key === "regn") {
      const v = pick((d) => d.precipitation);
      const hi = niceTop(rangeOf(v)?.max ?? 0, 5, 5);
      out.push({ key, label: label(key), color: col(key), height: 70, kind: "bars", values: v, lo: 0, hi,
        value: (i) => f1(v[i], "mm"), range: `summa kl. ${hours.from}–${hours.to}`, vinga: days.map((d) => d.precipitation_from_vinga) });
    } else if (key === "sikt") {
      const v = pick((d) => d.visibility_km);
      const hi = niceTop(rangeOf(v)?.max ?? 0, 10, 10);
      out.push({ key, label: label(key), color: col(key), height: 70, kind: "area", values: v, lo: 0, hi,
        value: (i) => f0(v[i], "km"), range: `0–${hi} km`, vinga: days.map((d) => d.visibility_from_vinga) });
    }
  }
  return out;
}

/**
 * "Väder och fångst dag för dag": the catch and the chosen weather
 * variables as stacked tracks that share one crosshair, and the dark
 * readout beside them for the day under the pointer (default: the day with
 * the most birds).
 */
export function TracksCard({
  query, meta, chosen, speciesName, species,
}: {
  query: CardQuery<Weather>;
  meta: Meta;
  chosen: readonly VarKey[];
  speciesName: string;
  species: string;
}) {
  const [hover, setHover] = useState(-1);
  return (
    <div className={styles.tracksLayout}>
      <ChartCard
        title="Väder och fångst dag för dag"
        subtitle={`Fångst av ${speciesName.toLowerCase()} och väder samma dygn, från SMHI:s station Nidingen A.`}
        query={query}
        isEmpty={(d) => d.days.length === 0}
        emptyMessage="Det finns inga väderuppgifter för den här säsongen."
        footnote={
          query.data && (
            <>
              {meta.weather_window.note}
              {query.data.is_gap_year && ` Stationen var bara delvis bemannad ${query.data.year}, så fångsten är lägre än ett vanligt år.`}
              {" "}Skuggade dagar i fångstdiagrammet: stationen märkte eller kontrollerade inga fåglar.
            </>
          )
        }
      >
        {(data) => <TrackGrid data={data} meta={meta} chosen={chosen} species={species} hover={hover} onHover={setHover} />}
      </ChartCard>
      {query.data && query.data.days.length > 0 && <Readout data={query.data} hover={hover} speciesName={speciesName} meta={meta} />}
    </div>
  );
}

function TrackGrid({
  data, meta, chosen, species, hover, onHover,
}: {
  data: Weather;
  meta: Meta;
  chosen: readonly VarKey[];
  species: string;
  hover: number;
  onHover: (i: number) => void;
}) {
  const [box, width] = useWidth<HTMLDivElement>();
  const days = data.days;
  const n = days.length;
  const tracks = buildTracks(days, chosen, { from: meta.weather_window.from_hour, to: meta.weather_window.to_hour });
  const best = bestIndex(days.map((d) => d.catch));
  const shown = hover >= 0 && hover < n ? hover : Math.max(0, best);
  const x = (i: number) => ((i + 0.5) / n) * W;
  const plotWidth = Math.max(0, width - LABEL_COL);
  const step = arrowStep(n, plotWidth);
  const key = `${data.season}-${data.year}-${species}`;
  const months = monthStarts(days.map((d) => d.date));

  const readout = `${formatDate(days[shown]!.date, true)}: ${tracks.map((t) => `${t.label} ${t.value(shown)}`).join(", ")}`;

  return (
    <div ref={box} className={styles.tracks} style={{ ["--rows" as string]: tracks.length }}>
      {tracks.map((t, k) => {
        const y = (v: number) => H - ((v - t.lo) / (t.hi - t.lo || 1)) * 92 - 4;
        const flagged = t.vinga?.some(Boolean) ?? false;
        const flaggedNow = t.vinga?.[shown] ?? false;
        return (
          <Fragment key={t.key}>
            <div className={styles.trackLabel} style={{ gridRow: k + 1 }}>
              <span className={styles.trackName}>
                <span className={styles.swatch} style={{ background: t.color }} aria-hidden="true" />
                {t.label}
              </span>
              <span className={`mono ${styles.trackValue}`}>{t.value(shown)}</span>
              <span className={`mono ${styles.trackRange}`}>{t.range}</span>
              {flagged && (
                <span className={styles.vinga} title="SMHI mäter inte längre detta på Nidingen A. Värdena kommer från stationen Vinga A.">
                  {flaggedNow ? "Vinga A (vald dag)" : "delvis Vinga A"}
                </span>
              )}
            </div>
            <div className={styles.trackPlot} style={{ gridRow: k + 1, height: t.height }}>
              {t.key === "fangst" &&
                days.map((d, i) =>
                  d.active ? null : <span key={d.date} className={styles.idle} style={{ left: `${(i / n) * 100}%`, width: `${100 / n}%` }} aria-hidden="true" />,
                )}
              {t.kind === "arrows" ? (
                <div className={styles.arrows} aria-hidden="true">
                  {days.map((d, i) =>
                    i % step === 0 ? (
                      <span key={d.date} style={{ left: `${((i + 0.5) / n) * 100}%` }}>
                        {d.wind_dir == null ? null : (
                          <svg width="14" height="14" viewBox="0 0 14 14" style={{ transform: `rotate(${arrowRotation(d.wind_dir)}deg)`, opacity: 0.35 + Math.min(1, (d.wind_speed ?? 0) / 14) * 0.65 }}>
                            <path d="M7 1.5v11M3.5 9L7 12.5 10.5 9" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
                          </svg>
                        )}
                      </span>
                    ) : null,
                  )}
                </div>
              ) : (
                <PlotSvg width={W} height={H} animationKey={key}>
                  {t.kind === "bars" ? (
                    <path d={barsPath(t.values, x, y, H, (W / n) * (n > 60 ? 0.8 : 0.66))} fill={t.color} fillOpacity={0.85} />
                  ) : (
                    <>
                      {t.kind === "area" && <path d={areaPath(t.values, x, y, H)} fill={t.color} fillOpacity={0.18} />}
                      <path d={linePath(t.values, x, y)} fill="none" stroke={t.color} strokeWidth={2} strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
                    </>
                  )}
                </PlotSvg>
              )}
            </div>
          </Fragment>
        );
      })}
      {/* One crosshair and one reader for all tracks. */}
      <div className={`${chartStyles.plot} ${styles.overlay}`} style={{ gridRow: `1 / span ${tracks.length}` }}>
        {hover >= 0 && <Crosshair left={((hover + 0.5) / n) * 100} />}
        <Scrubber n={n} hover={hover} onHover={onHover} valueText={readout} pageStep={7} label="Läs av dag för dag" />
      </div>
      <div className={styles.xAxis} style={{ gridRow: tracks.length + 1 }} aria-hidden="true">
        {months.map((m) => (
          <span key={m.index} className="mono" style={{ left: `${(m.index / n) * 100}%` }}>{monthName(m.month)}</span>
        ))}
      </div>
    </div>
  );
}

function Readout({ data, hover, speciesName, meta }: { data: Weather; hover: number; speciesName: string; meta: Meta }) {
  const days = data.days;
  const best = bestIndex(days.map((d) => d.catch));
  const i = hover >= 0 && hover < days.length ? hover : Math.max(0, best);
  const d = days[i]!;
  const picked = hover >= 0;
  const kicker = picked ? "Vald dag" : best >= 0 ? "Fångstrikaste dagen" : "Säsongens första dag";
  const rows: { label: string; v: string; src?: boolean }[] = [
    { label: "Temperatur", v: f1(d.temp_mean, "°C") },
    { label: "Vind", v: d.wind_speed == null ? "–" : `${formatNumber(d.wind_speed, 1)} m/s${d.wind_dir == null ? "" : ` ${dirName(d.wind_dir)}`}` },
    { label: "Lufttryck", v: f0(d.pressure, "hPa"), src: d.pressure_from_vinga && d.pressure != null },
    { label: "Nederbörd", v: f1(d.precipitation, "mm"), src: d.precipitation_from_vinga && d.precipitation != null },
    { label: "Sikt", v: f0(d.visibility_km, "km"), src: d.visibility_from_vinga && d.visibility_km != null },
    { label: "Dag i säsongen", v: `${i + 1} av ${days.length}` },
  ];
  return (
    <aside className={styles.readout}>
      <div>
        <div className={styles.kicker}>{kicker}</div>
        <div className={styles.readDate}>{formatDate(d.date, true)}</div>
      </div>
      <div className={styles.readCatch}>
        <span className="mono">{formatInt(d.catch)}</span>
        <span>{data.species === "ALLA" ? "fåglar ringmärkta" : `${speciesName.toLowerCase()} ringmärkta`}</span>
      </div>
      {!d.active && <div className={styles.readIdle}>Stationen märkte inga fåglar den här dagen.</div>}
      <dl className={styles.readGrid}>
        {rows.map((r) => (
          <div key={r.label}>
            <dt>{r.label}</dt>
            <dd className="mono">{r.v}</dd>
            {r.src && <dd className={styles.readSrc}>från Vinga A</dd>}
          </div>
        ))}
      </dl>
      <p className={styles.readHint}>
        Väder kl. {meta.weather_window.from_hour}–{meta.weather_window.to_hour}. Rör muspekaren över diagrammen. Utan markering visas säsongens fångstrikaste dag.
      </p>
    </aside>
  );
}
