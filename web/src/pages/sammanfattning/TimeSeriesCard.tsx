import { useMemo, useState } from "react";
import { ChartCard } from "../../components/ChartCard";
import { SegmentedControl } from "../../components/SegmentedControl";
import { useApi, type Meta, type PeriodInfo, type Schemas } from "../../lib/api";
import { formatInt } from "../../lib/format";
import { color } from "../../lib/tokens";
import { periodParams, yearSpan } from "../shared/selection";
import styles from "./Sammanfattning.module.css";
import { TimeSeriesChart, type ChartSeries } from "./TimeSeriesChart";
import { bucketLabel, defaultChartType, GRAIN_NOUN, type Grain } from "./timeseries";

type ChartType = "bar" | "line";

const GRAINS: readonly { value: Grain; label: string }[] = [
  { value: "day", label: "Dag" },
  { value: "week", label: "Vecka" },
  { value: "month", label: "Månad" },
  { value: "year", label: "År" },
];

export interface TimeSeriesCardProps {
  meta: Meta;
  period: PeriodInfo;
  species: readonly string[];
  colorOf: (code: string) => string | undefined;
  nameOf: (code: string) => string;
}

/** "Ringmärkta över tid": new ringings per day, week, month or year. */
export function TimeSeriesCard({ meta, period, species, colorOf, nameOf }: TimeSeriesCardProps) {
  const single = period.year_from === period.year_to;
  const [chosenGrain, setGrain] = useState<Grain>("year");
  const [type, setType] = useState<ChartType>("bar");
  const [showRecap, setShowRecap] = useState(false);
  const [hover, setHover] = useState(-1);
  // Daily resolution needs a single year; leaving one falls back to weeks
  // (design). A single year per year would be one bar, so that shows months.
  const grain: Grain = chosenGrain === "day" && !single ? "week" : chosenGrain === "year" && single ? "month" : chosenGrain;

  const query = useApi<Schemas["TimeSeries"]>(
    "/api/summary/timeseries",
    { species, grain, ...periodParams(period) },
    { keepPrevious: true },
  );

  const pickGrain = (g: Grain) => {
    setGrain(g);
    setType(defaultChartType(g));
    setHover(-1);
  };

  const gapInPeriod = period.year_from <= meta.gap_years.year_to && period.year_to >= meta.gap_years.year_from;
  const noun = GRAIN_NOUN[grain];

  return (
    <ChartCard
      title="Ringmärkta över tid"
      subtitle={`Antal ringmärkta per ${noun}${species.length > 1 && type === "bar" ? ", staplade per art" : ""}`}
      query={query}
      isEmpty={(d) => d.series.every((s) => s.total_new === 0 && s.total_recap === 0)}
      emptyMessage={`Inga fåglar av de valda arterna ringmärktes ${yearSpan(period)}.`}
      actions={
        <>
          <SegmentedControl
            label="Upplösning"
            value={grain}
            onChange={pickGrain}
            options={GRAINS.map((g) => {
              const off = (g.value === "day" && !single) || (g.value === "year" && single);
              const why = g.value === "day" ? "Välj ett enskilt år för daglig upplösning" : "Välj en period med flera år";
              return { ...g, disabled: off, title: off ? why : `Antal per ${GRAIN_NOUN[g.value]}` };
            })}
          />
          <SegmentedControl
            label="Diagramtyp"
            value={type}
            onChange={(t) => setType(t)}
            options={[
              { value: "bar", label: "Stapel" },
              { value: "line", label: "Linje" },
            ]}
          />
        </>
      }
      footnote={
        <>
          {grain === "day" || grain === "week"
            ? "Linje är förvalt för dag och vecka eftersom staplarna blir för tunna. Stationen är bemannad vår och höst, därav luckorna."
            : `Håll muspekaren över diagrammet, eller använd piltangenterna, för att läsa av en enskild ${noun}.`}
          {gapInPeriod &&
            ` ${meta.gap_years.year_from}–${meta.gap_years.year_to} är skuggat: ${meta.gap_years.reason.charAt(0).toLowerCase()}${meta.gap_years.reason.slice(1)}`}
          {showRecap && " Streckade linjer är kontroller, fåglar som redan var ringmärkta och fångades igen på Nidingen."}
        </>
      }
    >
      {(data) => (
        <TimeSeriesBody
          data={data}
          type={type}
          showRecap={showRecap}
          onToggleRecap={() => setShowRecap((v) => !v)}
          hover={hover}
          onHover={setHover}
          colorOf={colorOf}
          nameOf={nameOf}
        />
      )}
    </ChartCard>
  );
}

function TimeSeriesBody({
  data, type, showRecap, onToggleRecap, hover, onHover, colorOf, nameOf,
}: {
  data: Schemas["TimeSeries"];
  type: ChartType;
  showRecap: boolean;
  onToggleRecap: () => void;
  hover: number;
  onHover: (i: number) => void;
  colorOf: (code: string) => string | undefined;
  nameOf: (code: string) => string;
}) {
  const series: ChartSeries[] = useMemo(
    () =>
      data.series.map((s) => ({
        code: s.species,
        name: nameOf(s.species),
        // A species just removed keeps its old data until the new answer arrives.
        color: colorOf(s.species) ?? color.chart_mean,
        values: s.new,
        recap: showRecap ? s.recap : undefined,
      })),
    [data, colorOf, nameOf, showRecap],
  );

  const i = hover >= 0 && hover < data.buckets.length ? hover : -1;
  const bucket = i >= 0 ? data.buckets[i] : undefined;
  const title = bucket
    ? `${bucketLabel(bucket, data.grain)}${bucket.is_gap_year ? " · delvis bemannad" : ""}`
    : `Summa ${yearSpan(data.period)}`;
  const rows = data.series.map((s, k) => ({
    code: s.species,
    name: series[k]?.name ?? s.species,
    color: series[k]?.color ?? color.chart_mean,
    n: bucket ? (s.new[i] ?? 0) : s.total_new,
    recap: bucket ? (s.recap[i] ?? 0) : s.total_recap,
  }));
  const valueText = `${title}: ${rows
    .map((r) => `${r.name} ${formatInt(r.n)}${showRecap ? `, ${formatInt(r.recap)} kontroller` : ""}`)
    .join("; ")}`;
  const animationKey = `${data.grain}|${type}|${data.period.id}|${data.series.map((s) => s.species).join(",")}`;

  return (
    <div className={styles.tsBody}>
      <div className={styles.readoutRow}>
        <div className={styles.readout}>
          <span key={animationKey} className={`mono ${styles.readoutTitle}`}>
            {title}
          </span>
          {rows.map((r) => (
            <span key={r.code} className={styles.readoutItem}>
              <span className={styles.swatch} style={{ background: r.color }} aria-hidden="true" />
              {r.name} <span className={`mono ${styles.readoutValue}`}>{formatInt(r.n)}</span>
              {showRecap && <span className={`mono ${styles.readoutRecap}`}>+ {formatInt(r.recap)} kontr.</span>}
            </span>
          ))}
        </div>
        <button type="button" className={styles.toggle} aria-pressed={showRecap} onClick={onToggleRecap}>
          <svg width="22" height="10" aria-hidden="true">
            <line x1="1" y1="5" x2="21" y2="5" stroke="currentColor" strokeWidth="2" strokeDasharray="5 3" strokeLinecap="round" />
          </svg>
          Visa kontroller
        </button>
      </div>
      <TimeSeriesChart
        buckets={data.buckets}
        grain={data.grain}
        type={type}
        series={series}
        hover={i}
        onHover={onHover}
        valueText={valueText}
        animationKey={animationKey}
      />
    </div>
  );
}
