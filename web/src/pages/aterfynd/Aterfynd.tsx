import { useCallback, useEffect, useId, useMemo, useState } from "react";
import { ChartCard } from "../../components/ChartCard";
import { PageHeader, Section } from "../../components/PageFrame";
import { SegmentedControl } from "../../components/SegmentedControl";
import { useApi, useMeta, type Schemas } from "../../lib/api";
import { formatDate, formatInt, formatNumber } from "../../lib/format";
import { color } from "../../lib/tokens";
import { useChoiceParam, useSearchParam } from "../../lib/urlState";
import styles from "./Aterfynd.module.css";
import { Globe } from "./Globe";
import { findSentence, kmText, NIDINGEN, placeText, sameFind, type Find } from "./globe";

type Recoveries = Schemas["Recoveries"];
type Direction = "both" | "out" | "in";

const DIRECTIONS = ["both", "out", "in"] as const;
const DIRECTION_LABEL: Record<Direction, string> = { both: "Båda", out: "Utgående", in: "Inkommande" };
const ALL = "ALLA";
const COUNTRIES_SHOWN = 8;

/**
 * Återfynd (HANDOFF.md §8): birds ringed at Nidingen and found elsewhere,
 * and foreign-ringed birds caught here, on a draggable globe. The page has
 * its own controls (species, period on the find date, direction, lines);
 * the species picker of the other pages does not apply.
 */
export default function Aterfynd() {
  const meta = useMeta().data;
  const names = useMemo(() => new Map(meta?.species.map((s) => [s.code, s.name]) ?? []), [meta]);
  const nameOf = useCallback((code: string) => names.get(code) ?? code, [names]);

  const periodIds = useMemo(() => meta?.periods.map((p) => p.id) ?? [], [meta]);
  const [periodId, setPeriod] = useChoiceParam("period", periodIds, periodIds[0] ?? "");
  const [direction, setDirection] = useChoiceParam<Direction>("riktning", DIRECTIONS, "both");
  const [artParam, setArt] = useSearchParam("art");
  const art = artParam && artParam.toUpperCase() !== ALL ? artParam.toUpperCase() : ALL;
  const [lines, setLines] = useState(true);
  const [focus, setFocus] = useState(-1);
  const [fly, setFly] = useState<{ lon: number; lat: number; key: number } | null>(null);
  const speciesId = useId();

  const query = useApi<Recoveries>(
    "/api/recoveries",
    { species: art === ALL ? undefined : art, period: periodId, direction },
    { enabled: meta !== undefined && periodId !== "", keepPrevious: true },
  );
  const data = query.data;

  // A new selection starts without a picked find.
  useEffect(() => setFocus(-1), [data]);

  const options = useMemo(() => {
    const list = data?.species_options.map((o) => ({ code: o.species, label: `${nameOf(o.species)} (${formatInt(o.n)})` })) ?? [];
    list.sort((a, b) => a.label.localeCompare(b.label, "sv"));
    // A chosen species with no finds in this period stays selectable.
    if (art !== ALL && !list.some((o) => o.code === art)) list.push({ code: art, label: `${nameOf(art)} (0)` });
    return list;
  }, [data, art, nameOf]);

  const flyToFind = (p: Find) => setFly({ lon: (p.lon + NIDINGEN[0]) / 2, lat: (p.lat + NIDINGEN[1]) / 2, key: (fly?.key ?? 0) + 1 });
  const pickLongest = (p: Find) => {
    const i = data?.points.findIndex((q) => sameFind(q, p)) ?? -1;
    setFocus(i);
    flyToFind(p);
  };

  const period = data?.period;
  const through = meta?.build.recoveries_max_date;

  return (
    <>
      <PageHeader
        title="Återfynd"
        lead="Fåglar märkta på Nidingen som hittats någon annanstans, och fåglar märkta utomlands som fångats här. Dra i jordklotet för att snurra det."
      />
      <Section>
        <div className={styles.controls} role="region" aria-label="Urval">
          <label htmlFor={speciesId} className={styles.controlLabel}>Art</label>
          <select id={speciesId} className={styles.select} value={art} onChange={(e) => setArt(e.target.value === ALL ? null : e.target.value)}>
            <option value={ALL}>Alla arter</option>
            {options.map((o) => (
              <option key={o.code} value={o.code}>{o.label}</option>
            ))}
          </select>
          {meta && (
            <SegmentedControl
              label="Period"
              value={periodId}
              onChange={setPeriod}
              options={meta.periods.map((p) => ({ value: p.id, label: p.label }))}
            />
          )}
          <SegmentedControl
            label="Riktning"
            value={direction}
            onChange={setDirection}
            options={DIRECTIONS.map((d) => ({ value: d, label: DIRECTION_LABEL[d] }))}
          />
          <button type="button" className={styles.switch} aria-pressed={lines} onClick={() => setLines((v) => !v)}>
            <span className={styles.track}><span className={styles.knob} /></span>
            Visa linjer
          </button>
        </div>
      </Section>
      <Section last>
        <div className={styles.layout}>
          <ChartCard
            title="Återfynd på jordklotet"
            actions={
              <ul className={styles.key}>
                <li><span className={styles.keyDot} style={{ background: color.data }} aria-hidden="true" />Märkt här, hittad där</li>
                <li><span className={styles.keyDot} style={{ background: color.trend }} aria-hidden="true" />Märkt där, fångad här</li>
              </ul>
            }
            query={query}
            isEmpty={(d) => d.points.length === 0}
            emptyMessage="Inga återfynd för det här urvalet. Prova en längre period, en annan riktning eller en annan art."
            footnote={
              <>
                Perioden gäller fyndåret{period ? `: ${period.year_from}–${period.year_to}${through ? `, till och med ${formatDate(through, true)}` : ""}` : ""}.
                Linjerna följer storcirkeln, den kortaste vägen över jordytan. Gröna prickar är fyndplatsen för fåglar märkta här, röda är märkplatsen för fåglar
                märkta utomlands. Kustlinjer: Natural Earth.
              </>
            }
          >
            {(d) => <Globe points={d.points} lines={lines} focus={focus} onFocus={setFocus} nameOf={nameOf} fly={fly} />}
          </ChartCard>

          <aside className={styles.side}>
            {data && (
              <>
                <FocusPanel data={data} focus={focus} nameOf={nameOf} />
                <Tiles data={data} />
                <Countries data={data} />
                <Longest data={data} nameOf={nameOf} onPick={pickLongest} />
              </>
            )}
          </aside>
        </div>
      </Section>
    </>
  );
}

function FocusPanel({ data, focus, nameOf }: { data: Recoveries; focus: number; nameOf: (c: string) => string }) {
  const picked = data.points[focus];
  const far = data.longest[0];
  const text = picked
    ? findSentence(picked, nameOf(picked.species))
    : far
      ? { ...findSentence(far, nameOf(far.species)), kicker: "Längst bort i urvalet" }
      : { kicker: "Inga återfynd", title: "Inget att visa", text: "Prova en längre period eller en annan art." };
  return (
    <div className={styles.focus} aria-live="polite">
      <span className={styles.kicker}>{text.kicker}</span>
      <span className={styles.focusTitle}>{text.title}</span>
      <span className={styles.focusText}>{text.text}</span>
    </div>
  );
}

function Tiles({ data }: { data: Recoveries }) {
  const s = data.summary;
  return (
    <div>
      <dl className={styles.tiles}>
        <div><dd className="mono" style={{ color: color.data }}>{formatInt(s.n_out)}</dd><dt>utgående</dt></div>
        <div><dd className="mono" style={{ color: color.badge_down_fg }}>{formatInt(s.n_in)}</dd><dt>inkommande</dt></div>
        <div><dd className="mono">{s.longest_km == null ? "–" : kmText(s.longest_km)}</dd><dt>längsta avstånd</dt></div>
        <div><dd className="mono">{s.median_km == null ? "–" : `${formatNumber(s.median_km)} km`}</dd><dt>medianavstånd</dt></div>
      </dl>
      <p className={styles.tilesNote}>
        Antalen utgående och inkommande ignorerar riktningsvalet. Avstånden gäller de {formatInt(s.n_points)} återfynd som visas.
      </p>
    </div>
  );
}

function Countries({ data }: { data: Recoveries }) {
  const shown = data.countries.slice(0, COUNTRIES_SHOWN);
  const rest = data.countries.slice(COUNTRIES_SHOWN).reduce((sum, c) => sum + c.n, 0);
  const max = Math.max(1, ...shown.map((c) => c.n));
  return (
    <div className={styles.panel}>
      <h3 className={styles.panelTitle}>Länder</h3>
      {shown.length === 0 && <p className={styles.tilesNote}>Inga återfynd att fördela.</p>}
      {shown.map((c) => (
        <div key={c.country} className={styles.countryRow}>
          <span className={styles.countryName}>{c.country}</span>
          <span className={styles.countryTrack}>
            <span className={styles.countryBar} style={{ width: `${(c.n / max) * 100}%` }} />
          </span>
          <span className="mono">{formatInt(c.n)}</span>
        </div>
      ))}
      {rest > 0 && <p className={styles.tilesNote}>och {formatInt(rest)} återfynd i {data.countries.length - COUNTRIES_SHOWN} länder till.</p>}
    </div>
  );
}

function Longest({ data, nameOf, onPick }: { data: Recoveries; nameOf: (c: string) => string; onPick: (p: Find) => void }) {
  return (
    <div className={styles.panel}>
      <h3 className={styles.panelTitle}>Längsta återfynd</h3>
      {data.longest.map((p, i) => (
        <button key={i} type="button" className={styles.longest} onClick={() => onPick(p)}>
          <span className={styles.longestName}>{nameOf(p.species)}</span>
          <span className={`mono ${styles.longestKm}`}>{kmText(p.distance_km)}</span>
          <span className={styles.longestWhere}>{placeText(p)}</span>
          <span className={`mono ${styles.longestWhen}`}>{(p.direction === "out" ? p.find_date : (p.ring_date ?? p.find_date)).slice(0, 4)}</span>
        </button>
      ))}
    </div>
  );
}
