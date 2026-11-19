import { useCallback, useId, useMemo } from "react";
import { PageHeader, Section } from "../../components/PageFrame";
import { SegmentedControl } from "../../components/SegmentedControl";
import { useApi, useMeta, type Schemas } from "../../lib/api";
import { ALL_SPECIES, useChoiceParam, useSearchParam } from "../../lib/urlState";
import { BestDaysCard } from "./BestDaysCard";
import { RoseCard } from "./RoseCard";
import { TracksCard } from "./Tracks";
import styles from "./Vader.module.css";
import { parseVars, serializeVars, VARS, type VarKey } from "./weather";

const SEASONS = ["var", "host"] as const;
type SeasonChoice = (typeof SEASONS)[number];

/**
 * Väder (HANDOFF.md §8): the catch and SMHI's weather day by day for one
 * season of one year, a wind rose and the best days. The page has its own
 * controls; the species picker of the other pages does not apply. Seasons
 * are the two site seasons; weather covers only the catch window.
 */
export default function Vader() {
  const meta = useMeta().data;
  const [season, setSeason] = useChoiceParam<SeasonChoice>("sasong", SEASONS, "host");
  const labels = useMemo(() => new Map(meta?.seasons.map((s) => [s.id, s.label]) ?? []), [meta]);

  const wy = meta?.weather_years;
  const lastYear = wy && meta ? Math.min(wy.year_to, meta.current_year) : undefined;
  const years = useMemo(() => {
    if (!wy || lastYear === undefined) return [];
    const out: number[] = [];
    for (let y = lastYear; y >= wy.year_from; y--) out.push(y);
    return out;
  }, [wy, lastYear]);
  const defaultYear = meta ? Math.min(meta.complete_through, lastYear ?? meta.complete_through) : undefined;
  const [yearParam, setYearParam] = useSearchParam("ar");
  const year = yearParam && years.includes(Number(yearParam)) ? Number(yearParam) : defaultYear;

  const [speciesParam, setSpecies] = useSearchParam("art");
  const species = speciesParam && meta?.species.some((s) => s.code === speciesParam.toUpperCase()) ? speciesParam.toUpperCase() : ALL_SPECIES;
  const speciesOptions = useMemo(
    () =>
      (meta?.species ?? [])
        .filter((s) => s.code !== ALL_SPECIES && s.n_new_total > 0)
        .map((s) => ({ code: s.code, name: s.name }))
        .sort((a, b) => a.name.localeCompare(b.name, "sv")),
    [meta],
  );
  const speciesName = species === ALL_SPECIES ? "Alla arter" : (meta?.species.find((s) => s.code === species)?.name ?? species);

  const [visaParam, setVisa] = useSearchParam("visa");
  const chosen = useMemo(() => parseVars(visaParam), [visaParam]);
  const toggle = useCallback(
    (key: VarKey) => setVisa(serializeVars(chosen.includes(key) ? chosen.filter((k) => k !== key) : [...chosen, key])),
    [chosen, setVisa],
  );

  const query = useApi<Schemas["Weather"]>(
    "/api/weather",
    { season, year, species },
    { enabled: meta !== undefined && year !== undefined, keepPrevious: true },
  );

  const seasonId = useId();
  const yearId = useId();
  const speciesId = useId();
  const gap = meta?.gap_years;

  return (
    <>
      <PageHeader
        title="Väder"
        lead="Väder från SMHI:s station Nidingen A, dag för dag bredvid fångsten. Ser du toppar i fångsten efter ostliga vindar eller en passerande front?"
      />
      <Section>
        <div className={styles.controls} role="region" aria-label="Urval">
          <div className={styles.controlRow}>
            <span id={seasonId} className={styles.controlLabel}>Säsong</span>
            <SegmentedControl
              label="Säsong"
              value={season}
              onChange={setSeason}
              options={SEASONS.map((s) => ({ value: s, label: labels.get(s) ?? s }))}
            />
            <label htmlFor={yearId} className={styles.controlLabel}>År</label>
            <select id={yearId} className={styles.select} value={year ?? ""} onChange={(e) => setYearParam(Number(e.target.value) === defaultYear ? null : e.target.value)}>
              {years.map((y) => (
                <option key={y} value={y}>
                  {y}
                  {gap && y >= gap.year_from && y <= gap.year_to ? " (delvis bemannad)" : ""}
                </option>
              ))}
            </select>
            <label htmlFor={speciesId} className={styles.controlLabel}>Fångst av</label>
            <select id={speciesId} className={styles.select} value={species} onChange={(e) => setSpecies(e.target.value === ALL_SPECIES ? null : e.target.value)}>
              <option value={ALL_SPECIES}>Alla arter</option>
              {speciesOptions.map((o) => (
                <option key={o.code} value={o.code}>{o.name}</option>
              ))}
            </select>
          </div>
          <div className={styles.controlRow} role="group" aria-label="Visa">
            <span className={styles.controlLabel}>Visa</span>
            {VARS.map((v) => (
              <button key={v.key} type="button" className={styles.chip} aria-pressed={chosen.includes(v.key)} onClick={() => toggle(v.key)}>
                <span className={styles.swatch} style={{ background: v.color }} aria-hidden="true" />
                {v.label}
              </button>
            ))}
          </div>
        </div>
      </Section>
      {meta && year !== undefined && (
        <>
          <Section>
            <TracksCard query={query} meta={meta} chosen={chosen} speciesName={speciesName} species={species} />
          </Section>
          <Section last>
            <div className={styles.bottom}>
              <RoseCard query={query} speciesName={speciesName} />
              <BestDaysCard query={query} />
            </div>
          </Section>
        </>
      )}
    </>
  );
}
