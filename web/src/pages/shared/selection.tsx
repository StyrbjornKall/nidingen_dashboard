/**
 * The shared selection used by Sammanfattning, Trender, Biometri and
 * Fenologi: species (max 6, colours by order), period and season, all in
 * the URL. Each page passes its own defaults, as in the design.
 */
import { useMemo } from "react";
import { YearSelect } from "../../components/YearSelect";
import { pickableYears } from "../../lib/years";
import { BarWord, StickyBar } from "../../components/PageFrame";
import { SegmentedControl } from "../../components/SegmentedControl";
import { SpeciesPicker } from "../../components/SpeciesPicker";
import { useMeta, type Meta, type PeriodInfo, type SpeciesInfo } from "../../lib/api";
import { ALL_SPECIES, useChoiceParam, useSpeciesSelection, type SeasonId, type SpeciesSelection } from "../../lib/urlState";

export interface PageSelectionOptions {
  /** Species when the URL has none; null = the configured default selection. */
  defaults: readonly string[] | null;
  /** Replaces an emptied selection. */
  fallback: readonly string[];
  /** Season options on this page (none = the page has no season control). */
  seasons?: readonly SeasonId[];
  defaultSeason?: SeasonId;
  /** Add the last complete year as a period of its own (Sammanfattning). */
  singleYear?: boolean;
  /** Offer "Välj specifikt år": any complete year, not a gap year, as a period of its own. */
  yearPicker?: boolean;
}

export interface PageSelection {
  meta: Meta | undefined;
  species: SpeciesSelection;
  /** Species that can be picked: ALLA plus every species with new ringings. */
  pickable: SpeciesInfo[];
  /** Period choices: the configured presets, plus a single year if asked for. */
  periods: PeriodInfo[];
  period: PeriodInfo | undefined;
  setPeriod: (id: string) => void;
  /** Years in the "Välj specifikt år" dropdown (empty when the page has none). */
  years: number[];
  season: SeasonId;
  setSeason: (s: SeasonId) => void;
  seasons: readonly SeasonId[];
}

const NO_SEASONS: readonly SeasonId[] = [];

export function usePageSelection(opts: PageSelectionOptions): PageSelection {
  const meta = useMeta().data;
  const max = meta?.max_compare_species ?? 6;
  const known = useMemo(() => (meta ? new Set(meta.species.map((s) => s.code)) : undefined), [meta]);
  const defaults = opts.defaults ?? meta?.default_selection ?? [];
  const species = useSpeciesSelection({ defaults, fallback: opts.fallback, max, known });

  const pickable = useMemo(
    () => (meta ? meta.species.filter((s) => s.code === ALL_SPECIES || s.n_new_total > 0) : []),
    [meta],
  );

  const singleYear = opts.singleYear ?? false;
  const years = useMemo(
    () => (meta && opts.yearPicker ? pickableYears(meta.first_year, meta.complete_through, meta.gap_years) : []),
    [meta, opts.yearPicker],
  );
  const periods = useMemo(() => {
    if (!meta) return [];
    if (!singleYear) return meta.periods;
    const y = meta.complete_through;
    return [...meta.periods, { id: String(y), label: String(y), year_from: y, year_to: y }];
  }, [meta, singleYear]);
  // A specific year is a period of its own (`?period=1987`); presets are
  // the ones the segmented control shows.
  const periodIds = useMemo(() => [...periods.map((p) => p.id), ...years.map(String)], [periods, years]);
  const [periodId, setPeriod] = useChoiceParam("period", periodIds, periods[0]?.id ?? "");
  const period: PeriodInfo | undefined =
    periods.find((p) => p.id === periodId) ??
    (years.includes(Number(periodId)) ? { id: periodId, label: periodId, year_from: Number(periodId), year_to: Number(periodId) } : undefined);

  const seasons = opts.seasons ?? NO_SEASONS;
  const [season, setSeason] = useChoiceParam<SeasonId>("sasong", seasons, opts.defaultSeason ?? seasons[0] ?? "helar");

  return { meta, species, pickable, periods, period, setPeriod, years, season, setSeason, seasons };
}

const SEASON_LABELS: Record<SeasonId, string> = { var: "Vår", host: "Höst", helar: "Helår" };

/**
 * "Visar [arter] under [period]" — the sticky selection bar. `before`/`after`
 * are the connecting words from the design for each page.
 */
export function SelectionBar({
  selection, before, after, showPeriod = true, allowAll = true,
}: {
  selection: PageSelection;
  before: string;
  after?: string;
  showPeriod?: boolean;
  allowAll?: boolean;
}) {
  const { meta, species, pickable, periods, period, setPeriod, years, season, setSeason, seasons } = selection;
  return (
    <StickyBar label="Urval">
      <BarWord>{before}</BarWord>
      {meta ? (
        <SpeciesPicker species={pickable} selected={species.selected} onToggle={species.toggle} max={species.max} allowAll={allowAll} />
      ) : (
        <span style={{ minHeight: 40, display: "inline-flex", alignItems: "center", color: "var(--color-text-muted)" }}>Laddar arter …</span>
      )}
      {after && <BarWord>{after}</BarWord>}
      {seasons.length > 0 && (
        <SegmentedControl
          label="Säsong"
          value={season}
          onChange={setSeason}
          options={seasons.map((s) => ({ value: s, label: SEASON_LABELS[s] }))}
        />
      )}
      {showPeriod && meta && period && (
        <SegmentedControl
          label="Period"
          value={period.id}
          onChange={setPeriod}
          options={periods.map((p) => ({ value: p.id, label: p.label }))}
        />
      )}
      {showPeriod && meta && period && years.length > 0 && (
        <YearSelect
          years={years}
          value={period.year_from === period.year_to ? period.year_from : null}
          onChange={(y) => setPeriod(String(y))}
        />
      )}
    </StickyBar>
  );
}

/** API parameters for a period: `year=` for a single year, otherwise `period=`. */
export function periodParams(period: PeriodInfo): { period?: string; year?: number } {
  return period.year_from === period.year_to ? { year: period.year_from } : { period: period.id };
}

/** "1980–2025", or "2025" for a single year. */
export function yearSpan(period: Pick<PeriodInfo, "year_from" | "year_to">): string {
  return period.year_from === period.year_to ? String(period.year_from) : `${period.year_from}–${period.year_to}`;
}
