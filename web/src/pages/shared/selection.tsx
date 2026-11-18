/**
 * The shared selection used by Sammanfattning, Trender, Morfometri and
 * Fenologi: species (max 6, colours by order), period and season, all in
 * the URL. Each page passes its own defaults, as in the design.
 */
import { useMemo } from "react";
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
}

export interface PageSelection {
  meta: Meta | undefined;
  species: SpeciesSelection;
  /** Species that can be picked: ALLA plus every species with new ringings. */
  pickable: SpeciesInfo[];
  period: PeriodInfo | undefined;
  setPeriod: (id: string) => void;
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

  const periodIds = useMemo(() => meta?.periods.map((p) => p.id) ?? [], [meta]);
  const [periodId, setPeriod] = useChoiceParam("period", periodIds, periodIds[0] ?? "");
  const period = meta?.periods.find((p) => p.id === periodId);

  const seasons = opts.seasons ?? NO_SEASONS;
  const [season, setSeason] = useChoiceParam<SeasonId>("sasong", seasons, opts.defaultSeason ?? seasons[0] ?? "helar");

  return { meta, species, pickable, period, setPeriod, season, setSeason, seasons };
}

const SEASON_LABELS: Record<SeasonId, string> = { var: "Vår", host: "Höst", helar: "Helår" };

/**
 * "Visar [arter] under [period]" — the sticky selection bar. `before`/`after`
 * are the connecting words from the design for each page.
 */
export function SelectionBar({
  selection, before, after, showPeriod = true, allowAll = true, seasonLabels,
}: {
  selection: PageSelection;
  before: string;
  after?: string;
  showPeriod?: boolean;
  allowAll?: boolean;
  /** Override labels, e.g. Morfometri calls the whole year "Båda". */
  seasonLabels?: Partial<Record<SeasonId, string>>;
}) {
  const { meta, species, pickable, period, setPeriod, season, setSeason, seasons } = selection;
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
          options={seasons.map((s) => ({ value: s, label: seasonLabels?.[s] ?? SEASON_LABELS[s] }))}
        />
      )}
      {showPeriod && meta && period && (
        <SegmentedControl
          label="Period"
          value={period.id}
          onChange={setPeriod}
          options={meta.periods.map((p) => ({ value: p.id, label: p.label }))}
        />
      )}
    </StickyBar>
  );
}
