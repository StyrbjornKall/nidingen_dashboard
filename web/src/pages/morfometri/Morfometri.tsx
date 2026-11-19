import { useCallback, useMemo } from "react";
import type { CardQuery } from "../../components/ChartCard";
import { PageHeader, Section } from "../../components/PageFrame";
import { useApi, type Schemas } from "../../lib/api";
import { monthName } from "../../lib/format";
import { ALL_SPECIES } from "../../lib/urlState";
import { SelectionBar, usePageSelection } from "../shared/selection";
import { AgeSexCard } from "./AgeSexCard";
import { BubbleCard } from "./BubbleCard";
import { DistributionsCard } from "./DistributionsCard";
import { KeyFiguresCard } from "./KeyFiguresCard";
import styles from "./Morfometri.module.css";
import { seasonPhrase } from "./morph";

const DEFAULTS = ["RÖHAK", "ÄNPIP", "SKPIP"];
const FALLBACK = ["RÖHAK"];
const SEASONS = ["var", "host", "helar"] as const;

type Details = Schemas["MorphDetails"];

/**
 * Morfometri (HANDOFF.md §8): every species' mean weight against wing
 * length, then key figures, distributions, and age and sex for the
 * selected species. Descriptive statistics over all years, so there is
 * no period choice.
 */
export default function Morfometri() {
  const selection = usePageSelection({ defaults: DEFAULTS, fallback: FALLBACK, seasons: SEASONS, defaultSeason: "host" });
  const { meta, species, season } = selection;
  const info = useMemo(() => new Map(meta?.species.map((s) => [s.code, s]) ?? []), [meta]);
  const nameOf = useCallback((code: string) => info.get(code)?.name ?? code, [info]);

  // Measures are per species, so "Alla arter" has nothing to show in the detail cards.
  const codes = species.selected.filter((c) => c !== ALL_SPECIES);
  const hasAll = species.selected.includes(ALL_SPECIES);
  const means = useApi<Schemas["MorphMeans"]>("/api/morphometrics/means", { season }, { enabled: meta !== undefined, keepPrevious: true });
  const detail = useApi<Details>(
    "/api/morphometrics/detail",
    { species: codes, season },
    { enabled: meta !== undefined && codes.length > 0, keepPrevious: true },
  );
  const noSpecies: CardQuery<Details> = { data: { season, species: [] }, isPending: false, isError: false, refetch: () => undefined };
  const detailQuery = codes.length > 0 ? detail : noSpecies;

  const seasonInfo = meta?.seasons.find((s) => s.id === season);
  const phrase = seasonInfo ? seasonPhrase(seasonInfo, (m) => monthName(m, true)) : "";

  return (
    <>
      <PageHeader
        title="Morfometri"
        lead="Hur ser fåglarna ut när de fångas här? Vikt, vinglängd, ålder och kön för de arter du valt, mot alla andra arter."
      />
      <SelectionBar selection={selection} before="Jämför" after="fångade" showPeriod={false} seasonLabels={{ helar: "Båda" }} />
      {meta && (
        <>
          <Section>
            <div className={styles.top}>
              <div className={styles.topWide}>
                <BubbleCard
                  query={means}
                  season={phrase}
                  selected={species.selected}
                  colorOf={species.colorOf}
                  nameOf={nameOf}
                  onToggle={species.toggle}
                  max={species.max}
                />
              </div>
              <KeyFiguresCard
                query={detailQuery}
                colorOf={species.colorOf}
                nameOf={nameOf}
                note={hasAll && codes.length > 0 ? "Mått jämförs per art, så ”Alla arter” visas inte här." : undefined}
              />
            </div>
          </Section>
          <Section>
            <DistributionsCard query={detailQuery} colorOf={species.colorOf} nameOf={nameOf} season={phrase} />
          </Section>
          <Section last>
            <AgeSexCard query={detailQuery} colorOf={species.colorOf} nameOf={nameOf} season={phrase} />
          </Section>
        </>
      )}
    </>
  );
}
