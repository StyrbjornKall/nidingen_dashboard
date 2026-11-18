import { useCallback, useMemo } from "react";
import { PageHeader, Section } from "../../components/PageFrame";
import { useApi } from "../../lib/api";
import { SelectionBar, usePageSelection } from "../shared/selection";
import styles from "./Trender.module.css";
import { ComparisonCard } from "./ComparisonCard";
import type { Trends } from "./trendSeries";
import { OverviewCard } from "./OverviewCard";
import { SpeciesCards } from "./SpeciesCards";
import { trendYearsCaption } from "./trends";

const FALLBACK = ["RÖHAK"];
const SEASONS = ["var", "host", "helar"] as const;

export default function Trender() {
  const selection = usePageSelection({ defaults: null, fallback: FALLBACK, seasons: SEASONS, defaultSeason: "host" });
  const { meta, species, period, season } = selection;

  const info = useMemo(() => new Map(meta?.species.map((s) => [s.code, s]) ?? []), [meta]);
  const nameOf = useCallback((code: string) => info.get(code)?.name ?? code, [info]);
  const infoOf = useCallback((code: string) => info.get(code), [info]);

  const ready = meta !== undefined && period !== undefined && species.selected.length > 0;
  const trends = useApi<Trends>(
    "/api/trends",
    { species: species.selected, season, period: period?.id },
    { enabled: ready, keepPrevious: true },
  );
  const caption = meta && period ? trendYearsCaption(period, meta.gap_years, meta.current_year) : undefined;

  return (
    <>
      <PageHeader
        title="Trender"
        lead="Ökar eller minskar arterna? Varje diagram visar ett värde per år, en anpassad trendlinje och ett skuggat band för hur säker linjen är (95 % konfidensintervall)."
      />
      <SelectionBar selection={selection} before="Jämför" after="under" />
      {meta && period && (
        <>
          <Section>
            <ComparisonCard query={trends} colorOf={species.colorOf} nameOf={nameOf} caption={caption} />
          </Section>
          <Section>
            <div>
              <h2 className={styles.heading}>Art för art</h2>
              <p className={styles.headingLead}>Faktiskt antal ringmärkta per år, var art på sin egen skala.</p>
            </div>
            <SpeciesCards data={trends.data} colorOf={species.colorOf} infoOf={infoOf} />
          </Section>
          <Section last>
            <OverviewCard
              season={season}
              period={period}
              selected={species.selected}
              colorOf={species.colorOf}
              onToggle={species.toggle}
              max={species.max}
              caption={caption}
            />
          </Section>
        </>
      )}
    </>
  );
}
