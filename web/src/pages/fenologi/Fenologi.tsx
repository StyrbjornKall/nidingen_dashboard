import { useCallback, useMemo } from "react";
import { PageHeader, Section } from "../../components/PageFrame";
import { SelectionBar, usePageSelection } from "../shared/selection";
import { CalendarCard } from "./CalendarCard";
import styles from "./Fenologi.module.css";
import { RidgesCard } from "./RidgesCard";
import { SeasonToDateCard } from "./SeasonToDateCard";
import { SpeciesTableCard } from "./SpeciesTableCard";

const FALLBACK = ["RÖHAK"];

/**
 * Fenologi (HANDOFF.md §8): the migration calendar with its own controls at
 * the top, then the species bar (no season toggle), ridgelines, the
 * whole-year season-to-date chart and the species table.
 */
export default function Fenologi() {
  const selection = usePageSelection({ defaults: null, fallback: FALLBACK });
  const { meta, species, period } = selection;
  const names = useMemo(() => new Map(meta?.species.map((s) => [s.code, s.name]) ?? []), [meta]);
  const nameOf = useCallback((code: string) => names.get(code) ?? code, [names]);

  return (
    <>
      <PageHeader
        title="Fenologi"
        lead="När på året passerar arterna ön, och hur går årets säsong jämfört med ett normalt år?"
      />
      {meta && (
        <Section>
          <CalendarCard meta={meta} selected={species.selected} colorOf={species.colorOf} />
        </Section>
      )}
      <div className={styles.beforeBar} />
      <SelectionBar selection={selection} before="Jämför" />
      {meta && period && (
        <>
          <Section>
            <RidgesCard meta={meta} period={period} species={species.selected} colorOf={species.colorOf} nameOf={nameOf} />
            <SeasonToDateCard meta={meta} species={species.selected} colorOf={species.colorOf} nameOf={nameOf} />
          </Section>
          <Section last>
            <SpeciesTableCard
              period={period}
              selected={species.selected}
              colorOf={species.colorOf}
              onToggle={species.toggle}
              max={species.max}
            />
          </Section>
        </>
      )}
    </>
  );
}
