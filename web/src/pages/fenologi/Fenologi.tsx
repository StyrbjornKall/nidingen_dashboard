import { useCallback, useMemo } from "react";
import { PageHeader, Section } from "../../components/PageFrame";
import { SelectionBar, usePageSelection } from "../shared/selection";
import { CalendarCard } from "./CalendarCard";
import { RidgesCard } from "./RidgesCard";
import { SpeciesTableCard } from "./SpeciesTableCard";

const FALLBACK = ["RÖHAK"];

/**
 * Fenologi (HANDOFF.md §8): one selection bar (species, period or a
 * specific year) on top of the märkningskalendern, the ridgelines and the
 * species table. The season-to-date chart lives on Sammanfattning.
 */
export default function Fenologi() {
  const selection = usePageSelection({ defaults: null, fallback: FALLBACK, yearPicker: true });
  const { meta, species, period } = selection;
  const names = useMemo(() => new Map(meta?.species.map((s) => [s.code, s.name]) ?? []), [meta]);
  const nameOf = useCallback((code: string) => names.get(code) ?? code, [names]);

  return (
    <>
      <PageHeader
        title="Fenologi"
        lead="När på året passerar arterna ön? Välj arter och period, eller ett enskilt år."
      />
      <SelectionBar selection={selection} before="Jämför" after="under" />
      {meta && period && (
        <>
          <Section>
            <CalendarCard meta={meta} period={period} selected={species.selected} colorOf={species.colorOf} />
          </Section>
          <Section>
            <RidgesCard meta={meta} period={period} species={species.selected} colorOf={species.colorOf} nameOf={nameOf} />
          </Section>
          <Section last>
            <SpeciesTableCard
              period={period}
              defaultPeriod={meta.periods[0]!}
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
