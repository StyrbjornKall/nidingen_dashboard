import { useCallback, useMemo } from "react";
import { KeyFigures } from "../../components/KeyFigures";
import { PageHeader, Section } from "../../components/PageFrame";
import { useApi, type Schemas } from "../../lib/api";
import { formatInt } from "../../lib/format";
import { ALL_SPECIES } from "../../lib/urlState";
import { SelectionBar, usePageSelection } from "../shared/selection";
import { TimeSeriesCard } from "./TimeSeriesCard";
import { TopSpeciesCard } from "./TopSpeciesCard";

const DEFAULTS = [ALL_SPECIES];

export default function Sammanfattning() {
  const selection = usePageSelection({ defaults: DEFAULTS, fallback: DEFAULTS, singleYear: true });
  const { meta, species, period } = selection;
  const kpis = useApi<Schemas["Kpis"]>("/api/summary/kpis").data;

  const names = useMemo(() => new Map(meta?.species.map((s) => [s.code, s.name]) ?? []), [meta]);
  const nameOf = useCallback((code: string) => names.get(code) ?? code, [names]);

  const fmt = (n: number | undefined) => (n === undefined ? undefined : formatInt(n));

  return (
    <>
      <PageHeader
        title="Sammanfattning"
        lead="En överblick över all ringmärkning på Nidingen. Börja här, välj sedan arter att jämföra."
      >
        <KeyFigures
          label="Nyckeltal"
          items={[
            { value: fmt(kpis?.n_new), label: "ringmärkta fåglar" },
            { value: fmt(kpis?.n_recap), label: "kontroller" },
            { value: fmt(kpis?.n_recoveries), label: "återfynd världen över" },
            { value: fmt(kpis?.n_species), label: "unika arter och raser" },
          ]}
        />
      </PageHeader>
      <SelectionBar selection={selection} before="Visar" after="under" />
      {meta && period && (
        <>
          <Section>
            <TimeSeriesCard meta={meta} period={period} species={species.selected} colorOf={species.colorOf} nameOf={nameOf} />
          </Section>
          <Section last>
            <TopSpeciesCard
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
