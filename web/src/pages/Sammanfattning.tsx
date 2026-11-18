import { PageHeader, Section } from "../components/PageFrame";
import { ALL_SPECIES } from "../lib/urlState";
import { Placeholder } from "./shared/Placeholder";
import { SelectionBar, usePageSelection } from "./shared/selection";

const DEFAULTS = [ALL_SPECIES];

export default function Sammanfattning() {
  const selection = usePageSelection({ defaults: DEFAULTS, fallback: DEFAULTS });
  return (
    <>
      <PageHeader
        title="Sammanfattning"
        lead="En överblick över all ringmärkning på Nidingen. Börja här, välj sedan arter att jämföra."
      />
      <SelectionBar selection={selection} before="Visar" after="under" />
      <Section>
        <Placeholder title="Ringmärkta över tid" />
      </Section>
      <Section last>
        <Placeholder title="Flest ringmärkta arter" />
      </Section>
    </>
  );
}
