import { PageHeader, Section } from "../components/PageFrame";
import { Placeholder } from "./shared/Placeholder";
import { SelectionBar, usePageSelection } from "./shared/selection";

const FALLBACK = ["RÖHAK"];

export default function Fenologi() {
  const selection = usePageSelection({ defaults: null, fallback: FALLBACK });
  return (
    <>
      <PageHeader
        title="Fenologi"
        lead="När på året passerar arterna ön, och hur går årets säsong jämfört med ett normalt år?"
      />
      <Section>
        <Placeholder title="Flyttningskalendern" />
      </Section>
      <div style={{ height: 24 }} />
      <SelectionBar selection={selection} before="Jämför" />
      <Section>
        <Placeholder title="När passerar arterna i detalj?" />
      </Section>
      <Section last>
        <Placeholder title="Arterna" />
      </Section>
    </>
  );
}
