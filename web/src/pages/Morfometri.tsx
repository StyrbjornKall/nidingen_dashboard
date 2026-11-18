import { PageHeader, Section } from "../components/PageFrame";
import { Placeholder } from "./shared/Placeholder";
import { SelectionBar, usePageSelection } from "./shared/selection";

const DEFAULTS = ["RÖHAK", "ÄNPIP", "SKPIP"];
const FALLBACK = ["RÖHAK"];
const SEASONS = ["var", "host", "helar"] as const;

export default function Morfometri() {
  const selection = usePageSelection({ defaults: DEFAULTS, fallback: FALLBACK, seasons: SEASONS, defaultSeason: "host" });
  return (
    <>
      <PageHeader
        title="Morfometri"
        lead="Hur ser fåglarna ut när de fångas här? Vikt, vinglängd, ålder och kön för de arter du valt, mot alla andra arter."
      />
      <SelectionBar selection={selection} before="Jämför" after="fångade" showPeriod={false} seasonLabels={{ helar: "Båda" }} />
      <Section>
        <Placeholder title="Vikt mot vinglängd, alla arter" />
      </Section>
      <Section last>
        <Placeholder title="Fördelningar" />
      </Section>
    </>
  );
}
