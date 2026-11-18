import { PageHeader, Section } from "../components/PageFrame";
import { Placeholder } from "./shared/Placeholder";
import { SelectionBar, usePageSelection } from "./shared/selection";

const FALLBACK = ["RÖHAK"];
const SEASONS = ["var", "host", "helar"] as const;

export default function Trender() {
  const selection = usePageSelection({ defaults: null, fallback: FALLBACK, seasons: SEASONS, defaultSeason: "host" });
  return (
    <>
      <PageHeader
        title="Trender"
        lead="Ökar eller minskar arterna? Varje diagram visar ett värde per år, en anpassad trendlinje och ett skuggat band för hur säker linjen är (95 % konfidensintervall)."
      />
      <SelectionBar selection={selection} before="Jämför" after="under" />
      <Section>
        <Placeholder title="Antal ringmärkta per år, valda arter" />
      </Section>
      <Section last>
        <Placeholder title="Alla arter" />
      </Section>
    </>
  );
}
