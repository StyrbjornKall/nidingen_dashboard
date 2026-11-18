import { PageHeader, Section } from "../components/PageFrame";
import { useMeta } from "../lib/api";
import { Placeholder } from "./shared/Placeholder";

export default function Vader() {
  const note = useMeta().data?.weather_window.note;
  return (
    <>
      <PageHeader
        title="Väder"
        lead="Väder från SMHI:s station Nidingen A, dag för dag bredvid fångsten. Ser du toppar i fångsten efter ostliga vindar eller en passerande front?"
      />
      <Section last>
        <Placeholder title="Väder och fångst dag för dag">{note}</Placeholder>
      </Section>
    </>
  );
}
