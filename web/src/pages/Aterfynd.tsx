import { PageHeader, Section } from "../components/PageFrame";
import { Placeholder } from "./shared/Placeholder";

export default function Aterfynd() {
  return (
    <>
      <PageHeader
        title="Återfynd"
        lead="Fåglar märkta på Nidingen som hittats någon annanstans, och fåglar märkta utomlands som fångats här. Dra i jordklotet för att snurra det."
      />
      <Section last>
        <Placeholder title="Återfynd på jordklotet" />
      </Section>
    </>
  );
}
