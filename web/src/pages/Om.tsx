import { PageHeader, Section } from "../components/PageFrame";
import { Placeholder } from "./shared/Placeholder";

export default function Om() {
  return (
    <>
      <PageHeader title="Om webbplatsen" />
      <Section last>
        <Placeholder title="Om data, ordlista och vanliga frågor" />
      </Section>
    </>
  );
}
