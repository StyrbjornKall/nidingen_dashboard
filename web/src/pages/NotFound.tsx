import { Link } from "react-router";
import { PageHeader, Section } from "../components/PageFrame";

export default function NotFound() {
  return (
    <>
      <PageHeader title="Sidan finns inte" lead="Adressen leder inte till någon sida här." />
      <Section last>
        <p>
          <Link to="/">Gå till startsidan</Link>
        </p>
      </Section>
    </>
  );
}
