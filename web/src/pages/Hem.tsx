import { Link } from "react-router";
import { PageHeader, Section } from "../components/PageFrame";
import { PAGES } from "./registry";
import { Placeholder } from "./shared/Placeholder";

export default function Hem() {
  return (
    <>
      <PageHeader
        title="Nidingens Fågelstation"
        lead="Ringmärkningsdata från Nidingen sedan 1980: vilka arter som fångas, hur antalen förändras, när fåglarna passerar och vart de tar vägen."
      />
      <Section last>
        <Placeholder title="Startsidan">
          Bilden, nyckeltalen och ingångarna till sidorna byggs i nästa steg. Till dess finns sidorna i menyn:{" "}
          {PAGES.filter((p) => p.path !== "/").map((p, i) => (
            <span key={p.path}>
              {i > 0 && ", "}
              <Link to={p.path}>{p.label}</Link>
            </span>
          ))}
          .
        </Placeholder>
      </Section>
    </>
  );
}
