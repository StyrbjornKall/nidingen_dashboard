import { useId, useState } from "react";
import { PageHeader, Section } from "../../components/PageFrame";
import { useApi, useMeta, type Schemas } from "../../lib/api";
import gofLogo from "../../assets/gof-logo.png";
import nidingenLogo from "../../assets/nidingen-logo.png";
import { faq, glossary, materialSentence, type Facts, type Qa } from "./content";
import styles from "./Om.module.css";

const CONTACT_EMAIL = "skall@chalmers.se";
const STATION_URL = "https://nidingen.gof.nu/";

/**
 * Om webbplatsen: what the site is, who it is for, contact, a glossary and
 * the frequently asked questions. Numbers, years and the season boundaries
 * come from the API; the week and weather texts are the decided wording.
 */
export default function Om() {
  const meta = useMeta().data;
  const kpis = useApi<Schemas["Kpis"]>("/api/summary/kpis").data;

  const facts: Facts | undefined =
    meta && kpis
      ? {
          nNew: kpis.n_new,
          nRecap: kpis.n_recap,
          nRecoveries: kpis.n_recoveries,
          nSpecies: kpis.n_species,
          firstYear: meta.first_year,
          currentYear: meta.current_year,
          completeThrough: meta.complete_through,
          maxCompare: meta.max_compare_species,
          gap: { yearFrom: meta.gap_years.year_from, yearTo: meta.gap_years.year_to, reason: meta.gap_years.reason },
          seasons: meta.seasons.map((s) => ({ id: s.id, months: s.months })),
          weatherNote: meta.weather_window.note,
          ringingMaxDate: meta.build.ringing_max_date,
          builtAtUtc: meta.build.built_at_utc,
        }
      : undefined;

  return (
    <>
      <PageHeader title="Om webbplatsen" />
      <Section>
        <div className={styles.intro}>
          <div className={styles.text}>
            <p className={styles.lead}>
              Webbplatsen gör Nidingens Fågelstations ringmärkningsdata från {meta?.first_year ?? "början"} och framåt tillgänglig för alla som är nyfikna på
              fåglarna som passerar ön.
            </p>
            {facts && <p>{materialSentence(facts)}</p>}
            <h2 className={styles.h2}>Vem är sidan till för?</h2>
            <p>
              För fågelintresserade som vill se hur det går för sina favoritarter och för stationens egna ringmärkare som vill jämföra årets säsong med
              tidigare år.
            </p>
            <h2 className={styles.h2}>Källor</h2>
            <ul className={styles.sources}>
              <li>Ringmärkningsdata och kontroller: Nidingens Fågelstation, genom Ringmärkningscentralen.</li>
              <li>Återfynd: Ringmärkningscentralen vid Naturhistoriska riksmuseet.</li>
              <li>Väder: SMHI öppna data, stationerna Nidingen A och Vinga A.</li>
              <li>Kustlinjer på jordklotet: Natural Earth.</li>
            </ul>
          </div>

          <aside className={styles.contact} aria-labelledby="kontakt">
            <div className={styles.logos}>
              <img src={nidingenLogo} alt="Nidingens Fågelstations logotyp" width={72} height={72} />
              <img src={gofLogo} alt="Göteborgs Ornitologiska Förenings logotyp" width={72} height={72} />
            </div>
            <h2 id="kontakt" className={styles.contactTitle}>Kontakt</h2>
            <div className={styles.contactBody}>
              <p>
                <strong>Denna webbplats</strong>
                <br />
                Styrbjörn Käll
                <br />
                <a href={`mailto:${CONTACT_EMAIL}`}>{CONTACT_EMAIL}</a>
              </p>
              <p>
                <strong>Nidingens Fågelstation</strong>
                <br />
                Se <a href={STATION_URL}>{STATION_URL.replace("https://", "")}</a>
              </p>
            </div>
            <p className={styles.report}>
              Har du hittat en ringmärkt fågel? Rapportera den till Ringmärkningscentralen vid{" "}
              <a href="https://www.nrm.se/">Naturhistoriska riksmuseet</a>.
            </p>
          </aside>
        </div>
      </Section>

      <Section>
        <h2 className={styles.sectionTitle}>Ordlista</h2>
        <dl className={styles.terms}>
          {(facts ? glossary(facts) : []).map((t) => (
            <div key={t.term} className={styles.term}>
              <dt>{t.term}</dt>
              <dd>{t.text}</dd>
            </div>
          ))}
        </dl>
      </Section>

      <Section last>
        <h2 className={styles.sectionTitle}>Vanliga frågor</h2>
        <Faq items={facts ? faq(facts) : []} />
      </Section>
    </>
  );
}

/** An accordion: one answer open at a time (the first at the start), each question a button. */
function Faq({ items }: { items: readonly Qa[] }) {
  const [open, setOpen] = useState<string | null | undefined>(undefined);
  const base = useId();
  const current = open === undefined ? items[0]?.id : open;
  return (
    <div className={styles.faq}>
      {items.map((item) => {
        const isOpen = current === item.id;
        const panel = `${base}-${item.id}`;
        return (
          <div key={item.id} className={styles.faqItem}>
            <h3 className={styles.faqHead}>
              <button type="button" className={styles.question} aria-expanded={isOpen} aria-controls={panel} onClick={() => setOpen(isOpen ? null : item.id)}>
                <span>{item.q}</span>
                <svg className={styles.chev} width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                  <path d="M5 8l5 5 5-5" />
                </svg>
              </button>
            </h3>
            {isOpen && (
              <p id={panel} className={styles.answer}>
                {item.a}
              </p>
            )}
          </div>
        );
      })}
    </div>
  );
}
