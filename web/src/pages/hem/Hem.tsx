import { Link } from "react-router";
import { useApi, useMeta, type Schemas } from "../../lib/api";
import { formatApprox, formatInt } from "../../lib/format";
import hero from "../../assets/nidingen-hero.jpg";
import styles from "./Hem.module.css";

/** The entry cards: path, title, one line, and the design's 28 × 28 line icon. */
export const ENTRIES = [
  { path: "/sammanfattning", title: "Sammanfattning", icon: "M4 23V13M10 23V7M16 23v-8M22 23V4", text: "Nyckeltal, fångst över tid och vilka arter som ringmärks mest." },
  { path: "/trender", title: "Trender", icon: "M3 21l7-7 5 4 10-11M19 7h6v6", text: "Ökar eller minskar arterna? Antalet ringmärkta fåglar år för år, med osäkerhet." },
  { path: "/morfometri", title: "Morfometri", icon: "M5 22h18M14 6v16M7 10h14M7 10l-3 7h6zM21 10l-3 7h6z", text: "Vikt, vinglängd, ålder och kön. Hur ser fåglarna ut när de fångas här?" },
  { path: "/fenologi", title: "Fenologi", icon: "M4 7h20v17H4zM4 12h20M9 4v5M19 4v5", text: "När på året passerar arterna ön, vecka för vecka och dag för dag?" },
  { path: "/aterfynd", title: "Återfynd", icon: "M14 25s8-7.5 8-13a8 8 0 0 0-16 0c0 5.5 8 13 8 13zM14 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z", text: "Var hittas Nidingens fåglar, och varifrån kommer de som fångas här?" },
  { path: "/vader", title: "Väder", icon: "M3 11h14a3 3 0 1 0-3-3M3 16h19a3 3 0 1 1-3 3M3 21h8", text: "Vind, temperatur och tryck från SMHI Nidingen A, bredvid fångsten." },
  { path: "/om", title: "Om webbplatsen", icon: "M14 25a11 11 0 1 0 0-22 11 11 0 0 0 0 22zM14 13v7M14 9v.5", text: "Vem sidan är till för, ordlista och vanliga frågor." },
] as const;

/**
 * Hem: the aerial photo with a glass card, the key figures read from the
 * data, and an entry card for every page. Years, counts and the start year
 * all come from the API, never from the text.
 */
export default function Hem() {
  const meta = useMeta().data;
  const kpis = useApi<Schemas["Kpis"]>("/api/summary/kpis").data;
  const since = meta?.first_year;
  // Complete years of data: 1980–2025 is 46 years; the running year is not counted.
  const years = meta ? meta.complete_through - meta.first_year + 1 : undefined;
  const dash = "–";

  const figures = [
    { value: kpis ? formatApprox(kpis.n_new) : dash, label: "ringmärkta fåglar" },
    { value: kpis ? formatApprox(kpis.n_recap) : dash, label: "kontroller på ön" },
    { value: kpis ? formatApprox(kpis.n_recoveries) : dash, label: "återfynd världen över" },
    { value: kpis ? formatInt(kpis.n_species) : dash, label: "unika arter och raser" },
  ];

  return (
    <>
      <section className={styles.hero}>
        <img
          className={styles.heroImg}
          src={hero}
          alt="Flygfoto över Nidingen med fyrtornen, stationshusen och havet runt ön"
          width={2000}
          height={1598}
          fetchPriority="high"
        />
        <div className={`container ${styles.heroInner}`}>
          <div className={styles.glass}>
            <p className={styles.kicker}>Halland · Kattegatt</p>
            <h1 className={styles.title} tabIndex={-1} data-page-title>
              Nidingens Fågelstation
            </h1>
            <p className={styles.sub}>Historisk ringmärkningsdata{since ? ` från ${since}` : ""}</p>
            <div className={styles.actions}>
              <Link to="/sammanfattning" className={styles.primary}>
                Utforska datan
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                  <path d="M3 8h10M9 4l4 4-4 4" />
                </svg>
              </Link>
              <Link to="/om" className={styles.secondary}>
                Om webbplatsen
              </Link>
            </div>
          </div>
        </div>
      </section>

      <section className={`container ${styles.intro}`}>
        <h2 className={styles.introTitle}>{years ? `${years} år av ringmärkning på en ö i Kattegatt` : "Ringmärkning på en ö i Kattegatt"}</h2>
        <div className={styles.introText}>
          <p>
            Varje fågel som fångas på Nidingen vägs, mäts och ringmärks. Här kan du utforska hela materialet från {since ?? "början"} och framåt: hur många
            fåglar som passerar, när de flyttar och var de hittas igen.
          </p>
          <p>Välj en eller flera arter för att jämföra dem sida vid sida.</p>
        </div>
      </section>

      <section className={`container ${styles.figures}`} aria-label="Nyckeltal">
        <dl className={styles.figureGrid}>
          {figures.map((f) => (
            <div key={f.label}>
              <dt>{f.label}</dt>
              <dd className="mono">{f.value}</dd>
            </div>
          ))}
        </dl>
      </section>

      <section className={`container ${styles.entries}`}>
        <h2 className={styles.entriesTitle}>Vad vill du utforska?</h2>
        <div className={styles.cards}>
          {ENTRIES.map((e) => (
            <Link key={e.path} to={e.path} className={styles.card}>
              <span className={styles.icon} aria-hidden="true">
                <svg width="28" height="28" viewBox="0 0 28 28" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                  <path d={e.icon} />
                </svg>
              </span>
              <span className={styles.cardTitle}>{e.title}</span>
              <span className={styles.cardText}>{e.text}</span>
            </Link>
          ))}
        </div>
      </section>
    </>
  );
}
