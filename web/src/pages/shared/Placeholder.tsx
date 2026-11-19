import styles from "../../components/ChartCard.module.css";

/** Stands in for a page's charts until that page is built (HANDOFF.md §9, phase 5). */
export function Placeholder({ title, children }: { title: string; children?: React.ReactNode }) {
  return (
    <article className={styles.card}>
      <h2 className={styles.title}>{title}</h2>
      <div className={styles.state}>
        <p className={styles.message}>{children ?? "Den här delen av sidan byggs i nästa steg."}</p>
      </div>
    </article>
  );
}
