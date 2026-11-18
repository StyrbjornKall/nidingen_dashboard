import type { ReactNode } from "react";
import styles from "./PageFrame.module.css";

/** Page title and introduction, plus optional key figures underneath. */
export function PageHeader({ title, lead, children }: { title: string; lead?: ReactNode; children?: ReactNode }) {
  return (
    <header className={`container ${styles.header}`}>
      <h1 className={styles.title} tabIndex={-1} data-page-title>
        {title}
      </h1>
      {lead && <p className={styles.lead}>{lead}</p>}
      {children}
    </header>
  );
}

/**
 * The bar under the header that holds the page's selection ("Visar … under …").
 * It sticks to the top of the window while scrolling.
 */
export function StickyBar({ children, label }: { children: ReactNode; label: string }) {
  return (
    <div className={styles.sticky} role="region" aria-label={label}>
      <div className={`container ${styles.stickyInner}`}>{children}</div>
    </div>
  );
}

/** A connecting word in the selection bar ("Visar", "under"). */
export function BarWord({ children }: { children: ReactNode }) {
  return <span className={styles.word}>{children}</span>;
}

export function Section({ children, last = false }: { children: ReactNode; last?: boolean }) {
  return <section className={`container ${styles.section} ${last ? styles.last : ""}`}>{children}</section>;
}
