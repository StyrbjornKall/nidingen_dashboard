import styles from "./KeyFigures.module.css";

export interface KeyFigure {
  /** Already formatted, e.g. "395 777"; undefined while loading. */
  value: string | undefined;
  label: string;
}

/** A row of key figures in white tiles (design: Sammanfattning header). */
export function KeyFigures({ items, label }: { items: readonly KeyFigure[]; label: string }) {
  const loading = items.some((i) => i.value === undefined);
  return (
    <dl className={styles.grid} aria-label={label} aria-busy={loading}>
      {items.map((item) => (
        <div key={item.label} className={styles.tile}>
          <dt className={styles.label}>{item.label}</dt>
          <dd className={`mono ${styles.value}`}>
            {item.value ?? <span className={styles.placeholder} aria-label="Laddar" />}
          </dd>
        </div>
      ))}
    </dl>
  );
}
