import type { ReactNode } from "react";
import { formatDateTime } from "../lib/format";
import styles from "./ChartCard.module.css";

/** The parts of a TanStack Query result that a card needs. */
export interface CardQuery<T> {
  data: T | undefined;
  isPending: boolean;
  isError: boolean;
  /** True while older data is shown and new data loads (`keepPrevious`). */
  isPlaceholderData?: boolean;
  refetch: () => unknown;
}

export interface ChartCardProps<T> {
  title: string;
  subtitle?: ReactNode;
  /** Controls shown at the right of the title (segmented controls etc.). */
  actions?: ReactNode;
  query: CardQuery<T>;
  /** Return true when the data has nothing to draw. */
  isEmpty?: (data: T) => boolean;
  emptyMessage?: string;
  emptyAction?: { label: string; onClick: () => void };
  /** UTC timestamp of precomputed data; shows "Data från …". */
  cachedAt?: string;
  /** Skeleton shape while loading. */
  skeleton?: "bars" | "rows";
  /** Note under the chart. */
  footnote?: ReactNode;
  children: (data: T) => ReactNode;
}

/**
 * A card that loads on its own (design/components/Tillstand.dc.html): a
 * skeleton and spinner while loading, an explanation and a useful action
 * when empty, and an error that only affects this card, with a retry.
 */
export function ChartCard<T>({
  title, subtitle, actions, query, isEmpty, emptyMessage = "Det finns inga data för det här urvalet.",
  emptyAction, cachedAt, skeleton = "bars", footnote, children,
}: ChartCardProps<T>) {
  const loading = query.isPending;
  const refreshing = !loading && (query.isPlaceholderData ?? false);
  const failed = !loading && query.isError;
  const data = query.data;
  const empty = !loading && !failed && data !== undefined && (isEmpty?.(data) ?? false);

  return (
    <article className={styles.card} aria-busy={loading || refreshing}>
      <div className={styles.head}>
        <div>
          <h2 className={styles.title}>{title}</h2>
          {subtitle && <p className={styles.subtitle}>{subtitle}</p>}
        </div>
        <div className={styles.headRight}>
          {(loading || refreshing) && (
            <span className={styles.loading} role="status">
              <svg className={styles.spin} width="18" height="18" viewBox="0 0 18 18" fill="none" aria-hidden="true">
                <circle cx="9" cy="9" r="7" stroke="#DCE4D7" strokeWidth="2.5" />
                <path d="M9 2a7 7 0 0 1 7 7" stroke="var(--color-data)" strokeWidth="2.5" strokeLinecap="round" />
              </svg>
              Hämtar data …
            </span>
          )}
          {cachedAt && !loading && !refreshing && (
            <span className={styles.cached}>
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" aria-hidden="true">
                <circle cx="7" cy="7" r="5.5" />
                <path d="M7 4v3l2 1.5" />
              </svg>
              Data från {formatDateTime(cachedAt)}
            </span>
          )}
          {actions}
        </div>
      </div>

      {loading && <Skeleton shape={skeleton} />}

      {failed && (
        <div className={`${styles.state} ${styles.error}`}>
          <svg width="40" height="40" viewBox="0 0 40 40" fill="none" stroke="#A04A36" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            <path d="M20 6l16 28H4z" />
            <path d="M20 17v8M20 29v.5" />
          </svg>
          <p className={styles.message} role="alert">
            Kunde inte hämta data just nu. Övriga diagram påverkas inte.
          </p>
          <button type="button" className={`${styles.button} ${styles.outline}`} onClick={() => query.refetch()}>
            Försök igen
          </button>
        </div>
      )}

      {empty && (
        <div className={styles.state}>
          <svg width="40" height="40" viewBox="0 0 40 40" fill="none" stroke="#53605A" strokeWidth="1.6" strokeLinecap="round" aria-hidden="true">
            <circle cx="18" cy="18" r="11" />
            <path d="M26 26l8 8M14 18h8" />
          </svg>
          <p className={styles.message}>{emptyMessage}</p>
          {emptyAction && (
            <button type="button" className={`${styles.button} ${styles.solid}`} onClick={emptyAction.onClick}>
              {emptyAction.label}
            </button>
          )}
        </div>
      )}

      {!loading && !failed && !empty && data !== undefined && <div className={styles.body}>{children(data)}</div>}

      {footnote && !loading && !failed && <p className={styles.footnote}>{footnote}</p>}
    </article>
  );
}

function Skeleton({ shape }: { shape: "bars" | "rows" }) {
  if (shape === "rows") {
    return (
      <div className={styles.skeletonRows} aria-hidden="true">
        {[90, 75, 62, 54, 47, 40].map((w, i) => (
          <div key={i} className={styles.sk} style={{ width: `${w}%`, height: 16 }} />
        ))}
      </div>
    );
  }
  return (
    <div aria-hidden="true">
      <div className={styles.sk} style={{ width: "40%", height: 14, marginBottom: 16 }} />
      <div className={styles.skeletonBars}>
        {[40, 65, 50, 80, 60, 72, 45, 58].map((h, i) => (
          <div key={i} className={styles.sk} style={{ flex: 1, height: `${h}%` }} />
        ))}
      </div>
    </div>
  );
}
