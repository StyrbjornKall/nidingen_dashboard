import type { Verdict } from "../lib/api";

const LABELS: Record<Verdict, string> = {
  up: "Ökar",
  down: "Minskar",
  none: "Ingen säker trend",
};

const COLORS: Record<Verdict, { background: string; color: string }> = {
  up: { background: "var(--color-badge-up-bg)", color: "var(--color-badge-up-fg)" },
  down: { background: "var(--color-badge-down-bg)", color: "var(--color-badge-down-fg)" },
  none: { background: "var(--color-badge-neutral-bg)", color: "var(--color-text-secondary)" },
};

/**
 * "Ökar" / "Minskar" when the 95 % CI excludes zero, otherwise "Ingen säker
 * trend". With a prefix: "Antal: minskar".
 */
export function TrendBadge({ verdict, prefix }: { verdict: Verdict; prefix?: string }) {
  return (
    <span
      style={{
        ...COLORS[verdict],
        display: "inline-flex",
        alignItems: "center",
        padding: "3px 10px",
        borderRadius: "var(--radius-pill)",
        fontSize: 13,
        fontWeight: 600,
        whiteSpace: "nowrap",
      }}
    >
      {prefix ? `${prefix}${LABELS[verdict].toLowerCase()}` : LABELS[verdict]}
    </span>
  );
}

export const verdictLabel = (v: Verdict) => LABELS[v];
