export interface LegendItem {
  label: string;
  color: string;
  /** "dot" for series, "line" for lines, "dashed" for comparison lines, "band" for areas. */
  shape?: "dot" | "line" | "dashed" | "band";
}

function Swatch({ color, shape = "dot" }: Pick<LegendItem, "color" | "shape">) {
  if (shape === "line" || shape === "dashed") {
    return (
      <svg width="22" height="10" aria-hidden="true">
        <line x1="1" y1="5" x2="21" y2="5" stroke={color} strokeWidth="2.5" strokeDasharray={shape === "dashed" ? "4 3" : undefined} strokeLinecap="round" />
      </svg>
    );
  }
  if (shape === "band") {
    return <span aria-hidden="true" style={{ width: 18, height: 10, borderRadius: 3, background: color }} />;
  }
  return <span aria-hidden="true" style={{ width: 10, height: 10, borderRadius: 3, background: color }} />;
}

export function Legend({ items }: { items: readonly LegendItem[] }) {
  return (
    <ul
      style={{
        listStyle: "none", margin: 0, padding: 0,
        display: "flex", flexWrap: "wrap", gap: "8px 18px", fontSize: 14,
      }}
    >
      {items.map((item) => (
        <li key={item.label} style={{ display: "inline-flex", alignItems: "center", gap: 7 }}>
          <Swatch color={item.color} shape={item.shape} />
          {item.label}
        </li>
      ))}
    </ul>
  );
}
