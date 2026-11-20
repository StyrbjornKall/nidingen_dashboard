import { Fragment, useMemo, useState, type PointerEvent } from "react";
import { ChartCard, type CardQuery } from "../../components/ChartCard";
import { Tooltip, type TipState } from "../../components/charts/Plot";
import { needsLightText } from "../../lib/color";
import type { Schemas } from "../../lib/api";
import { formatInt } from "../../lib/format";
import { ageRamp, color, sexColors } from "../../lib/tokens";
import styles from "./Biometri.module.css";
import { ageColors, ageLabel, sexLabel, sharePercent, sortAgeCodes, sortSexCodes } from "./morph";

type Category = Schemas["Category"];

interface Seg {
  key: string;
  label: string;
  n: number;
  share: number;
  fill: string;
  text: string;
}

const UNKNOWN_BORDER = "1px solid var(--color-line)";

/** Fill for a sex code: the design's colours for M, F and missing, tints of the mean colour for rare codes. */
function sexFill(code: string | null, rare: number): string {
  if (code === "M") return sexColors.Hane;
  if (code === "F") return sexColors.Hona;
  if (code === null) return sexColors.Okänd;
  return `color-mix(in srgb, ${color.chart_mean} ${[70, 48, 32][rare % 3]}%, white)`;
}

/**
 * "Ålder och kön": share of the ringed birds per raw age code and per sex
 * code, as stacked bars. Age codes are shown as stored, never grouped; a
 * missing value is "Okänd".
 */
export function AgeSexCard({
  query, colorOf, nameOf, season,
}: {
  query: CardQuery<Schemas["MorphDetails"]>;
  colorOf: (c: string) => string | undefined;
  nameOf: (c: string) => string;
  season: string;
}) {
  const [tip, setTip] = useState<TipState>(null);
  const species = query.data?.species;

  // One colour per age code across all species shown, so a code looks the same in every row.
  const ageCodes = useMemo(() => {
    const set = new Set<string>();
    species?.forEach((s) => s.age.forEach((c) => c.code !== null && set.add(c.code)));
    return sortAgeCodes([...set]);
  }, [species]);
  const ageFill = useMemo(() => ageColors(ageCodes, ageRamp), [ageCodes]);
  const sexCodes = useMemo(() => {
    const set = new Set<string | null>();
    species?.forEach((s) => s.sex.forEach((c) => set.add(c.code)));
    return sortSexCodes([...set]);
  }, [species]);
  const rareRank = (code: string | null) => sexCodes.filter((c) => c !== "M" && c !== "F" && c !== null).indexOf(code as string);

  const ageSegs = (cats: readonly Category[]): Seg[] => {
    const byCode = new Map(cats.map((c) => [c.code, c]));
    return [...sortAgeCodes(cats.flatMap((c) => (c.code === null ? [] : [c.code]))), ...(byCode.has(null) ? [null] : [])].map((code) => {
      const c = byCode.get(code)!;
      const fill = code === null ? sexColors.Okänd : (ageFill.get(code) ?? sexColors.Okänd);
      return { key: String(code), label: ageLabel(code), n: c.n, share: c.share, fill, text: needsLightText(fill) ? "#fff" : color.ink };
    });
  };
  const sexSegs = (cats: readonly Category[]): Seg[] => {
    const byCode = new Map(cats.map((c) => [c.code, c]));
    return sortSexCodes(cats.map((c) => c.code)).map((code) => {
      const c = byCode.get(code)!;
      const fill = sexFill(code, rareRank(code));
      return { key: String(code), label: sexLabel(code), n: c.n, share: c.share, fill, text: needsLightText(fill) ? "#fff" : color.ink };
    });
  };

  const show = (kind: string, name: string) => (s: Seg) => ({
    onPointerMove: (e: PointerEvent) =>
      setTip({
        x: e.clientX,
        y: e.clientY,
        lines: (
          <>
            <strong>{name}</strong>
            <br />
            {kind} {s.label}
            <br />
            <span className="mono">{formatInt(s.n)} fåglar · {sharePercent(s.share)}</span>
          </>
        ),
      }),
    onPointerLeave: () => setTip(null),
  });

  return (
    <ChartCard
      title="Ålder och kön"
      subtitle="Andel av de ringmärkta fåglarna. Åldern visas med databasens egna koder, utan gruppering. Många arter går inte att könsbestämma i handen, och då står könet som okänt."
      query={query}
      skeleton="rows"
      isEmpty={(d) => d.species.length === 0}
      emptyMessage="Välj minst en art i artväljaren. Mått jämförs per art, så ”Alla arter” visas inte här."
      footnote={`Fåglar som ringmärkts under ${season}, alla år sedan 1980. Okänd = ålder eller kön är inte noterat. Segment smalare än 9 % har ingen text; håll muspekaren över dem.`}
    >
      {(data) => (
        <div className={styles.scroll}>
          <div className={styles.ageGrid}>
            <span />
            <span className={styles.colHead}>Ålder</span>
            <span className={styles.colHead}>Kön</span>
            {data.species.map((s) => {
              const name = nameOf(s.species);
              return (
                <Fragment key={s.species}>
                  <span className={styles.rowName}>
                    <span className={styles.dot} style={{ background: colorOf(s.species) ?? color.chart_mean }} aria-hidden="true" />
                    {name}
                  </span>
                  <div>
                    <span className={styles.kind} aria-hidden="true">Ålder</span>
                    <Stack segs={ageSegs(s.age)} label={`${name}, ålder`} bind={show("Ålder", name)} />
                  </div>
                  <div>
                    <span className={styles.kind} aria-hidden="true">Kön</span>
                    <Stack segs={sexSegs(s.sex)} label={`${name}, kön`} bind={show("Kön", name)} />
                  </div>
                </Fragment>
              );
            })}
            <span />
            <ul className={styles.keyList}>
              {ageCodes.map((c) => (
                <li key={c}>
                  <span className={styles.keySwatch} style={{ background: ageFill.get(c) }} aria-hidden="true" />
                  {c}
                </li>
              ))}
              {data.species.some((s) => s.age.some((c) => c.code === null)) && (
                <li>
                  <span className={styles.keySwatch} style={{ background: sexColors.Okänd, border: UNKNOWN_BORDER }} aria-hidden="true" />
                  Okänd
                </li>
              )}
            </ul>
            <ul className={styles.keyList}>
              {sexCodes.map((c) => (
                <li key={String(c)}>
                  <span className={styles.keySwatch} style={{ background: sexFill(c, rareRank(c)), border: UNKNOWN_BORDER }} aria-hidden="true" />
                  {sexLabel(c)}
                </li>
              ))}
            </ul>
          </div>
          <Tooltip tip={tip} />
        </div>
      )}
    </ChartCard>
  );
}

function Stack({
  segs, label, bind,
}: {
  segs: readonly Seg[];
  label: string;
  bind: (s: Seg) => { onPointerMove: (e: PointerEvent) => void; onPointerLeave: () => void };
}) {
  return (
    <div className={styles.stack} role="img" aria-label={`${label}: ${segs.map((s) => `${s.label} ${sharePercent(s.share)}`).join(", ")}`}>
      {segs.map((s) => (
        <span
          key={s.key}
          className={styles.seg}
          style={{ flex: `${s.n} 1 0`, background: s.fill, color: s.text, border: s.label === "Okänd" ? UNKNOWN_BORDER : undefined }}
          {...bind(s)}
        >
          {s.share >= 0.16 ? `${s.label} · ${sharePercent(s.share)}` : s.share >= 0.09 ? sharePercent(s.share) : ""}
        </span>
      ))}
    </div>
  );
}
