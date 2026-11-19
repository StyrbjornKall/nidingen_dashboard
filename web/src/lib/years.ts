/** Year helpers for generated captions. */

/** [1980, 1981, 1982, 1990] -> "1980–1982 samt 1990"; three runs: "a, b samt c". */
export function yearRuns(years: readonly number[]): string {
  const runs: [number, number][] = [];
  for (const y of [...years].sort((a, b) => a - b)) {
    const last = runs[runs.length - 1];
    if (last && last[1] === y - 1) last[1] = y;
    else runs.push([y, y]);
  }
  const parts = runs.map(([a, b]) => (a === b ? String(a) : `${a}–${b}`));
  if (parts.length <= 1) return parts[0] ?? "";
  return `${parts.slice(0, -1).join(", ")} samt ${parts[parts.length - 1]}`;
}

/** Years a visitor can pick: newest first, gap years left out. */
export function pickableYears(first: number, last: number, gap: { year_from: number; year_to: number }): number[] {
  const out: number[] = [];
  for (let y = last; y >= first; y--) if (y < gap.year_from || y > gap.year_to) out.push(y);
  return out;
}
