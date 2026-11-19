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
