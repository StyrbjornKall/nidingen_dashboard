/** Colour helpers: interpolation along a ramp of hex stops, and readable text on a fill. */

function hex(c: string): [number, number, number] {
  const n = parseInt(c.slice(1), 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
}

/** 0–1 along the stops (evenly spaced), as "rgb(r,g,b)". */
export function rampColor(stops: readonly string[], t: number): string {
  const rgb = stops.map(hex);
  if (rgb.length === 1) return `rgb(${rgb[0]!.join(",")})`;
  const x = Math.max(0, Math.min(1, t)) * (rgb.length - 1);
  const i = Math.min(rgb.length - 2, Math.floor(x));
  const u = x - i;
  const a = rgb[i]!;
  const b = rgb[i + 1]!;
  return `rgb(${a.map((v, k) => Math.round(v + (b[k]! - v) * u)).join(",")})`;
}

/** True when white text is readable on this "rgb(r,g,b)" or "#rrggbb" fill (WCAG contrast ≥ 4.5 vs white). */
export function needsLightText(fill: string): boolean {
  const m = /^rgb\((\d+),(\d+),(\d+)\)$/.exec(fill);
  const [r, g, b] = m ? [Number(m[1]), Number(m[2]), Number(m[3])] : hex(fill);
  const lin = (v: number) => {
    const c = v / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  };
  const lum = 0.2126 * lin(r!) + 0.7152 * lin(g!) + 0.0722 * lin(b!);
  return 1.05 / (lum + 0.05) >= 4.5;
}
