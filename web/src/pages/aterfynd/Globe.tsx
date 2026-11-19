import { geoDistance, geoGraticule10, geoOrthographic, geoPath, type GeoPermissibleObjects } from "d3-geo";
import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent, type PointerEvent } from "react";
import { feature } from "topojson-client";
import type { GeometryCollection, Topology } from "topojson-specification";
import { Tooltip, type TipState } from "../../components/charts/Plot";
import { formatDate } from "../../lib/format";
import { color } from "../../lib/tokens";
import styles from "./Aterfynd.module.css";
import { flyView, dragView, kmText, nearestIndex, NIDINGEN, placeText, zoomBy, type Find, type View } from "./globe";

/** Drawing units: the globe is laid out on a 600 × 600 square, scaled to the screen. */
const UNITS = 600;
const RADIUS = 286;
const HOME: View = { lon: NIDINGEN[0], lat: 50, zoom: 1.6 };
const OVERVIEW = { lon: 8, lat: 30 };
/** Above this many finds, the lines are left out while the globe moves (they are the costly part). */
const MANY_LINES = 1200;

/**
 * Natural Earth land (world-atlas) at two levels of detail, each loaded on
 * demand: 1:110m for the first view, 1:50m (170 kB gzipped) once zoomed in.
 * The 1:10m file is not used: it paints the whole canvas as land at high zoom.
 */
const LAND_LEVELS = [
  { from: 0, load: () => import("world-atlas/land-110m.json") },
  { from: 2.5, load: () => import("world-atlas/land-50m.json") },
];
const landLoaded = new Map<number, GeoPermissibleObjects>();
const landLoading = new Set<number>();

/** Start loading the levels this zoom needs; `done` runs after each one arrives. */
function ensureLand(zoom: number, done: () => void) {
  LAND_LEVELS.forEach((level, i) => {
    if (zoom < level.from || landLoaded.has(i) || landLoading.has(i)) return;
    landLoading.add(i);
    level.load().then((m) => {
      const topo = (m.default ?? m) as unknown as Topology<{ land: GeometryCollection }>;
      landLoaded.set(i, feature(topo, topo.objects.land) as GeoPermissibleObjects);
      landLoading.delete(i);
      done();
    });
  });
}

/** The most detailed land loaded that suits this zoom. */
function bestLand(zoom: number): GeoPermissibleObjects | undefined {
  for (let i = LAND_LEVELS.length - 1; i >= 0; i--) {
    if (zoom >= LAND_LEVELS[i]!.from && landLoaded.has(i)) return landLoaded.get(i);
  }
  return landLoaded.get(0);
}

const reducedMotion = () => typeof window !== "undefined" && window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;

/**
 * A draggable orthographic globe drawn on a canvas (thousands of points
 * stay smooth). Outbound finds are green, inbound ones red; lines follow
 * the great circle to Nidingen. Pointer: drag to turn, hover to read a
 * find, tap or click to pick it. Keyboard: arrows turn, + and − zoom.
 */
export function Globe({
  points, lines, focus, onFocus, nameOf, fly,
}: {
  points: readonly Find[];
  lines: boolean;
  /** Index of the picked find, or −1. */
  focus: number;
  onFocus: (i: number) => void;
  nameOf: (code: string) => string;
  /** Turn to this place when `key` changes. */
  fly: { lon: number; lat: number; key: number } | null;
}) {
  const stage = useRef<HTMLDivElement>(null);
  const canvas = useRef<HTMLCanvasElement>(null);
  const nid = useRef<HTMLDivElement>(null);
  const [landReady, setLandReady] = useState(landLoaded.has(0));
  const [tip, setTip] = useState<TipState>(null);

  const view = useRef<View>({ lon: OVERVIEW.lon + 110, lat: 20, zoom: 1 });
  const size = useRef(0);
  const hit = useRef<{ x: number; y: number; i: number }[]>([]);
  const hover = useRef(-1);
  const moving = useRef(false);
  const frame = useRef(0);
  const anim = useRef(0);

  const lineShapes = useMemo(() => {
    const make = (dir: "out" | "in") => ({
      type: "MultiLineString" as const,
      coordinates: points.filter((p) => p.direction === dir).map((p) => [NIDINGEN, [p.lon, p.lat]]),
    });
    return { out: make("out"), in: make("in") };
  }, [points]);

  // What draw() reads. Kept in a ref so the animation loop never sees stale props.
  const latest = useRef({ points, lines, focus, lineShapes });
  latest.current = { points, lines, focus, lineShapes };

  const drawRef = useRef<() => void>(() => undefined);
  const draw = useCallback(() => {
    const cv = canvas.current;
    const px = size.current;
    if (!cv || !px) return;
    const dpr = window.devicePixelRatio || 1;
    const target = Math.round(px * dpr);
    if (cv.width !== target) {
      cv.width = target;
      cv.height = target;
    }
    const ctx = cv.getContext("2d");
    if (!ctx) return;
    const { points: pts, lines: showLines, focus: picked, lineShapes: shapes } = latest.current;
    const k = UNITS / px; // units per CSS pixel
    ctx.setTransform((dpr * px) / UNITS, 0, 0, (dpr * px) / UNITS, 0, 0);
    ctx.clearRect(0, 0, UNITS, UNITS);

    const v = view.current;
    ensureLand(v.zoom, schedule);
    const ground = bestLand(v.zoom);
    const proj = geoOrthographic().scale(RADIUS * v.zoom).translate([UNITS / 2, UNITS / 2]).rotate([-v.lon, -v.lat]).clipAngle(90).precision(0.6);
    const path = geoPath(proj, ctx);

    ctx.beginPath();
    path({ type: "Sphere" });
    ctx.fillStyle = color.ocean;
    ctx.fill();

    ctx.lineJoin = "round";
    ctx.beginPath();
    path(geoGraticule10());
    ctx.strokeStyle = "rgba(255,255,255,0.6)";
    ctx.lineWidth = 0.8;
    ctx.stroke();

    if (ground) {
      ctx.beginPath();
      path(ground);
      ctx.fillStyle = color.land;
      ctx.fill();
      ctx.strokeStyle = color.chart_mean;
      ctx.globalAlpha = 0.5;
      ctx.lineWidth = 0.8;
      ctx.stroke();
      ctx.globalAlpha = 1;
    }

    ctx.beginPath();
    path({ type: "Sphere" });
    ctx.strokeStyle = color.chart_mean;
    ctx.globalAlpha = 0.35;
    ctx.lineWidth = 1.5;
    ctx.stroke();
    ctx.globalAlpha = 1;

    if (showLines && !(moving.current && pts.length > MANY_LINES)) {
      ctx.globalAlpha = 0.42;
      ctx.lineWidth = 1.3;
      for (const [shape, stroke] of [[shapes.out, color.data], [shapes.in, color.trend]] as const) {
        ctx.beginPath();
        path(shape);
        ctx.strokeStyle = stroke;
        ctx.stroke();
      }
      ctx.globalAlpha = 1;
    }

    // Points on the near side only; the hit list uses CSS pixels.
    const centre: [number, number] = [v.lon, v.lat];
    const seen: { x: number; y: number; i: number }[] = [];
    const r = 5 * k;
    // One path per direction: thousands of separate fills and strokes are slow.
    const byDirection = { out: new Path2D(), in: new Path2D() };
    pts.forEach((p, i) => {
      if (geoDistance([p.lon, p.lat], centre) >= Math.PI / 2 - 0.01) return;
      const xy = proj([p.lon, p.lat]);
      if (!xy) return;
      seen.push({ x: xy[0] / k, y: xy[1] / k, i });
      const path2d = byDirection[p.direction];
      path2d.moveTo(xy[0] + r, xy[1]);
      path2d.arc(xy[0], xy[1], r, 0, 2 * Math.PI);
    });
    for (const [dir, fill] of [["out", color.data], ["in", color.trend]] as const) {
      ctx.fillStyle = fill;
      ctx.fill(byDirection[dir]);
    }
    hit.current = seen;

    for (const i of new Set([hover.current, picked])) {
      const p = pts[i];
      if (!p || geoDistance([p.lon, p.lat], centre) >= Math.PI / 2 - 0.01) continue;
      const xy = proj([p.lon, p.lat]);
      if (!xy) continue;
      ctx.beginPath();
      ctx.arc(xy[0], xy[1], r * 1.7, 0, 2 * Math.PI);
      ctx.fillStyle = p.direction === "out" ? color.data : color.trend;
      ctx.fill();
      ctx.lineWidth = 2.5 * k;
      ctx.strokeStyle = color.ink;
      ctx.stroke();
    }

    // The station marker is an HTML element (it pulses with CSS).
    const marker = nid.current;
    if (marker) {
      const xy = proj(NIDINGEN);
      const visible = !!xy && geoDistance(NIDINGEN, centre) < Math.PI / 2 - 0.01;
      marker.style.display = visible ? "" : "none";
      if (visible && xy) marker.style.transform = `translate(${xy[0] / k}px, ${xy[1] / k}px)`;
    }
  }, []);

  const schedule = useCallback(() => {
    if (frame.current) return;
    frame.current = requestAnimationFrame(() => {
      frame.current = 0;
      drawRef.current();
    });
  }, []);

  drawRef.current = draw;

  const flyTo = useCallback(
    (to: { lon: number; lat: number; zoom?: number }, ms: number) => {
      cancelAnimationFrame(anim.current);
      const from = { ...view.current };
      if (ms <= 0 || reducedMotion()) {
        view.current = flyView(from, to, 1);
        moving.current = false;
        schedule();
        return;
      }
      const t0 = performance.now();
      moving.current = true;
      const step = (now: number) => {
        const t = Math.min(1, (now - t0) / ms);
        view.current = flyView(from, to, t);
        if (t < 1) anim.current = requestAnimationFrame(step);
        else moving.current = false;
        schedule();
      };
      anim.current = requestAnimationFrame(step);
    },
    [schedule],
  );

  // Size, and the first turn towards the overview.
  useEffect(() => {
    const el = stage.current;
    if (!el) return;
    const measure = () => {
      size.current = el.clientWidth;
      schedule();
    };
    measure();
    const ro = typeof ResizeObserver === "undefined" ? undefined : new ResizeObserver(measure);
    ro?.observe(el);
    flyTo(OVERVIEW, 1600);
    return () => {
      ro?.disconnect();
      cancelAnimationFrame(anim.current);
      cancelAnimationFrame(frame.current);
    };
  }, [flyTo, schedule]);

  useEffect(() => {
    if (!landReady) ensureLand(0, () => setLandReady(true));
  }, [landReady]);

  useEffect(schedule, [points, lines, focus, landReady, schedule]);

  const flyKey = fly?.key;
  useEffect(() => {
    if (fly) flyTo({ lon: fly.lon, lat: fly.lat }, 1100);
    // Only a new key means a new request.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flyKey]);

  // --- Pointer and keyboard ---------------------------------------------------

  const drag = useRef<{ x: number; y: number; view: View; moved: boolean } | null>(null);

  const pickAt = (e: PointerEvent<HTMLDivElement>, reach: number) => {
    const r = e.currentTarget.getBoundingClientRect();
    return nearestIndex(hit.current, e.clientX - r.left, e.clientY - r.top, reach);
  };

  const showTip = (i: number, x: number, y: number) => {
    const p = latest.current.points[i];
    if (!p) return setTip(null);
    setTip({
      x,
      y,
      lines: (
        <>
          <strong>{nameOf(p.species)}</strong>
          <br />
          {placeText(p)}
          <br />
          <span className="mono">
            {kmText(p.distance_km)} · {p.direction === "out" ? formatDate(p.find_date, true) : `märkt ${p.ring_date ? formatDate(p.ring_date, true) : "okänt datum"}`}
          </span>
        </>
      ),
    });
  };

  const onDown = (e: PointerEvent<HTMLDivElement>) => {
    cancelAnimationFrame(anim.current);
    e.currentTarget.setPointerCapture?.(e.pointerId);
    drag.current = { x: e.clientX, y: e.clientY, view: { ...view.current }, moved: false };
  };

  const onMove = (e: PointerEvent<HTMLDivElement>) => {
    const d = drag.current;
    if (d) {
      const dx = e.clientX - d.x;
      const dy = e.clientY - d.y;
      if (!d.moved && Math.hypot(dx, dy) > 4) {
        d.moved = true;
        moving.current = true;
        hover.current = -1;
        setTip(null);
      }
      if (d.moved) {
        view.current = dragView(d.view, dx, dy, UNITS / size.current);
        schedule();
      }
      return;
    }
    if (e.pointerType === "touch") return;
    const i = pickAt(e, 9);
    if (i !== hover.current) {
      hover.current = i;
      schedule();
    }
    if (i >= 0) showTip(i, e.clientX, e.clientY);
    else setTip(null);
  };

  const onUp = (e: PointerEvent<HTMLDivElement>) => {
    const d = drag.current;
    drag.current = null;
    if (!d) return;
    if (d.moved) {
      moving.current = false;
      schedule();
    } else {
      const i = pickAt(e, e.pointerType === "touch" ? 16 : 10);
      if (i >= 0) onFocus(i);
    }
  };

  const onLeave = () => {
    if (hover.current !== -1) {
      hover.current = -1;
      schedule();
    }
    setTip(null);
  };

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const step = 12 / view.current.zoom;
    const v = view.current;
    const next: View | undefined =
      e.key === "ArrowLeft" ? { ...v, lon: v.lon - step }
      : e.key === "ArrowRight" ? { ...v, lon: v.lon + step }
      : e.key === "ArrowUp" ? { ...v, lat: Math.min(75, v.lat + step) }
      : e.key === "ArrowDown" ? { ...v, lat: Math.max(-75, v.lat - step) }
      : e.key === "+" || e.key === "=" ? zoomBy(v, 1.4)
      : e.key === "-" || e.key === "_" ? zoomBy(v, 1 / 1.4)
      : e.key === "Home" ? HOME
      : undefined;
    if (!next) return;
    e.preventDefault();
    view.current = next;
    schedule();
  };

  return (
    <div className={styles.globeBody}>
      <div
        ref={stage}
        className={styles.stage}
        role="group"
        aria-label="Jordklot med återfynd. Piltangenterna snurrar det, plus och minus zoomar. Återfynden finns också som lista till höger."
        tabIndex={0}
        onPointerDown={onDown}
        onPointerMove={onMove}
        onPointerUp={onUp}
        onPointerCancel={onUp}
        onPointerLeave={onLeave}
        onKeyDown={onKey}
      >
        {!landReady && <div className={styles.loading}>Laddar jordklotet …</div>}
        <canvas ref={canvas} className={styles.canvas} aria-hidden="true" />
        <div ref={nid} className={styles.nid} aria-hidden="true">
          <span className={styles.nidPulse} />
          <span className={styles.nidDot} />
          <span className={styles.nidLabel}>Nidingen</span>
        </div>
      </div>
      <div className={styles.globeButtons}>
        <button type="button" className={styles.globeButton} aria-label="Zooma in" onClick={() => { view.current = zoomBy(view.current, 1.4); schedule(); }}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true"><path d="M8 3v10M3 8h10" /></svg>
        </button>
        <button type="button" className={styles.globeButton} aria-label="Zooma ut" onClick={() => { view.current = zoomBy(view.current, 1 / 1.4); schedule(); }}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true"><path d="M3 8h10" /></svg>
        </button>
        <button type="button" className={styles.globeButton} onClick={() => flyTo(HOME, 900)}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true"><circle cx="8" cy="8" r="5.5" /><circle cx="8" cy="8" r="1.5" /><path d="M8 .5V3M8 13v2.5M.5 8H3M13 8h2.5" /></svg>
          Nidingen
        </button>
        <span className={styles.globeHint}>Dra för att snurra. Klicka på ett återfynd, eller välj ett i listan, för att läsa om det.</span>
      </div>
      <Tooltip tip={tip} />
    </div>
  );
}
