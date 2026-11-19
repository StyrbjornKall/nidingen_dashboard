import { useEffect, useId, useMemo, useRef, useState } from "react";
import type { SpeciesInfo } from "../lib/api";
import { speciesPalette } from "../lib/tokens";
import { ALL_SPECIES } from "../lib/urlState";
import styles from "./SpeciesPicker.module.css";

export interface SpeciesPickerProps {
  /** Species that can be picked, in display order. */
  species: readonly SpeciesInfo[];
  /** Selected codes, in selection order (colours follow this order). */
  selected: readonly string[];
  onToggle: (code: string) => void;
  max?: number;
  /** Whether "Alla arter" (ALLA) can be picked. */
  allowAll?: boolean;
  startOpen?: boolean;
}

/** Search on code, Swedish and scientific name, case-insensitively. */
export function matchesQuery(sp: Pick<SpeciesInfo, "code" | "name" | "name_sci">, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return `${sp.code} ${sp.name} ${sp.name_sci ?? ""}`.toLowerCase().includes(q);
}

/**
 * Chips for the selected species (each in its fixed colour) and a button
 * that opens a searchable list. Up to `max` species; when full, the other
 * rows are disabled until one is removed.
 */
export function SpeciesPicker({ species, selected, onToggle, max = 6, allowAll = true, startOpen = false }: SpeciesPickerProps) {
  const [open, setOpen] = useState(startOpen);
  const [query, setQuery] = useState("");
  const rootRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const panelId = useId();
  const inputId = useId();

  const byCode = useMemo(() => new Map(species.map((s) => [s.code, s])), [species]);
  const options = useMemo(
    () => species.filter((s) => (allowAll || s.code !== ALL_SPECIES) && matchesQuery(s, query)),
    [species, allowAll, query],
  );
  const full = selected.length >= max;

  const close = (returnFocus = true) => {
    setOpen(false);
    setQuery("");
    if (returnFocus) buttonRef.current?.focus();
  };

  useEffect(() => {
    if (!open) return;
    inputRef.current?.focus();
    const onPointer = (e: PointerEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) close(false);
    };
    document.addEventListener("pointerdown", onPointer);
    return () => document.removeEventListener("pointerdown", onPointer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  return (
    <div
      ref={rootRef}
      className={styles.root}
      onKeyDown={(e) => {
        if (e.key === "Escape" && open) {
          e.stopPropagation();
          close();
        }
      }}
    >
      <div className={styles.chips}>
        {selected.map((code, i) => {
          const sp = byCode.get(code);
          const name = sp?.name ?? code;
          const color = speciesPalette[i % speciesPalette.length];
          return (
            <span key={code} className={styles.chip} style={{ borderColor: color }}>
              <span className={styles.dot} style={{ background: color }} aria-hidden="true" />
              {name}
              <button type="button" className={styles.remove} aria-label={`Ta bort ${name}`} onClick={() => onToggle(code)}>
                <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true">
                  <path d="M2 2l8 8M10 2l-8 8" />
                </svg>
              </button>
            </span>
          );
        })}
        <button
          ref={buttonRef}
          type="button"
          className={styles.add}
          aria-expanded={open}
          aria-controls={panelId}
          onClick={() => (open ? close() : setOpen(true))}
        >
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true">
            <path d="M7 2v10M2 7h10" />
          </svg>
          {selected.length ? "Lägg till art" : "Välj art"}
        </button>
      </div>

      {open && (
        <div id={panelId} className={styles.panel} role="dialog" aria-label="Välj arter">
          <label htmlFor={inputId} className={styles.label}>
            Sök art
          </label>
          <input
            ref={inputRef}
            id={inputId}
            type="search"
            className={styles.input}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Namn, ringkod eller latinskt namn"
            autoComplete="off"
          />
          <div className={styles.list}>
            {options.map((sp) => {
              const index = selected.indexOf(sp.code);
              const on = index >= 0;
              const disabled = !on && full;
              const color = on ? speciesPalette[index % speciesPalette.length] : undefined;
              return (
                <button
                  key={sp.code}
                  type="button"
                  className={styles.row}
                  aria-pressed={on}
                  disabled={disabled}
                  onClick={() => onToggle(sp.code)}
                >
                  <span className={styles.box} style={on ? { background: color, borderColor: color } : undefined} aria-hidden="true">
                    {on && (
                      <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M2.5 6.5l2.5 2.5 4.5-6" />
                      </svg>
                    )}
                  </span>
                  <span className={styles.names}>
                    <span className={styles.name}>{sp.name}</span>
                    <span className={styles.sci}>{sp.code === ALL_SPECIES ? "Summa av samtliga arter" : sp.name_sci ?? ""}</span>
                  </span>
                  <span className={`mono ${styles.code}`}>{sp.code === ALL_SPECIES ? "Σ" : sp.code}</span>
                </button>
              );
            })}
            {options.length === 0 && <p className={styles.empty}>Ingen art matchar.</p>}
          </div>
          <div className={styles.footer}>
            <span className={styles.count} aria-live="polite">
              {selected.length} av {max} valda{full ? " — ta bort en för att byta" : ""}
            </span>
            <button type="button" className={styles.done} onClick={() => close()}>
              Klar
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
