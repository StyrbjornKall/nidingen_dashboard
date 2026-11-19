import { useId } from "react";
import styles from "./YearSelect.module.css";

/**
 * "Välj specifikt år": a dropdown beside the period presets. `value` is the
 * chosen year, or null while a preset period is active.
 */
export function YearSelect({
  years, value, onChange, label = "Välj specifikt år",
}: {
  years: readonly number[];
  value: number | null;
  onChange: (year: number) => void;
  label?: string;
}) {
  const id = useId();
  return (
    <>
      <label htmlFor={id} className="visually-hidden">
        {label}
      </label>
      <select
        id={id}
        className={styles.select}
        data-active={value !== null}
        value={value === null ? "" : String(value)}
        onChange={(e) => e.target.value && onChange(Number(e.target.value))}
      >
        <option value="">{label}</option>
        {years.map((y) => (
          <option key={y} value={String(y)}>
            {y}
          </option>
        ))}
      </select>
    </>
  );
}
