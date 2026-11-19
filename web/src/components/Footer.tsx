import { useMeta } from "../lib/api";
import { formatDate } from "../lib/format";
import styles from "./Footer.module.css";

export function Footer() {
  const meta = useMeta();
  const built = meta.data?.build.built_at_utc;
  return (
    <footer className={styles.footer}>
      <div className={`container ${styles.inner}`}>
        <span>Data: Nidingens Fågelstation / Ringmärkningscentralen · Väder: SMHI</span>
        {built && <span>Senast uppdaterad {formatDate(built.slice(0, 10), true)}</span>}
      </div>
    </footer>
  );
}
