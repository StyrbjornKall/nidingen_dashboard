import { NavLink, useLocation } from "react-router";
import logo from "../assets/nidingen-logo.png";
import { sharedSearch } from "../lib/urlState";
import { PAGES } from "../pages/registry";
import styles from "./Nav.module.css";

/** Site header: colour stripe, station logo and name, and the main menu. */
export function Nav() {
  const location = useLocation();
  // Carry the shared selection (species, period, season) between pages.
  const search = sharedSearch(location.search);
  return (
    <header className={styles.header}>
      <div className={styles.stripe} aria-hidden="true">
        <span style={{ flex: 4, background: "#C2CDBE" }} />
        <span style={{ flex: 3, background: "var(--color-data)" }} />
        <span style={{ flex: 2, background: "var(--color-trend)" }} />
        <span style={{ flex: 1, background: "var(--color-highlight)" }} />
      </div>
      <div className={`container ${styles.bar}`}>
        <NavLink to={{ pathname: "/", search }} className={styles.brand}>
          <img src={logo} alt="" width={46} height={46} className={styles.logo} />
          <span className={styles.brandText}>
            <span className={styles.brandName}>Nidingens Fågelstation</span>
            <span className={styles.brandSub}>Ringmärkningsdata sedan 1980</span>
          </span>
        </NavLink>
        <nav aria-label="Huvudmeny" className={styles.menu}>
          {PAGES.map((page) => (
            <NavLink
              key={page.path}
              to={{ pathname: page.path, search }}
              end={page.path === "/"}
              className={({ isActive }) => (isActive ? `${styles.link} ${styles.active}` : styles.link)}
            >
              {page.label}
            </NavLink>
          ))}
        </nav>
      </div>
    </header>
  );
}
