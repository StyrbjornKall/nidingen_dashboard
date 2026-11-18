import { useEffect, useRef } from "react";
import { Outlet, useLocation } from "react-router";
import { PAGES, SITE_NAME } from "../pages/registry";
import { Footer } from "./Footer";
import { Nav } from "./Nav";
import styles from "./Layout.module.css";

/** Skip link, header, page content and footer, with title and focus handling per route. */
export function Layout() {
  const { pathname } = useLocation();
  const first = useRef(true);

  useEffect(() => {
    const page = PAGES.find((p) => p.path === pathname);
    document.title = !page || page.path === "/" ? (page ? SITE_NAME : `Sidan finns inte · ${SITE_NAME}`) : `${page.title} · ${SITE_NAME}`;
    // After navigating (not on first load), start at the top and move focus to
    // the new page's heading so screen readers announce it.
    if (first.current) {
      first.current = false;
      return;
    }
    window.scrollTo(0, 0);
    document.querySelector<HTMLElement>("[data-page-title]")?.focus();
  }, [pathname]);

  return (
    <>
      <a href="#innehall" className={styles.skip}>
        Hoppa till innehållet
      </a>
      <Nav />
      <main id="innehall" className={styles.main}>
        <Outlet />
      </main>
      <Footer />
    </>
  );
}
