/** Routes in menu order (design/components/Nav.dc.html). */
export interface PageInfo {
  path: string;
  label: string;
  /** Used in the browser tab: "<title> · Nidingens Fågelstation". */
  title: string;
}

export const PAGES: readonly PageInfo[] = [
  { path: "/", label: "Hem", title: "Nidingens Fågelstation" },
  { path: "/sammanfattning", label: "Sammanfattning", title: "Sammanfattning" },
  { path: "/trender", label: "Trender", title: "Trender" },
  { path: "/morfometri", label: "Morfometri", title: "Morfometri" },
  { path: "/fenologi", label: "Fenologi", title: "Fenologi" },
  { path: "/aterfynd", label: "Återfynd", title: "Återfynd" },
  { path: "/vader", label: "Väder", title: "Väder" },
  { path: "/om", label: "Om", title: "Om webbplatsen" },
];

export const SITE_NAME = "Nidingens Fågelstation";
