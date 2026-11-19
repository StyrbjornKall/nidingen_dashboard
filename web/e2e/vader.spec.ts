import { expect, test, type Page } from "@playwright/test";

const card = (page: Page, title: string | RegExp) => page.locator("article").filter({ has: page.getByRole("heading", { name: title }) });
const tracks = (page: Page) => card(page, "Väder och fångst dag för dag");
const readout = (page: Page) => page.locator("aside[class*=readout]");
const slider = (page: Page) => tracks(page).getByRole("slider", { name: "Läs av dag för dag" });

const fmt = (n: number) => new Intl.NumberFormat("sv-SE").format(n).replace(/ /g, " ");
const MONTHS = ["jan", "feb", "mar", "apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov", "dec"];
const date = (iso: string) => `${Number(iso.slice(8, 10))} ${MONTHS[Number(iso.slice(5, 7)) - 1]} ${iso.slice(0, 4)}`;

test("the page is in the planned order with the design's defaults", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  await page.goto("/vader");
  await expect(page.getByRole("heading", { level: 2 })).toHaveText(["Väder och fångst dag för dag", "Fångst per vindriktning", "Säsongens bästa fångstdagar"]);
  await expect(page.getByRole("button", { name: "Höst" })).toHaveAttribute("aria-pressed", "true");
  // Only the two site seasons: no Sommar.
  await expect(page.getByRole("button", { name: "Sommar" })).toHaveCount(0);
  await expect(page.getByLabel("År")).toHaveValue(String(meta.complete_through));
  for (const name of ["Temperatur", "Vindstyrka", "Vindriktning", "Lufttryck"]) await expect(page.getByRole("button", { name })).toHaveAttribute("aria-pressed", "true");
  for (const name of ["Nederbörd", "Sikt"]) await expect(page.getByRole("button", { name })).toHaveAttribute("aria-pressed", "false");
  // The catch window is named on the page.
  await expect(tracks(page).getByText(meta.weather_window.note)).toBeVisible();
});

test("the readout starts on the day with the most birds, from the API", async ({ page, request }) => {
  const api = await (await request.get("/api/weather?season=host&year=2025&species=ALLA")).json();
  const best = api.best_days[0];
  await page.goto("/vader");
  await expect(readout(page).getByText("Fångstrikaste dagen")).toBeVisible();
  await expect(readout(page)).toContainText(date(best.date));
  await expect(readout(page)).toContainText(fmt(best.catch));
  // The best-days table agrees.
  const rows = card(page, "Säsongens bästa fångstdagar").locator("tbody tr");
  await expect(rows).toHaveCount(api.best_days.length);
  await expect(rows.first()).toContainText(date(best.date));
  await expect(rows.first()).toContainText(fmt(best.catch));
});

test("one crosshair reads every track: pointer and keyboard", async ({ page }, info) => {
  await page.goto("/vader");
  const s = slider(page);
  await s.focus();
  await page.keyboard.press("Home");
  await expect(readout(page).getByText("Vald dag")).toBeVisible();
  await expect(s).toHaveAttribute("aria-valuetext", /^1 jul 2025: Fångst .* Temperatur .* Lufttryck /);
  await expect(readout(page)).toContainText("1 jul 2025");
  await page.keyboard.press("ArrowRight");
  await expect(readout(page)).toContainText("2 jul 2025");
  await page.keyboard.press("Tab");
  if (info.project.name === "mobile") return;
  const box = (await s.boundingBox())!;
  await page.mouse.move(box.x + box.width * 0.5, box.y + 40);
  await expect(readout(page).getByText("Vald dag")).toBeVisible();
  await page.mouse.move(0, 0);
  await expect(readout(page).getByText("Fångstrikaste dagen")).toBeVisible();
});

test("the Visa chips add and remove tracks and live in the URL", async ({ page }) => {
  await page.goto("/vader");
  const names = tracks(page).locator("[class*=trackName]");
  await expect(names).toHaveText(["Fångst", "Temperatur", "Vindstyrka", "Vindriktning", "Lufttryck"]);
  await page.getByRole("button", { name: "Nederbörd" }).click();
  await page.getByRole("button", { name: "Temperatur" }).click();
  await expect(names).toHaveText(["Fångst", "Vindstyrka", "Vindriktning", "Lufttryck", "Nederbörd"]);
  await expect(page).toHaveURL(/visa=vind,rikt,tryck,regn/);
  await expect(tracks(page).getByText(/summa kl\. 3–12/)).toBeVisible();
  await page.reload();
  await expect(names).toHaveText(["Fångst", "Vindstyrka", "Vindriktning", "Lufttryck", "Nederbörd"]);
});

test("values gap-filled from Vinga A are marked, and only where they apply", async ({ page }) => {
  await page.goto("/vader?ar=2025");
  await expect(tracks(page).getByText(/Vinga A/).first()).toBeVisible();
  await page.goto("/vader?ar=1985");
  await expect(page.getByLabel("År")).toHaveValue("1985");
  await expect(tracks(page).locator("[class*=trackName]").first()).toBeVisible();
  await expect(tracks(page).getByText(/Vinga A/)).toHaveCount(0);
});

test("season, year and species are their own choices in the URL", async ({ page, request }) => {
  const api = await (await request.get("/api/weather?season=var&year=2010&species=R%C3%96HAK")).json();
  await page.goto("/vader");
  await page.getByRole("button", { name: "Vår" }).click();
  await page.getByLabel("År").selectOption("2010");
  await page.getByLabel("Fångst av").selectOption("RÖHAK");
  await expect(page).toHaveURL(/sasong=var/);
  await expect(page).toHaveURL(/ar=2010/);
  await expect(page).toHaveURL(/art=R%C3%96HAK|art=RÖHAK/);
  await expect(tracks(page).getByText(/Fångst av rödhake/)).toBeVisible();
  await expect(readout(page)).toContainText("rödhake ringmärkta");
  const best = api.best_days[0];
  await expect(readout(page)).toContainText(date(best.date));
  // Gap years can be picked but are named.
  const options = await page.getByLabel("År").locator("option").allTextContents();
  expect(options.some((o) => /^1995 \(delvis bemannad\)$/.test(o))).toBe(true);
  expect(options).toContain("1982");
});

test("the wind rose names the best direction and every sector can be read", async ({ page, request }) => {
  const api = await (await request.get("/api/weather?season=host&year=2025&species=ALLA")).json();
  const eligible = api.wind_rose.map((s: { label: string; days: number; mean_catch: number | null }) => s).filter((s: { days: number }) => s.days >= 3);
  const top = eligible.reduce((a: { mean_catch: number }, b: { mean_catch: number }) => (b.mean_catch > a.mean_catch ? b : a));
  await page.goto("/vader");
  const rose = card(page, "Fångst per vindriktning");
  await expect(rose.getByText(new RegExp(`vid vind från ${top.label} \\(`))).toBeVisible();
  await expect(rose.locator("li")).toHaveCount(8);
});

test("the wind rose wedges read on hover", async ({ page }, info) => {
  test.skip(info.project.name === "mobile", "pointer hover");
  await page.goto("/vader");
  const svg = card(page, "Fångst per vindriktning").locator("svg[role=img]");
  await svg.scrollIntoViewIfNeeded();
  const box = (await svg.boundingBox())!;
  const tip = page.locator("[class*=tip]");
  // Sweep round the centre until the pointer is over a wedge.
  for (let a = 0; a < 360 && (await tip.count()) === 0; a += 30) {
    const r = box.width * 0.2;
    await page.mouse.move(box.x + box.width / 2 + r * Math.sin((a * Math.PI) / 180), box.y + box.height / 2 - r * Math.cos((a * Math.PI) / 180));
  }
  await expect(tip).toContainText(/Vind från .* per dag · \d+ dagar/);
});

test("no sideways page scroll", async ({ page }) => {
  await page.goto("/vader");
  await expect(card(page, "Säsongens bästa fångstdagar").locator("tbody tr").first()).toBeVisible();
  const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(over).toBeLessThanOrEqual(0);
});
