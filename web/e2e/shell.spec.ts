import { expect, test, type Page } from "@playwright/test";

const ROUTES = [
  { path: "/", h1: "Nidingens Fågelstation", nav: "Hem", title: "Nidingens Fågelstation" },
  { path: "/sammanfattning", h1: "Sammanfattning", nav: "Sammanfattning", title: "Sammanfattning · Nidingens Fågelstation" },
  { path: "/trender", h1: "Trender", nav: "Trender", title: "Trender · Nidingens Fågelstation" },
  { path: "/morfometri", h1: "Morfometri", nav: "Morfometri", title: "Morfometri · Nidingens Fågelstation" },
  { path: "/fenologi", h1: "Fenologi", nav: "Fenologi", title: "Fenologi · Nidingens Fågelstation" },
  { path: "/aterfynd", h1: "Återfynd", nav: "Återfynd", title: "Återfynd · Nidingens Fågelstation" },
  { path: "/vader", h1: "Väder", nav: "Väder", title: "Väder · Nidingens Fågelstation" },
  { path: "/om", h1: "Om webbplatsen", nav: "Om", title: "Om webbplatsen · Nidingens Fågelstation" },
];

const menu = (page: Page) => page.getByRole("navigation", { name: "Huvudmeny" });
const chips = (page: Page) => page.getByRole("region", { name: "Urval" }).getByRole("button", { name: /^Ta bort / });

for (const r of ROUTES) {
  test(`${r.path} renders with its menu item active`, async ({ page }) => {
    await page.goto(r.path);
    await expect(page.getByRole("heading", { level: 1 })).toHaveText(r.h1);
    await expect(page).toHaveTitle(r.title);
    await expect(menu(page).getByRole("link", { name: r.nav, exact: true })).toHaveAttribute("aria-current", "page");
    await expect(page.getByText(/Senast uppdaterad \d+ \w+ \d{4}/)).toBeVisible();
  });

  test(`${r.path} has no sideways page scroll`, async ({ page }) => {
    await page.goto(r.path);
    await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    expect(overflow).toBeLessThanOrEqual(0);
  });
}

test("unknown paths show a Swedish not-found page", async ({ page }) => {
  await page.goto("/finns-inte");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Sidan finns inte");
});

test("the species selection lives in the URL and follows the menu", async ({ page }) => {
  await page.goto("/trender");
  // Default selection from config/analysis.json.
  await expect(chips(page)).toHaveCount(3);
  for (const name of ["Rödhake", "Lövsångare", "Kungsfågel"]) {
    await expect(page.getByRole("button", { name: `Ta bort ${name}` })).toBeVisible();
  }
  await page.getByRole("button", { name: "Lägg till art" }).click();
  await page.getByLabel("Sök art").fill("ängspip");
  await page.getByRole("dialog").getByRole("button", { name: /Ängspiplärka/ }).click();
  await page.getByRole("button", { name: "Klar" }).click();
  await expect(page).toHaveURL(/arter=R%C3%96HAK,L%C3%96S%C3%85N,KUF%C3%85G,%C3%84NPIP/);

  // Another picker page keeps the selection.
  await menu(page).getByRole("link", { name: "Fenologi" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Fenologi");
  await expect(page.getByRole("button", { name: "Ta bort Ängspiplärka" })).toBeVisible();

  // Emptying the selection falls back to the page's fallback (Rödhake).
  for (const name of ["Rödhake", "Lövsångare", "Kungsfågel", "Ängspiplärka"]) {
    await page.getByRole("button", { name: `Ta bort ${name}` }).click();
  }
  await expect(page.getByRole("button", { name: "Ta bort Rödhake" })).toBeVisible();
  await expect(page).toHaveURL(/arter=R%C3%96HAK$/);
});

test("Sammanfattning defaults to all species", async ({ page }) => {
  await page.goto("/sammanfattning");
  await expect(page.getByRole("button", { name: "Ta bort Alla arter" })).toBeVisible();
});

test("period and season are kept in the URL across reloads", async ({ page }) => {
  await page.goto("/trender");
  await page.getByRole("group", { name: "Period" }).getByRole("button", { name: "Senaste 20 år" }).click();
  await page.getByRole("group", { name: "Säsong" }).getByRole("button", { name: "Vår" }).click();
  await expect(page).toHaveURL(/period=r20/);
  await expect(page).toHaveURL(/sasong=var/);
  await page.reload();
  await expect(page.getByRole("group", { name: "Period" }).getByRole("button", { name: "Senaste 20 år" })).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByRole("group", { name: "Säsong" }).getByRole("button", { name: "Vår" })).toHaveAttribute("aria-pressed", "true");
});

test("a shared link opens with its species", async ({ page }) => {
  await page.goto("/trender?arter=SKPIP,FOO");
  await expect(chips(page)).toHaveCount(1);
  await expect(page.getByRole("button", { name: "Ta bort Skärpiplärka" })).toBeVisible();
});

test("the skip link is the first thing a keyboard reaches", async ({ page }, info) => {
  test.skip(info.project.name === "mobile", "keyboard test");
  await page.goto("/sammanfattning");
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "Hoppa till innehållet" })).toBeFocused();
});

test("screenshots for design review", async ({ page }, info) => {
  for (const path of ["/sammanfattning", "/fenologi"]) {
    await page.goto(path);
    await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
    await page.waitForLoadState("networkidle");
    await page.screenshot({ path: info.outputPath(`${path.slice(1)}.png`), fullPage: true });
  }
  await page.goto("/trender");
  await page.getByRole("button", { name: "Lägg till art" }).click();
  await page.screenshot({ path: info.outputPath("trender-picker-open.png") });
});

test("the selection bar sticks on wide screens only", async ({ page }, info) => {
  await page.goto("/trender");
  const position = await page.getByRole("region", { name: "Urval" }).evaluate((el) => getComputedStyle(el).position);
  expect(position).toBe(info.project.name === "mobile" ? "static" : "sticky");
});
