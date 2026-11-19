import { expect, test } from "@playwright/test";

const fmt = (n: number) => new Intl.NumberFormat("sv-SE").format(n).replace(/ /g, " ");
const rounded = (n: number) => fmt(Math.round(n / 1000) * 1000);

test("the numbers and years come from the API", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  const k = await (await request.get("/api/summary/kpis")).json();
  await page.goto("/om");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Om webbplatsen");
  await expect(page.getByText(`ringmärkningsdata från ${meta.first_year} och framåt`)).toBeVisible();
  await expect(page.getByText(new RegExp(`omkring ${rounded(k.n_new)} nyringmärkta fåglar, ${rounded(k.n_recap)} kontroller på ön och ${rounded(k.n_recoveries)} återfynd`))).toBeVisible();
});

test("both logos are there, with alternative texts", async ({ page }) => {
  await page.goto("/om");
  const contact = page.getByRole("complementary");
  await expect(contact.getByRole("img", { name: "Nidingens Fågelstations logotyp" })).toBeVisible();
  await expect(contact.getByRole("img", { name: "Göteborgs Ornitologiska Förenings logotyp" })).toBeVisible();
  // Both really loaded.
  for (const img of await contact.locator("img").all()) expect(await img.evaluate((e: HTMLImageElement) => e.naturalWidth)).toBeGreaterThan(0);
});

test("the contact links are real", async ({ page }) => {
  await page.goto("/om");
  const contact = page.getByRole("complementary");
  await expect(contact.getByRole("link", { name: "skall@chalmers.se" })).toHaveAttribute("href", "mailto:skall@chalmers.se");
  await expect(contact.getByRole("link", { name: "nidingen.gof.nu/" })).toHaveAttribute("href", "https://nidingen.gof.nu/");
  const hrefs = await page.locator("a").evaluateAll((els) => els.map((e) => e.getAttribute("href") ?? ""));
  expect(hrefs.filter((h) => h.includes("claudeusercontent"))).toEqual([]);
});

test("the glossary uses the site's seasons, weeks and raw age codes", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  await page.goto("/om");
  const terms = page.locator("dl dt");
  await expect(terms).toContainText(["Märkning", "Ringmärkta", "Kontroll", "Återfynd", "Säsong", "Medianpassage", "Vecka", "Kön", "Ålderskoder"]);
  const season = page.locator("dl > div").filter({ has: page.getByText("Säsong", { exact: true }) });
  await expect(season).toContainText("Vår är januari–juni och höst är juli–december.");
  expect(meta.seasons.map((s: { id: string }) => s.id)).toContain("var");
  await expect(page.locator("dl > div").filter({ has: page.getByText("Vecka", { exact: true }) })).toContainText("(vecka 1 = 1–7 januari), inte kalendervecka");
  // The design's 1K/2K classes are not what the site shows.
  await expect(page.locator("dl > div").filter({ has: page.getByText("Ålderskoder", { exact: true }) })).toContainText("10 är född i år");
});

test("the FAQ opens one answer at a time, with the first open", async ({ page }) => {
  await page.goto("/om");
  const questions = page.getByRole("button", { expanded: false }).or(page.getByRole("button", { expanded: true }));
  await expect(page.getByRole("button", { name: "Hur jämför jag flera arter?" })).toHaveAttribute("aria-expanded", "true");
  await expect(page.getByText(/Du kan välja upp till 6 arter/)).toBeVisible();
  await page.getByRole("button", { name: "Var gäller filtren?" }).click();
  await expect(page.getByRole("button", { name: "Hur jämför jag flera arter?" })).toHaveAttribute("aria-expanded", "false");
  await expect(page.getByText(/Du kan välja upp till 6 arter/)).toHaveCount(0);
  await expect(page.getByText(/Återfynd och Väder har egna/)).toBeVisible();
  // Clicking the open one closes it.
  await page.getByRole("button", { name: "Var gäller filtren?" }).click();
  await expect(page.getByRole("button", { name: "Var gäller filtren?" })).toHaveAttribute("aria-expanded", "false");
  expect(await questions.count()).toBeGreaterThan(8);
});

test("the decided entries are there: weeks, weather hours and the partly staffed years", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  await page.goto("/om");
  await page.getByRole("button", { name: "Varför stämmer inte veckorna med almanackan?" }).click();
  await expect(page.getByText(/Vecka 12 på webbplatsen är alltså alltid 19–25 mars, inte kalendervecka 12\./)).toBeVisible();
  await page.getByRole("button", { name: "Vilka timmar gäller väderuppgifterna?" }).click();
  await expect(page.getByText(meta.weather_window.note)).toBeVisible();
  const gap = meta.gap_years;
  await page.getByRole("button", { name: `Varför saknas ${gap.year_from}–${gap.year_to} i trenderna?` }).click();
  await expect(page.getByText(new RegExp(gap.reason))).toBeVisible();
  await page.getByRole("button", { name: `Varför är ${meta.current_year} inte med i trenderna?` }).click();
  await expect(page.getByText(/Trenderna räknas på hela säsonger/)).toBeVisible();
});

test("the FAQ works from the keyboard", async ({ page }, info) => {
  test.skip(info.project.name === "mobile", "keyboard test");
  await page.goto("/om");
  const second = page.getByRole("button", { name: "Var gäller filtren?" });
  await second.focus();
  await page.keyboard.press("Enter");
  await expect(second).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Space");
  await expect(second).toHaveAttribute("aria-expanded", "false");
});

test("no sideways page scroll", async ({ page }) => {
  await page.goto("/om");
  await expect(page.getByRole("heading", { name: "Vanliga frågor" })).toBeVisible();
  const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(over).toBeLessThanOrEqual(0);
});
