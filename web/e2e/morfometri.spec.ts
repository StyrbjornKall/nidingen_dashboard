import { expect, test, type Page } from "@playwright/test";

const card = (page: Page, title: string | RegExp) => page.locator("article").filter({ has: page.getByRole("heading", { name: title }) });
const bubbles = (page: Page) => card(page, "Vikt mot vinglängd, alla arter");
const keys = (page: Page) => card(page, "Nyckeltal");
const dists = (page: Page) => card(page, "Fördelningar");
const ageSex = (page: Page) => card(page, "Ålder och kön");

test("the page is in the planned order and starts with the design's three species", async ({ page }) => {
  await page.goto("/morfometri");
  await expect(page.getByRole("heading", { level: 2 })).toHaveText(["Vikt mot vinglängd, alla arter", "Nyckeltal", "Fördelningar", "Ålder och kön"]);
  await expect(keys(page).getByText("Rödhake", { exact: true })).toBeVisible();
  await expect(keys(page).getByText("Ängspiplärka", { exact: true })).toBeVisible();
  await expect(keys(page).getByText("Skärpiplärka", { exact: true })).toBeVisible();
  // Season has "Båda" for the whole year; the default is höst, and there is no period choice.
  await expect(page.getByRole("button", { name: "Höst" })).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByRole("group", { name: "Period" })).toHaveCount(0);
});

test("key figures agree with the API", async ({ page, request }) => {
  const api = await (await request.get("/api/morphometrics/detail?species=R%C3%96HAK&season=host")).json();
  const w = api.species[0].weight;
  await page.goto("/morfometri?arter=R%C3%96HAK");
  const text = (n: number) => n.toFixed(1).replace(".", ",");
  await expect(keys(page).getByText(`${text(w.median)} g`)).toBeVisible();
  await expect(keys(page).getByText(`${text(w.q25)}–${text(w.q75)}`)).toBeVisible();
});

test("bubbles: one per species with enough birds, a click adds and removes a species", async ({ page, request }) => {
  const means = await (await request.get("/api/morphometrics/means?season=host")).json();
  await page.goto("/morfometri?arter=R%C3%96HAK");
  await expect(bubbles(page).locator("button[aria-pressed]")).toHaveCount(means.rows.length);
  const tern = bubbles(page).getByRole("button", { name: "Taltrast, lägg till i jämförelsen" });
  await tern.click({ force: true });
  await expect(page).toHaveURL(/arter=R%C3%96HAK,TATRA|arter=RÖHAK,TATRA/);
  await expect(keys(page).getByText("Taltrast", { exact: true })).toBeVisible();
  await bubbles(page).getByRole("button", { name: "Taltrast, vald" }).click({ force: true });
  await expect(keys(page).getByText("Taltrast", { exact: true })).toHaveCount(0);
});

test("bubbles: hovering one reads its means and count", async ({ page }) => {
  await page.goto("/morfometri?arter=R%C3%96HAK");
  await bubbles(page).getByRole("button", { name: "Taltrast, lägg till i jämförelsen" }).hover({ force: true });
  await expect(bubbles(page).getByText(/ringmärkta sedan 1980$/).first()).toBeVisible();
  await expect(bubbles(page).locator("[aria-live=polite]")).toContainText("Taltrast");
  await expect(bubbles(page).locator("[aria-live=polite]")).toContainText(/\d+,\d g · \d+,\d mm/);
});

test("distributions: hovering a bar reads its range and count", async ({ page }, info) => {
  test.skip(info.project.name === "mobile", "pointer hover");
  await page.goto("/morfometri?arter=R%C3%96HAK");
  const hist = dists(page).locator("[class*=hist]:has(svg)").first();
  await hist.scrollIntoViewIfNeeded();
  const box = (await hist.boundingBox())!;
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  const tip = page.locator("[class*=tip]");
  await expect(tip).toContainText("Rödhake");
  await expect(tip).toContainText(/fåglar$/);
});

test("age is shown as raw codes, and missing values as Okänd", async ({ page }) => {
  await page.goto("/morfometri?arter=R%C3%96HAK&sasong=helar");
  const card_ = ageSex(page);
  await expect(card_.getByRole("img", { name: /^Rödhake, ålder: 10 \d+ %/ })).toBeVisible();
  await expect(card_.getByRole("img", { name: /^Rödhake, kön: .*Okänd \d+ %/ })).toBeVisible();
  await expect(card_.getByText("Hane", { exact: true })).toBeVisible();
  for (const grouped of ["1K", "2K", "+1K", "+2K"]) await expect(card_.getByText(grouped, { exact: true })).toHaveCount(0);
});

test("'Alla arter' has no measures: the detail cards say so and the bubbles stay", async ({ page }) => {
  await page.goto("/morfometri?arter=ALLA");
  await expect(keys(page).getByText(/Välj minst en art i artväljaren/)).toBeVisible();
  await expect(dists(page).getByText(/Välj minst en art i artväljaren/)).toBeVisible();
  await expect(bubbles(page).locator("button[aria-pressed]").first()).toBeVisible();
});

test("no sideways page scroll", async ({ page }) => {
  await page.goto("/morfometri?arter=R%C3%96HAK,L%C3%96S%C3%85N,KUF%C3%85G,%C3%84NPIP,SKPIP,TATRA");
  await expect(ageSex(page).getByRole("img").first()).toBeVisible();
  const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(over).toBeLessThanOrEqual(0);
});
