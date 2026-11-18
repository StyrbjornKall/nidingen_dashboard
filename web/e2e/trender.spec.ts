import { expect, test, type Page } from "@playwright/test";

const card = (page: Page, title: string) => page.locator("article").filter({ has: page.getByRole("heading", { name: title, exact: true }) });
const comparison = (page: Page) => card(page, "Antal ringmärkta per år, valda arter");
const overview = (page: Page) => card(page, "Alla arter");
const rows = (page: Page) => overview(page).getByRole("listitem");
const sv1 = (n: number) => {
  const text = new Intl.NumberFormat("sv-SE", { maximumFractionDigits: Math.abs(n) < 10 ? 1 : 0, minimumFractionDigits: Math.abs(n) < 10 ? 1 : 0 }).format(n).replace("-", "−");
  return n > 0 ? `+${text}` : text;
};

test("the comparison chart shows each species' trend from the API", async ({ page, request }) => {
  const api = await (await request.get("/api/trends?species=R%C3%96HAK,L%C3%96S%C3%85N,KUF%C3%85G&season=host&period=all")).json();
  await page.goto("/trender");
  for (const s of api.species) {
    await expect(comparison(page).getByText(`${sv1(s.fit.slope)} fåglar per år`)).toBeVisible();
  }
  // Lövsångare decreases in autumn over 1980–2025 (CI below zero).
  await expect(comparison(page).getByText("Minskar", { exact: true })).toBeVisible();
  await expect(comparison(page).getByText("delvis bemannad", { exact: true })).toBeVisible();
  await expect(comparison(page).getByText(
    "Trenderna bygger på hela säsonger 1980–1989 samt 2005–2025. 1990–2004 ingår inte: stationen var bara delvis bemannad dessa år.",
    { exact: false },
  )).toBeVisible();
});

test("the caption follows the period", async ({ page }) => {
  await page.goto("/trender?period=r10");
  await expect(comparison(page).getByText(/^Trenderna bygger på hela säsonger 2016–2025\./)).toBeVisible();
  await expect(comparison(page).getByText("delvis bemannad", { exact: true })).toHaveCount(0);
});

test("one card per selected species, each with its verdict", async ({ page }) => {
  await page.goto("/trender?arter=L%C3%96S%C3%85N,SKPIP");
  const cards = page.locator("article").filter({ has: page.getByRole("heading", { level: 3 }) });
  await expect(cards).toHaveCount(2);
  await expect(cards.first().getByRole("heading")).toHaveText("Lövsångare");
  await expect(cards.first().getByText("Phylloscopus trochilus")).toBeVisible();
  await expect(cards.first().getByText(/^Antal: (ökar|minskar|ingen säker trend)$/)).toBeVisible();
});

test("the comparison chart reads years with the keyboard", async ({ page }, info) => {
  test.skip(info.project.name === "mobile", "keyboard test");
  await page.goto("/trender?arter=R%C3%96HAK");
  const slider = comparison(page).getByRole("slider", { name: "Läs av diagrammet" });
  await slider.focus();
  await page.keyboard.press("Home");
  await expect(slider).toHaveAttribute("aria-valuetext", /^1980 · Rödhake \d/);
  await page.keyboard.press("PageUp");
  await page.keyboard.press("PageUp");
  await expect(slider).toHaveAttribute("aria-valuetext", /^1990 \(delvis bemannad\) · Rödhake/);
});

test("all species: selected first, then by change; click adds to the comparison", async ({ page, request }) => {
  const api = await (await request.get("/api/trends/overview?season=host&period=all&min_total=100&species=R%C3%96HAK,L%C3%96S%C3%85N,KUF%C3%85G")).json();
  await page.goto("/trender");
  await expect(rows(page)).toHaveCount(api.rows.length);
  await expect(rows(page).nth(0)).toContainText("Rödhake");
  await expect(rows(page).nth(1)).toContainText("Lövsångare");
  await expect(rows(page).nth(2)).toContainText("Kungsfågel");
  await expect(rows(page).nth(3)).toContainText(api.rows[3].name);

  const name = api.rows[3].name as string;
  await rows(page).nth(3).getByRole("button").click();
  await expect(page.getByRole("button", { name: `Ta bort ${name}` })).toBeVisible();
  await expect(rows(page).nth(3).getByRole("button")).toHaveAttribute("aria-pressed", "true");
  await expect(page.locator("article").filter({ has: page.getByRole("heading", { level: 3, name }) })).toBeVisible();
});

test("species with few birds are hidden until asked for", async ({ page, request }) => {
  const all = await (await request.get("/api/trends/overview?season=host&period=all&min_total=0&species=R%C3%96HAK,L%C3%96S%C3%85N,KUF%C3%85G")).json();
  await page.goto("/trender");
  await expect(overview(page).getByText(/arter med minst 100 ringmärkta fåglar/)).toBeVisible();
  await overview(page).getByRole("button", { name: "Visa alla arter" }).click();
  await expect(rows(page)).toHaveCount(all.rows.length);
});

test("season is in the URL and changes the data", async ({ page }) => {
  await page.goto("/trender?arter=L%C3%96S%C3%85N");
  const autumn = await comparison(page).getByText(/fåglar per år$/).first().textContent();
  await page.getByRole("group", { name: "Säsong" }).getByRole("button", { name: "Vår" }).click();
  await expect(page).toHaveURL(/sasong=var/);
  await expect(comparison(page).getByText(/fåglar per år$/).first()).not.toHaveText(autumn ?? "");
});
