import { expect, test, type Page } from "@playwright/test";

const card = (page: Page, title: string | RegExp) => page.locator("article").filter({ has: page.getByRole("heading", { name: title }) });
const calendar = (page: Page) => card(page, "Märkningskalendern");
const ridges = (page: Page) => card(page, "När passerar arterna i detalj?");
const table = (page: Page) => card(page, "Arterna");

test("the selection bar is on top, then the calendar, the ridgelines and the table", async ({ page }) => {
  await page.goto("/fenologi");
  const headings = page.getByRole("heading", { level: 2 });
  await expect(headings).toHaveText(["Märkningskalendern", "När passerar arterna i detalj?", "Arterna"]);
  const barBottom = (await page.getByRole("region", { name: "Urval" }).boundingBox())!.y;
  const calTop = (await calendar(page).boundingBox())!.y;
  expect(barBottom).toBeLessThan(calTop);
  await expect(page.getByRole("group", { name: "Säsong" })).toHaveCount(0); // no season toggle on this page
});

test("calendar: selected species first, top N", async ({ page }) => {
  await page.goto("/fenologi");
  const names = calendar(page).locator("[class*=calName]");
  await expect(names).toHaveCount(10);
  await expect(names.nth(0)).toContainText("Rödhake");
  await expect(names.nth(1)).toContainText("Lövsångare");
  await expect(names.nth(2)).toContainText("Kungsfågel");

  await calendar(page).getByRole("button", { name: "Topp 15" }).click();
  await expect(names).toHaveCount(15);

});

test("the year dropdown has no gap years and drives calendar, ridgelines and table", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  await page.goto("/fenologi");
  const bar = page.getByRole("region", { name: "Urval" });
  await expect(bar.getByLabel("Välj specifikt år")).toBeVisible();
  const options = await bar.getByLabel("Välj specifikt år").locator("option").allTextContents();
  expect(options[0]).toBe("Välj specifikt år");
  expect(options).not.toContain("1995");
  expect(options).toContain("2005");
  expect(options).toContain(String(meta.complete_through));

  const ridgesCall = page.waitForResponse((r) => r.url().includes("/api/phenology/ridges") && r.url().includes("year=2008"));
  await bar.getByLabel("Välj specifikt år").selectOption("2008");
  const body = await (await ridgesCall).json();
  expect(body.reference_years).toEqual([2008]); // the data is that one year, not the period average
  await expect(page).toHaveURL(/period=2008/);
  await expect(calendar(page).getByText("totalt", { exact: true })).toBeVisible();
  await expect(calendar(page).getByText(/^Snitt per år för/)).toHaveCount(0);
  // The ridgelines show 2008 against the last complete year.
  await expect(ridges(page).getByText(/Säsongens fångst per dag, 2008, jämfört med det senaste kompletta året \(\d{4}\)/)).toBeVisible();
  // No preset is pressed while a specific year is chosen.
  await expect(bar.getByRole("group", { name: "Period" }).locator("[aria-pressed=true]")).toHaveCount(0);

  await bar.getByRole("button", { name: "Senaste 10 år" }).click();
  await expect(calendar(page).getByText(/^Snitt per år för/)).toBeVisible();
});

test("the period presets change the calendar's average", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  await page.goto("/fenologi");
  await page.getByRole("region", { name: "Urval" }).getByRole("button", { name: "Senaste 10 år" }).click();
  const from = meta.complete_through - 9;
  await expect(calendar(page).getByText(new RegExp(`^Snitt per år för ${from}–${meta.complete_through}`))).toBeVisible();
});

test("calendar weeks are 7-day periods from 1 January", async ({ page }) => {
  await page.goto("/fenologi");
  await expect(calendar(page).getByText(/sjudagarsperioder från 1 januari/)).toBeVisible();
});

test("calendar: hovering a cell shows its species, week with dates and value", async ({ page }, info) => {
  test.skip(info.project.name === "mobile", "pointer hover");
  await page.goto("/fenologi");
  const row = calendar(page).locator("[class*=calRow]").first();
  const cells = row.locator("[class*=heat] > span");
  // Active weeks start at week 10, so the 3rd cell is week 12 (19–25 mar).
  const box = (await cells.nth(2).boundingBox())!;
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  const tip = page.locator("[class*=tip]");
  await expect(tip).toContainText("Rödhake");
  await expect(tip).toContainText(/v\.12 · 19–25 mar/);
  await expect(tip).toContainText(/i snitt/);
  await page.mouse.move(0, 0);
  await expect(tip).toHaveCount(0);
});

test("ridgelines: one row per species, medians, single-season species", async ({ page }) => {
  await page.goto("/fenologi?arter=R%C3%96HAK,%C3%84NPIP");
  await expect(ridges(page).getByText(/^median vår: \d+ \w+$/)).toHaveCount(1); // Rödhake
  await expect(ridges(page).getByText(/^median höst: \d+ \w+$/)).toHaveCount(2);
  // Ängspiplärka is ringed almost only in autumn: one median.
  await expect(ridges(page).getByText("median vår: –")).toBeVisible();
  await expect(ridges(page).getByText("Ringmärks nästan bara på hösten")).toBeVisible();
  await expect(ridges(page).getByText(/Säsongens fångst per dag, snitt för 1980–1990 samt 2005–2025, jämfört med/)).toBeVisible();
});

test("species table: selected first, ten rows, then all", async ({ page }) => {
  await page.goto("/fenologi");
  const rows = table(page).locator("tbody tr");
  await expect(rows).toHaveCount(10);
  await expect(rows.nth(0)).toContainText("Rödhake");
  await expect(table(page).getByRole("columnheader", { name: "Snitt 2015–2024" })).toBeVisible();
  await table(page).getByRole("button", { name: /^Visa alla \d+ arter$/ }).click();
  expect(await rows.count()).toBeGreaterThan(100);

  await rows.nth(5).getByRole("button").click();
  await expect(page.getByRole("region", { name: "Urval" }).getByRole("button", { name: /^Ta bort / })).toHaveCount(4);
});

test("the species bar changes the calendar order, ridgelines and table", async ({ page }) => {
  await page.goto("/fenologi");
  await expect(calendar(page).locator("[class*=calName]")).toHaveCount(10);
  const before = await calendar(page).locator("[class*=calName]").allTextContents();
  await page.getByRole("button", { name: "Ta bort Kungsfågel" }).click();
  await expect(ridges(page).getByText("Kungsfågel")).toHaveCount(0);
  // The calendar follows the selection order but keeps its own list of species.
  await expect(calendar(page).locator("[class*=calName]").nth(2)).not.toHaveText(before[2] ?? "");
  await expect(calendar(page).locator("[class*=calName]")).toHaveCount(before.length);
});

test("ridgelines are daily: hovering a panel reads one date", async ({ page }) => {
  await page.goto("/fenologi?arter=R%C3%96HAK");
  const panel = ridges(page).locator("[class*=panel]:has([role=slider])").first();
  await panel.scrollIntoViewIfNeeded();
  await page.waitForTimeout(900); // the rows slide in
  const box = (await panel.boundingBox())!;
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await expect(ridges(page).locator("[class*=ridgeReadout]")).toContainText(/^\d+ \w+ · snitt /);
  await expect(ridges(page).getByText(/per vecka/)).toHaveCount(0);
});

test("species table: hovering a sparkline reads one year", async ({ page }) => {
  await page.goto("/fenologi");
  const spark = table(page).locator("[class*=sparkBox]").first();
  await spark.scrollIntoViewIfNeeded();
  const box = (await spark.boundingBox())!;
  await page.mouse.move(box.x + box.width - 2, box.y + box.height / 2);
  await expect(page.locator("[class*=tip]")).toContainText(/^\d{4}: /);
});
