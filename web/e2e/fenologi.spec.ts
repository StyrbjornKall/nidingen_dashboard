import { expect, test, type Page } from "@playwright/test";

const card = (page: Page, title: string | RegExp) => page.locator("article").filter({ has: page.getByRole("heading", { name: title }) });
const calendar = (page: Page) => card(page, "Flyttningskalendern");
const ridges = (page: Page) => card(page, "När passerar arterna i detalj?");
const std = (page: Page) => card(page, /^Säsongen \d{4} mot alla tidigare år$/);
const table = (page: Page) => card(page, "Arterna");

test("the page is in the planned order, with the calendar above the species bar", async ({ page }) => {
  await page.goto("/fenologi");
  const headings = page.getByRole("heading", { level: 2 });
  await expect(headings).toHaveText(["Flyttningskalendern", "När passerar arterna i detalj?", /^Säsongen \d{4}/, "Arterna"]);
  const barTop = (await page.getByRole("region", { name: "Urval" }).boundingBox())!.y;
  const calBottom = (await calendar(page).boundingBox())!.y;
  expect(calBottom).toBeLessThan(barTop);
  await expect(page.getByRole("group", { name: "Säsong" })).toHaveCount(0); // no season toggle on this page
});

test("calendar: selected species first, top N, own year choice without gap years", async ({ page }) => {
  await page.goto("/fenologi");
  await expect(calendar(page).getByText("Påverkas inte av artväljaren")).toBeVisible();
  const names = calendar(page).locator("[class*=calName]");
  await expect(names).toHaveCount(10);
  await expect(names.nth(0)).toContainText("Rödhake");
  await expect(names.nth(1)).toContainText("Lövsångare");
  await expect(names.nth(2)).toContainText("Kungsfågel");

  await calendar(page).getByRole("button", { name: "Topp 15" }).click();
  await expect(names).toHaveCount(15);

  const options = await calendar(page).getByLabel("År").locator("option").allTextContents();
  expect(options[0]).toBe("Snitt alla år");
  expect(options).not.toContain("1995");
  expect(options).toContain("2005");
  await calendar(page).getByLabel("År").selectOption("2008");
  await expect(calendar(page).getByText("totalt", { exact: true })).toBeVisible();
  await expect(calendar(page).getByText(/^Snitt per år för/)).toHaveCount(0);
});

test("calendar weeks are 7-day periods from 1 January", async ({ page }) => {
  await page.goto("/fenologi");
  await expect(calendar(page).getByText(/sjudagarsperioder från 1 januari/)).toBeVisible();
});

test("calendar: hovering a cell shows its species, week with dates and value", async ({ page }) => {
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
  await expect(ridges(page).getByText(/Säsongens fångst per dag, snitt för 1980–1990 samt 2005–2025\./)).toBeVisible();
});

test("season to date compares this year with the reference years", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  await page.goto("/fenologi");
  await expect(std(page).getByRole("heading")).toHaveText(`Säsongen ${meta.current_year} mot alla tidigare år`);
  await expect(std(page).getByText(/^Rödhake, ringmärkta \d{4}/)).toBeVisible();
  await expect(std(page).getByText(/fåglar mot ett normalt år/)).toBeVisible();
  await expect(std(page).getByText("Medel 1980–1990 samt 2005–2025")).toBeVisible();

  // The running year ends in a blinking dot; there is no "idag" line.
  await expect(std(page).locator("[class*=liveDot]")).toHaveCount(1);
  await expect(std(page).getByText(/^idag/)).toHaveCount(0);

  await std(page).getByRole("button", { name: "Lövsångare" }).click();
  await expect(std(page).getByText(/^Lövsångare, ringmärkta/)).toBeVisible();
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

test("the species bar changes ridgelines and table, not the calendar", async ({ page }) => {
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
