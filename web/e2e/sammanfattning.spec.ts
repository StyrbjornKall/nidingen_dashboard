import { expect, test, type Page } from "@playwright/test";

const card = (page: Page, title: string) => page.locator("article").filter({ has: page.getByRole("heading", { name: title }) });
const ts = (page: Page) => card(page, "Ringmärkta över tid");
const top = (page: Page) => card(page, "Flest ringmärkta arter");
const grain = (page: Page, name: string) => ts(page).getByRole("group", { name: "Upplösning" }).getByRole("button", { name });
const chartType = (page: Page, name: string) => ts(page).getByRole("group", { name: "Diagramtyp" }).getByRole("button", { name });
const slider = (page: Page) => ts(page).getByRole("slider", { name: "Läs av diagrammet" });
const sv = (n: number) => new Intl.NumberFormat("sv-SE").format(n);

test("key figures come from the database", async ({ page, request }) => {
  const kpis = await (await request.get("/api/summary/kpis")).json();
  await page.goto("/sammanfattning");
  const figures = page.getByRole("definition");
  await expect(figures.nth(0)).toHaveText(sv(kpis.n_new));
  await expect(figures.nth(1)).toHaveText(sv(kpis.n_recap));
  await expect(figures.nth(2)).toHaveText(sv(kpis.n_recoveries));
  await expect(figures.nth(3)).toHaveText(sv(kpis.n_species));
});

test("counts per year as bars by default, with the partly staffed years marked", async ({ page, request }) => {
  const series = await (await request.get("/api/summary/timeseries?species=ALLA&grain=year&period=all")).json();
  await page.goto("/sammanfattning");
  await expect(grain(page, "År")).toHaveAttribute("aria-pressed", "true");
  await expect(chartType(page, "Stapel")).toHaveAttribute("aria-pressed", "true");
  await expect(grain(page, "Dag")).toBeDisabled();
  await expect(ts(page).getByText("Summa 1980–2025")).toBeVisible();
  await expect(ts(page).getByText(sv(series.series[0].total_new), { exact: true })).toBeVisible();
  await expect(ts(page).getByText("delvis bemannad", { exact: true })).toBeVisible();
  await expect(ts(page).getByText(/1991–2004 är skuggat/)).toBeVisible();
});

test("week and day default to a line, and day needs a single year", async ({ page }) => {
  await page.goto("/sammanfattning");
  await grain(page, "Vecka").click();
  await expect(chartType(page, "Linje")).toHaveAttribute("aria-pressed", "true");
  await expect(ts(page).getByText("Antal ringmärkta per vecka")).toBeVisible();

  await page.getByRole("group", { name: "Period" }).getByRole("button", { name: "2025", exact: true }).click();
  await expect(page).toHaveURL(/period=2025/);
  await expect(grain(page, "År")).toBeDisabled();
  await grain(page, "Dag").click();
  await expect(ts(page).getByText("Antal ringmärkta per dag")).toBeVisible();

  // Leaving the single year falls back from days to weeks.
  await page.getByRole("group", { name: "Period" }).getByRole("button", { name: "Senaste 10 år" }).click();
  await expect(grain(page, "Vecka")).toHaveAttribute("aria-pressed", "true");
  await expect(grain(page, "Dag")).toBeDisabled();
});

test("the chart can be read with the keyboard", async ({ page }, info) => {
  test.skip(info.project.name === "mobile", "keyboard test");
  await page.goto("/sammanfattning?period=2025");
  await grain(page, "Vecka").click();
  await slider(page).focus();
  await page.keyboard.press("Home");
  await expect(ts(page).getByText("v.1 · 1–7 jan 2025")).toBeVisible();
  await page.keyboard.press("ArrowRight");
  await expect(slider(page)).toHaveAttribute("aria-valuetext", /^v\.2 · 8–14 jan 2025: Alla arter \d/);
  await page.keyboard.press("End");
  await expect(ts(page).getByText("v.52 · 24–31 dec 2025")).toBeVisible();
});

test("hovering a year shows that year's count", async ({ page, request }, info) => {
  test.skip(info.project.name === "mobile", "pointer test");
  const s = await (await request.get("/api/summary/timeseries?species=ALLA&grain=year&period=all")).json();
  await page.goto("/sammanfattning");
  const box = (await slider(page).boundingBox())!;
  // 2008 is bucket 28 of 46.
  await page.mouse.move(box.x + ((28 + 0.5) / 46) * box.width, box.y + box.height / 2);
  await expect(ts(page).getByText("2008", { exact: true }).first()).toBeVisible();
  await expect(ts(page).getByText(sv(s.series[0].new[28]), { exact: true })).toBeVisible();
});

test("recaptures are an optional series, never added to ringings", async ({ page }) => {
  await page.goto("/sammanfattning?arter=R%C3%96HAK");
  const toggle = ts(page).getByRole("button", { name: "Visa kontroller" });
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-pressed", "true");
  await expect(ts(page).getByText(/kontr\./)).toBeVisible();
  await expect(ts(page).getByText(/Streckade linjer är kontroller/)).toBeVisible();
});

test("top species: top 100, selected first, click to compare", async ({ page }) => {
  await page.goto("/sammanfattning");
  const rows = top(page).getByRole("listitem");
  await expect(rows).toHaveCount(100);
  await expect(rows.first()).toContainText("Rödhake");
  await expect(top(page).getByText("Visar topp 100 av 210 arter.")).toBeVisible();

  // Kungsfågel is 4th; selecting it moves it to the top and colours it.
  await top(page).getByRole("button", { name: /Kungsfågel/ }).click();
  await expect(page).toHaveURL(/arter=ALLA,KUF%C3%85G/);
  await expect(rows.first()).toContainText("Kungsfågel");
  await expect(top(page).getByRole("button", { name: /Kungsfågel/ })).toHaveAttribute("aria-pressed", "true");
  await expect(top(page).getByText("Valda arter ligger överst.", { exact: false })).toBeVisible();
  await expect(page.getByRole("button", { name: "Ta bort Kungsfågel" })).toBeVisible();

  await top(page).getByRole("group", { name: "Visa som" }).getByRole("button", { name: "Andel av alla" }).click();
  await expect(rows.nth(1)).toContainText(/\d+,\d\s%$/);

  await top(page).getByRole("button", { name: "Visa alla 210 arter" }).click();
  await expect(rows).toHaveCount(210);
});

test("a full comparison disables the other rows", async ({ page }) => {
  await page.goto("/sammanfattning?arter=R%C3%96HAK,L%C3%96S%C3%85N,KUF%C3%85G,%C3%84NPIP,SKPIP,STARE");
  await expect(top(page).getByRole("button", { name: /Gransångare/ })).toBeDisabled();
  await expect(top(page).getByRole("button", { name: /Rödhake/ })).toBeEnabled();
});
