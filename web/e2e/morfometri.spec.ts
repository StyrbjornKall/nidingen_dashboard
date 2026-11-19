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
  await expect(bubbles(page).locator("div[class*=readout]")).toContainText("Taltrast");
  await expect(bubbles(page).locator("div[class*=readout]")).toContainText(/\d+,\d g · \d+,\d mm/);
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

const plot = (page: Page) => bubbles(page).locator("[class*=clip]");
const level = (page: Page) => bubbles(page).locator("[class*=zoomLevel]");
const inView = (page: Page) => bubbles(page).locator("button[aria-pressed]");

test("the bubble chart zooms with the buttons, and 'Visa alla' goes back", async ({ page }) => {
  await page.goto("/morfometri?arter=R%C3%96HAK");
  await expect(inView(page).first()).toBeVisible();
  const all = await inView(page).count();
  const zoomIn = bubbles(page).getByRole("button", { name: "Zooma in" });
  const out = bubbles(page).getByRole("button", { name: "Zooma ut" });
  const reset = bubbles(page).getByRole("button", { name: "Visa alla" });
  await expect(out).toBeDisabled();
  await expect(reset).toBeDisabled();
  await expect(level(page)).toHaveText("1×");
  const ticksBefore = await bubbles(page).locator("[class*=xTick]").allTextContents();
  await zoomIn.click();
  await zoomIn.click();
  await expect(level(page)).toHaveText("2,6×");
  await expect(out).toBeEnabled();
  expect(await inView(page).count()).toBeLessThan(all); // what is outside the view is left out
  expect(await bubbles(page).locator("[class*=xTick]").allTextContents()).not.toEqual(ticksBefore);
  await reset.click();
  await expect(level(page)).toHaveText("1×");
  await expect(inView(page)).toHaveCount(all);
  expect(await bubbles(page).locator("[class*=xTick]").allTextContents()).toEqual(ticksBefore);
});

test("the bubble chart can be moved: drag and the arrow keys", async ({ page }, info) => {
  await page.goto("/morfometri?arter=R%C3%96HAK");
  const zoomIn = bubbles(page).getByRole("button", { name: "Zooma in" });
  for (let i = 0; i < 4; i++) await zoomIn.click();
  await page.waitForTimeout(300);
  const start = await bubbles(page).locator("[class*=xTick]").allTextContents();
  await plot(page).focus();
  await page.keyboard.press("ArrowRight");
  await page.waitForTimeout(300);
  const moved = await bubbles(page).locator("[class*=xTick]").allTextContents();
  expect(moved).not.toEqual(start);
  await page.keyboard.press("ArrowLeft");
  await page.waitForTimeout(300);
  expect(await bubbles(page).locator("[class*=xTick]").allTextContents()).toEqual(start);
  await page.keyboard.press("Home");
  await expect(level(page)).toHaveText("1×");
  if (info.project.name === "mobile") return;
  for (let i = 0; i < 4; i++) await zoomIn.click();
  const box = (await plot(page).boundingBox())!;
  await page.mouse.move(box.x + box.width * 0.6, box.y + box.height * 0.5);
  await page.mouse.down();
  await page.mouse.move(box.x + box.width * 0.2, box.y + box.height * 0.5, { steps: 6 });
  await page.mouse.up();
  await page.waitForTimeout(300);
  expect(await bubbles(page).locator("[class*=xTick]").allTextContents()).not.toEqual(start);
});

test("a species hidden behind another can be reached by zooming in and selected", async ({ page }, info) => {
  test.skip(info.project.name === "mobile", "pointer wheel");
  await page.goto("/morfometri?arter=R%C3%96HAK,%C3%84NPIP,SKPIP");
  await page.waitForTimeout(1200); // the bubbles pop in
  // A grey bubble whose centre is covered by another bubble at full view.
  const hidden = await page.evaluate(() => {
    const els = [...document.querySelectorAll<HTMLButtonElement>("button[class*=bubble][aria-pressed=false]")];
    for (const el of els) {
      const r = el.getBoundingClientRect();
      const top = document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2);
      if (top && top !== el && top.matches("button[class*=bubble]")) return { label: el.getAttribute("aria-label")!, x: r.x + r.width / 2, y: r.y + r.height / 2 };
    }
    return null;
  });
  expect(hidden, "some bubble should be covered at full view").not.toBeNull();
  const species = hidden!.label.replace(/, lägg till i jämförelsen$/, "");
  // Zoom in around it with Ctrl + wheel until it is on top at its own centre.
  const target = page.getByRole("button", { name: `${species}, lägg till i jämförelsen` });
  await plot(page).scrollIntoViewIfNeeded();
  let reachable = false;
  for (let step = 0; step < 25 && !reachable; step++) {
    const r = await target.evaluate((el) => {
      const b = el.getBoundingClientRect();
      const top = document.elementFromPoint(b.x + b.width / 2, b.y + b.height / 2);
      return { x: b.x + b.width / 2, y: b.y + b.height / 2, own: top === el };
    }).catch(() => null);
    if (!r) break;
    if (r.own) {
      reachable = true;
      break;
    }
    await page.keyboard.down("Control");
    await page.mouse.move(r.x, r.y);
    await page.mouse.wheel(0, -120);
    await page.keyboard.up("Control");
    await page.waitForTimeout(260);
  }
  expect(reachable).toBe(true);
  await expect(level(page)).not.toHaveText("1×");
  await target.click();
  await expect(keys(page).getByText(species, { exact: true })).toBeVisible();
});
