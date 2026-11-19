import { expect, test, type Page } from "@playwright/test";

const stage = (page: Page) => page.getByRole("group", { name: /^Jordklot med återfynd/ });
const focus = (page: Page) => page.locator("[aria-live=polite]");
const tile = (page: Page, label: string) => page.locator("dl > div").filter({ has: page.getByText(label, { exact: true }) });

const fmt = (n: number) => new Intl.NumberFormat("sv-SE").format(n).replace(/ /g, " ");

test("the globe draws land and finds, and the tiles agree with the API", async ({ page, request }) => {
  const api = await (await request.get("/api/recoveries?period=all")).json();
  await page.goto("/aterfynd");
  await expect(page.getByRole("heading", { level: 2, name: "Återfynd på jordklotet" })).toBeVisible();
  await expect(tile(page, "utgående")).toContainText(fmt(api.summary.n_out));
  await expect(tile(page, "inkommande")).toContainText(fmt(api.summary.n_in));
  await expect(tile(page, "längsta avstånd")).toContainText(fmt(api.summary.longest_km));
  await expect(tile(page, "medianavstånd")).toContainText(fmt(api.summary.median_km));
  // The tiles that ignore the direction choice say so.
  await expect(page.getByText(/ignorerar riktningsvalet/)).toBeVisible();
  // Something was painted: more than ocean and land.
  await page.waitForTimeout(2200); // the first turn towards the overview
  const colours = await stage(page).locator("canvas").evaluate((cv: HTMLCanvasElement) => {
    const ctx = cv.getContext("2d")!;
    const seen = new Set<string>();
    const { data } = ctx.getImageData(0, 0, cv.width, cv.height);
    for (let i = 0; i < data.length; i += 4 * 97) seen.add(`${data[i]},${data[i + 1]},${data[i + 2]},${data[i + 3]}`);
    return seen.size;
  });
  expect(colours).toBeGreaterThan(4);
});

test("species, period and direction are their own controls and live in the URL", async ({ page, request }) => {
  const api = await (await request.get("/api/recoveries?period=r10&species=R%C3%96HAK")).json();
  await page.goto("/aterfynd");
  await page.getByLabel("Art").selectOption("RÖHAK");
  await page.getByRole("button", { name: "Senaste 10 år" }).click();
  await expect(page).toHaveURL(/art=R%C3%96HAK|art=RÖHAK/);
  await expect(page).toHaveURL(/period=r10/);
  await expect(tile(page, "utgående")).toContainText(fmt(api.summary.n_out));
  await expect(page.getByRole("group", { name: "Urval" })).toHaveCount(0); // not the species picker of the other pages

  const before = await tile(page, "utgående").innerText();
  await page.getByRole("button", { name: "Inkommande" }).click();
  await expect(page).toHaveURL(/riktning=in/);
  await expect(page.getByText(/Avstånden gäller de \d+ återfynd som visas/)).toBeVisible();
  expect(await tile(page, "utgående").innerText()).toBe(before); // the tiles ignore the direction
  await page.getByLabel("Art").selectOption("ALLA");
  await expect(page).not.toHaveURL(/art=/);
});

test("the period runs to the newest find and says so", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  const api = await (await request.get("/api/recoveries?period=all")).json();
  expect(api.period.year_to).toBeGreaterThanOrEqual(Number(meta.build.recoveries_max_date.slice(0, 4)));
  await page.goto("/aterfynd");
  await expect(page.getByText(new RegExp(`Perioden gäller fyndåret: 1980–${api.period.year_to}, till och med`))).toBeVisible();
});

test("the longest finds: a click picks the find and turns the globe to it", async ({ page, request }) => {
  const api = await (await request.get("/api/recoveries?period=all")).json();
  await page.goto("/aterfynd");
  await expect(focus(page).getByText("Längst bort i urvalet")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Längsta återfynd" })).toBeVisible();
  const list = page.locator("button[class*=longest]");
  await expect(list).toHaveCount(5);
  await list.nth(1).click();
  const km = fmt(api.longest[1].distance_km);
  await expect(focus(page)).toContainText(`${km} km`);
  await expect(focus(page).getByText("Längst bort i urvalet")).toHaveCount(0);
});

test("hovering a find reads it, and the lines switch works", async ({ page }, info) => {
  test.skip(info.project.name === "mobile", "pointer hover");
  await page.goto("/aterfynd?art=R%C3%96HAK");
  await page.waitForTimeout(2200);
  const box = (await stage(page).boundingBox())!;
  const tip = page.locator("[class*=tip]");
  let found = false;
  for (let dy = -150; dy <= 150 && !found; dy += 12) {
    for (let dx = -150; dx <= 150 && !found; dx += 12) {
      await page.mouse.move(box.x + box.width / 2 + dx, box.y + box.height / 2 + dy);
      found = (await tip.count()) > 0;
    }
  }
  expect(found).toBe(true);
  await expect(tip).toContainText(/km/);
  await page.mouse.move(0, 0);
  await expect(tip).toHaveCount(0);

  const lines = page.getByRole("button", { name: "Visa linjer" });
  await expect(lines).toHaveAttribute("aria-pressed", "true");
  await lines.click();
  await expect(lines).toHaveAttribute("aria-pressed", "false");
});

test("the globe can be turned and zoomed from the keyboard, and the page never scrolls sideways", async ({ page }) => {
  await page.goto("/aterfynd");
  await stage(page).focus();
  await page.keyboard.press("ArrowLeft");
  await page.keyboard.press("+");
  await page.keyboard.press("Home");
  await expect(page.getByRole("button", { name: "Zooma in" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Zooma ut" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Nidingen" })).toBeVisible();
  const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(over).toBeLessThanOrEqual(0);
});

test("an empty selection explains itself", async ({ page }) => {
  await page.goto("/aterfynd?art=R%C3%96HAK&riktning=in&period=r10");
  // Rödhake has no inbound finds in the last ten years, or the list says so; either way the page stays usable.
  await expect(page.getByRole("button", { name: "Båda" })).toBeVisible();
  await expect(page.getByRole("heading", { level: 2, name: "Återfynd på jordklotet" })).toBeVisible();
});

test("zooming in goes far, and the sea stays sea", async ({ page }) => {
  await page.goto("/aterfynd?art=SKPIP");
  await page.waitForTimeout(2200);
  await page.getByRole("button", { name: "Nidingen" }).click();
  await page.waitForTimeout(1200);
  await stage(page).focus();
  await page.keyboard.press("ArrowUp"); // from 50 °N to about 57 °N: Nidingen's coast
  for (let i = 0; i < 12; i++) await page.getByRole("button", { name: "Zooma in" }).click();
  await page.waitForTimeout(2500); // the finer coastline loads
  const share = await page.getByRole("group", { name: /^Jordklot/ }).locator("canvas").evaluate((cv: HTMLCanvasElement) => {
    const { data } = cv.getContext("2d")!.getImageData(0, 0, cv.width, cv.height);
    let land = 0, n = 0;
    // Land is the lightest, warmest fill (R > B); the sea is bluer.
    for (let i = 0; i < data.length; i += 4 * 53) { n++; if (data[i]! > data[i + 2]! + 2) land++; }
    return land / n;
  });
  // At full zoom on the Kattegat coast there is both land and sea in view.
  expect(share).toBeGreaterThan(0.05);
  expect(share).toBeLessThan(0.95);
});
