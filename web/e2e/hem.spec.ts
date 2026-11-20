import { expect, test } from "@playwright/test";

const fmt = (n: number) => new Intl.NumberFormat("sv-SE").format(n).replace(/ /g, " ");
const approx = (n: number) => `~${fmt(Math.round(n / 1000) * 1000)}`;

test("the hero names the station and the start year from the data", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  await page.goto("/");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Nidingens Fågelstation");
  await expect(page.getByText(`Historisk ringmärkningsdata från ${meta.first_year}`)).toBeVisible();
  await expect(page.getByRole("img", { name: /^Flygfoto över Nidingen/ })).toBeVisible();
  await expect(page).toHaveTitle("Nidingens Fågelstation");
  await expect(page.getByRole("link", { name: "Hem" })).toHaveAttribute("aria-current", "page");
});

test("the years and the key figures come from the API, not from the text", async ({ page, request }) => {
  const meta = await (await request.get("/api/meta")).json();
  const k = await (await request.get("/api/summary/kpis")).json();
  await page.goto("/");
  const years = meta.complete_through - meta.first_year + 1;
  await expect(page.getByRole("heading", { level: 2, name: `${years} år av ringmärkning på en ö i Kattegatt` })).toBeVisible();
  const figures = page.getByRole("region", { name: "Nyckeltal" });
  const tile = (label: string) => figures.locator("dl > div").filter({ has: page.getByText(label, { exact: true }) });
  await expect(tile("ringmärkta fåglar")).toContainText(approx(k.n_new));
  await expect(tile("kontroller på ön")).toContainText(approx(k.n_recap));
  await expect(tile("återfynd världen över")).toContainText(approx(k.n_recoveries));
  await expect(tile("unika arter och raser")).toContainText(fmt(k.n_species));
  // Value above label, as in the design.
  const box = async (loc: ReturnType<typeof tile>, part: string) => (await loc.locator(part).boundingBox())!.y;
  expect(await box(tile("ringmärkta fåglar"), "dd")).toBeLessThan(await box(tile("ringmärkta fåglar"), "dt"));
});

test("every page has an entry card, and the buttons lead on", async ({ page }) => {
  await page.goto("/");
  const cards = page.locator("a[class*=card]");
  await expect(cards).toHaveCount(7);
  const targets = await cards.evaluateAll((els) => els.map((e) => e.getAttribute("href")));
  expect(targets).toEqual(["/sammanfattning", "/trender", "/biometri", "/fenologi", "/aterfynd", "/vader", "/om"]);
  await page.getByRole("link", { name: "Utforska datan" }).click();
  await expect(page).toHaveURL(/\/sammanfattning$/);
  await page.goBack();
  await page.getByRole("link", { name: "Om webbplatsen" }).first().click();
  await expect(page).toHaveURL(/\/om$/);
  await page.goBack();
  await cards.nth(5).click();
  await expect(page).toHaveURL(/\/vader$/);
});

test("the text on the glass card stays readable over the photo", async ({ page }) => {
  await page.goto("/");
  await page.waitForTimeout(1800);
  const glass = page.locator("[class*=glass]");
  const items = await glass.evaluate((el) => {
    const out: { x: number; y: number; w: number; h: number; color: string }[] = [];
    for (const sel of ["[class*=kicker]", "h1", "[class*=sub]"]) {
      const n = el.querySelector(sel)!;
      const r = n.getBoundingClientRect();
      out.push({ x: r.x, y: r.y, w: r.width, h: r.height, color: getComputedStyle(n).color });
    }
    el.querySelectorAll("a").forEach((a) => ((a as HTMLElement).style.visibility = "hidden"));
    el.querySelectorAll("p,h1").forEach((n) => ((n as HTMLElement).style.visibility = "hidden"));
    return out;
  });
  const box = (await glass.boundingBox())!;
  const shot = await page.screenshot({ clip: box });
  const ratios = await page.evaluate(async ({ b64, items, box }) => {
    const img = new Image();
    img.src = `data:image/png;base64,${b64}`;
    await img.decode();
    const cv = document.createElement("canvas");
    cv.width = img.width;
    cv.height = img.height;
    const ctx = cv.getContext("2d")!;
    ctx.drawImage(img, 0, 0);
    const lum = (r: number, g: number, b: number) => {
      const f = (v: number) => ((v /= 255) <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4);
      return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
    };
    return items.map((it) => {
      const m = /rgb\((\d+), (\d+), (\d+)/.exec(it.color)!;
      const lt = lum(+m[1]!, +m[2]!, +m[3]!);
      const x0 = Math.max(0, Math.floor(it.x - box.x));
      const y0 = Math.max(0, Math.floor(it.y - box.y));
      const d = ctx.getImageData(x0, y0, Math.min(img.width - x0, Math.ceil(it.w)), Math.min(img.height - y0, Math.ceil(it.h))).data;
      let worst = 99;
      for (let i = 0; i < d.length; i += 8) {
        const lb = lum(d[i]!, d[i + 1]!, d[i + 2]!);
        worst = Math.min(worst, (Math.max(lb, lt) + 0.05) / (Math.min(lb, lt) + 0.05));
      }
      return worst;
    });
  }, { b64: shot.toString("base64"), items, box });
  // WCAG AA for normal text, on the worst pixel behind each line.
  for (const r of ratios) expect(r).toBeGreaterThanOrEqual(4.5);
});

test("no sideways page scroll", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("a[class*=card]").first()).toBeVisible();
  const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(over).toBeLessThanOrEqual(0);
});
