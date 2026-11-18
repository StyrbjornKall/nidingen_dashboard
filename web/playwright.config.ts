import { defineConfig, devices } from "@playwright/test";

// End-to-end tests against the real API and the built site:
//   npm run build && npm run e2e
// FastAPI serves web/dist and reads the database in DUCKDB_PATH (.env).
const PORT = 8765;

export default defineConfig({
  testDir: "e2e",
  outputDir: "test-results",
  fullyParallel: true,
  workers: 4,
  reporter: [["list"]],
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    locale: "sv-SE",
    timezoneId: "Europe/Stockholm",
  },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } } },
    { name: "mobile", use: { ...devices["Pixel 7"] } },
  ],
  webServer: {
    command: `uv run uvicorn app.api.main:app --host 127.0.0.1 --port ${PORT}`,
    cwd: "..",
    url: `http://127.0.0.1:${PORT}/api/health`,
    reuseExistingServer: false,
    timeout: 60_000,
  },
});
