import { defineConfig, devices } from "@playwright/test";

const configuredBaseUrl = process.env.HOOKRELAY_E2E_BASE_URL;
const baseURL = configuredBaseUrl ?? "http://127.0.0.1:4173/console/";
const hasInspectionSeed = Boolean(
  process.env.HOOKRELAY_E2E_BOOTSTRAP_TOKEN ||
    (process.env.HOOKRELAY_E2E_API_KEY &&
      process.env.HOOKRELAY_E2E_DELIVERY_ID &&
      process.env.HOOKRELAY_E2E_EVENT_ID),
);

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  reporter: process.env.CI ? [["line"], ["html", { open: "never" }]] : "list",
  timeout: 30_000,
  expect: { timeout: 8_000 },
  use: {
    baseURL,
    // Traces contain network headers; never retain tenant or bootstrap credentials.
    trace: "off",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  ...(configuredBaseUrl || !hasInspectionSeed
    ? {}
    : {
        webServer: {
          command: "pnpm dev --host 127.0.0.1 --port 4173",
          url: "http://127.0.0.1:4173/console/",
          reuseExistingServer: !process.env.CI,
          timeout: 120_000,
        },
      }),
});
