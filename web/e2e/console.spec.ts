import { expect, test, type APIRequestContext } from "@playwright/test";

const configuredApiKey = process.env.HOOKRELAY_E2E_API_KEY;
const configuredDeliveryId = process.env.HOOKRELAY_E2E_DELIVERY_ID;
const configuredEventId = process.env.HOOKRELAY_E2E_EVENT_ID;
const bootstrapToken = process.env.HOOKRELAY_E2E_BOOTSTRAP_TOKEN;
const replayDeliveryId = process.env.HOOKRELAY_E2E_REPLAY_DELIVERY_ID;
const replayEventId = process.env.HOOKRELAY_E2E_REPLAY_EVENT_ID;
const consoleBaseUrl =
  process.env.HOOKRELAY_E2E_BASE_URL ?? "http://127.0.0.1:4173/console/";
const apiOrigin = new URL(consoleBaseUrl).origin;

interface InspectionSeed {
  apiKey: string;
  deliveryId: string;
  eventId: string;
}

interface BootstrapResponse {
  api_key: { key: string };
}

interface EndpointResponse {
  id: string;
}

interface EventResponse {
  id: string;
  deliveries: Array<{ id: string }>;
}

async function requireOk(response: Awaited<ReturnType<APIRequestContext["post"]>>, step: string) {
  if (!response.ok()) {
    throw new Error(`${step} failed with HTTP ${response.status()}.`);
  }
}

async function provisionSeed(request: APIRequestContext): Promise<InspectionSeed> {
  if (configuredApiKey && configuredDeliveryId && configuredEventId) {
    return {
      apiKey: configuredApiKey,
      deliveryId: configuredDeliveryId,
      eventId: configuredEventId,
    };
  }
  if (!bootstrapToken) {
    throw new Error("No external E2E seed or bootstrap token was provided.");
  }

  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const tenantResponse = await request.post(`${apiOrigin}/v1/bootstrap/tenants`, {
    headers: {
      Authorization: `Bearer ${bootstrapToken}`,
      "Content-Type": "application/json",
    },
    data: {
      name: `Console E2E ${suffix}`,
      initial_api_key_name: "playwright",
    },
  });
  await requireOk(tenantResponse, "Tenant bootstrap");
  const tenant = (await tenantResponse.json()) as BootstrapResponse;

  const endpointResponse = await request.post(`${apiOrigin}/v1/endpoints`, {
    headers: {
      Authorization: `Bearer ${tenant.api_key.key}`,
      "Content-Type": "application/json",
    },
    data: {
      name: "Playwright receiver",
      url: process.env.HOOKRELAY_E2E_WEBHOOK_URL ?? "http://127.0.0.1:9000/webhooks",
    },
  });
  await requireOk(endpointResponse, "Endpoint seed");
  const endpoint = (await endpointResponse.json()) as EndpointResponse;

  const eventResponse = await request.post(`${apiOrigin}/v1/events`, {
    headers: {
      Authorization: `Bearer ${tenant.api_key.key}`,
      "Content-Type": "application/json",
      "Idempotency-Key": `console-e2e-${suffix}`,
    },
    data: {
      type: "console.e2e.pending",
      payload: { source: "playwright" },
      endpoint_ids: [endpoint.id],
    },
  });
  await requireOk(eventResponse, "Event seed");
  const event = (await eventResponse.json()) as EventResponse;
  const seededDelivery = event.deliveries[0];
  if (!seededDelivery) throw new Error("Event seed returned no delivery.");

  return {
    apiKey: tenant.api_key.key,
    deliveryId: seededDelivery.id,
    eventId: event.id,
  };
}

test.describe("operations console against a seeded HookRelay API", () => {
  let seed: InspectionSeed;

  test.skip(
    !bootstrapToken && !(configuredApiKey && configuredDeliveryId && configuredEventId),
    "Set an external seed or HOOKRELAY_E2E_BOOTSTRAP_TOKEN.",
  );

  test.beforeAll(async ({ request }) => {
    seed = await provisionSeed(request);
  });

  test("filters and inspects persisted attempt evidence", async ({ page }) => {
    await page.goto("./");
    await page.getByLabel("Tenant API key").fill(seed.apiKey);
    await page.getByRole("button", { name: "Open console" }).click();

    await expect(page.getByRole("heading", { name: "Delivery history" })).toBeVisible();
    await page.getByLabel("Event ID").fill(seed.eventId);
    await page.getByRole("button", { name: "Apply filters" }).click();

    const row = page.getByTestId(`delivery-${seed.deliveryId}`);
    await expect(row).toBeVisible();
    await row.getByRole("button", { name: /Inspect delivery/ }).click();
    await expect(page.getByRole("dialog")).toBeVisible();
    await expect(page.getByRole("heading", { name: "Attempt timeline" })).toBeVisible();
    await expect(page.getByText("No attempts recorded")).toBeVisible();
  });

  test("replays a deliberately seeded dead letter after confirmation", async ({ page }) => {
    test.skip(
      !replayDeliveryId || !replayEventId,
      "Set the single-use HOOKRELAY_E2E_REPLAY_DELIVERY_ID and HOOKRELAY_E2E_REPLAY_EVENT_ID seed.",
    );

    await page.goto("./");
    await page.getByLabel("Tenant API key").fill(seed.apiKey);
    await page.getByRole("button", { name: "Open console" }).click();
    await page.getByLabel("Event ID").fill(replayEventId!);
    await page.getByRole("button", { name: "Apply filters" }).click();

    const row = page.getByTestId(`delivery-${replayDeliveryId}`);
    await expect(row).toBeVisible();
    await row.getByRole("button", { name: /Inspect delivery/ }).click();
    await page.getByRole("button", { name: "Replay delivery" }).click();
    await expect(page.getByRole("alertdialog")).toBeVisible();
    await page.getByRole("button", { name: "Confirm replay" }).click();
    await expect(page.getByRole("button", { name: "Replay unavailable" })).toBeDisabled();
  });
});
