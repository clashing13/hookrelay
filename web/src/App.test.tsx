import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import type {
  DeliveryAttempt,
  DeliveryHistoryPage,
  DeliveryInspection,
  ProblemDetail,
} from "./api";

const delivery: DeliveryInspection = {
  id: "11111111-1111-4111-8111-111111111111",
  event_id: "22222222-2222-4222-8222-222222222222",
  event_type: "invoice.created",
  endpoint_id: "33333333-3333-4333-8333-333333333333",
  endpoint_name: "Billing ledger",
  status: "dead_lettered",
  dispatch_generation: 3,
  created_at: "2026-08-07T12:00:00Z",
  next_attempt_at: null,
  dead_lettered_at: "2026-08-07T12:02:00Z",
  dead_letter_reason: "attempts_exhausted",
  attempt_count: 1,
  last_attempt_at: "2026-08-07T12:01:00Z",
  replayable: true,
};

const secondDelivery: DeliveryInspection = {
  ...delivery,
  id: "44444444-4444-4444-8444-444444444444",
  event_id: "55555555-5555-4555-8555-555555555555",
  event_type: "subscription.renewed",
};

const attempt: DeliveryAttempt = {
  id: "66666666-6666-4666-8666-666666666666",
  delivery_id: delivery.id,
  attempt_number: 1,
  dispatch_generation: 3,
  is_circuit_probe: true,
  started_at: "2026-08-07T12:00:30Z",
  finished_at: "2026-08-07T12:00:31Z",
  outcome: "transient_failure",
  response_status_code: 503,
  error_code: "http_5xx",
  duration_ms: 987,
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": status >= 400 ? "application/problem+json" : "application/json",
    },
  });
}

function page(
  items: DeliveryInspection[],
  nextCursor: string | null = null,
): DeliveryHistoryPage {
  return { items, next_cursor: nextCursor };
}

async function unlockConsole(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText("Tenant API key"), "hrk_public.secret");
  await user.click(screen.getByRole("button", { name: "Open console" }));
}

describe("HookRelay operations console", () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    window.localStorage.clear();
    window.sessionStorage.clear();
  });

  it("authenticates with a memory-only bearer credential and clears it on sign out", async () => {
    const storageWrite = vi.spyOn(Storage.prototype, "setItem");
    fetchMock
      .mockResolvedValueOnce(jsonResponse(page([])))
      .mockResolvedValueOnce(jsonResponse(page([delivery])));
    const user = userEvent.setup();

    render(<App />);
    await unlockConsole(user);

    expect(await screen.findByText("invoice.created")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/deliveries?limit=1",
      expect.objectContaining({ cache: "no-store" }),
    );
    const firstInit = fetchMock.mock.calls[0]?.[1] as RequestInit;
    expect(new Headers(firstInit.headers).get("Authorization")).toBe(
      "Bearer hrk_public.secret",
    );
    expect(storageWrite).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Sign out" }));
    expect(screen.getByLabelText("Tenant API key")).toHaveValue("");
    expect(window.localStorage).toHaveLength(0);
    expect(window.sessionStorage).toHaveLength(0);
  });

  it("applies tenant-safe filters, appends a cursor page, and refreshes manually", async () => {
    fetchMock
      .mockResolvedValueOnce(jsonResponse(page([])))
      .mockResolvedValueOnce(jsonResponse(page([delivery], "cursor-one")))
      .mockResolvedValueOnce(jsonResponse(page([delivery], "cursor-two")))
      .mockResolvedValueOnce(jsonResponse(page([secondDelivery])))
      .mockResolvedValueOnce(jsonResponse(page([delivery])));
    const user = userEvent.setup();

    render(<App />);
    await unlockConsole(user);
    expect(await screen.findByText("invoice.created")).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText("Status"), "dead_lettered");
    await user.type(screen.getByLabelText("Endpoint ID"), delivery.endpoint_id);
    await user.type(screen.getByLabelText("Event ID"), delivery.event_id);
    await user.click(screen.getByRole("button", { name: "Apply filters" }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    expect(String(fetchMock.mock.calls[2]?.[0])).toContain("status=dead_lettered");
    expect(String(fetchMock.mock.calls[2]?.[0])).toContain(
      `endpoint_id=${delivery.endpoint_id}`,
    );
    expect(String(fetchMock.mock.calls[2]?.[0])).toContain(`event_id=${delivery.event_id}`);

    await user.click(await screen.findByRole("button", { name: "Load more" }));
    expect(await screen.findByText("subscription.renewed")).toBeInTheDocument();
    expect(String(fetchMock.mock.calls[3]?.[0])).toContain("cursor=cursor-two");

    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(5));
    expect(screen.queryByText("subscription.renewed")).not.toBeInTheDocument();
  });

  it("shows attempt detail and confirms replay with the observed generation", async () => {
    const conflict: ProblemDetail = {
      type: "urn:hookrelay:problem:replay-conflict",
      title: "Replay conflict",
      status: 409,
      code: "replay_conflict",
      detail: "The delivery generation changed before replay was accepted.",
    };
    fetchMock
      .mockResolvedValueOnce(jsonResponse(page([])))
      .mockResolvedValueOnce(jsonResponse(page([delivery])))
      .mockResolvedValueOnce(jsonResponse(delivery))
      .mockResolvedValueOnce(jsonResponse({ items: [attempt], next_cursor: null }))
      .mockResolvedValueOnce(jsonResponse(conflict, 409))
      .mockResolvedValueOnce(
        jsonResponse({
          id: delivery.id,
          event_id: delivery.event_id,
          status: "pending",
          dispatch_generation: 4,
        }),
      );
    const user = userEvent.setup();

    render(<App />);
    await unlockConsole(user);
    const row = await screen.findByTestId(`delivery-${delivery.id}`);
    await user.click(within(row).getByRole("button", { name: /Inspect delivery/ }));

    expect(await screen.findByRole("heading", { name: "Attempt 1" })).toBeInTheDocument();
    expect(screen.getByText("Circuit probe")).toBeInTheDocument();
    expect(screen.getByText("http_5xx")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Replay delivery" }));
    expect(screen.getByRole("alertdialog")).toHaveTextContent("observed generation 3");
    expect(fetchMock).toHaveBeenCalledTimes(4);

    await user.click(screen.getByRole("button", { name: "Confirm replay" }));
    expect(await screen.findByText("Replay conflict")).toBeInTheDocument();
    const replayInit = fetchMock.mock.calls[4]?.[1] as RequestInit;
    expect(replayInit.method).toBe("POST");
    expect(replayInit.body).toBe(JSON.stringify({ expected_dispatch_generation: 3 }));

    await user.click(screen.getByRole("button", { name: "Confirm replay" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Replay unavailable" })).toBeDisabled(),
    );
    expect(screen.getAllByText("pending").length).toBeGreaterThan(0);
  });

  it("renders sanitized RFC Problem Details at the credential boundary", async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse(
        {
          type: "urn:hookrelay:problem:invalid-credentials",
          title: "Authentication failed",
          status: 401,
          code: "invalid_credentials",
          detail: "A valid HookRelay bearer credential is required.",
        },
        401,
      ),
    );
    const user = userEvent.setup();

    render(<App />);
    await unlockConsole(user);

    expect(await screen.findByRole("alert")).toHaveTextContent("Authentication failed");
    expect(screen.getByRole("alert")).toHaveTextContent("invalid_credentials");
    expect(screen.getByLabelText("Tenant API key")).toBeInTheDocument();
  });
});
