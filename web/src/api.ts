export const DELIVERY_STATUSES = [
  "pending",
  "delivering",
  "retry_scheduled",
  "succeeded",
  "dead_lettered",
] as const;

export type DeliveryStatus = (typeof DELIVERY_STATUSES)[number];

export type DeadLetterReason =
  | "permanent_failure"
  | "attempts_exhausted"
  | "target_blocked";

export type DeliveryAttemptOutcome =
  | "succeeded"
  | "transient_failure"
  | "permanent_failure"
  | "abandoned";

export interface InvalidParameter {
  pointer: string;
  code: string;
  message: string;
}
export interface ProblemDetail {
  type: string;
  title: string;
  status: number;
  code: string;
  detail: string;
  errors?: InvalidParameter[];
}

export interface DeliveryInspection {
  id: string;
  event_id: string;
  event_type: string;
  endpoint_id: string;
  endpoint_name: string;
  status: DeliveryStatus;
  dispatch_generation: number;
  created_at: string;
  next_attempt_at: string | null;
  dead_lettered_at: string | null;
  dead_letter_reason: DeadLetterReason | null;
  attempt_count: number;
  last_attempt_at: string | null;
  replayable: boolean;
}

export interface DeliveryHistoryPage {
  items: DeliveryInspection[];
  next_cursor: string | null;
}

export interface DeliveryAttempt {
  id: string;
  delivery_id: string;
  attempt_number: number;
  dispatch_generation: number;
  is_circuit_probe: boolean;
  started_at: string;
  finished_at: string | null;
  outcome: DeliveryAttemptOutcome | null;
  response_status_code: number | null;
  error_code: string | null;
  duration_ms: number | null;
}

export interface DeliveryAttemptPage {
  items: DeliveryAttempt[];
  next_cursor: string | null;
}

export interface DeliveryReplayResponse {
  id: string;
  event_id: string;
  status: "pending";
  dispatch_generation: number;
}

export interface DeliveryFilters {
  status: DeliveryStatus | "";
  endpointId: string;
  eventId: string;
}

export interface ListDeliveryOptions {
  filters?: DeliveryFilters;
  cursor?: string;
  limit?: number;
}

const API_PREFIX = "/v1";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isProblemDetail(value: unknown): value is ProblemDetail {
  return (
    isRecord(value) &&
    typeof value.type === "string" &&
    typeof value.title === "string" &&
    typeof value.status === "number" &&
    typeof value.code === "string" &&
    typeof value.detail === "string"
  );
}

function fallbackProblem(status: number): ProblemDetail {
  return {
    type: "about:blank",
    title: "Request failed",
    status,
    code: "request_failed",
    detail: "The request could not be completed.",
  };
}

export class ApiProblemError extends Error {
  readonly problem: ProblemDetail;

  constructor(problem: ProblemDetail) {
    super(problem.detail);
    this.name = "ApiProblemError";
    this.problem = problem;
  }
}

async function parseJson(response: Response): Promise<unknown> {
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  if (!contentType.includes("json")) {
    return null;
  }
  try {
    return await response.json();
  } catch {
    return null;
  }
}

function queryString(options: ListDeliveryOptions): string {
  const params = new URLSearchParams();
  const filters = options.filters;
  if (filters?.status) params.set("status", filters.status);
  if (filters?.endpointId.trim()) params.set("endpoint_id", filters.endpointId.trim());
  if (filters?.eventId.trim()) params.set("event_id", filters.eventId.trim());
  if (options.cursor) params.set("cursor", options.cursor);
  if (options.limit !== undefined) params.set("limit", String(options.limit));
  const encoded = params.toString();
  return encoded ? `?${encoded}` : "";
}

/**
 * A tenant-scoped client whose origin is deliberately fixed to the current host.
 * The API key lives only in this instance and is never persisted by the console.
 */
export class HookRelayClient {
  readonly #apiKey: string;

  constructor(apiKey: string) {
    this.#apiKey = apiKey;
  }

  async #request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    headers.set("Accept", "application/json, application/problem+json");
    headers.set("Authorization", `Bearer ${this.#apiKey}`);

    const response = await fetch(`${API_PREFIX}${path}`, {
      ...init,
      cache: "no-store",
      credentials: "same-origin",
      headers,
    });
    const body = await parseJson(response);
    if (!response.ok) {
      throw new ApiProblemError(
        isProblemDetail(body) ? body : fallbackProblem(response.status),
      );
    }
    return body as T;
  }

  listDeliveries(options: ListDeliveryOptions = {}): Promise<DeliveryHistoryPage> {
    return this.#request<DeliveryHistoryPage>(`/deliveries${queryString(options)}`);
  }

  getDelivery(deliveryId: string): Promise<DeliveryInspection> {
    return this.#request<DeliveryInspection>(
      `/deliveries/${encodeURIComponent(deliveryId)}`,
    );
  }

  listDeliveryAttempts(
    deliveryId: string,
    options: { cursor?: string; limit?: number } = {},
  ): Promise<DeliveryAttemptPage> {
    const params = new URLSearchParams();
    if (options.cursor) params.set("cursor", options.cursor);
    if (options.limit !== undefined) params.set("limit", String(options.limit));
    const encoded = params.toString();
    return this.#request<DeliveryAttemptPage>(
      `/deliveries/${encodeURIComponent(deliveryId)}/attempts${encoded ? `?${encoded}` : ""}`,
    );
  }

  replayDelivery(
    deliveryId: string,
    expectedDispatchGeneration: number,
  ): Promise<DeliveryReplayResponse> {
    return this.#request<DeliveryReplayResponse>(
      `/deliveries/${encodeURIComponent(deliveryId)}/replay`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          expected_dispatch_generation: expectedDispatchGeneration,
        }),
      },
    );
  }
}

export function problemFrom(error: unknown): ProblemDetail {
  if (error instanceof ApiProblemError) return error.problem;
  return {
    type: "about:blank",
    title: "Connection failed",
    status: 0,
    code: "network_error",
    detail: "HookRelay could not be reached. Check the service and try again.",
  };
}
