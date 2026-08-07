import {
  type FormEvent,
  type KeyboardEvent as ReactKeyboardEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  DELIVERY_STATUSES,
  HookRelayClient,
  type DeliveryAttempt,
  type DeliveryFilters,
  type DeliveryHistoryPage,
  type DeliveryInspection,
  type DeliveryReplayResponse,
  type DeliveryStatus,
  type ProblemDetail,
  problemFrom,
} from "./api";

const EMPTY_FILTERS: DeliveryFilters = {
  status: "",
  endpointId: "",
  eventId: "",
};

const PAGE_SIZE = 50;
const ATTEMPT_PAGE_SIZE = 100;

function statusLabel(status: DeliveryStatus): string {
  return status.replaceAll("_", " ");
}

function outcomeLabel(outcome: DeliveryAttempt["outcome"]): string {
  return outcome?.replaceAll("_", " ") ?? "in progress";
}

function formatTimestamp(value: string | null): string {
  if (value === null) return "—";
  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.valueOf())) return "—";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(timestamp);
}

function formatDuration(value: number | null): string {
  if (value === null) return "—";
  if (value < 1_000) return `${value} ms`;
  return `${(value / 1_000).toFixed(2)} s`;
}

function shortId(value: string): string {
  return value.length > 14 ? `${value.slice(0, 8)}…${value.slice(-4)}` : value;
}

function ProblemNotice({ problem }: { problem: ProblemDetail }) {
  return (
    <div className="problem-notice" role="alert">
      <div>
        <strong>{problem.title}</strong>
        <p>{problem.detail}</p>
      </div>
      <code>{problem.code}</code>
      {problem.errors && problem.errors.length > 0 ? (
        <ul>
          {problem.errors.map((error) => (
            <li key={`${error.pointer}-${error.code}`}>
              <code>{error.pointer}</code>: {error.message}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

interface CredentialGateProps {
  onAuthenticated: (apiKey: string) => void;
}

function CredentialGate({ onAuthenticated }: CredentialGateProps) {
  const [apiKey, setApiKey] = useState("");
  const [problem, setProblem] = useState<ProblemDetail | null>(null);
  const [isChecking, setIsChecking] = useState(false);

  async function authenticate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const candidate = apiKey.trim();
    if (!candidate) {
      setProblem({
        type: "about:blank",
        title: "API key required",
        status: 0,
        code: "credential_required",
        detail: "Enter a HookRelay tenant API key to continue.",
      });
      return;
    }

    setIsChecking(true);
    setProblem(null);
    try {
      const client = new HookRelayClient(candidate);
      await client.listDeliveries({ limit: 1 });
      setApiKey("");
      onAuthenticated(candidate);
    } catch (error) {
      setProblem(problemFrom(error));
    } finally {
      setIsChecking(false);
    }
  }

  return (
    <main className="gate-shell">
      <section className="credential-card" aria-labelledby="credential-title">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">
            HR
          </span>
          <span>HookRelay</span>
        </div>
        <p className="eyebrow">Operations console</p>
        <h1 id="credential-title">Inspect delivery evidence</h1>
        <p className="gate-intro">
          Authenticate with a tenant API key. The credential is held in memory
          for this tab and cleared on sign out or reload.
        </p>

        <form onSubmit={(event) => void authenticate(event)}>
          <label htmlFor="api-key">Tenant API key</label>
          <input
            id="api-key"
            name="api-key"
            type="password"
            value={apiKey}
            onChange={(event) => setApiKey(event.target.value)}
            autoComplete="off"
            autoCapitalize="none"
            spellCheck={false}
            disabled={isChecking}
            aria-describedby="credential-help"
          />
          <p id="credential-help" className="field-help">
            Sent only as a Bearer credential to this origin’s <code>/v1</code>{" "}
            API.
          </p>
          {problem ? <ProblemNotice problem={problem} /> : null}
          <button className="button button-primary button-wide" disabled={isChecking}>
            {isChecking ? "Verifying…" : "Open console"}
          </button>
        </form>
      </section>
    </main>
  );
}

interface OperationsConsoleProps {
  apiKey: string;
  onSignOut: () => void;
}

function OperationsConsole({ apiKey, onSignOut }: OperationsConsoleProps) {
  const client = useMemo(() => new HookRelayClient(apiKey), [apiKey]);
  const [draftFilters, setDraftFilters] = useState<DeliveryFilters>(EMPTY_FILTERS);
  const [activeFilters, setActiveFilters] = useState<DeliveryFilters>(EMPTY_FILTERS);
  const [deliveries, setDeliveries] = useState<DeliveryInspection[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [selectedDeliveryId, setSelectedDeliveryId] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [problem, setProblem] = useState<ProblemDetail | null>(null);
  const [announcement, setAnnouncement] = useState("");
  const requestSerial = useRef(0);
  const inspectButtons = useRef(new Map<string, HTMLButtonElement>());

  const loadDeliveries = useCallback(
    async (
      mode: "replace" | "append",
      filters: DeliveryFilters,
      cursor?: string,
    ) => {
      const serial = ++requestSerial.current;
      setIsLoading(true);
      setProblem(null);
      try {
        const page = await client.listDeliveries({
          filters,
          limit: PAGE_SIZE,
          ...(cursor ? { cursor } : {}),
        });
        if (serial !== requestSerial.current) return;
        setDeliveries((current) => {
          if (mode === "replace") return page.items;
          const seen = new Set(current.map((item) => item.id));
          return [...current, ...page.items.filter((item) => !seen.has(item.id))];
        });
        setNextCursor(page.next_cursor);
        setAnnouncement(
          mode === "append"
            ? `${page.items.length} more deliveries loaded.`
            : `${page.items.length} deliveries loaded.`,
        );
      } catch (error) {
        if (serial !== requestSerial.current) return;
        setProblem(problemFrom(error));
        setAnnouncement("Delivery history could not be loaded.");
      } finally {
        if (serial === requestSerial.current) setIsLoading(false);
      }
    },
    [client],
  );

  useEffect(() => {
    void loadDeliveries("replace", EMPTY_FILTERS);
  }, [loadDeliveries]);

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const filters = {
      ...draftFilters,
      endpointId: draftFilters.endpointId.trim(),
      eventId: draftFilters.eventId.trim(),
    };
    setActiveFilters(filters);
    setSelectedDeliveryId(null);
    void loadDeliveries("replace", filters);
  }

  function clearFilters() {
    setDraftFilters(EMPTY_FILTERS);
    setActiveFilters(EMPTY_FILTERS);
    setSelectedDeliveryId(null);
    void loadDeliveries("replace", EMPTY_FILTERS);
  }

  function closeDetail() {
    const deliveryId = selectedDeliveryId;
    setSelectedDeliveryId(null);
    if (deliveryId) {
      queueMicrotask(() => inspectButtons.current.get(deliveryId)?.focus());
    }
  }

  function deliveryReplayed(replay: DeliveryReplayResponse) {
    setDeliveries((current) =>
      current.map((delivery) =>
        delivery.id === replay.id
          ? {
              ...delivery,
              status: replay.status,
              dispatch_generation: replay.dispatch_generation,
              next_attempt_at: null,
              dead_lettered_at: null,
              dead_letter_reason: null,
              replayable: false,
            }
          : delivery,
      ),
    );
    setAnnouncement(
      `Delivery replay accepted as generation ${replay.dispatch_generation}.`,
    );
  }

  const selectedSummary =
    deliveries.find((delivery) => delivery.id === selectedDeliveryId) ?? null;

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand-lockup brand-lockup-light">
          <span className="brand-mark" aria-hidden="true">
            HR
          </span>
          <span>HookRelay</span>
          <span className="environment-label">Operations</span>
        </div>
        <div className="session-actions">
          <span className="session-state">
            <span className="session-dot" aria-hidden="true" /> Tenant session
          </span>
          <button className="button button-ghost" type="button" onClick={onSignOut}>
            Sign out
          </button>
        </div>
      </header>

      <main className="workspace">
        <section className="page-heading" aria-labelledby="delivery-history-title">
          <div>
            <p className="eyebrow">Delivery evidence</p>
            <h1 id="delivery-history-title">Delivery history</h1>
            <p>
              Inspect current state, lifetime attempts, retry timing, and explicit
              dead-letter replays.
            </p>
          </div>
          <button
            className="button button-secondary"
            type="button"
            disabled={isLoading}
            onClick={() => void loadDeliveries("replace", activeFilters)}
          >
            {isLoading ? "Refreshing…" : "Refresh"}
          </button>
        </section>

        <section className="filter-card" aria-labelledby="filter-title">
          <h2 id="filter-title">Filter deliveries</h2>
          <form className="filter-grid" onSubmit={applyFilters}>
            <div className="field-group">
              <label htmlFor="status-filter">Status</label>
              <select
                id="status-filter"
                value={draftFilters.status}
                onChange={(event) =>
                  setDraftFilters((current) => ({
                    ...current,
                    status: event.target.value as DeliveryFilters["status"],
                  }))
                }
              >
                <option value="">All statuses</option>
                {DELIVERY_STATUSES.map((status) => (
                  <option key={status} value={status}>
                    {statusLabel(status)}
                  </option>
                ))}
              </select>
            </div>
            <div className="field-group">
              <label htmlFor="endpoint-filter">Endpoint ID</label>
              <input
                id="endpoint-filter"
                value={draftFilters.endpointId}
                onChange={(event) =>
                  setDraftFilters((current) => ({
                    ...current,
                    endpointId: event.target.value,
                  }))
                }
                placeholder="UUID"
                autoCapitalize="none"
                spellCheck={false}
              />
            </div>
            <div className="field-group">
              <label htmlFor="event-filter">Event ID</label>
              <input
                id="event-filter"
                value={draftFilters.eventId}
                onChange={(event) =>
                  setDraftFilters((current) => ({
                    ...current,
                    eventId: event.target.value,
                  }))
                }
                placeholder="UUID"
                autoCapitalize="none"
                spellCheck={false}
              />
            </div>
            <div className="filter-actions">
              <button className="button button-primary" disabled={isLoading}>
                Apply filters
              </button>
              <button
                className="button button-link"
                type="button"
                disabled={isLoading}
                onClick={clearFilters}
              >
                Clear
              </button>
            </div>
          </form>
        </section>

        <p className="sr-only" aria-live="polite" aria-atomic="true">
          {announcement}
        </p>
        {problem ? <ProblemNotice problem={problem} /> : null}

        <section className="table-card" aria-busy={isLoading}>
          <div className="table-meta">
            <h2>Results</h2>
            <span>{deliveries.length} shown</span>
          </div>
          <div className="table-scroll">
            <table>
              <caption className="sr-only">
                Tenant delivery history matching the active filters
              </caption>
              <thead>
                <tr>
                  <th scope="col">Status</th>
                  <th scope="col">Event</th>
                  <th scope="col">Endpoint</th>
                  <th scope="col">Attempts</th>
                  <th scope="col">Created</th>
                  <th scope="col">
                    <span className="sr-only">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {deliveries.map((delivery) => (
                  <tr key={delivery.id} data-testid={`delivery-${delivery.id}`}>
                    <td>
                      <span className={`status-badge status-${delivery.status}`}>
                        {statusLabel(delivery.status)}
                      </span>
                      <span className="generation-label">
                        Generation {delivery.dispatch_generation}
                      </span>
                    </td>
                    <td>
                      <strong>{delivery.event_type}</strong>
                      <code title={delivery.event_id}>{shortId(delivery.event_id)}</code>
                    </td>
                    <td>
                      <strong>{delivery.endpoint_name}</strong>
                      <code title={delivery.endpoint_id}>
                        {shortId(delivery.endpoint_id)}
                      </code>
                    </td>
                    <td>{delivery.attempt_count}</td>
                    <td>
                      <time dateTime={delivery.created_at}>
                        {formatTimestamp(delivery.created_at)}
                      </time>
                    </td>
                    <td className="row-action">
                      <button
                        className="button button-small"
                        type="button"
                        ref={(node) => {
                          if (node) inspectButtons.current.set(delivery.id, node);
                          else inspectButtons.current.delete(delivery.id);
                        }}
                        onClick={() => setSelectedDeliveryId(delivery.id)}
                        aria-label={`Inspect delivery ${delivery.id}`}
                      >
                        Inspect
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {!isLoading && deliveries.length === 0 && !problem ? (
            <div className="empty-state">
              <strong>No deliveries found</strong>
              <p>Change the filters or refresh after new events are accepted.</p>
            </div>
          ) : null}

          {nextCursor ? (
            <div className="load-more-row">
              <button
                className="button button-secondary"
                type="button"
                disabled={isLoading}
                onClick={() =>
                  void loadDeliveries("append", activeFilters, nextCursor)
                }
              >
                {isLoading ? "Loading…" : "Load more"}
              </button>
            </div>
          ) : null}
        </section>
      </main>

      {selectedSummary ? (
        <DeliveryDetailPanel
          key={selectedSummary.id}
          client={client}
          summary={selectedSummary}
          onClose={closeDetail}
          onReplayed={deliveryReplayed}
        />
      ) : null}
    </div>
  );
}

interface DeliveryDetailPanelProps {
  client: HookRelayClient;
  summary: DeliveryInspection;
  onClose: () => void;
  onReplayed: (replay: DeliveryReplayResponse) => void;
}

function DeliveryDetailPanel({
  client,
  summary,
  onClose,
  onReplayed,
}: DeliveryDetailPanelProps) {
  const [delivery, setDelivery] = useState(summary);
  const [attempts, setAttempts] = useState<DeliveryAttempt[]>([]);
  const [nextAttemptCursor, setNextAttemptCursor] = useState<string | null>(null);
  const [selectedAttemptId, setSelectedAttemptId] = useState<string | null>(null);
  const [problem, setProblem] = useState<ProblemDetail | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [isConfirmingReplay, setIsConfirmingReplay] = useState(false);
  const panelRef = useRef<HTMLElement>(null);
  const replayButtonRef = useRef<HTMLButtonElement>(null);

  const loadDetail = useCallback(async () => {
    setIsLoading(true);
    setProblem(null);
    try {
      const [currentDelivery, attemptPage] = await Promise.all([
        client.getDelivery(summary.id),
        client.listDeliveryAttempts(summary.id, { limit: ATTEMPT_PAGE_SIZE }),
      ]);
      const sortedAttempts = [...attemptPage.items].sort(
        (left, right) => left.attempt_number - right.attempt_number,
      );
      setDelivery(currentDelivery);
      setAttempts(sortedAttempts);
      setNextAttemptCursor(attemptPage.next_cursor);
      setSelectedAttemptId(
        sortedAttempts.length > 0
          ? (sortedAttempts[sortedAttempts.length - 1]?.id ?? null)
          : null,
      );
    } catch (error) {
      setProblem(problemFrom(error));
    } finally {
      setIsLoading(false);
    }
  }, [client, summary.id]);

  useEffect(() => {
    panelRef.current?.focus();
    void loadDetail();
  }, [loadDetail]);

  async function loadMoreAttempts() {
    if (!nextAttemptCursor) return;
    setIsLoadingMore(true);
    setProblem(null);
    try {
      const page = await client.listDeliveryAttempts(summary.id, {
        cursor: nextAttemptCursor,
        limit: ATTEMPT_PAGE_SIZE,
      });
      setAttempts((current) => {
        const seen = new Set(current.map((attempt) => attempt.id));
        return [...current, ...page.items.filter((attempt) => !seen.has(attempt.id))].sort(
          (left, right) => left.attempt_number - right.attempt_number,
        );
      });
      setNextAttemptCursor(page.next_cursor);
    } catch (error) {
      setProblem(problemFrom(error));
    } finally {
      setIsLoadingMore(false);
    }
  }

  function finishReplayConfirmation() {
    setIsConfirmingReplay(false);
    queueMicrotask(() => replayButtonRef.current?.focus());
  }

  function replaySucceeded(replay: DeliveryReplayResponse) {
    setDelivery((current) => ({
      ...current,
      status: replay.status,
      dispatch_generation: replay.dispatch_generation,
      next_attempt_at: null,
      dead_lettered_at: null,
      dead_letter_reason: null,
      replayable: false,
    }));
    setIsConfirmingReplay(false);
    onReplayed(replay);
    queueMicrotask(() => replayButtonRef.current?.focus());
  }

  const selectedAttempt =
    attempts.find((attempt) => attempt.id === selectedAttemptId) ?? null;

  return (
    <div className="detail-backdrop" role="presentation">
      <section
        className="detail-panel"
        ref={panelRef}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-labelledby="delivery-detail-title"
      >
        <header className="detail-header">
          <div>
            <p className="eyebrow">Delivery detail</p>
            <h2 id="delivery-detail-title">{delivery.event_type}</h2>
            <code>{delivery.id}</code>
          </div>
          <button
            className="icon-button"
            type="button"
            onClick={onClose}
            aria-label="Close delivery detail"
          >
            ×
          </button>
        </header>

        <div className="detail-body" aria-busy={isLoading}>
          <div className="detail-toolbar">
            <span className={`status-badge status-${delivery.status}`}>
              {statusLabel(delivery.status)}
            </span>
            <span>Generation {delivery.dispatch_generation}</span>
            <button
              className="button button-link push-right"
              type="button"
              disabled={isLoading}
              onClick={() => void loadDetail()}
            >
              Refresh detail
            </button>
          </div>

          {problem ? <ProblemNotice problem={problem} /> : null}

          <section className="detail-section" aria-labelledby="delivery-state-title">
            <h3 id="delivery-state-title">Current state</h3>
            <dl className="fact-grid">
              <div>
                <dt>Event ID</dt>
                <dd>
                  <code>{delivery.event_id}</code>
                </dd>
              </div>
              <div>
                <dt>Endpoint</dt>
                <dd>
                  <strong>{delivery.endpoint_name}</strong>
                  <code>{delivery.endpoint_id}</code>
                </dd>
              </div>
              <div>
                <dt>Created</dt>
                <dd>{formatTimestamp(delivery.created_at)}</dd>
              </div>
              <div>
                <dt>Next attempt</dt>
                <dd>{formatTimestamp(delivery.next_attempt_at)}</dd>
              </div>
              <div>
                <dt>Last attempt</dt>
                <dd>{formatTimestamp(delivery.last_attempt_at)}</dd>
              </div>
              <div>
                <dt>Dead-letter reason</dt>
                <dd>{delivery.dead_letter_reason?.replaceAll("_", " ") ?? "—"}</dd>
              </div>
            </dl>
          </section>

          <section className="detail-section" aria-labelledby="attempt-timeline-title">
            <div className="section-heading-row">
              <div>
                <h3 id="attempt-timeline-title">Attempt timeline</h3>
                <p>Lifetime attempt evidence is retained across replay generations.</p>
              </div>
              <span className="count-pill">{attempts.length}</span>
            </div>

            {attempts.length > 0 ? (
              <ol className="attempt-timeline">
                {attempts.map((attempt) => (
                  <li key={attempt.id}>
                    <button
                      type="button"
                      className={
                        selectedAttemptId === attempt.id
                          ? "attempt-card attempt-card-selected"
                          : "attempt-card"
                      }
                      onClick={() => setSelectedAttemptId(attempt.id)}
                      aria-pressed={selectedAttemptId === attempt.id}
                    >
                      <span className="attempt-number">#{attempt.attempt_number}</span>
                      <span>
                        <strong>{outcomeLabel(attempt.outcome)}</strong>
                        <small>Generation {attempt.dispatch_generation}</small>
                      </span>
                      <time dateTime={attempt.started_at}>
                        {formatTimestamp(attempt.started_at)}
                      </time>
                    </button>
                  </li>
                ))}
              </ol>
            ) : !isLoading ? (
              <div className="empty-state compact-empty">
                <strong>No attempts recorded</strong>
                <p>This delivery has not crossed the outbound attempt boundary.</p>
              </div>
            ) : null}

            {nextAttemptCursor ? (
              <button
                className="button button-secondary"
                type="button"
                disabled={isLoadingMore}
                onClick={() => void loadMoreAttempts()}
              >
                {isLoadingMore ? "Loading…" : "Load more attempts"}
              </button>
            ) : null}
          </section>

          {selectedAttempt ? (
            <section className="attempt-detail" aria-labelledby="attempt-detail-title">
              <div className="section-heading-row">
                <div>
                  <p className="eyebrow">Selected evidence</p>
                  <h3 id="attempt-detail-title">
                    Attempt {selectedAttempt.attempt_number}
                  </h3>
                </div>
                {selectedAttempt.is_circuit_probe ? (
                  <span className="probe-badge">Circuit probe</span>
                ) : null}
              </div>
              <dl className="fact-grid attempt-facts">
                <div>
                  <dt>Outcome</dt>
                  <dd>{outcomeLabel(selectedAttempt.outcome)}</dd>
                </div>
                <div>
                  <dt>HTTP status</dt>
                  <dd>{selectedAttempt.response_status_code ?? "—"}</dd>
                </div>
                <div>
                  <dt>Error code</dt>
                  <dd>
                    {selectedAttempt.error_code ? (
                      <code>{selectedAttempt.error_code}</code>
                    ) : (
                      "—"
                    )}
                  </dd>
                </div>
                <div>
                  <dt>Duration</dt>
                  <dd>{formatDuration(selectedAttempt.duration_ms)}</dd>
                </div>
                <div>
                  <dt>Started</dt>
                  <dd>{formatTimestamp(selectedAttempt.started_at)}</dd>
                </div>
                <div>
                  <dt>Finished</dt>
                  <dd>{formatTimestamp(selectedAttempt.finished_at)}</dd>
                </div>
                <div className="fact-span">
                  <dt>Attempt ID</dt>
                  <dd>
                    <code>{selectedAttempt.id}</code>
                  </dd>
                </div>
              </dl>
            </section>
          ) : null}
        </div>

        <footer className="detail-footer">
          <div>
            <strong>Dead-letter replay</strong>
            <p>Starts a new generation without deleting prior attempt evidence.</p>
          </div>
          <button
            className="button button-danger"
            type="button"
            ref={replayButtonRef}
            disabled={!delivery.replayable || isLoading}
            onClick={() => setIsConfirmingReplay(true)}
          >
            {delivery.replayable ? "Replay delivery" : "Replay unavailable"}
          </button>
        </footer>

        {isConfirmingReplay ? (
          <ReplayConfirmation
            client={client}
            delivery={delivery}
            onCancel={finishReplayConfirmation}
            onSuccess={replaySucceeded}
          />
        ) : null}
      </section>
    </div>
  );
}

interface ReplayConfirmationProps {
  client: HookRelayClient;
  delivery: DeliveryInspection;
  onCancel: () => void;
  onSuccess: (replay: DeliveryReplayResponse) => void;
}

function ReplayConfirmation({
  client,
  delivery,
  onCancel,
  onSuccess,
}: ReplayConfirmationProps) {
  const [problem, setProblem] = useState<ProblemDetail | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const cancelRef = useRef<HTMLButtonElement>(null);
  const confirmRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    confirmRef.current?.focus();
  }, []);

  async function confirmReplay() {
    setIsSubmitting(true);
    setProblem(null);
    try {
      const replay = await client.replayDelivery(
        delivery.id,
        delivery.dispatch_generation,
      );
      onSuccess(replay);
    } catch (error) {
      setProblem(problemFrom(error));
    } finally {
      setIsSubmitting(false);
    }
  }

  function handleKeyDown(event: ReactKeyboardEvent<HTMLDivElement>) {
    if (event.key === "Escape" && !isSubmitting) {
      event.preventDefault();
      onCancel();
      return;
    }
    if (event.key !== "Tab") return;

    const first = cancelRef.current;
    const last = confirmRef.current;
    if (!first || !last) return;
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  return (
    <div className="confirmation-backdrop" onKeyDown={handleKeyDown}>
      <section
        className="confirmation-dialog"
        role="alertdialog"
        aria-modal="true"
        aria-labelledby="replay-confirmation-title"
        aria-describedby="replay-confirmation-description"
      >
        <p className="eyebrow">Explicit operator action</p>
        <h3 id="replay-confirmation-title">Replay this dead letter?</h3>
        <p id="replay-confirmation-description">
          HookRelay will replay the observed generation{" "}
          <strong>{delivery.dispatch_generation}</strong>. A concurrent replay will
          be rejected instead of advancing the delivery twice.
        </p>
        <dl className="confirmation-facts">
          <div>
            <dt>Delivery</dt>
            <dd>
              <code>{delivery.id}</code>
            </dd>
          </div>
          <div>
            <dt>Reason</dt>
            <dd>{delivery.dead_letter_reason?.replaceAll("_", " ") ?? "unknown"}</dd>
          </div>
        </dl>
        {problem ? <ProblemNotice problem={problem} /> : null}
        <div className="confirmation-actions">
          <button
            className="button button-secondary"
            type="button"
            ref={cancelRef}
            disabled={isSubmitting}
            onClick={onCancel}
          >
            Cancel
          </button>
          <button
            className="button button-danger"
            type="button"
            ref={confirmRef}
            disabled={isSubmitting}
            onClick={() => void confirmReplay()}
          >
            {isSubmitting ? "Submitting…" : "Confirm replay"}
          </button>
        </div>
      </section>
    </div>
  );
}

export function App() {
  const [apiKey, setApiKey] = useState<string | null>(null);

  if (apiKey === null) {
    return <CredentialGate onAuthenticated={setApiKey} />;
  }
  return <OperationsConsole apiKey={apiKey} onSignOut={() => setApiKey(null)} />;
}

export type { DeliveryHistoryPage };
