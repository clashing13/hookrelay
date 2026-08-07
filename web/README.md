# HookRelay operations console

This is the deliberately small Stage 6 React/TypeScript console. It is built at
`/console/` and calls only the same origin's fixed `/v1` API. There is no runtime
API-origin selector.

The tenant API key is kept only in React memory. HookRelay does not write it to
local storage, session storage, cookies, the URL, or application logs. Reloading
the page or choosing **Sign out** clears the credential.

## Local commands

Run these commands from `web/` with Node 24 and pnpm 11:

```text
pnpm install --frozen-lockfile
pnpm typecheck
pnpm test
pnpm build
```

`pnpm dev` serves `/console/` and proxies `/v1` to the fixed local development
API at `http://127.0.0.1:8000`. In a deployed environment, the reverse proxy must
serve both `/console/` and `/v1` from the same origin.

Install Chromium once before the real-API browser suite. Linux CI can add
`--with-deps` when its runner image does not already contain browser libraries:

```text
pnpm exec playwright install chromium
```

## Real-API Playwright seed

The browser suite always targets a real HookRelay API. In CI, enable bootstrap
and let Playwright provision a fresh tenant, endpoint, event, and pending
delivery (the worker is not required):

```text
HOOKRELAY_E2E_BOOTSTRAP_TOKEN=<the-api-bootstrap-token>
pnpm test:e2e
```

The default seed endpoint is `http://127.0.0.1:9000/webhooks`, which must be an
explicitly allowed local destination. Override only the seed destination with
`HOOKRELAY_E2E_WEBHOOK_URL` when the API's local policy uses a different test
receiver.

Alternatively, provide an existing tenant-owned seed:

```text
HOOKRELAY_E2E_API_KEY=<tenant-api-key>
HOOKRELAY_E2E_DELIVERY_ID=<delivery-uuid>
HOOKRELAY_E2E_EVENT_ID=<event-uuid>
pnpm test:e2e
```

Without `HOOKRELAY_E2E_BASE_URL`, Playwright starts Vite on
`http://127.0.0.1:4173/console/`; Vite's fixed development proxy targets the API
on port 8000. Set `HOOKRELAY_E2E_BASE_URL` to a deployed same-origin console URL
to test a running deployment instead.

Replay is intentionally opt-in because it mutates durable state. Seed one
dead-lettered, replayable delivery and additionally set:

```text
HOOKRELAY_E2E_REPLAY_DELIVERY_ID=<dead-lettered-delivery-uuid>
HOOKRELAY_E2E_REPLAY_EVENT_ID=<its-event-uuid>
```

That replay seed is single-use: a successful test advances its dispatch
generation and the next run must use a fresh dead letter. The pending-history
test runs whenever either the bootstrap token or all three external seed values
are present; it cannot silently skip in the configured CI path.
