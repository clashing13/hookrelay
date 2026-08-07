# ADR 0001: Python for the core MVP instead of Go

- Status: Accepted
- Date: 2026-08-01

## Context

HookRelay is a seven-stage learning project whose main risks are delivery
semantics, failure recovery, concurrency, security, observability, and
measurement. Tarun already has production-oriented Python and FastAPI
experience. The project has limited time, so introducing a second language in
the core path would spend learning budget on syntax, tooling, and two runtime
ecosystems rather than on those system-design risks.

The workload is primarily network and database I/O. Python's asynchronous
ecosystem can overlap that waiting efficiently, provided every library used on
the event loop is genuinely asynchronous and concurrency is bounded.

## Decision

Use Python 3.12 or newer for the API, publishers, and workers in the seven core
stages. Use `asyncio`-compatible libraries for PostgreSQL, messaging, and HTTP.
Do not introduce a mixed Python/Go production architecture during the MVP.

## Serious alternative: Go

Go offers inexpensive goroutines, a strong standard library, simple static
binaries, predictable deployment, and often lower memory use. It would be a
credible choice for a high-throughput delivery worker. It was not selected for
the MVP because learning a new language and maintaining cross-language
contracts would increase delivery risk without proving that Python is a
bottleneck.

After the Python MVP is correct and benchmarked, a Go worker can be an optional
controlled experiment. It should consume the same contract and be compared
with identical load, limits, and hardware. A rewrite should follow measured
evidence, not intuition.

## Consequences

- Development can focus on distributed-systems behavior using familiar tools.
- FastAPI, Pydantic, SQLAlchemy, and pytest form one coherent ecosystem.
- CPU-bound work must not be placed directly on the event loop. `async def`
  neither makes CPU work parallel nor turns a blocking library into a
  nonblocking one.
- Worker concurrency, memory, and tail latency must be measured in Stage 7.
- Horizontal process scaling or a later measured Go experiment remain
  available if Python becomes the demonstrated constraint.
