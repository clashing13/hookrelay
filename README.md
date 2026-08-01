# HookRelay

HookRelay is a fault-tolerant webhook delivery platform built to explore durable
state, at-least-once delivery, failure recovery, concurrency, security, testing,
observability, and performance measurement.

Stage 1 establishes a small FastAPI service foundation. The complete setup and
learning guide will be added before the Stage 1 pull request is opened.

## Current scope

- Python 3.12+
- FastAPI application factory and typed settings
- structured JSON logging and lifespan hooks
- `GET /health/live`

Webhook ingestion and delivery are deliberately not implemented yet.

