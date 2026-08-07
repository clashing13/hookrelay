"""Pure state transitions for shared per-endpoint delivery traffic controls."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from hookrelay.config import Settings
from hookrelay.models import EndpointTrafficControl

TrafficDeferralReason = Literal["rate_limited", "circuit_open", "circuit_probe_active"]
CircuitOutcome = Literal["succeeded", "transient_failure", "permanent_failure", "target_blocked"]


@dataclass(frozen=True, slots=True)
class TrafficAdmission:
    """One atomic endpoint admission decision made while its row is locked."""

    allowed: bool
    is_probe: bool = False
    retry_at: datetime | None = None
    reason: TrafficDeferralReason | None = None

    def __post_init__(self) -> None:
        if self.allowed:
            if self.retry_at is not None or self.reason is not None:
                raise ValueError("allowed traffic cannot carry a deferral")
        elif self.retry_at is None or self.reason is None or self.is_probe:
            raise ValueError("deferred traffic requires a due time and reason")


def close_circuit(control: EndpointTrafficControl) -> None:
    """Return a reachable endpoint to the closed state."""

    control.circuit_state = "closed"
    control.circuit_consecutive_failures = 0
    control.circuit_opened_at = None
    control.probe_token = None
    control.probe_expires_at = None


def record_circuit_outcome(
    control: EndpointTrafficControl,
    *,
    outcome: CircuitOutcome,
    database_now: datetime,
    failure_threshold: int,
    was_probe: bool,
) -> None:
    """Record a completed/abandoned request under the endpoint row lock."""

    if outcome in {"succeeded", "permanent_failure", "target_blocked"}:
        close_circuit(control)
        return

    control.circuit_consecutive_failures += 1
    if (
        was_probe
        or control.circuit_state in {"open", "half_open"}
        or control.circuit_consecutive_failures >= failure_threshold
    ):
        control.circuit_state = "open"
        control.circuit_opened_at = database_now
        control.probe_token = None
        control.probe_expires_at = None


def admit_endpoint_traffic(
    control: EndpointTrafficControl,
    *,
    database_now: datetime,
    claim_token: UUID,
    settings: Settings,
) -> TrafficAdmission:
    """Reserve one fixed-window slot and, when needed, the sole recovery probe."""

    ready_for_probe = False
    if control.circuit_state == "open":
        if control.circuit_opened_at is None:
            raise RuntimeError("open circuit is missing its timestamp")
        cooldown_ends_at = control.circuit_opened_at + timedelta(
            seconds=settings.delivery_circuit_cooldown_seconds
        )
        if cooldown_ends_at > database_now:
            return TrafficAdmission(
                allowed=False,
                retry_at=cooldown_ends_at,
                reason="circuit_open",
            )
        ready_for_probe = True
    elif control.circuit_state == "half_open":
        if control.probe_expires_at is None:
            raise RuntimeError("half-open circuit is missing its probe lease")
        if control.probe_expires_at > database_now:
            return TrafficAdmission(
                allowed=False,
                retry_at=control.probe_expires_at,
                reason="circuit_probe_active",
            )
        ready_for_probe = True
    elif control.circuit_state != "closed":
        raise RuntimeError("endpoint circuit state is invalid")

    window = timedelta(seconds=settings.delivery_rate_limit_window_seconds)
    if (
        control.rate_window_started_at is None
        or control.rate_window_started_at + window <= database_now
    ):
        control.rate_window_started_at = database_now
        control.rate_window_count = 0
    window_ends_at = control.rate_window_started_at + window
    if control.rate_window_count >= settings.delivery_rate_limit_requests:
        return TrafficAdmission(
            allowed=False,
            retry_at=window_ends_at,
            reason="rate_limited",
        )

    control.rate_window_count += 1
    if ready_for_probe:
        control.circuit_state = "half_open"
        control.probe_token = claim_token
        control.probe_expires_at = database_now + timedelta(
            seconds=settings.delivery_claim_ttl_seconds
        )
        return TrafficAdmission(allowed=True, is_probe=True)
    return TrafficAdmission(allowed=True)
