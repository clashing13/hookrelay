"""Pure Stage 5 shared rate-limit and circuit-breaker transition contracts."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from hookrelay.config import Settings
from hookrelay.models import EndpointTrafficControl
from hookrelay.traffic_control import (
    CircuitOutcome,
    TrafficAdmission,
    admit_endpoint_traffic,
    record_circuit_outcome,
)

NOW = datetime(2026, 8, 5, 12, 0, tzinfo=UTC)
TENANT_ID = UUID(int=1)
ENDPOINT_ID = UUID(int=2)


def _settings() -> Settings:
    return Settings(
        environment="test",
        delivery_rate_limit_requests=2,
        delivery_rate_limit_window_seconds=10,
        delivery_circuit_failure_threshold=3,
        delivery_circuit_cooldown_seconds=30,
        _env_file=None,
    )


def _control(
    *,
    tenant_id: UUID = TENANT_ID,
    endpoint_id: UUID = ENDPOINT_ID,
) -> EndpointTrafficControl:
    return EndpointTrafficControl(
        tenant_id=tenant_id,
        endpoint_id=endpoint_id,
        rate_window_started_at=None,
        rate_window_count=0,
        circuit_state="closed",
        circuit_consecutive_failures=0,
        circuit_opened_at=None,
        probe_token=None,
        probe_expires_at=None,
    )


def _half_open_control(*, probe_token: UUID, probe_expires_at: datetime) -> EndpointTrafficControl:
    control = _control()
    control.circuit_state = "half_open"
    control.circuit_consecutive_failures = 3
    control.circuit_opened_at = NOW
    control.probe_token = probe_token
    control.probe_expires_at = probe_expires_at
    return control


def test_fixed_window_resets_at_exact_boundary_without_charging_denials() -> None:
    control = _control()
    settings = _settings()

    first = admit_endpoint_traffic(
        control,
        database_now=NOW,
        claim_token=UUID(int=10),
        settings=settings,
    )
    second = admit_endpoint_traffic(
        control,
        database_now=NOW + timedelta(seconds=9, microseconds=999_999),
        claim_token=UUID(int=11),
        settings=settings,
    )
    denied = admit_endpoint_traffic(
        control,
        database_now=NOW + timedelta(seconds=9, microseconds=999_999),
        claim_token=UUID(int=12),
        settings=settings,
    )

    assert first == TrafficAdmission(allowed=True)
    assert second == TrafficAdmission(allowed=True)
    assert denied == TrafficAdmission(
        allowed=False,
        retry_at=NOW + timedelta(seconds=10),
        reason="rate_limited",
    )
    assert control.rate_window_started_at == NOW
    assert control.rate_window_count == 2

    boundary = admit_endpoint_traffic(
        control,
        database_now=NOW + timedelta(seconds=10),
        claim_token=UUID(int=13),
        settings=settings,
    )

    assert boundary == TrafficAdmission(allowed=True)
    assert control.rate_window_started_at == NOW + timedelta(seconds=10)
    assert control.rate_window_count == 1


def test_transitions_mutate_only_the_selected_endpoint_object() -> None:
    first_endpoint = _control(endpoint_id=UUID(int=2))
    second_endpoint = _control(endpoint_id=UUID(int=3))
    settings = _settings()

    for token_value in (20, 21):
        assert admit_endpoint_traffic(
            first_endpoint,
            database_now=NOW,
            claim_token=UUID(int=token_value),
            settings=settings,
        ).allowed

    denied = admit_endpoint_traffic(
        first_endpoint,
        database_now=NOW,
        claim_token=UUID(int=22),
        settings=settings,
    )
    admitted_other = admit_endpoint_traffic(
        second_endpoint,
        database_now=NOW,
        claim_token=UUID(int=23),
        settings=settings,
    )

    assert denied.reason == "rate_limited"
    assert admitted_other == TrafficAdmission(allowed=True)
    assert first_endpoint.rate_window_count == 2
    assert second_endpoint.rate_window_count == 1
    assert first_endpoint.endpoint_id != second_endpoint.endpoint_id


def test_transient_failure_threshold_opens_the_circuit() -> None:
    control = _control()

    for failure_number in (1, 2):
        record_circuit_outcome(
            control,
            outcome="transient_failure",
            database_now=NOW + timedelta(seconds=failure_number),
            failure_threshold=3,
            was_probe=False,
        )
        assert control.circuit_state == "closed"
        assert control.circuit_consecutive_failures == failure_number
        assert control.circuit_opened_at is None

    opened_at = NOW + timedelta(seconds=3)
    record_circuit_outcome(
        control,
        outcome="transient_failure",
        database_now=opened_at,
        failure_threshold=3,
        was_probe=False,
    )

    assert control.circuit_state == "open"
    assert control.circuit_consecutive_failures == 3
    assert control.circuit_opened_at == opened_at
    assert control.probe_token is None
    assert control.probe_expires_at is None


def test_open_circuit_defers_until_the_exact_cooldown_boundary() -> None:
    control = _control()
    control.circuit_state = "open"
    control.circuit_consecutive_failures = 3
    control.circuit_opened_at = NOW
    settings = _settings()

    admission = admit_endpoint_traffic(
        control,
        database_now=NOW + timedelta(seconds=29, microseconds=999_999),
        claim_token=UUID(int=30),
        settings=settings,
    )

    assert admission == TrafficAdmission(
        allowed=False,
        retry_at=NOW + timedelta(seconds=30),
        reason="circuit_open",
    )
    assert control.circuit_state == "open"
    assert control.rate_window_started_at is None
    assert control.rate_window_count == 0


def test_exactly_one_half_open_probe_lease_is_active() -> None:
    control = _control()
    control.circuit_state = "open"
    control.circuit_consecutive_failures = 3
    control.circuit_opened_at = NOW
    settings = _settings()
    cooldown_end = NOW + timedelta(seconds=30)
    first_token = UUID(int=40)

    probe = admit_endpoint_traffic(
        control,
        database_now=cooldown_end,
        claim_token=first_token,
        settings=settings,
    )
    competing = admit_endpoint_traffic(
        control,
        database_now=cooldown_end,
        claim_token=UUID(int=41),
        settings=settings,
    )

    lease_end = cooldown_end + timedelta(seconds=settings.delivery_claim_ttl_seconds)
    assert probe == TrafficAdmission(allowed=True, is_probe=True)
    assert competing == TrafficAdmission(
        allowed=False,
        retry_at=lease_end,
        reason="circuit_probe_active",
    )
    assert control.circuit_state == "half_open"
    assert control.probe_token == first_token
    assert control.probe_expires_at == lease_end
    assert control.rate_window_count == 1


def test_expired_half_open_probe_is_replaced_at_its_exact_boundary() -> None:
    control = _control()
    control.circuit_state = "half_open"
    control.circuit_consecutive_failures = 3
    control.circuit_opened_at = NOW
    control.probe_token = UUID(int=50)
    control.probe_expires_at = NOW + timedelta(seconds=20)
    settings = _settings()
    replacement_token = UUID(int=51)
    replacement_time = NOW + timedelta(seconds=20)

    admission = admit_endpoint_traffic(
        control,
        database_now=replacement_time,
        claim_token=replacement_token,
        settings=settings,
    )

    assert admission == TrafficAdmission(allowed=True, is_probe=True)
    assert control.circuit_state == "half_open"
    assert control.probe_token == replacement_token
    assert control.probe_expires_at == replacement_time + timedelta(
        seconds=settings.delivery_claim_ttl_seconds
    )
    assert control.rate_window_count == 1


@pytest.mark.parametrize("outcome", ["succeeded", "permanent_failure", "target_blocked"])
def test_reachable_or_permanent_outcomes_close_and_clear_the_circuit(
    outcome: CircuitOutcome,
) -> None:
    control = _half_open_control(
        probe_token=UUID(int=60),
        probe_expires_at=NOW + timedelta(seconds=20),
    )

    record_circuit_outcome(
        control,
        outcome=outcome,
        database_now=NOW + timedelta(seconds=1),
        failure_threshold=3,
        was_probe=True,
    )

    assert control.circuit_state == "closed"
    assert control.circuit_consecutive_failures == 0
    assert control.circuit_opened_at is None
    assert control.probe_token is None
    assert control.probe_expires_at is None


def test_transient_probe_failure_reopens_and_starts_a_new_cooldown() -> None:
    control = _half_open_control(
        probe_token=UUID(int=70),
        probe_expires_at=NOW + timedelta(seconds=20),
    )
    failed_at = NOW + timedelta(seconds=5)

    record_circuit_outcome(
        control,
        outcome="transient_failure",
        database_now=failed_at,
        failure_threshold=3,
        was_probe=True,
    )

    assert control.circuit_state == "open"
    assert control.circuit_consecutive_failures == 4
    assert control.circuit_opened_at == failed_at
    assert control.probe_token is None
    assert control.probe_expires_at is None


def test_traffic_admission_rejects_internally_inconsistent_decisions() -> None:
    with pytest.raises(ValueError, match="allowed traffic cannot carry a deferral"):
        TrafficAdmission(
            allowed=True,
            retry_at=NOW,
            reason="rate_limited",
        )
    with pytest.raises(ValueError, match="deferred traffic requires"):
        TrafficAdmission(allowed=False)
    with pytest.raises(ValueError, match="deferred traffic requires"):
        TrafficAdmission(
            allowed=False,
            is_probe=True,
            retry_at=NOW,
            reason="circuit_probe_active",
        )
