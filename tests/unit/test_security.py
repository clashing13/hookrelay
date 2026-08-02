"""Unit contracts for credentials and recoverable endpoint secrets."""

import re
from uuid import uuid4

import pytest

from hookrelay.security import (
    SecretCipher,
    generate_api_key,
    generate_signing_secret,
    parse_api_key,
    verify_api_key_secret,
)

pytestmark = pytest.mark.security


def test_generated_api_key_round_trips_without_persisting_or_reprising_the_secret() -> None:
    generated = generate_api_key()
    parsed = parse_api_key(generated.token)

    assert parsed is not None
    assert generated.token == f"hrk_{generated.public_id}.{parsed.secret}"
    assert len(generated.public_id) == 12
    assert len(parsed.secret) == 43
    assert len(generated.secret_hash) == 32
    assert generated.secret_last_four == parsed.secret[-4:]
    assert verify_api_key_secret(parsed.secret, generated.secret_hash)
    assert not verify_api_key_secret("A" * 43, generated.secret_hash)
    assert generated.token not in repr(generated)
    assert parsed.secret not in repr(parsed)


@pytest.mark.parametrize(
    "token",
    [
        "",
        "hrk_missing-secret",
        f"wrong_{'A' * 12}.{'B' * 43}",
        f"hrk_{'A' * 11}.{'B' * 43}",
        f"hrk_{'A' * 12}.{'B' * 42}",
        f"hrk_{'A' * 12}.{'B' * 43}!",
        f"hrk_{'A' * 12}.{'B' * 43}\n",
    ],
)
def test_parse_api_key_rejects_every_malformed_shape(token: str) -> None:
    assert parse_api_key(token) is None


def test_generated_credentials_are_unique_and_have_versioned_prefixes() -> None:
    first_key = generate_api_key()
    second_key = generate_api_key()
    first_signing_secret = generate_signing_secret()
    second_signing_secret = generate_signing_secret()

    assert first_key.token != second_key.token
    assert first_signing_secret != second_signing_secret
    assert re.fullmatch(r"whsec_[A-Za-z0-9_-]{43}", first_signing_secret)
    assert re.fullmatch(r"whsec_[A-Za-z0-9_-]{43}", second_signing_secret)


def test_endpoint_secret_encryption_round_trips_with_random_nonces() -> None:
    cipher = SecretCipher(bytes(range(32)), key_version=7)
    tenant_id = uuid4()
    endpoint_id = uuid4()
    secret_id = uuid4()
    plaintext = generate_signing_secret()

    first = cipher.encrypt_endpoint_secret(tenant_id, endpoint_id, secret_id, 3, plaintext)
    second = cipher.encrypt_endpoint_secret(tenant_id, endpoint_id, secret_id, 3, plaintext)

    assert cipher.key_version == 7
    assert first != second
    assert plaintext.encode() not in first
    assert plaintext.encode() not in second
    assert (
        cipher.decrypt_endpoint_secret(tenant_id, endpoint_id, secret_id, 3, 7, first) == plaintext
    )
    assert (
        cipher.decrypt_endpoint_secret(tenant_id, endpoint_id, secret_id, 3, 7, second) == plaintext
    )


def test_endpoint_secret_envelope_detects_tampering_and_context_swaps() -> None:
    cipher = SecretCipher(b"K" * 32, key_version=2)
    tenant_id = uuid4()
    endpoint_id = uuid4()
    secret_id = uuid4()
    plaintext = "whsec_do-not-leak-this-value"
    envelope = cipher.encrypt_endpoint_secret(tenant_id, endpoint_id, secret_id, 1, plaintext)
    tampered = envelope[:-1] + bytes([envelope[-1] ^ 1])

    invalid_contexts = [
        (tenant_id, endpoint_id, secret_id, 1, tampered),
        (uuid4(), endpoint_id, secret_id, 1, envelope),
        (tenant_id, uuid4(), secret_id, 1, envelope),
        (tenant_id, endpoint_id, uuid4(), 1, envelope),
        (tenant_id, endpoint_id, secret_id, 2, envelope),
    ]
    for (
        context_tenant,
        context_endpoint,
        context_secret,
        secret_version,
        candidate,
    ) in invalid_contexts:
        with pytest.raises(ValueError, match="authentication") as error:
            cipher.decrypt_endpoint_secret(
                context_tenant,
                context_endpoint,
                context_secret,
                secret_version,
                2,
                candidate,
            )
        assert plaintext not in str(error.value)


def test_endpoint_secret_requires_the_recorded_master_key_version_and_key() -> None:
    tenant_id = uuid4()
    endpoint_id = uuid4()
    secret_id = uuid4()
    plaintext = "whsec_rotation-contract"
    original = SecretCipher(b"A" * 32, key_version=11)
    wrong_key = SecretCipher(b"B" * 32, key_version=11)
    envelope = original.encrypt_endpoint_secret(tenant_id, endpoint_id, secret_id, 4, plaintext)

    with pytest.raises(ValueError, match="unavailable encryption key version"):
        original.decrypt_endpoint_secret(tenant_id, endpoint_id, secret_id, 4, 10, envelope)
    with pytest.raises(ValueError, match="authentication"):
        wrong_key.decrypt_endpoint_secret(tenant_id, endpoint_id, secret_id, 4, 11, envelope)


def test_endpoint_secret_rejects_invalid_envelopes_and_cipher_configuration() -> None:
    with pytest.raises(ValueError, match="256-bit"):
        SecretCipher(b"short")
    with pytest.raises(ValueError, match="positive small integer"):
        SecretCipher(b"K" * 32, key_version=0)
    with pytest.raises(ValueError, match="positive small integer"):
        SecretCipher(b"K" * 32, key_version=32768)

    cipher = SecretCipher(b"K" * 32)
    with pytest.raises(ValueError, match="unsupported or malformed"):
        cipher.decrypt_endpoint_secret(uuid4(), uuid4(), uuid4(), 1, 1, b"too-short")
