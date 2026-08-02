"""Credential generation, verification, and endpoint-secret encryption."""

import base64
import hashlib
import hmac
import re
import secrets
from dataclasses import dataclass, field
from uuid import UUID

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

API_KEY_PATTERN = re.compile(
    r"^hrk_(?P<public_id>[A-Za-z0-9_-]{12})\.(?P<secret>[A-Za-z0-9_-]{43})$"
)
SECRET_ENVELOPE_VERSION = 1
AES_GCM_NONCE_BYTES = 12
DUMMY_API_KEY_SECRET_HASH = hashlib.sha256(b"hookrelay-invalid-api-key").digest()


@dataclass(frozen=True, slots=True)
class GeneratedApiKey:
    """The one-time credential plus the non-secret fields safe to persist."""

    token: str = field(repr=False)
    public_id: str
    secret_hash: bytes
    secret_last_four: str


@dataclass(frozen=True, slots=True)
class ParsedApiKey:
    """A syntactically valid API key split into lookup and secret components."""

    public_id: str
    secret: str = field(repr=False)


def _hash_api_key_secret(secret: str) -> bytes:
    return hashlib.sha256(secret.encode("ascii")).digest()


def generate_api_key() -> GeneratedApiKey:
    """Generate a high-entropy bearer credential that is displayed exactly once."""

    public_id = secrets.token_urlsafe(9)
    secret = secrets.token_urlsafe(32)
    return GeneratedApiKey(
        token=f"hrk_{public_id}.{secret}",
        public_id=public_id,
        secret_hash=_hash_api_key_secret(secret),
        secret_last_four=secret[-4:],
    )


def parse_api_key(token: str) -> ParsedApiKey | None:
    """Return components for the exact versioned key format or reject it uniformly."""

    match = API_KEY_PATTERN.fullmatch(token)
    if match is None:
        return None
    return ParsedApiKey(
        public_id=match.group("public_id"),
        secret=match.group("secret"),
    )


def verify_api_key_secret(secret: str, expected_hash: bytes) -> bool:
    """Compare a presented high-entropy key secret without data-dependent timing."""

    return hmac.compare_digest(_hash_api_key_secret(secret), expected_hash)


def generate_signing_secret() -> str:
    """Create the shared secret later used to HMAC-sign delivery requests."""

    return f"whsec_{secrets.token_urlsafe(32)}"


class SecretCipher:
    """Encrypt recoverable endpoint secrets with an authenticated AES-GCM envelope."""

    def __init__(self, key: bytes, key_version: int = 1) -> None:
        if len(key) != 32:
            msg = "SecretCipher requires a 256-bit key"
            raise ValueError(msg)
        if not 1 <= key_version <= 32767:
            msg = "SecretCipher key version must be a positive small integer"
            raise ValueError(msg)
        self._cipher = AESGCM(key)
        self._key_version = key_version

    @property
    def key_version(self) -> int:
        """Identify the master key required to decrypt persisted envelopes."""

        return self._key_version

    def _additional_data(
        self, tenant_id: UUID, endpoint_id: UUID, secret_id: UUID, version: int
    ) -> bytes:
        return (
            f"hookrelay:endpoint-secret:v{SECRET_ENVELOPE_VERSION}:"
            f"key:{self._key_version}:secret:{version}:"
            f"{tenant_id}:{endpoint_id}:{secret_id}"
        ).encode()

    def encrypt_endpoint_secret(
        self,
        tenant_id: UUID,
        endpoint_id: UUID,
        secret_id: UUID,
        version: int,
        secret: str,
    ) -> bytes:
        """Bind ciphertext to its endpoint so rows cannot be swapped undetected."""

        nonce = secrets.token_bytes(AES_GCM_NONCE_BYTES)
        ciphertext = self._cipher.encrypt(
            nonce,
            secret.encode("utf-8"),
            self._additional_data(tenant_id, endpoint_id, secret_id, version),
        )
        return bytes([SECRET_ENVELOPE_VERSION]) + nonce + ciphertext

    def decrypt_endpoint_secret(
        self,
        tenant_id: UUID,
        endpoint_id: UUID,
        secret_id: UUID,
        version: int,
        encryption_key_version: int,
        envelope: bytes,
    ) -> str:
        """Authenticate and decrypt a supported endpoint-secret envelope."""

        if encryption_key_version != self._key_version:
            msg = "endpoint secret requires an unavailable encryption key version"
            raise ValueError(msg)
        minimum_size = 1 + AES_GCM_NONCE_BYTES + 16
        if len(envelope) < minimum_size or envelope[0] != SECRET_ENVELOPE_VERSION:
            msg = "unsupported or malformed secret envelope"
            raise ValueError(msg)
        nonce = envelope[1 : 1 + AES_GCM_NONCE_BYTES]
        ciphertext = envelope[1 + AES_GCM_NONCE_BYTES :]
        try:
            plaintext = self._cipher.decrypt(
                nonce,
                ciphertext,
                self._additional_data(tenant_id, endpoint_id, secret_id, version),
            )
        except InvalidTag as exc:
            msg = "endpoint secret failed authentication"
            raise ValueError(msg) from exc
        return plaintext.decode("utf-8")


def decode_secret_encryption_key(encoded_key: str) -> bytes:
    """Decode a validated URL-safe key for callers outside ``Settings``."""

    return base64.urlsafe_b64decode(encoded_key)
