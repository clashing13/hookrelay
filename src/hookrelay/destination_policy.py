"""SSRF-safe destination resolution and IP-pinned asynchronous HTTP transport.

The policy validates every address returned for a destination before a socket is
opened.  The network backend then connects to the selected numeric address, so
the HTTP stack cannot perform a second, potentially rebound DNS lookup.  The
original URL hostname remains the HTTP ``Host`` value and the TLS SNI/certificate
hostname.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
import ssl
import time
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol, cast

import httpcore2
import httpx2

Environment = Literal["local", "test", "staging", "production"]
IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
AddressReason = Literal[
    "public",
    "invalid",
    "ipv4_mapped",
    "metadata",
    "not_global",
    "special_transition",
]

# Metadata endpoints are already covered by the non-global rule in almost all
# environments.  Keeping explicit networks makes the intent auditable and
# protects against standard-library classification changes.
_METADATA_NETWORKS: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = (
    ipaddress.ip_network("169.254.169.254/32"),
    ipaddress.ip_network("169.254.170.2/32"),
    ipaddress.ip_network("100.100.100.200/32"),
    ipaddress.ip_network("fd00:ec2::254/128"),
)

# Globally formatted transition/special addresses can tunnel an embedded IPv4
# destination.  They are unnecessary for webhook delivery and are rejected
# rather than trying to duplicate every translator's routing policy.
_SPECIAL_IPV4_NETWORKS: tuple[ipaddress.IPv4Network, ...] = (
    # Python 3.12 classifies parts of this deprecated 6to4 relay range as
    # globally reachable even though IANA explicitly marks 192.88.99.2/32 as
    # not globally reachable. Deny the retired parent allocation fail-closed.
    ipaddress.IPv4Network("192.88.99.0/24"),
)
_SPECIAL_IPV6_NETWORKS: tuple[ipaddress.IPv6Network, ...] = (
    ipaddress.IPv6Network("64:ff9b::/96"),  # well-known NAT64 prefix
    ipaddress.IPv6Network("64:ff9b:1::/48"),  # local-use NAT64 prefix
    ipaddress.IPv6Network("2001::/32"),  # Teredo
    ipaddress.IPv6Network("2001:20::/28"),  # ORCHIDv2
    ipaddress.IPv6Network("2002::/16"),  # 6to4
)


class DestinationPolicyBlocked(RuntimeError):
    """The destination violates the outbound security policy and must not run."""


class DestinationTransportError(httpx2.TransportError):
    """A sanitized transient error occurred below the HTTP response boundary."""


class DestinationResolutionError(DestinationTransportError):
    """DNS failed, timed out, or returned no usable A/AAAA answers."""


@dataclass(frozen=True, slots=True)
class IPAddressClassification:
    """Normalized address plus the exact reason it is allowed or blocked."""

    original: str
    normalized: str | None
    version: Literal[4, 6] | None
    is_public: bool
    reason: AddressReason


@dataclass(frozen=True, slots=True)
class ResolvedDestination:
    """One validated connection target and the complete DNS answer set."""

    hostname: str
    port: int
    addresses: tuple[str, ...]
    selected_address: str
    local_exemption: bool


class AddressResolver(Protocol):
    """Resolve one hostname to every TCP-capable A/AAAA address."""

    async def resolve(
        self,
        hostname: str,
        port: int,
        timeout_seconds: float,
    ) -> Sequence[str]: ...


class SystemAddressResolver:
    """Bound the platform ``getaddrinfo`` resolver with an asyncio deadline."""

    async def resolve(
        self,
        hostname: str,
        port: int,
        timeout_seconds: float,
    ) -> Sequence[str]:
        try:
            async with asyncio.timeout(timeout_seconds):
                records = await asyncio.get_running_loop().getaddrinfo(
                    hostname,
                    port,
                    family=socket.AF_UNSPEC,
                    type=socket.SOCK_STREAM,
                    proto=socket.IPPROTO_TCP,
                )
        except TimeoutError as exc:
            raise DestinationResolutionError("destination name resolution timed out") from exc
        except (OSError, UnicodeError) as exc:
            raise DestinationResolutionError("destination name resolution failed") from exc

        addresses: list[str] = []
        for family, _kind, _protocol, _canonical_name, socket_address in records:
            if family not in {socket.AF_INET, socket.AF_INET6}:
                continue
            address = str(socket_address[0])
            if address not in addresses:
                addresses.append(address)
        if not addresses:
            raise DestinationResolutionError("destination name resolution returned no addresses")
        return tuple(addresses)


def _parsed_ip(value: str) -> IPAddress | None:
    try:
        return ipaddress.ip_address(value)
    except ValueError:
        return None


def classify_ip_address(value: str) -> IPAddressClassification:
    """Classify a normalized IP without treating unusual syntax as public.

    IPv4-mapped IPv6 addresses are normalized to their embedded IPv4 value for
    diagnostics, but the mapped representation itself is rejected.  This keeps
    alternate address syntax from bypassing an IPv4 deny decision.
    """

    candidate = value.strip()
    parsed = _parsed_ip(candidate)
    if parsed is None:
        return IPAddressClassification(candidate, None, None, False, "invalid")

    original_version: Literal[4, 6] = 4 if isinstance(parsed, ipaddress.IPv4Address) else 6
    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
        mapped = parsed.ipv4_mapped
        return IPAddressClassification(
            candidate,
            mapped.compressed,
            4,
            False,
            "ipv4_mapped",
        )

    normalized = parsed.compressed
    if any(parsed in network for network in _METADATA_NETWORKS):
        return IPAddressClassification(candidate, normalized, original_version, False, "metadata")
    if isinstance(parsed, ipaddress.IPv4Address) and any(
        parsed in network for network in _SPECIAL_IPV4_NETWORKS
    ):
        return IPAddressClassification(
            candidate,
            normalized,
            original_version,
            False,
            "special_transition",
        )
    if isinstance(parsed, ipaddress.IPv6Address) and any(
        parsed in network for network in _SPECIAL_IPV6_NETWORKS
    ):
        return IPAddressClassification(
            candidate,
            normalized,
            original_version,
            False,
            "special_transition",
        )
    if (
        not parsed.is_global
        or parsed.is_private
        or parsed.is_loopback
        or parsed.is_link_local
        or parsed.is_multicast
        or parsed.is_reserved
        or parsed.is_unspecified
    ):
        return IPAddressClassification(
            candidate,
            normalized,
            original_version,
            False,
            "not_global",
        )
    return IPAddressClassification(candidate, normalized, original_version, True, "public")


def is_public_destination_address(value: str) -> bool:
    """Return whether an address may be contacted outside an explicit local exemption."""

    return classify_ip_address(value).is_public


def normalize_hostname(value: str) -> str:
    """Return an exact lowercase ASCII hostname/IP suitable for policy comparison."""

    candidate = value.strip()
    if candidate.startswith("[") and candidate.endswith("]"):
        candidate = candidate[1:-1]
    candidate = candidate.rstrip(".")
    if not candidate or any(character.isspace() for character in candidate) or "\x00" in candidate:
        raise DestinationPolicyBlocked("destination hostname is invalid")

    parsed = _parsed_ip(candidate)
    if parsed is not None:
        return parsed.compressed.lower()
    try:
        normalized = candidate.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise DestinationPolicyBlocked("destination hostname is invalid") from exc
    if (
        not normalized
        or len(normalized) > 253
        or any(not label or len(label) > 63 for label in normalized.split("."))
        or "*" in normalized
    ):
        raise DestinationPolicyBlocked("destination hostname is invalid")
    return normalized


@dataclass(frozen=True, slots=True)
class DestinationPolicy:
    """Validate URL and DNS state for one deployment environment."""

    environment: Environment
    local_exempt_hosts: frozenset[str] = frozenset()
    dns_timeout_seconds: float = 2.0
    max_dns_answers: int = 32
    resolver: AddressResolver = field(
        default_factory=SystemAddressResolver,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if self.environment not in {"local", "test", "staging", "production"}:
            raise ValueError("unsupported destination-policy environment")
        if not 0 < self.dns_timeout_seconds <= 30:
            raise ValueError("dns_timeout_seconds must be between zero and 30 seconds")
        if not 1 <= self.max_dns_answers <= 256:
            raise ValueError("max_dns_answers must be between 1 and 256")
        normalized_exemptions = frozenset(
            normalize_hostname(hostname) for hostname in self.local_exempt_hosts
        )
        if self.environment in {"staging", "production"} and normalized_exemptions:
            raise ValueError("local destination exemptions are forbidden in staging and production")
        object.__setattr__(self, "local_exempt_hosts", normalized_exemptions)

    def validate_url(self, url: httpx2.URL) -> str:
        """Validate immutable URL properties and return its normalized hostname."""

        if url.scheme not in {"http", "https"}:
            raise DestinationPolicyBlocked("destination URL scheme is unsupported")
        if self.environment in {"staging", "production"} and url.scheme != "https":
            raise DestinationPolicyBlocked("HTTPS is required outside local and test")
        if url.username or url.password or url.fragment:
            raise DestinationPolicyBlocked("destination URL contains forbidden components")
        hostname = normalize_hostname(url.host)
        literal = _parsed_ip(hostname)
        if (
            literal is not None
            and hostname not in self.local_exempt_hosts
            and not classify_ip_address(hostname).is_public
        ):
            raise DestinationPolicyBlocked("destination URL contains a blocked IP address")
        return hostname

    def is_local_exemption(self, hostname: str) -> bool:
        """Use exact normalized equality; suffixes and wildcards never match."""

        return normalize_hostname(hostname) in self.local_exempt_hosts

    async def resolve(
        self,
        hostname: str,
        port: int,
        *,
        timeout_seconds: float | None = None,
    ) -> ResolvedDestination:
        """Resolve every answer and fail closed if any non-exempt answer is unsafe."""

        if not 1 <= port <= 65535:
            raise DestinationPolicyBlocked("destination port is invalid")
        normalized_hostname = normalize_hostname(hostname)
        local_exemption = normalized_hostname in self.local_exempt_hosts
        literal = _parsed_ip(normalized_hostname)
        addresses: tuple[str, ...]
        if literal is not None:
            addresses = (literal.compressed,)
        else:
            resolution_timeout = self.dns_timeout_seconds
            if timeout_seconds is not None:
                if timeout_seconds <= 0:
                    raise DestinationResolutionError("destination name resolution timed out")
                resolution_timeout = min(resolution_timeout, timeout_seconds)
            addresses = tuple(
                dict.fromkeys(
                    await self.resolver.resolve(
                        normalized_hostname,
                        port,
                        resolution_timeout,
                    )
                )
            )
        if not addresses:
            raise DestinationResolutionError("destination name resolution returned no addresses")
        if len(addresses) > self.max_dns_answers:
            raise DestinationResolutionError("destination returned too many DNS addresses")

        classifications = tuple(classify_ip_address(address) for address in addresses)
        if any(classification.normalized is None for classification in classifications):
            raise DestinationResolutionError("destination name resolution returned invalid data")
        if not local_exemption and any(
            not classification.is_public for classification in classifications
        ):
            raise DestinationPolicyBlocked("destination resolved to a blocked address")

        return ResolvedDestination(
            hostname=normalized_hostname,
            port=port,
            addresses=addresses,
            selected_address=addresses[0],
            local_exemption=local_exemption,
        )


class PolicyNetworkBackend(httpcore2.AsyncNetworkBackend):
    """Resolve once, connect to the validated IP, and verify the resulting peer."""

    def __init__(
        self,
        policy: DestinationPolicy,
        backend: httpcore2.AsyncNetworkBackend | None = None,
    ) -> None:
        self._policy = policy
        self._backend = backend or httpcore2.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109 -- HTTP Core interface name.
        local_address: str | None = None,
        socket_options: Iterable[httpcore2.SOCKET_OPTION] | None = None,
    ) -> httpcore2.AsyncNetworkStream:
        started_at = time.monotonic()
        destination = await self._policy.resolve(host, port, timeout_seconds=timeout)
        remaining_timeout = timeout
        if timeout is not None:
            remaining_timeout = timeout - (time.monotonic() - started_at)
            if remaining_timeout <= 0:
                raise DestinationResolutionError("destination name resolution timed out")

        stream = await self._backend.connect_tcp(
            destination.selected_address,
            port,
            timeout=remaining_timeout,
            local_address=local_address,
            socket_options=socket_options,
        )
        try:
            peer = stream.get_extra_info("server_addr")
            if not isinstance(peer, (tuple, list)) or not peer:
                raise DestinationTransportError("destination peer address is unavailable")
            peer_classification = classify_ip_address(str(peer[0]))
            selected_classification = classify_ip_address(destination.selected_address)
            if (
                peer_classification.normalized is None
                or selected_classification.normalized is None
                or peer_classification.normalized != selected_classification.normalized
            ):
                raise DestinationPolicyBlocked("connected peer did not match the validated address")
            if not destination.local_exemption and not peer_classification.is_public:
                raise DestinationPolicyBlocked("connected peer address is blocked")
        except BaseException:
            await stream.aclose()
            raise
        return stream

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,  # noqa: ASYNC109 -- HTTP Core interface name.
        socket_options: Iterable[httpcore2.SOCKET_OPTION] | None = None,
    ) -> httpcore2.AsyncNetworkStream:
        del path, timeout, socket_options
        raise DestinationPolicyBlocked("Unix-domain destination sockets are forbidden")

    async def sleep(self, seconds: float) -> None:
        await self._backend.sleep(seconds)


def _transport_error(
    exc: Exception,
    request: httpx2.Request | None = None,
) -> DestinationTransportError:
    if isinstance(exc, DestinationResolutionError):
        return DestinationResolutionError("destination name resolution failed", request=request)
    return DestinationTransportError("destination HTTP transport failed", request=request)


def _is_httpcore_transport_error(exc: Exception) -> bool:
    return isinstance(
        exc,
        (
            httpcore2.TimeoutException,
            httpcore2.NetworkError,
            httpcore2.ProtocolError,
            httpcore2.ProxyError,
            httpcore2.UnsupportedProtocol,
        ),
    )


class _ResponseStream(httpx2.AsyncByteStream):
    def __init__(
        self,
        stream: AsyncIterator[bytes],
        request: httpx2.Request,
    ) -> None:
        self._stream = stream
        self._request = request

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for item in self._stream:
                yield item
        except Exception as exc:
            if _is_httpcore_transport_error(exc):
                raise _transport_error(exc, self._request) from exc
            raise

    async def aclose(self) -> None:
        close = getattr(self._stream, "aclose", None)
        if close is None:
            return
        try:
            await close()
        except Exception as exc:
            if _is_httpcore_transport_error(exc):
                raise _transport_error(exc, self._request) from exc
            raise


class SSRFSafeAsyncTransport(httpx2.AsyncBaseTransport):
    """Public HTTPX transport using public HTTP Core extension interfaces only."""

    def __init__(
        self,
        policy: DestinationPolicy,
        *,
        ssl_context: ssl.SSLContext | None = None,
        max_connections: int = 10,
        network_backend: httpcore2.AsyncNetworkBackend | None = None,
        socket_options: Iterable[httpcore2.SOCKET_OPTION] | None = None,
    ) -> None:
        if max_connections < 1:
            raise ValueError("max_connections must be positive")
        self._policy = policy
        self._pool = httpcore2.AsyncConnectionPool(
            ssl_context=ssl_context or httpx2.create_ssl_context(verify=True, trust_env=False),
            max_connections=max_connections,
            max_keepalive_connections=0,
            keepalive_expiry=0,
            http1=True,
            http2=False,
            retries=0,
            network_backend=PolicyNetworkBackend(policy, network_backend),
            socket_options=socket_options,
        )

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        hostname = self._policy.validate_url(request.url)
        canonical_url = request.url.copy_with(host=hostname)
        if not isinstance(request.stream, httpx2.AsyncByteStream):
            raise DestinationTransportError("destination request stream must be asynchronous")

        # Never trust a caller-supplied Host or SNI override. Both values come
        # from the validated canonical hostname while only the TCP peer is pinned.
        headers = [(name, value) for name, value in request.headers.raw if name.lower() != b"host"]
        headers.append((b"Host", canonical_url.netloc))
        extensions = dict(request.extensions)
        extensions["sni_hostname"] = hostname
        core_request = httpcore2.Request(
            method=request.method,
            url=httpcore2.URL(
                scheme=canonical_url.raw_scheme,
                host=canonical_url.raw_host,
                port=canonical_url.port,
                target=request.url.raw_path,
            ),
            headers=headers,
            content=request.stream,
            extensions=extensions,
        )
        try:
            core_response = await self._pool.handle_async_request(core_request)
        except DestinationPolicyBlocked:
            raise
        except DestinationResolutionError as exc:
            raise _transport_error(exc, request) from exc
        except Exception as exc:
            if _is_httpcore_transport_error(exc):
                raise _transport_error(exc, request) from exc
            raise

        stream = cast("AsyncIterator[bytes]", core_response.stream)
        return httpx2.Response(
            status_code=core_response.status,
            headers=core_response.headers,
            stream=_ResponseStream(stream, request),
            extensions=core_response.extensions,
        )

    async def aclose(self) -> None:
        try:
            await self._pool.aclose()
        except Exception as exc:
            if _is_httpcore_transport_error(exc):
                raise _transport_error(exc) from exc
            raise
