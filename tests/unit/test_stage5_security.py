"""SSRF policy and pinned-transport security tests for Stage 5."""

import asyncio
import socket
import ssl
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

import httpcore2
import httpx2
import pytest

from hookrelay.destination_policy import (
    DestinationPolicy,
    DestinationPolicyBlocked,
    DestinationResolutionError,
    DestinationTransportError,
    PolicyNetworkBackend,
    SSRFSafeAsyncTransport,
    SystemAddressResolver,
    classify_ip_address,
)


@dataclass(slots=True)
class _Resolver:
    answers: tuple[str, ...] = ()
    error: Exception | None = None
    calls: list[tuple[str, int, float]] = field(default_factory=list)

    async def resolve(
        self,
        hostname: str,
        port: int,
        timeout_seconds: float,
    ) -> Sequence[str]:
        self.calls.append((hostname, port, timeout_seconds))
        if self.error is not None:
            raise self.error
        return self.answers


class _TLSInfo:
    def selected_alpn_protocol(self) -> str:
        return "http/1.1"


class _RecordingStream(httpcore2.AsyncNetworkStream):
    def __init__(self, peer_address: str) -> None:
        self.peer_address = peer_address
        self.response_parts = [b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"]
        self.writes: list[bytes] = []
        self.tls_hostnames: list[str | None] = []
        self.closed = False
        self.tls_started = False

    async def read(
        self,
        max_bytes: int,
        timeout: float | None = None,  # noqa: ASYNC109 -- HTTP Core interface name.
    ) -> bytes:
        del max_bytes, timeout
        return self.response_parts.pop(0) if self.response_parts else b""

    async def write(
        self,
        buffer: bytes,
        timeout: float | None = None,  # noqa: ASYNC109 -- HTTP Core interface name.
    ) -> None:
        del timeout
        self.writes.append(buffer)

    async def aclose(self) -> None:
        self.closed = True

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109 -- HTTP Core interface name.
    ) -> httpcore2.AsyncNetworkStream:
        del ssl_context, timeout
        self.tls_started = True
        self.tls_hostnames.append(server_hostname)
        return self

    def get_extra_info(self, info: str) -> object | None:
        if info == "server_addr":
            return (self.peer_address, 443)
        if info == "ssl_object" and self.tls_started:
            return _TLSInfo()
        if info == "is_readable":
            return False
        return None


@dataclass(slots=True)
class _RecordingBackend(httpcore2.AsyncNetworkBackend):
    peer_override: str | None = None
    error: Exception | None = None
    connections: list[tuple[str, int]] = field(default_factory=list)
    streams: list[_RecordingStream] = field(default_factory=list)

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109 -- HTTP Core interface name.
        local_address: str | None = None,
        socket_options: Iterable[httpcore2.SOCKET_OPTION] | None = None,
    ) -> httpcore2.AsyncNetworkStream:
        del timeout, local_address, socket_options
        self.connections.append((host, port))
        if self.error is not None:
            raise self.error
        stream = _RecordingStream(self.peer_override or host)
        self.streams.append(stream)
        return stream

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,  # noqa: ASYNC109 -- HTTP Core interface name.
        socket_options: Iterable[httpcore2.SOCKET_OPTION] | None = None,
    ) -> httpcore2.AsyncNetworkStream:
        del path, timeout, socket_options
        raise AssertionError("Unix sockets are not expected")

    async def sleep(self, seconds: float) -> None:
        del seconds


@pytest.mark.parametrize(
    "address",
    [
        "8.8.8.8",
        "1.1.1.1",
        "2606:4700:4700::1111",
    ],
)
def test_only_plain_global_unicast_addresses_are_public(address: str) -> None:
    classification = classify_ip_address(address)

    assert classification.is_public is True
    assert classification.reason == "public"


@pytest.mark.parametrize(
    ("address", "reason"),
    [
        ("127.0.0.1", "not_global"),
        ("10.0.0.1", "not_global"),
        ("100.64.0.1", "not_global"),
        ("169.254.169.254", "metadata"),
        ("169.254.170.2", "metadata"),
        ("100.100.100.200", "metadata"),
        ("224.0.0.1", "not_global"),
        ("0.0.0.0", "not_global"),
        ("::1", "not_global"),
        ("fe80::1", "not_global"),
        ("fc00::1", "not_global"),
        ("fd00:ec2::254", "metadata"),
        ("ff02::1", "not_global"),
        ("::ffff:8.8.8.8", "ipv4_mapped"),
        ("::ffff:169.254.169.254", "ipv4_mapped"),
        ("64:ff9b::808:808", "special_transition"),
        ("192.88.99.0", "special_transition"),
        ("192.88.99.2", "special_transition"),
        ("192.88.99.255", "special_transition"),
        ("not-an-ip", "invalid"),
    ],
)
def test_non_global_special_metadata_and_mapped_addresses_are_blocked(
    address: str,
    reason: str,
) -> None:
    classification = classify_ip_address(address)

    assert classification.is_public is False
    assert classification.reason == reason


@pytest.mark.asyncio
async def test_system_resolver_collects_every_a_and_aaaa_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()

    async def fake_getaddrinfo(
        hostname: str,
        port: int,
        *,
        family: int,
        type: int,
        proto: int,
    ) -> list[tuple[int, int, int, str, tuple[str, int] | tuple[str, int, int, int]]]:
        assert (hostname, port, family, type, proto) == (
            "public.example",
            443,
            socket.AF_UNSPEC,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP,
        )
        return [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("8.8.8.8", 443)),
            (
                socket.AF_INET6,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "",
                ("2606:4700:4700::1111", 443, 0, 0),
            ),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("8.8.8.8", 443)),
        ]

    monkeypatch.setattr(loop, "getaddrinfo", fake_getaddrinfo)
    addresses = await SystemAddressResolver().resolve("public.example", 443, 0.5)

    assert addresses == ("8.8.8.8", "2606:4700:4700::1111")


@pytest.mark.asyncio
async def test_system_resolver_has_a_hard_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    loop = asyncio.get_running_loop()

    async def stalled_getaddrinfo(
        _hostname: str,
        _port: int,
        **_kwargs: int,
    ) -> list[tuple[int, int, int, str, tuple[str, int]]]:
        await asyncio.sleep(1)
        return []

    monkeypatch.setattr(loop, "getaddrinfo", stalled_getaddrinfo)
    with pytest.raises(DestinationResolutionError):
        await SystemAddressResolver().resolve("stalled.example", 443, 0.01)


@pytest.mark.asyncio
async def test_mixed_public_and_private_dns_answers_fail_closed() -> None:
    resolver = _Resolver(("8.8.8.8", "10.0.0.4"))
    policy = DestinationPolicy(environment="production", resolver=resolver)

    with pytest.raises(DestinationPolicyBlocked, match="blocked address"):
        await policy.resolve("mixed.example", 443)

    assert resolver.calls == [("mixed.example", 443, 2.0)]


@pytest.mark.asyncio
async def test_exact_local_exemption_never_becomes_a_suffix_or_production_rule() -> None:
    resolver = _Resolver(("172.18.0.4",))
    policy = DestinationPolicy(
        environment="test",
        local_exempt_hosts=frozenset({"Receiver."}),
        resolver=resolver,
    )

    accepted = await policy.resolve("receiver", 9000)
    assert accepted.local_exemption is True
    assert accepted.selected_address == "172.18.0.4"

    with pytest.raises(DestinationPolicyBlocked):
        await policy.resolve("receiver.example", 9000)
    with pytest.raises(ValueError, match="forbidden"):
        DestinationPolicy(
            environment="production",
            local_exempt_hosts=frozenset({"receiver"}),
        )


@pytest.mark.asyncio
async def test_transport_connects_to_validated_ip_and_preserves_host_and_tls_sni() -> None:
    resolver = _Resolver(("93.184.216.34",))
    policy = DestinationPolicy(environment="production", resolver=resolver)
    backend = _RecordingBackend()
    transport = SSRFSafeAsyncTransport(
        policy,
        max_connections=2,
        network_backend=backend,
    )

    async with httpx2.AsyncClient(
        transport=transport,
        follow_redirects=False,
        trust_env=False,
        timeout=1,
    ) as client:
        for _ in range(2):
            response = await client.post(
                "https://Webhook.Example.:444/deliver",
                headers={"Host": "attacker.internal"},
                content=b"{}",
                extensions={"sni_hostname": "attacker.internal"},
            )
            assert response.status_code == 204

    assert backend.connections == [
        ("93.184.216.34", 444),
        ("93.184.216.34", 444),
    ]
    assert [call[:2] for call in resolver.calls] == [
        ("webhook.example", 444),
        ("webhook.example", 444),
    ]
    assert [stream.tls_hostnames for stream in backend.streams] == [
        ["webhook.example"],
        ["webhook.example"],
    ]
    written = b"".join(part for stream in backend.streams for part in stream.writes).lower()
    assert b"host: webhook.example:444\r\n" in written
    assert b"attacker.internal" not in written


@pytest.mark.asyncio
async def test_connected_peer_must_match_the_validated_ip() -> None:
    policy = DestinationPolicy(
        environment="production",
        resolver=_Resolver(("93.184.216.34",)),
    )
    backend = _RecordingBackend(peer_override="1.1.1.1")
    protected_backend = PolicyNetworkBackend(policy, backend)

    with pytest.raises(DestinationPolicyBlocked, match="did not match"):
        await protected_backend.connect_tcp("webhook.example", 443)

    assert backend.streams[0].closed is True


@pytest.mark.asyncio
async def test_transport_maps_dns_and_core_network_errors_for_delivery_policy() -> None:
    dns_policy = DestinationPolicy(
        environment="production",
        resolver=_Resolver(error=DestinationResolutionError("dns failed")),
    )
    dns_transport = SSRFSafeAsyncTransport(dns_policy, network_backend=_RecordingBackend())
    async with httpx2.AsyncClient(transport=dns_transport, timeout=1) as client:
        with pytest.raises(DestinationResolutionError) as dns_error:
            await client.get("https://dns.example/")
    assert dns_error.value.request.url.host == "dns.example"

    network_policy = DestinationPolicy(
        environment="production",
        resolver=_Resolver(("93.184.216.34",)),
    )
    network_transport = SSRFSafeAsyncTransport(
        network_policy,
        network_backend=_RecordingBackend(error=httpcore2.ConnectError("connect failed")),
    )
    async with httpx2.AsyncClient(transport=network_transport, timeout=1) as client:
        with pytest.raises(DestinationTransportError) as network_error:
            await client.get("https://network.example/")
    assert network_error.value.request.url.host == "network.example"


@pytest.mark.parametrize(
    "url",
    [
        "http://public.example/webhook",
        "https://user:password@public.example/webhook",
        "https://public.example/webhook#fragment",
        "https://127.0.0.1/webhook",
        "https://169.254.169.254/latest/meta-data",
        "https://[::ffff:8.8.8.8]/webhook",
    ],
)
def test_production_url_policy_rejects_insecure_or_ambiguous_urls(url: str) -> None:
    policy = DestinationPolicy(environment="production")

    with pytest.raises(DestinationPolicyBlocked):
        policy.validate_url(httpx2.URL(url))


def test_local_exact_exemption_allows_an_unsafe_literal_url() -> None:
    policy = DestinationPolicy(
        environment="local",
        local_exempt_hosts=frozenset({"127.0.0.1"}),
    )

    assert policy.validate_url(httpx2.URL("http://127.0.0.1:9000/hook")) == "127.0.0.1"
