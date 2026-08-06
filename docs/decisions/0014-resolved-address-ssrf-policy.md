# ADR 0014: Validate every resolved address and pin each outbound connection

- Status: Accepted; supersedes ADR 0010 for outbound destination enforcement
- Date: 2026-08-05

## Context

HookRelay sends HTTP requests to tenant-configured URLs. That makes the worker a
potential server-side request forgery (SSRF) primitive: an attacker could ask a
process inside HookRelay's network to contact loopback services, private networks,
link-local cloud metadata, or a hostname that changes from a public address during
validation to a private address during connection.

Parsing a URL or checking a hostname when the endpoint is created is not enough.
DNS can return both A and AAAA records, answers can change, and a conventional HTTP
client can perform a second DNS lookup after an earlier validation. Redirects and
environment-configured proxies create additional destinations that did not pass the
original policy.

HookRelay must support arbitrary public customer endpoints, so a production-wide
hostname allowlist is not practical. The local Compose receiver is intentionally on
a private Docker network and therefore needs a narrow development exception.

## Decision

Use `DestinationPolicy` and `SSRFSafeAsyncTransport` for endpoint creation and every
worker delivery.

The policy:

- accepts only `http` and `https`, and requires `https` in staging and production;
- rejects URL user information and fragments;
- synchronously rejects unsafe literal IP URLs before a worker reserves an attempt;
- normalizes hostnames and IP literals before comparison;
- resolves a hostname with `getaddrinfo(AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP)` under
  a two-second default deadline;
- collects every distinct A and AAAA answer, with a default maximum of 32;
- fails closed when any answer is invalid or non-global, including private,
  loopback, link-local, shared, multicast, reserved, unspecified, metadata,
  IPv4-mapped IPv6, NAT64, Teredo, ORCHIDv2, 6to4 forms, and the deprecated
  `192.88.99.0/24` IPv4 6to4 relay-anycast range;
- selects an address only after the complete answer set passes;
- connects the socket to that numeric address and verifies the resulting peer;
- retains the URL hostname for the HTTP `Host` header, TLS SNI, and certificate
  hostname verification;
- disables connection retries, HTTP/2, and keepalive reuse so a new connection gets
  a new policy decision;
- does not configure a proxy and is used by an HTTP client with
  `trust_env=False` and `follow_redirects=False`.

An immutable exact-host exemption set is allowed only in `local` and `test`.
`receiver`, `127.0.0.1`, and `localhost` are the local defaults. Exemptions do not
match suffixes or wildcards. Staging and production reject any non-empty exemption
set during configuration validation.

Endpoint creation performs an early resolution check for non-exempt hosts. That is
useful feedback, not the security boundary. The authoritative validation occurs
immediately before each TCP connection through the public HTTP Core asynchronous
network-backend interface. The transport is built only from public HTTPX 2 and HTTP
Core 2 interfaces; it does not patch private client attributes.

## Serious alternatives

### Validate only when the endpoint is created

This catches obvious mistakes but leaves a time-of-check/time-of-use window. DNS can
change before a delivery or replay. Creation-time validation remains an ergonomic
check, not the connection security boundary.

### Resolve, validate, then give the hostname to a normal client

The client could resolve again and connect to a different answer. HookRelay instead
hands the validated numeric address to the socket backend while preserving the
original hostname above the TCP layer.

### Validate only the first DNS answer

Resolvers may return a mixture of public and private answers or change their order.
Accepting one safe member would permit rebinding and selection ambiguity. HookRelay
rejects the entire destination if any member is unsafe.

### Follow redirects and validate only the first URL

A public endpoint could redirect to an internal service. Redirect following is
disabled. A future redirect feature would have to validate every hop independently
and bound the hop count.

### Trust proxy environment variables

`HTTP_PROXY`, `HTTPS_PROXY`, and related variables can move the real connection to an
unvalidated intermediary. The worker ignores environment proxy configuration. A
future explicitly managed egress proxy would need its own trusted policy.

### Use only an application denylist

The application check is necessary but cannot constrain compromised code, another
HTTP stack, kernel routing, or infrastructure mistakes. Network egress controls are
still required in a production deployment.

## Consequences

- DNS rebinding between validation and socket connection is materially constrained.
- Mixed public/private A and AAAA answer sets fail closed.
- TLS still authenticates the customer hostname even though TCP connects to a
  numeric address.
- A policy rejection is distinct from DNS and transport failures. A connection-time
  policy rejection dead-letters the delivery as `target_blocked`; DNS and network
  failures remain transient retry candidates.
- Disabling keepalive and HTTP/2 connection reuse favors a simple auditable boundary
  over throughput. Every delivery pays DNS and connection setup cost.
- Selecting the first fully validated answer does not provide sophisticated address
  failover or load balancing.
- A two-second resolver deadline bounds the await but the platform resolver uses a
  thread-pool-backed synchronous operation that may finish later in the background.
- IP classification depends partly on Python's versioned IANA-derived address tables;
  explicit metadata and transition ranges reduce, but do not eliminate, registry
  drift risk. `192.88.99.0/24` is explicit because the supported Python 3.12 runtime
  can classify members such as `192.88.99.2` as global even though IANA marks the
  special-purpose block deprecated. Runtime upgrades require registry-vector tests.
- Local/test exemptions intentionally permit private destinations and must never be
  copied into staging or production.
- This control does not replace an outbound firewall, private-service authentication,
  a trusted DNS resolver, service-mesh policy, or monitoring.

## Primary references

- [OWASP SSRF Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html)
- [Python `ipaddress` reference](https://docs.python.org/3/library/ipaddress.html)
- [IANA IPv4 Special-Purpose Address Registry](https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml)
- [IANA IPv6 Special-Purpose Address Registry](https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml)
- [Python asynchronous DNS reference](https://docs.python.org/3/library/asyncio-eventloop.html#dns)
- [HTTPX custom transports](https://www.python-httpx.org/advanced/transports/)
- [HTTPX environment variables](https://www.python-httpx.org/environment_variables/)
- [HTTP Core network backends](https://www.encode.io/httpcore/network-backends/)
- [HTTP Core connection pools](https://www.encode.io/httpcore/connection-pools/)
