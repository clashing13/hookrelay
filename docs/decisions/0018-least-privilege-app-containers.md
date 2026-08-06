# ADR 0018: Run application containers with a least-privilege baseline

- Status: Accepted
- Date: 2026-08-05

## Context

A process compromise should not automatically grant root identity inside the
container, permission to rewrite application or migration code, Linux capabilities it
does not need, unbounded process creation, or writable persistent storage. Container
isolation is useful, but a container is not a security boundary equivalent to a
separate physical machine and defaults are not automatically least privilege.

HookRelay's API, outbox publisher, delivery worker, and local receiver use the same
application image. They need network access and a small temporary directory, but they
do not need root, write access to the image filesystem, additional capabilities, or
host filesystem mounts.

PostgreSQL and NATS are upstream stateful images with required writable volumes and
different runtime needs. They are not covered by the application-container baseline
in this decision.

## Decision

The runtime image creates an explicit `hookrelay` account and switches permanently to
`USER 10001:10001`. The account has no home directory and uses `nologin`. Application
and migration artifacts copied from the builder remain root-owned and readable, so
the service account cannot rewrite code that a later operator command may execute.

The shared Compose application anchor applies to API, publisher, worker, and test
receiver:

- `user: "10001:10001"`;
- `read_only: true` for the root filesystem;
- `cap_drop: [ALL]`;
- `security_opt: [no-new-privileges:true]`;
- `pids_limit: 256`;
- a 16 MiB `/tmp` `tmpfs` with `rw,noexec,nosuid,nodev,mode=1777`;
- no host filesystem or Docker socket mount;
- an init process for signal and child reaping.

Published local ports bind to `127.0.0.1`. The Dockerfile remains multi-stage and the
runtime image excludes tests, documentation, `.env`, VCS metadata, and build caches.

## Serious alternatives

### Run as the image default root user

It avoids permission troubleshooting but grants privileges the application does not
need and increases the impact of a process escape or writable mount mistake.

### Set only `USER` in the Dockerfile

Non-root identity is necessary but does not make the root filesystem immutable,
remove inherited capabilities, block privilege gain, or bound process creation. The
Compose runtime controls provide independent layers.

### Make the entire filesystem unwritable without `/tmp`

Some Python and TLS/runtime paths may need temporary files. A bounded ephemeral
`tmpfs` grants only that need, with execution and device semantics disabled.

### Apply the application anchor blindly to PostgreSQL and NATS

Stateful upstream images require writable data paths and have image-specific user and
capability assumptions. They need separate hardening reviews rather than an
unverified copy of the app policy.

### Treat container settings as the network security boundary

These settings do not restrict outbound destinations. Production still needs egress
firewall or network-policy rules, segmented credentials, and host/runtime hardening.

## Consequences

- The application runs as numeric UID/GID 10001 even if image metadata is overridden
  accidentally elsewhere in Compose.
- A compromised app process cannot normally modify its image filesystem or migration
  files and receives no added Linux capabilities.
- The explicitly configured application temporary path is ephemeral and limited to
  16 MiB under `/tmp`.
- A process tree is bounded to 256 PIDs per app container.
- Dependencies that assume arbitrary writable paths or privileged syscalls will fail
  and must be adapted deliberately.
- Read-only filesystems do not protect secrets already present in environment
  variables or process memory.
- This is not rootless Docker, a custom seccomp/AppArmor/SELinux profile, a distroless
  image, image signing, vulnerability management, or sandboxing of PostgreSQL/NATS.
- The default Compose network still permits service-to-service traffic. A production
  egress firewall remains mandatory defense in depth for SSRF and compromise.

## Primary references

- [Docker build best practices](https://docs.docker.com/build/building/best-practices/)
- [Dockerfile `USER` reference](https://docs.docker.com/reference/dockerfile/#user)
- [Docker Compose service controls](https://docs.docker.com/reference/compose-file/services/)
- [Docker rootless mode](https://docs.docker.com/engine/security/rootless/)
