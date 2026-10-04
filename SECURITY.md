# Security

WindowFlow v0.1 is experimental. There is no production-support or security-response SLA, and no released version is represented here as certified for production.

## Report a concern

If GitHub private vulnerability reporting is enabled for this repository, use its **Security → Report a vulnerability** flow. If that option is unavailable, open a public issue containing only a request for a private reporting channel. Do not post exploit instructions, credentials, private data paths, or a detailed unsafe-deletion reproduction publicly before a private channel is established. This repository does not currently advertise a separate security email address.

Useful private report details include the affected commit/image, a sanitized plan, the violated ownership or release invariant, and a reproduction using disposable files. Do not send production access keys or kubeconfigs.

## Trust boundaries

- Cluster administrators and subjects able to modify plans, Jobs, PVCs, or coordination locks can affect data safety. Kubernetes RBAC and namespace isolation are part of the deployment boundary.
- Reader IDs are protocol identifiers, not independently authenticated principals. A client allowed to write leases must be trusted not to release another reader's work.
- The local source must be immutable and trusted. Path and mutation checks reduce accidental unsafe behavior but are not a substitute for controlling concurrent writers.
- `expectedBytes` checks size, not content identity or authenticity.
- Workers can write and delete ownership-verified cache generation directories. Do not give training clients unnecessary worker or cloud credentials.
- CPFS tasks can outlive their submitting Pods. Recovery must check provider activity before cleanup or lock removal.

No lease timeout grants permission to delete. Please report any path that lets an unknown/stale lease, mismatched ownership marker, symlink/path escape, or failed worker violate that rule.

## Credentials and diagnostics

Keep Alibaba Cloud credentials in namespace-local Secrets and inject them only into workers that need them. Use the least provider permissions required for the configured operations. Never commit credentials, Secret exports, service-account tokens, or kubeconfigs.

Before sharing diagnostics, remove credential values and private source details. Retain nonsensitive identity fields needed to investigate, such as generation, sanitized plan UID, phases, and error categories. Do not publish an entire environment dump.
