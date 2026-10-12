# Security policy

Do not open a public issue for a suspected vulnerability. Use GitHub private vulnerability
reporting for the repository, and include affected versions, reproduction steps, impact, and any
suggested mitigation. Do not include production credentials or customer data.

This reference project blocks private-network browser targets, requires explicit approval for
mutating browser tools, and keeps inference endpoints server-controlled. Deployers are still
responsible for TLS/mTLS, external secret management, egress policy, managed Redis hardening,
immutable audit retention, dependency scanning, and incident response.
