# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | ✅ |

## Reporting a vulnerability

**Please do not open a public issue for security reports.**

Email **1510365643@qq.com** with:

- affected version (commit hash if built from source)
- reproduction steps or a minimal proof of concept
- impact assessment (what an attacker could do)

You will get an acknowledgement within 48 hours. Please keep the details
private until a fix is released; we will credit reporters in the release
notes unless you prefer otherwise.

## Scope notes

- The server is designed for **trusted networks**. Transport is plain HTTP by
  default — put a TLS-terminating proxy in front if you need encryption in
  transit. Do not expose an unauthenticated (`--token` unset) instance to an
  untrusted network.
- Credentials are stored as salted iterated SHA-256 hashes in
  `<catalog>/users.json`; the super token is supplied via `--token` / config at
  startup and should be treated as a secret.
