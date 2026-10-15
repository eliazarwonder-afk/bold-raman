# Security Policy

## 🔒 Security Doctrine

Distributed task queues operate at the core of enterprise application infrastructure. PulseQueue was specifically architected with security as a non-negotiable primary constraint.

### 1. Zero-Pickle Guarantee (Protection Against Remote Code Execution)
Legacy task engines (like Celery) have historically suffered from vulnerabilities where compromised message brokers allowed attackers to inject pickled Python payloads, executing arbitrary shell code inside worker nodes (`__reduce__` exploit vector).

* **PulseQueue strictly forbids `pickle`**.
* All task payloads are serialized via **binary MessagePack (`msgpack`)** combined with **Pydantic v2 runtime validation**.
* Deserialization will never invoke bytecode or construct arbitrary Python class instances.

### 2. Environment Variables & Secrets Handling
* **Never pass raw API keys or database passwords inside task arguments (`args` or `kwargs`)**. Task envelopes are written to the Redis stream and dead-letter queues, which can be inspected by monitoring tools.
* Instead, pass database IDs or record UUIDs in task parameters, and retrieve secrets inside the task via environment variables (`os.environ`) or a secrets manager (Vault, AWS Secrets Manager).

### 3. Concurrency & Resource Exhaustion (DoS Defense)
* Each worker instance enforces a strict concurrency ceiling via `asyncio.Semaphore`.
* Tasks exceeding their specified `timeout_seconds` are forcefully cancelled by the worker event loop to prevent zombie processes and memory starvation.

---

## 🛡️ Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

---

## 🚨 Reporting a Vulnerability

If you discover a security vulnerability within PulseQueue, please do **NOT** open a public issue on GitHub.

Instead, please send an email with:
1. A description of the vulnerability.
2. Steps or a proof-of-concept script to reproduce the issue.
3. The potential impact on workers or brokers.

To: **security@pulsequeue.dev** (or open a Private Vulnerability Advisory on GitHub under `Security -> Advisories -> Report a vulnerability`).

You will receive an acknowledgment within 24 hours, and a remediation patch will be published within 7 business days.
