# NOTICE & ARCHITECTURAL ATTRIBUTION

**PulseQueue**
Copyright (c) 2026 PulseQueue Authors & Contributors.

Licensed under the MIT License. You may obtain a copy of the License in the `LICENSE` file.

---

## 1. Architectural Influences & Attributions

PulseQueue's distributed systems architecture is influenced by key research, standards, and engineering papers:

* **Redis Streams & Consumer Groups Specification:**
  Task leasing, consumer acknowledgement (`XACK`), and crashed-worker reclamation (`XAUTOCLAIM` / Pending Entries List) follow the Redis Streams specification introduced by Salvatore Sanfilippo (antirez).
* **Exponential Backoff and Full Jitter:**
  The jitter and retry scheduling algorithms implemented in `pulsequeue.worker.retry` are based on the canonical distributed systems paper *"Exponential Backoff And Jitter"* by Marc Brooker (AWS Architecture Blog, 2015).
* **Safe Binary Serialization:**
  Wire format serialization avoids Python object pickling vulnerabilities by strictly adhering to the MessagePack format specification (Sadayuki Furuhashi) coupled with type validation via Pydantic v2 (Samuel Colvin).

---

## 2. Production Deployment Notice

When deploying PulseQueue in multi-node production environments:

1. **Broker High Availability:**
   For high-availability Redis deployments, ensure Redis Sentinel or Redis Cluster is used with append-only file persistence (`appendonly yes`) enabled to prevent task loss during catastrophic node restarts.
2. **Task Idempotency:**
   PulseQueue guarantees **at-least-once delivery**. In rare scenarios involving network splits or ungraceful hardware failures before an acknowledgement completes, tasks may be re-delivered. Application tasks that perform mutations (e.g. charging credit cards, sending emails) should implement idempotency keys at the application or database layer.
3. **Visibility Timeouts:**
   Long-running tasks should have an explicitly configured `timeout_seconds` in `@app.task(...)` to match expected execution ceilings and prevent premature recovery by watchdog processes.
