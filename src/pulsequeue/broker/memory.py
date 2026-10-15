from __future__ import annotations

import asyncio
import heapq
import time
from typing import Any

from pulsequeue.broker.base import AbstractBroker
from pulsequeue.core.models import TaskEnvelope, TaskResult, TaskStatus


class MemoryBroker(AbstractBroker):
    """In-memory async broker implementation.

    Designed for zero-infra local development, rapid unit testing, and embedded runners.
    Simulates at-least-once delivery, delayed retries, dead-letter queues, and worker heartbeats.
    """

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[TaskEnvelope]] = {}
        # Min-heap of delayed tasks: list of (eta, sequence_id, queue_name, TaskEnvelope)
        self._delayed: list[tuple[float, int, str, TaskEnvelope]] = []
        self._delayed_counter: int = 0
        self._delayed_lock = asyncio.Lock()

        # Pending/In-flight unacked tasks: task_id -> (queue, worker_id, envelope, expire_time)
        self._pending: dict[str, tuple[str, str, TaskEnvelope, float]] = {}
        self._pending_lock = asyncio.Lock()

        # Dead letter queues: queue_name -> dict[task_id, TaskEnvelope]
        self._dlq: dict[str, dict[str, TaskEnvelope]] = {}

        # Result store: task_id -> TaskResult
        self._results: dict[str, TaskResult] = {}
        self._result_waiters: dict[str, asyncio.Event] = {}

        # Worker heartbeats: worker_id -> (last_ping_timestamp, metadata)
        self._workers: dict[str, tuple[float, dict[str, Any]]] = {}

        self._connected: bool = False
        self._maintenance_task: asyncio.Task[None] | None = None

    def _get_queue(self, queue: str) -> asyncio.Queue[TaskEnvelope]:
        if queue not in self._queues:
            self._queues[queue] = asyncio.Queue()
        return self._queues[queue]

    def _get_dlq(self, queue: str) -> dict[str, TaskEnvelope]:
        if queue not in self._dlq:
            self._dlq[queue] = {}
        return self._dlq[queue]

    async def connect(self) -> None:
        self._connected = True
        if self._maintenance_task is None or self._maintenance_task.done():
            self._maintenance_task = asyncio.create_task(self._delayed_scheduler_loop())

    async def close(self) -> None:
        self._connected = False
        if self._maintenance_task and not self._maintenance_task.done():
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except asyncio.CancelledError:
                pass

    async def _delayed_scheduler_loop(self) -> None:
        """Polls delayed heap and transfers due tasks into active queues."""
        while self._connected:
            try:
                now = time.time()
                async with self._delayed_lock:
                    while self._delayed and self._delayed[0][0] <= now:
                        _, _, queue_name, envelope = heapq.heappop(self._delayed)
                        q = self._get_queue(queue_name)
                        await q.put(envelope)
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.1)

    async def enqueue(self, envelope: TaskEnvelope, queue: str = "default") -> str:
        envelope.queue = queue
        now = time.time()
        if envelope.eta and envelope.eta > now:
            async with self._delayed_lock:
                self._delayed_counter += 1
                heapq.heappush(self._delayed, (envelope.eta, self._delayed_counter, queue, envelope))
        else:
            q = self._get_queue(queue)
            await q.put(envelope)
        return envelope.task_id

    async def dequeue(self, queue: str, worker_id: str, timeout: float = 1.0) -> TaskEnvelope | None:
        q = self._get_queue(queue)
        try:
            envelope = await asyncio.wait_for(q.get(), timeout=timeout)
        except (TimeoutError, asyncio.TimeoutError):
            return None

        # Track as in-flight pending task
        envelope.started_at = time.time()
        visibility_timeout = envelope.timeout_seconds or 30.0
        async with self._pending_lock:
            self._pending[envelope.task_id] = (
                queue,
                worker_id,
                envelope,
                time.time() + visibility_timeout,
            )

        return envelope

    async def ack(self, envelope: TaskEnvelope) -> None:
        async with self._pending_lock:
            self._pending.pop(envelope.task_id, None)

    async def nack(self, envelope: TaskEnvelope, error: str, retry_delay: float | None = None) -> None:
        async with self._pending_lock:
            self._pending.pop(envelope.task_id, None)

        envelope.error = error
        if retry_delay is not None and retry_delay > 0:
            envelope.eta = time.time() + retry_delay
        else:
            envelope.eta = None

        await self.enqueue(envelope, queue=envelope.queue)

    async def dead_letter(self, envelope: TaskEnvelope, error: str, traceback_str: str) -> None:
        async with self._pending_lock:
            self._pending.pop(envelope.task_id, None)

        envelope.error = error
        envelope.traceback = traceback_str
        envelope.completed_at = time.time()
        dlq = self._get_dlq(envelope.queue)
        dlq[envelope.task_id] = envelope

        # Also register in result store as DEAD_LETTER
        result = TaskResult(
            task_id=envelope.task_id,
            name=envelope.name,
            status=TaskStatus.DEAD_LETTER,
            error=error,
            traceback=traceback_str,
            retries_attempted=envelope.retries,
        )
        await self.store_result(result)

    async def get_queue_depth(self, queue: str = "default") -> int:
        q = self._get_queue(queue)
        return q.qsize()

    async def get_dlq_depth(self, queue: str = "default") -> int:
        dlq = self._get_dlq(queue)
        return len(dlq)

    async def get_dlq_tasks(self, queue: str = "default", limit: int = 50) -> list[TaskEnvelope]:
        dlq = self._get_dlq(queue)
        tasks = list(dlq.values())[:limit]
        return tasks

    async def requeue_dlq(self, task_id: str, queue: str = "default") -> bool:
        dlq = self._get_dlq(queue)
        envelope = dlq.pop(task_id, None)
        if envelope is None:
            return False

        # Reset retry counter and error to allow a fresh run
        envelope.retries = 0
        envelope.error = None
        envelope.traceback = None
        envelope.eta = None
        await self.enqueue(envelope, queue=queue)
        return True

    async def purge_dlq(self, queue: str = "default") -> int:
        dlq = self._get_dlq(queue)
        count = len(dlq)
        dlq.clear()
        return count

    async def store_result(self, result: TaskResult) -> None:
        self._results[result.task_id] = result
        if result.task_id in self._result_waiters:
            self._result_waiters[result.task_id].set()

    async def get_result(self, task_id: str, timeout: float | None = None) -> TaskResult | None:
        if task_id in self._results:
            return self._results[task_id]

        if timeout is None or timeout <= 0:
            return None

        event = self._result_waiters.setdefault(task_id, asyncio.Event())
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return self._results.get(task_id)
        except (TimeoutError, asyncio.TimeoutError):
            return None
        finally:
            self._result_waiters.pop(task_id, None)

    async def heartbeat(self, worker_id: str, metadata: dict[str, Any]) -> None:
        self._workers[worker_id] = (time.time(), metadata)

    async def get_active_workers(self) -> dict[str, dict[str, Any]]:
        now = time.time()
        active: dict[str, dict[str, Any]] = {}
        for worker_id, (ping_time, meta) in list(self._workers.items()):
            # Consider workers alive if heartbeat received within last 15 seconds
            if now - ping_time <= 15.0:
                active[worker_id] = {**meta, "last_seen_seconds_ago": round(now - ping_time, 2)}
            else:
                self._workers.pop(worker_id, None)
        return active
