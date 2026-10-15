from __future__ import annotations

import asyncio
import os
import signal
import socket
import time
import traceback
import uuid
from typing import Any

from pulsequeue.broker.base import AbstractBroker
from pulsequeue.core.models import TaskEnvelope, TaskResult, TaskStatus
from pulsequeue.core.registry import TaskRegistry, default_registry
from pulsequeue.monitoring.metrics import MetricsCollector
from pulsequeue.worker.retry import calculate_backoff


class Worker:
    """Production-grade asynchronous task execution worker.

    Features:
    - Multi-queue prioritization and polling.
    - Semaphore-bounded async concurrency.
    - Graceful shutdown handling SIGINT/SIGTERM with drain period.
    - Automated exponential backoff with full jitter.
    - Dead-letter queue (DLQ) routing upon retries exhaustion.
    - Periodic heartbeat registration.
    """

    def __init__(
        self,
        broker: AbstractBroker,
        queues: list[str] | None = None,
        concurrency: int = 10,
        worker_id: str | None = None,
        registry: TaskRegistry | None = None,
        grace_period: float = 10.0,
    ) -> None:
        self.broker = broker
        self.queues = queues or ["default"]
        self.concurrency = max(1, concurrency)
        self.semaphore = asyncio.Semaphore(self.concurrency)
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.registry = registry or default_registry
        self.grace_period = grace_period

        self.metrics = MetricsCollector()
        self._stopping: bool = False
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._heartbeat_task: asyncio.Task[None] | None = None

    def _setup_signal_handlers(self) -> None:
        """Register OS signal listeners for graceful shutdown."""
        loop = asyncio.get_running_loop()

        def _sig_handler(signum: int, frame: Any) -> None:
            self._sync_signal_stop()

        def _async_sig_handler() -> None:
            asyncio.create_task(self.stop())

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                # Windows event loop doesn't always support add_signal_handler; fallback to signal.signal
                loop.add_signal_handler(sig, _async_sig_handler)
            except (NotImplementedError, AttributeError):
                signal.signal(sig, _sig_handler)

    def _sync_signal_stop(self) -> None:
        self._stopping = True

    async def start(self) -> None:
        """Connect to broker and start the worker execution loop."""
        await self.broker.connect()
        self._setup_signal_handlers()
        self._stopping = False
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        try:
            await self._run_loop()
        finally:
            await self._cleanup()

    async def stop(self) -> None:
        """Initiate graceful shutdown: stop fetching, await running tasks."""
        if self._stopping:
            return
        self._stopping = True

    async def _cleanup(self) -> None:
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Drain remaining running tasks within grace period
        if self._active_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._active_tasks, return_exceptions=True),
                    timeout=self.grace_period,
                )
            except (TimeoutError, asyncio.TimeoutError):
                for t in self._active_tasks:
                    t.cancel()

        await self.broker.close()

    async def _heartbeat_loop(self) -> None:
        while not self._stopping:
            try:
                meta = {
                    "queues": self.queues,
                    "concurrency": self.concurrency,
                    "metrics": self.metrics.snapshot(),
                }
                await self.broker.heartbeat(self.worker_id, meta)
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(2.0)

    async def _run_loop(self) -> None:
        """Main dispatcher polling configured queues."""
        while not self._stopping:
            # Check concurrency slot availability before fetching
            await self.semaphore.acquire()
            if self._stopping:
                self.semaphore.release()
                break

            envelope: TaskEnvelope | None = None
            for queue in self.queues:
                try:
                    envelope = await self.broker.dequeue(queue, self.worker_id, timeout=0.2)
                    if envelope:
                        break
                except Exception:
                    continue

            if envelope is None:
                self.semaphore.release()
                await asyncio.sleep(0.05)
                continue

            # Spawn task execution in background task tracking concurrency
            task = asyncio.create_task(self._execute_envelope(envelope))
            self._active_tasks.add(task)

            def _on_done(t: asyncio.Task[None]) -> None:
                self._active_tasks.discard(t)
                self.semaphore.release()

            task.add_done_callback(_on_done)

    async def _execute_envelope(self, envelope: TaskEnvelope) -> None:
        self.metrics.record_start()
        start_time = time.perf_counter()
        task_def = self.registry.get(envelope.name)

        if task_def is None:
            # Fatal: Unknown task handler registered
            err = f"Task '{envelope.name}' is not registered in worker TaskRegistry"
            self.metrics.record_dead_letter()
            await self.broker.dead_letter(envelope, error=err, traceback_str="")
            return

        try:
            # Execute with optional timeout
            res = await task_def.execute(*envelope.args, **envelope.kwargs)
            duration_ms = (time.perf_counter() - start_time) * 1000.0

            result = TaskResult(
                task_id=envelope.task_id,
                name=envelope.name,
                status=TaskStatus.SUCCESS,
                return_value=res,
                execution_time_ms=round(duration_ms, 2),
                retries_attempted=envelope.retries,
            )
            await self.broker.store_result(result)
            await self.broker.ack(envelope)
            self.metrics.record_success(duration_ms)

        except Exception as exc:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            tb_str = traceback.format_exc()

            if envelope.retries < envelope.max_retries:
                # Schedule retry with exponential backoff & jitter
                envelope.retries += 1
                delay = calculate_backoff(
                    attempt=envelope.retries,
                    base=envelope.backoff_base,
                    max_backoff=envelope.backoff_max,
                    jitter=envelope.backoff_jitter,
                )
                self.metrics.record_retry()
                await self.broker.nack(envelope, error=str(exc), retry_delay=delay)
            else:
                # Retries exhausted -> route to Dead Letter Queue (DLQ)
                self.metrics.record_dead_letter()
                await self.broker.dead_letter(envelope, error=str(exc), traceback_str=tb_str)

            self.metrics.record_failure(duration_ms)
