from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from pulsequeue.broker.base import AbstractBroker
from pulsequeue.broker.memory import MemoryBroker
from pulsequeue.broker.redis import RedisStreamsBroker
from pulsequeue.core.decorator import TaskWrapper, set_active_broker, task
from pulsequeue.core.models import TaskEnvelope, TaskResult
from pulsequeue.core.registry import TaskRegistry, default_registry
from pulsequeue.worker.worker import Worker

F = TypeVar("F", bound=Callable[..., Any])


class PulseQueue:
    """The central application coordinator for PulseQueue producers and workers.

    Example:
        app = PulseQueue("redis://localhost:6379/0")

        @app.task(max_retries=3)
        async def send_welcome_email(user_id: int):
            ...

        # Dispatch
        await send_welcome_email.delay(42)
    """

    def __init__(
        self,
        broker_url: str = "memory://",
        default_queue: str = "default",
        registry: TaskRegistry | None = None,
    ) -> None:
        self.broker_url = broker_url
        self.default_queue = default_queue
        self.registry = registry or default_registry

        if broker_url.startswith("redis://") or broker_url.startswith("rediss://"):
            self.broker: AbstractBroker = RedisStreamsBroker(broker_url)
        elif broker_url.startswith("memory://") or broker_url == "memory":
            self.broker = MemoryBroker()
        else:
            raise ValueError(
                f"Unsupported broker URL scheme: '{broker_url}'. Supported schemes: 'memory://' or 'redis://...'"
            )

        # Set as active broker for .delay() invocations
        set_active_broker(self.broker)

    async def connect(self) -> None:
        """Connect the underlying message broker."""
        await self.broker.connect()

    async def close(self) -> None:
        """Close the underlying message broker."""
        await self.broker.close()

    @overload
    def task(self, func: F) -> TaskWrapper: ...

    @overload
    def task(
        self,
        *,
        name: str | None = None,
        queue: str | None = None,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        backoff_max: float = 60.0,
        backoff_jitter: bool = True,
        timeout_seconds: float | None = None,
    ) -> Callable[[F], TaskWrapper]: ...

    def task(
        self,
        func: F | None = None,
        *,
        name: str | None = None,
        queue: str | None = None,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        backoff_max: float = 60.0,
        backoff_jitter: bool = True,
        timeout_seconds: float | None = None,
    ) -> Any:
        """Decorator to register a function directly with this PulseQueue instance."""
        target_queue = queue or self.default_queue
        dec = task(
            name=name,
            queue=target_queue,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            backoff_jitter=backoff_jitter,
            timeout_seconds=timeout_seconds,
        )
        if func is not None:
            return dec(func)
        return dec

    async def send_task(
        self,
        name: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        delay_seconds: float | None = None,
    ) -> str:
        """Send a task by name dynamically without needing direct function import."""
        envelope = TaskEnvelope(
            name=name,
            args=args or [],
            kwargs=kwargs or {},
            queue=queue or self.default_queue,
        )
        if delay_seconds is not None:
            import time

            envelope.eta = time.time() + max(0.0, delay_seconds)

        await self.broker.enqueue(envelope, queue=envelope.queue)
        return envelope.task_id

    async def get_result(self, task_id: str, timeout: float | None = None) -> TaskResult | None:
        """Fetch result for a given task ID."""
        return await self.broker.get_result(task_id, timeout=timeout)

    def worker(
        self,
        queues: list[str] | None = None,
        concurrency: int = 10,
        worker_id: str | None = None,
        grace_period: float = 10.0,
    ) -> Worker:
        """Create a Worker instance preconfigured with this application's broker and registry."""
        return Worker(
            broker=self.broker,
            queues=queues or [self.default_queue],
            concurrency=concurrency,
            worker_id=worker_id,
            registry=self.registry,
            grace_period=grace_period,
        )
