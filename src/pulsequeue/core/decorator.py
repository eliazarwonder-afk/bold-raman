from __future__ import annotations

import functools
import time
from collections.abc import Callable
from typing import Any, TypeVar, overload

from pulsequeue.core.models import TaskEnvelope
from pulsequeue.core.registry import TaskDefinition, default_registry

F = TypeVar("F", bound=Callable[..., Any])

_active_broker: Any = None


def set_active_broker(broker: Any) -> None:
    """Set the process-wide active broker for task.delay() calls."""
    global _active_broker
    _active_broker = broker


def get_active_broker() -> Any:
    """Retrieve the current active broker."""
    return _active_broker


class TaskWrapper:
    """Wraps user functions to provide `.delay()` and `.delay_after()` asynchronous dispatch."""

    def __init__(self, definition: TaskDefinition) -> None:
        self.definition = definition
        functools.update_wrapper(self, definition.func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Calling directly runs the original function synchronously/asynchronously."""
        return self.definition.func(*args, **kwargs)

    def signature(self, *args: Any, **kwargs: Any) -> TaskEnvelope:
        """Create a TaskEnvelope representing this execution without dispatching it."""
        return TaskEnvelope(
            name=self.definition.name,
            args=list(args),
            kwargs=kwargs,
            queue=self.definition.queue,
            max_retries=self.definition.max_retries,
            backoff_base=self.definition.backoff_base,
            backoff_max=self.definition.backoff_max,
            backoff_jitter=self.definition.backoff_jitter,
            timeout_seconds=self.definition.timeout_seconds,
        )

    async def delay(self, *args: Any, **kwargs: Any) -> str:
        """Dispatch task asynchronously to the active broker. Returns task_id."""
        broker = get_active_broker()
        if broker is None:
            raise RuntimeError(
                f"No active broker configured. Initialize PulseQueue(broker=...) "
                f"or call set_active_broker() before calling {self.definition.name}.delay()"
            )
        envelope = self.signature(*args, **kwargs)
        await broker.enqueue(envelope, queue=self.definition.queue)
        return envelope.task_id

    async def delay_after(self, delay_seconds: float, *args: Any, **kwargs: Any) -> str:
        """Dispatch task with a delayed execution time (ETA). Returns task_id."""
        broker = get_active_broker()
        if broker is None:
            raise RuntimeError("No active broker configured.")
        envelope = self.signature(*args, **kwargs)
        envelope.eta = time.time() + max(0.0, delay_seconds)
        await broker.enqueue(envelope, queue=self.definition.queue)
        return envelope.task_id


@overload
def task(func: F) -> TaskWrapper: ...


@overload
def task(
    *,
    name: str | None = None,
    queue: str = "default",
    max_retries: int = 3,
    backoff_base: float = 1.0,
    backoff_max: float = 60.0,
    backoff_jitter: bool = True,
    timeout_seconds: float | None = None,
) -> Callable[[F], TaskWrapper]: ...


def task(
    func: F | None = None,
    *,
    name: str | None = None,
    queue: str = "default",
    max_retries: int = 3,
    backoff_base: float = 1.0,
    backoff_max: float = 60.0,
    backoff_jitter: bool = True,
    timeout_seconds: float | None = None,
) -> Any:
    """Decorator to register a function as a distributed background task."""

    def decorator(fn: F) -> TaskWrapper:
        definition = default_registry.register(
            func=fn,
            name=name,
            queue=queue,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            backoff_jitter=backoff_jitter,
            timeout_seconds=timeout_seconds,
        )
        return TaskWrapper(definition)

    if func is not None:
        return decorator(func)
    return decorator
