from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class TaskDefinition:
    """Metadata and callable reference for a registered task."""

    name: str
    func: Callable[..., Any]
    queue: str
    max_retries: int
    backoff_base: float
    backoff_max: float
    backoff_jitter: bool
    timeout_seconds: float | None
    is_async: bool

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the task safely whether it is sync or async."""
        if self.is_async:
            if self.timeout_seconds:
                return await asyncio.wait_for(self.func(*args, **kwargs), timeout=self.timeout_seconds)
            return await self.func(*args, **kwargs)
        else:
            # Run blocking/synchronous functions in the default thread pool to avoid blocking asyncio event loop
            loop = asyncio.get_running_loop()
            if self.timeout_seconds:
                return await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: self.func(*args, **kwargs)),
                    timeout=self.timeout_seconds,
                )
            return await loop.run_in_executor(None, lambda: self.func(*args, **kwargs))


class TaskRegistry:
    """Registry maintaining mapping between task names and their executable definitions."""

    def __init__(self) -> None:
        self._tasks: dict[str, TaskDefinition] = {}

    def register(
        self,
        func: Callable[..., Any],
        name: str | None = None,
        queue: str = "default",
        max_retries: int = 3,
        backoff_base: float = 1.0,
        backoff_max: float = 60.0,
        backoff_jitter: bool = True,
        timeout_seconds: float | None = None,
    ) -> TaskDefinition:
        task_name = name or f"{func.__module__}.{func.__qualname__}"
        if task_name in self._tasks:
            # Allow re-registration with updated definition (useful during testing/reloads)
            pass

        is_async = inspect.iscoroutinefunction(func)
        definition = TaskDefinition(
            name=task_name,
            func=func,
            queue=queue,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            backoff_jitter=backoff_jitter,
            timeout_seconds=timeout_seconds,
            is_async=is_async,
        )
        self._tasks[task_name] = definition
        return definition

    def get(self, name: str) -> TaskDefinition | None:
        return self._tasks.get(name)

    def list_tasks(self) -> list[str]:
        return list(self._tasks.keys())

    def clear(self) -> None:
        self._tasks.clear()


default_registry = TaskRegistry()
