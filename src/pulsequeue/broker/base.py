from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pulsequeue.core.models import TaskEnvelope, TaskResult


class AbstractBroker(ABC):
    """Abstract interface defining the message broker transport contract."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connections and initialize broker resources."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Gracefully close connections."""
        pass

    @abstractmethod
    async def enqueue(self, envelope: TaskEnvelope, queue: str = "default") -> str:
        """Enqueue a task envelope. If envelope.eta is in the future, delay delivery."""
        pass

    @abstractmethod
    async def dequeue(self, queue: str, worker_id: str, timeout: float = 1.0) -> TaskEnvelope | None:
        """Pop the next due task envelope from the queue."""
        pass

    @abstractmethod
    async def ack(self, envelope: TaskEnvelope) -> None:
        """Acknowledge successful completion of a task, removing it from pending state."""
        pass

    @abstractmethod
    async def nack(self, envelope: TaskEnvelope, error: str, retry_delay: float | None = None) -> None:
        """Negative acknowledgement: schedule task for retry if delay given or re-enqueue."""
        pass

    @abstractmethod
    async def dead_letter(self, envelope: TaskEnvelope, error: str, traceback_str: str) -> None:
        """Move an unrecoverable/exhausted task to the Dead Letter Queue (DLQ)."""
        pass

    @abstractmethod
    async def get_queue_depth(self, queue: str = "default") -> int:
        """Get the count of ready-to-process tasks in the queue."""
        pass

    @abstractmethod
    async def get_dlq_depth(self, queue: str = "default") -> int:
        """Get the count of failed tasks in the dead letter queue."""
        pass

    @abstractmethod
    async def get_dlq_tasks(self, queue: str = "default", limit: int = 50) -> list[TaskEnvelope]:
        """Fetch tasks currently quarantined in the dead letter queue."""
        pass

    @abstractmethod
    async def requeue_dlq(self, task_id: str, queue: str = "default") -> bool:
        """Re-enqueue a task from the DLQ back to the active queue for execution."""
        pass

    @abstractmethod
    async def purge_dlq(self, queue: str = "default") -> int:
        """Delete all tasks currently in the DLQ."""
        pass

    @abstractmethod
    async def store_result(self, result: TaskResult) -> None:
        """Persist execution outcome."""
        pass

    @abstractmethod
    async def get_result(self, task_id: str, timeout: float | None = None) -> TaskResult | None:
        """Retrieve task execution result, optionally waiting up to timeout seconds."""
        pass

    @abstractmethod
    async def heartbeat(self, worker_id: str, metadata: dict[str, Any]) -> None:
        """Register worker liveness ping and metadata."""
        pass

    @abstractmethod
    async def get_active_workers(self) -> dict[str, dict[str, Any]]:
        """List currently live workers and their metadata."""
        pass
