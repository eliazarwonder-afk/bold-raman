from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Lifecycle states of a distributed task."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEAD_LETTER = "DEAD_LETTER"


class TaskEnvelope(BaseModel):
    """The wire-format envelope carrying task parameters, routing, and retry metadata."""

    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    queue: str = "default"
    retries: int = 0
    max_retries: int = 3
    backoff_base: float = 1.0
    backoff_max: float = 60.0
    backoff_jitter: bool = True
    timeout_seconds: float | None = None
    eta: float | None = None  # Unix timestamp (in seconds) for delayed / scheduled execution
    created_at: float = Field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    error: str | None = None
    traceback: str | None = None

    def is_due(self, now: float | None = None) -> bool:
        """Check if task is due for execution based on its ETA."""
        current = now if now is not None else time.time()
        return self.eta is None or self.eta <= current


class TaskResult(BaseModel):
    """Execution outcome stored for completed or failed tasks."""

    task_id: str
    name: str
    status: TaskStatus
    return_value: Any | None = None
    error: str | None = None
    traceback: str | None = None
    execution_time_ms: float = 0.0
    retries_attempted: int = 0
    completed_at: float = Field(default_factory=time.time)
