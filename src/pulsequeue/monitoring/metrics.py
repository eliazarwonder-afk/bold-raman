from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MetricsCollector:
    """Thread-safe and async-safe metrics collector for task execution."""

    tasks_processed_total: int = 0
    tasks_succeeded_total: int = 0
    tasks_failed_total: int = 0
    tasks_retried_total: int = 0
    tasks_dead_lettered_total: int = 0
    total_execution_time_ms: float = 0.0
    current_active_tasks: int = 0
    start_time: float = field(default_factory=time.time)

    def record_start(self) -> None:
        self.current_active_tasks += 1
        self.tasks_processed_total += 1

    def record_success(self, execution_ms: float) -> None:
        self.current_active_tasks = max(0, self.current_active_tasks - 1)
        self.tasks_succeeded_total += 1
        self.total_execution_time_ms += execution_ms

    def record_failure(self, execution_ms: float) -> None:
        self.current_active_tasks = max(0, self.current_active_tasks - 1)
        self.tasks_failed_total += 1
        self.total_execution_time_ms += execution_ms

    def record_retry(self) -> None:
        self.tasks_retried_total += 1

    def record_dead_letter(self) -> None:
        self.tasks_dead_lettered_total += 1

    @property
    def avg_execution_time_ms(self) -> float:
        completed = self.tasks_succeeded_total + self.tasks_failed_total
        if completed == 0:
            return 0.0
        return round(self.total_execution_time_ms / completed, 2)

    @property
    def throughput_per_second(self) -> float:
        uptime = max(0.001, time.time() - self.start_time)
        return round(self.tasks_processed_total / uptime, 2)

    def snapshot(self) -> dict[str, Any]:
        return {
            "uptime_seconds": round(time.time() - self.start_time, 1),
            "active_tasks": self.current_active_tasks,
            "processed_total": self.tasks_processed_total,
            "succeeded_total": self.tasks_succeeded_total,
            "failed_total": self.tasks_failed_total,
            "retried_total": self.tasks_retried_total,
            "dead_lettered_total": self.tasks_dead_lettered_total,
            "avg_execution_ms": self.avg_execution_time_ms,
            "tasks_per_sec": self.throughput_per_second,
        }

    def to_prometheus_format(self, worker_id: str = "default") -> str:
        """Export metrics formatted for Prometheus scrapers (/metrics endpoint)."""
        labels = f'worker_id="{worker_id}"'
        lines = [
            "# HELP pulsequeue_tasks_total Total count of tasks processed",
            "# TYPE pulsequeue_tasks_total counter",
            f'pulsequeue_tasks_total{{{labels},status="processed"}} {self.tasks_processed_total}',
            f'pulsequeue_tasks_total{{{labels},status="succeeded"}} {self.tasks_succeeded_total}',
            f'pulsequeue_tasks_total{{{labels},status="failed"}} {self.tasks_failed_total}',
            f'pulsequeue_tasks_total{{{labels},status="retried"}} {self.tasks_retried_total}',
            f'pulsequeue_tasks_total{{{labels},status="dead_letter"}} {self.tasks_dead_lettered_total}',
            "",
            "# HELP pulsequeue_active_tasks Current number of tasks actively executing",
            "# TYPE pulsequeue_active_tasks gauge",
            f"pulsequeue_active_tasks{{{labels}}} {self.current_active_tasks}",
            "",
            "# HELP pulsequeue_execution_time_ms_total Cumulative task execution time in milliseconds",
            "# TYPE pulsequeue_execution_time_ms_total counter",
            f"pulsequeue_execution_time_ms_total{{{labels}}} {self.total_execution_time_ms:.2f}",
        ]
        return "\n".join(lines) + "\n"
