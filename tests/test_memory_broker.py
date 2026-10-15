from __future__ import annotations

import asyncio
import time

import pytest

from pulsequeue.broker.memory import MemoryBroker
from pulsequeue.core.models import TaskEnvelope, TaskResult, TaskStatus


@pytest.mark.asyncio
async def test_memory_broker_enqueue_dequeue_ack() -> None:
    broker = MemoryBroker()
    await broker.connect()

    try:
        env = TaskEnvelope(name="send.notification", args=["user_123"])
        tid = await broker.enqueue(env, queue="default")
        assert tid == env.task_id

        depth = await broker.get_queue_depth("default")
        assert depth == 1

        dequeued = await broker.dequeue("default", worker_id="worker-1", timeout=0.5)
        assert dequeued is not None
        assert dequeued.task_id == env.task_id
        assert dequeued.name == "send.notification"

        # Ack task
        await broker.ack(dequeued)
        depth_after = await broker.get_queue_depth("default")
        assert depth_after == 0
    finally:
        await broker.close()


@pytest.mark.asyncio
async def test_memory_broker_delayed_tasks() -> None:
    broker = MemoryBroker()
    await broker.connect()

    try:
        # Schedule task for 0.15s in the future
        env = TaskEnvelope(name="delayed.task", eta=time.time() + 0.15)
        await broker.enqueue(env, queue="default")

        # Immediate dequeue should return None because it's not due yet
        immediate = await broker.dequeue("default", worker_id="w-1", timeout=0.05)
        assert immediate is None

        # Wait until due
        await asyncio.sleep(0.18)
        due = await broker.dequeue("default", worker_id="w-1", timeout=0.2)
        assert due is not None
        assert due.task_id == env.task_id
        await broker.ack(due)
    finally:
        await broker.close()


@pytest.mark.asyncio
async def test_memory_broker_dlq_workflow() -> None:
    broker = MemoryBroker()
    await broker.connect()

    try:
        env = TaskEnvelope(name="failing.job")
        await broker.enqueue(env, queue="default")

        dequeued = await broker.dequeue("default", worker_id="w-1")
        assert dequeued is not None

        # Move to Dead Letter Queue
        await broker.dead_letter(dequeued, error="Database unreachable", traceback_str="Traceback...")

        dlq_depth = await broker.get_dlq_depth("default")
        assert dlq_depth == 1

        dlq_tasks = await broker.get_dlq_tasks("default")
        assert len(dlq_tasks) == 1
        assert dlq_tasks[0].task_id == env.task_id
        assert dlq_tasks[0].error == "Database unreachable"

        # Requeue from DLQ back to active queue
        requeued = await broker.requeue_dlq(env.task_id, queue="default")
        assert requeued is True
        assert await broker.get_dlq_depth("default") == 0
        assert await broker.get_queue_depth("default") == 1

        # Dequeue again
        re_dequeued = await broker.dequeue("default", worker_id="w-1")
        assert re_dequeued is not None
        assert re_dequeued.task_id == env.task_id
        await broker.ack(re_dequeued)
    finally:
        await broker.close()


@pytest.mark.asyncio
async def test_memory_broker_results_and_heartbeats() -> None:
    broker = MemoryBroker()
    await broker.connect()

    try:
        # Test heartbeat
        await broker.heartbeat("node-alpha", {"concurrency": 8, "queues": ["default"]})
        workers = await broker.get_active_workers()
        assert "node-alpha" in workers
        assert workers["node-alpha"]["concurrency"] == 8

        # Test results with async wait
        async def _produce_result_later() -> None:
            await asyncio.sleep(0.05)
            res = TaskResult(
                task_id="task-999",
                name="async.calc",
                status=TaskStatus.SUCCESS,
                return_value=12345,
            )
            await broker.store_result(res)

        asyncio.create_task(_produce_result_later())
        result = await broker.get_result("task-999", timeout=1.0)
        assert result is not None
        assert result.status == TaskStatus.SUCCESS
        assert result.return_value == 12345
    finally:
        await broker.close()
