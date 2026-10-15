from __future__ import annotations

import asyncio

import pytest

from pulsequeue.app import PulseQueue
from pulsequeue.core.models import TaskStatus


@pytest.mark.asyncio
async def test_worker_successful_task_execution() -> None:
    app = PulseQueue(broker_url="memory://")
    await app.connect()

    executed = False

    @app.task
    async def process_order(order_id: int) -> dict[str, str]:
        nonlocal executed
        executed = True
        return {"order_id": str(order_id), "status": "shipped"}

    worker = app.worker(concurrency=2)
    worker_task = asyncio.create_task(worker.start())

    try:
        task_id = await process_order.delay(5001)
        res = await app.get_result(task_id, timeout=2.0)

        assert res is not None
        assert res.status == TaskStatus.SUCCESS
        assert res.return_value == {"order_id": "5001", "status": "shipped"}
        assert executed is True
    finally:
        await worker.stop()
        await worker_task
        await app.close()


@pytest.mark.asyncio
async def test_worker_retries_and_dlq() -> None:
    app = PulseQueue(broker_url="memory://")
    await app.connect()

    attempts = 0

    @app.task(max_retries=2, backoff_base=0.01)
    async def always_failing_task() -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError(f"Crash on attempt {attempts}")

    worker = app.worker(concurrency=2)
    worker_task = asyncio.create_task(worker.start())

    try:
        task_id = await always_failing_task.delay()

        # Wait until task completes all retries and lands in DLQ
        for _ in range(30):
            dlq_depth = await app.broker.get_dlq_depth("default")
            if dlq_depth == 1:
                break
            await asyncio.sleep(0.05)

        assert await app.broker.get_dlq_depth("default") == 1
        # Original attempt (retries=0) + 2 retries = 3 attempts total
        assert attempts == 3

        # Result store should reflect DEAD_LETTER status
        res = await app.get_result(task_id, timeout=1.0)
        assert res is not None
        assert res.status == TaskStatus.DEAD_LETTER
        assert "Crash on attempt 3" in (res.error or "")
    finally:
        await worker.stop()
        await worker_task
        await app.close()


@pytest.mark.asyncio
async def test_worker_concurrency_semaphore() -> None:
    app = PulseQueue(broker_url="memory://")
    await app.connect()

    in_flight = 0
    max_observed_concurrency = 0

    @app.task
    async def concurrent_job() -> None:
        nonlocal in_flight, max_observed_concurrency
        in_flight += 1
        max_observed_concurrency = max(max_observed_concurrency, in_flight)
        await asyncio.sleep(0.05)
        in_flight -= 1

    worker = app.worker(concurrency=3)
    worker_task = asyncio.create_task(worker.start())

    try:
        # Dispatch 6 tasks
        tids = [await concurrent_job.delay() for _ in range(6)]
        for tid in tids:
            await app.get_result(tid, timeout=3.0)

        assert max_observed_concurrency <= 3
    finally:
        await worker.stop()
        await worker_task
        await app.close()
