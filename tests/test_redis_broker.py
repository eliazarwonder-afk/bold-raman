from __future__ import annotations

import fakeredis.aioredis as fake_aioredis
import pytest

from pulsequeue.broker.redis import RedisStreamsBroker
from pulsequeue.core.models import TaskEnvelope


@pytest.mark.asyncio
async def test_redis_streams_broker_with_fakeredis() -> None:
    fake_client = fake_aioredis.FakeRedis(decode_responses=False)
    broker = RedisStreamsBroker(redis_url="redis://localhost:6379/0")
    broker._redis = fake_client

    await broker.connect()

    try:
        env = TaskEnvelope(name="send.invoice", args=[882])
        tid = await broker.enqueue(env, queue="billing")
        assert tid == env.task_id

        depth = await broker.get_queue_depth("billing")
        assert depth == 1

        dequeued = await broker.dequeue("billing", worker_id="worker-test-1", timeout=0.5)
        assert dequeued is not None
        assert dequeued.task_id == env.task_id

        await broker.ack(dequeued)

        # Verify DLQ operations on Redis
        failed_env = TaskEnvelope(name="failing.card")
        await broker.enqueue(failed_env, queue="billing")
        f_dequeued = await broker.dequeue("billing", worker_id="worker-test-1", timeout=0.5)
        assert f_dequeued is not None

        await broker.dead_letter(f_dequeued, error="Card expired", traceback_str="Traceback...")
        assert await broker.get_dlq_depth("billing") == 1

        dlq_tasks = await broker.get_dlq_tasks("billing")
        assert len(dlq_tasks) == 1
        assert dlq_tasks[0].task_id == failed_env.task_id
        assert dlq_tasks[0].error == "Card expired"

        # Requeue from DLQ
        requeued = await broker.requeue_dlq(failed_env.task_id, queue="billing")
        assert requeued is True
        assert await broker.get_dlq_depth("billing") == 0

        # Purge
        await broker.purge_dlq("billing")
        assert await broker.get_dlq_depth("billing") == 0
    finally:
        await broker.close()
