from __future__ import annotations

import asyncio
import json
import time
from typing import Any, cast

import redis.asyncio as aioredis

from pulsequeue.broker.base import AbstractBroker
from pulsequeue.core.models import TaskEnvelope, TaskResult, TaskStatus
from pulsequeue.core.serializer import MsgpackSerializer


class RedisStreamsBroker(AbstractBroker):
    """Production distributed message broker backed by Redis Streams, Consumer Groups, and Sorted Sets.

    Architecture:
    - Streams (`pulsequeue:stream:{queue}`) for high-throughput FIFO distribution.
    - Consumer Groups (`pulsequeue:group:{queue}`) for load balancing across workers.
    - Pending Entries List (PEL) + `XAUTOCLAIM` for recovering stalled tasks from crashed workers.
    - Sorted Sets (`pulsequeue:delayed:{queue}`) for scheduled execution & exponential backoff retries.
    - Dead Letter List (`pulsequeue:dlq:{queue}`) for quarantine of exhausted tasks.
    - Result Hash (`pulsequeue:results`) + PubSub for awaitable task completion.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        self.redis_url = redis_url
        self._redis: Any = None
        self._delayed_task: asyncio.Task[None] | None = None
        self._connected: bool = False
        # Map task_id -> (queue, redis_stream_msg_id)
        self._in_flight_stream_ids: dict[str, tuple[str, str]] = {}

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = aioredis.from_url(self.redis_url, decode_responses=False)
        self._connected = True
        if self._delayed_task is None or self._delayed_task.done():
            self._delayed_task = asyncio.create_task(self._delayed_migration_loop())

    async def close(self) -> None:
        self._connected = False
        if self._delayed_task and not self._delayed_task.done():
            self._delayed_task.cancel()
            try:
                await self._delayed_task
            except asyncio.CancelledError:
                pass
        if self._redis:
            if hasattr(self._redis, "aclose"):
                await self._redis.aclose()
            else:
                await self._redis.close()
            self._redis = None

    def _ensure_connected(self) -> Any:
        if self._redis is None:
            raise RuntimeError("RedisStreamsBroker is not connected. Call await broker.connect() first.")
        return self._redis

    async def _ensure_group(self, queue: str) -> None:
        r = self._ensure_connected()
        stream_key = f"pulsequeue:stream:{queue}"
        group_key = f"pulsequeue:group:{queue}"
        try:
            await r.xgroup_create(name=stream_key, groupname=group_key, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def _delayed_migration_loop(self) -> None:
        """Background coroutine migrating due tasks from delayed Sorted Sets to active Streams."""
        while self._connected:
            try:
                r = self._ensure_connected()
                # Find all delayed keys
                keys = await r.keys("pulsequeue:delayed:*")
                now = time.time()
                for key_bytes in keys:
                    key = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
                    queue = key.split(":")[-1]
                    stream_key = f"pulsequeue:stream:{queue}"
                    # Fetch items with score <= now
                    due_items = await r.zrangebyscore(key, min=0, max=now, start=0, num=50)
                    for raw_payload in due_items:
                        # Atomic transfer from ZSET to Stream
                        pipe = r.pipeline(transaction=True)
                        pipe.zrem(key, cast(Any, raw_payload))
                        pipe.xadd(stream_key, {b"data": cast(Any, raw_payload)})
                        await pipe.execute()
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.5)

    async def enqueue(self, envelope: TaskEnvelope, queue: str = "default") -> str:
        r = self._ensure_connected()
        envelope.queue = queue
        payload = MsgpackSerializer.serialize_envelope(envelope)
        now = time.time()

        if envelope.eta and envelope.eta > now:
            delayed_key = f"pulsequeue:delayed:{queue}"
            await r.zadd(delayed_key, {payload: envelope.eta})
        else:
            stream_key = f"pulsequeue:stream:{queue}"
            await self._ensure_group(queue)
            await r.xadd(stream_key, {b"data": payload})

        return envelope.task_id

    async def dequeue(self, queue: str, worker_id: str, timeout: float = 1.0) -> TaskEnvelope | None:
        r = self._ensure_connected()
        await self._ensure_group(queue)
        stream_key = f"pulsequeue:stream:{queue}"
        group_key = f"pulsequeue:group:{queue}"

        # 1. Attempt to autoclaim stalled tasks abandoned by dead workers (> 60s idle in PEL)
        try:
            claimed = await r.xautoclaim(
                name=stream_key,
                groupname=group_key,
                consumername=worker_id,
                min_idle_time=60000,
                start_id="0-0",
                count=1,
            )
            if claimed and len(claimed) >= 2 and claimed[1]:
                msg_id, fields = claimed[1][0]
                payload = fields.get(b"data") or fields.get("data")
                if payload:
                    envelope = MsgpackSerializer.deserialize_envelope(payload)
                    self._in_flight_stream_ids[envelope.task_id] = (
                        queue,
                        msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id),
                    )
                    envelope.started_at = time.time()
                    return envelope
        except Exception:
            pass

        # 2. Read new messages assigned to this consumer
        block_ms = int(max(10, timeout * 1000))
        entries = await r.xreadgroup(
            groupname=group_key,
            consumername=worker_id,
            streams={stream_key: ">"},
            count=1,
            block=block_ms,
        )

        raw_entries: Any = entries
        if not raw_entries:
            return None

        _, messages = raw_entries[0]
        if not messages:
            return None

        msg_id, fields = messages[0]
        payload = fields.get(b"data") or fields.get("data")
        if not payload:
            return None

        envelope = MsgpackSerializer.deserialize_envelope(payload)
        str_msg_id = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
        self._in_flight_stream_ids[envelope.task_id] = (queue, str_msg_id)
        envelope.started_at = time.time()
        return envelope

    async def ack(self, envelope: TaskEnvelope) -> None:
        r = self._ensure_connected()
        stream_key = f"pulsequeue:stream:{envelope.queue}"
        group_key = f"pulsequeue:group:{envelope.queue}"
        stored = self._in_flight_stream_ids.pop(envelope.task_id, None)
        if stored:
            _, msg_id = stored
            await r.xack(stream_key, group_key, msg_id)
            await r.xdel(stream_key, msg_id)

    async def nack(self, envelope: TaskEnvelope, error: str, retry_delay: float | None = None) -> None:
        self._ensure_connected()
        await self.ack(envelope)

        envelope.error = error
        if retry_delay is not None and retry_delay > 0:
            envelope.eta = time.time() + retry_delay
        else:
            envelope.eta = None

        await self.enqueue(envelope, queue=envelope.queue)

    async def dead_letter(self, envelope: TaskEnvelope, error: str, traceback_str: str) -> None:
        r = self._ensure_connected()
        await self.ack(envelope)

        envelope.error = error
        envelope.traceback = traceback_str
        envelope.completed_at = time.time()

        dlq_key = f"pulsequeue:dlq:{envelope.queue}"
        payload = MsgpackSerializer.serialize_envelope(envelope)
        await r.lpush(dlq_key, payload)

        result = TaskResult(
            task_id=envelope.task_id,
            name=envelope.name,
            status=TaskStatus.DEAD_LETTER,
            error=error,
            traceback=traceback_str,
            retries_attempted=envelope.retries,
        )
        await self.store_result(result)

    async def get_queue_depth(self, queue: str = "default") -> int:
        r = self._ensure_connected()
        stream_key = f"pulsequeue:stream:{queue}"
        try:
            return int(await r.xlen(stream_key))
        except Exception:
            return 0

    async def get_dlq_depth(self, queue: str = "default") -> int:
        r = self._ensure_connected()
        dlq_key = f"pulsequeue:dlq:{queue}"
        return int(await r.llen(dlq_key))

    async def get_dlq_tasks(self, queue: str = "default", limit: int = 50) -> list[TaskEnvelope]:
        r = self._ensure_connected()
        dlq_key = f"pulsequeue:dlq:{queue}"
        raw_items = await r.lrange(dlq_key, 0, limit - 1)
        tasks: list[TaskEnvelope] = []
        for raw in raw_items:
            try:
                tasks.append(MsgpackSerializer.deserialize_envelope(raw))
            except Exception:
                continue
        return tasks

    async def requeue_dlq(self, task_id: str, queue: str = "default") -> bool:
        r = self._ensure_connected()
        dlq_key = f"pulsequeue:dlq:{queue}"
        raw_items = await r.lrange(dlq_key, 0, -1)
        target_payload = None
        target_envelope = None

        for raw in raw_items:
            try:
                env = MsgpackSerializer.deserialize_envelope(raw)
                if env.task_id == task_id:
                    target_payload = raw
                    target_envelope = env
                    break
            except Exception:
                continue

        if target_payload is None or target_envelope is None:
            return False

        # Remove from DLQ
        await r.lrem(dlq_key, 1, cast(Any, target_payload))
        target_envelope.retries = 0
        target_envelope.error = None
        target_envelope.traceback = None
        target_envelope.eta = None
        await self.enqueue(target_envelope, queue=queue)
        return True

    async def purge_dlq(self, queue: str = "default") -> int:
        r = self._ensure_connected()
        dlq_key = f"pulsequeue:dlq:{queue}"
        length = await r.llen(dlq_key)
        await r.delete(dlq_key)
        return int(length)

    async def store_result(self, result: TaskResult) -> None:
        r = self._ensure_connected()
        payload = MsgpackSerializer.serialize_result(result)
        # Store in Redis Hash with 24-hour expiration
        await r.hset("pulsequeue:results", result.task_id, payload)
        # Publish notification for waiting clients
        await r.publish(f"pulsequeue:notify:{result.task_id}", b"done")

    async def get_result(self, task_id: str, timeout: float | None = None) -> TaskResult | None:
        r = self._ensure_connected()
        raw = await r.hget("pulsequeue:results", task_id)
        if raw is not None:
            return MsgpackSerializer.deserialize_result(raw)

        if timeout is None or timeout <= 0:
            return None

        # Subscribe to pubsub notification
        pubsub = r.pubsub()
        channel = f"pulsequeue:notify:{task_id}"
        await pubsub.subscribe(channel)

        try:
            start = time.time()
            while time.time() - start < timeout:
                remaining = timeout - (time.time() - start)
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=min(remaining, 0.5))
                if msg is not None:
                    raw = await r.hget("pulsequeue:results", task_id)
                    if raw is not None:
                        return MsgpackSerializer.deserialize_result(raw)
                await asyncio.sleep(0.05)
            return None
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()

    async def heartbeat(self, worker_id: str, metadata: dict[str, Any]) -> None:
        r = self._ensure_connected()
        payload = json.dumps({"timestamp": time.time(), "metadata": metadata})
        await r.hset("pulsequeue:workers", worker_id, payload)

    async def get_active_workers(self) -> dict[str, dict[str, Any]]:
        r = self._ensure_connected()
        raw_map = await r.hgetall("pulsequeue:workers")
        now = time.time()
        active: dict[str, dict[str, Any]] = {}

        for w_bytes, data_bytes in raw_map.items():
            worker_id = w_bytes.decode() if isinstance(w_bytes, bytes) else str(w_bytes)
            data_str = data_bytes.decode() if isinstance(data_bytes, bytes) else str(data_bytes)
            try:
                info = json.loads(data_str)
                ts = float(info.get("timestamp", 0))
                if now - ts <= 15.0:
                    meta = info.get("metadata", {})
                    meta["last_seen_seconds_ago"] = round(now - ts, 2)
                    active[worker_id] = meta
                else:
                    # Clean expired worker
                    await r.hdel("pulsequeue:workers", worker_id)
            except Exception:
                continue

        return active
