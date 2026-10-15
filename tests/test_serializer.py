from __future__ import annotations

import time
import uuid

from pulsequeue.core.models import TaskEnvelope, TaskResult, TaskStatus
from pulsequeue.core.serializer import MsgpackSerializer


def test_task_envelope_serialization_roundtrip() -> None:
    env = TaskEnvelope(
        task_id=str(uuid.uuid4()),
        name="test.email.send",
        args=[101, "welcome"],
        kwargs={"subject": "Hello World", "tags": ["onboarding", "vip"]},
        queue="high-priority",
        max_retries=5,
        backoff_base=2.0,
        timeout_seconds=10.0,
        eta=time.time() + 60.0,
    )

    payload = MsgpackSerializer.serialize_envelope(env)
    assert isinstance(payload, bytes)
    assert len(payload) > 0

    restored = MsgpackSerializer.deserialize_envelope(payload)
    assert restored.task_id == env.task_id
    assert restored.name == "test.email.send"
    assert restored.args == [101, "welcome"]
    assert restored.kwargs["subject"] == "Hello World"
    assert restored.queue == "high-priority"
    assert restored.max_retries == 5
    assert restored.timeout_seconds == 10.0
    assert restored.eta == env.eta


def test_task_result_serialization_roundtrip() -> None:
    result = TaskResult(
        task_id="abc-123",
        name="compute.stats",
        status=TaskStatus.SUCCESS,
        return_value={"count": 42, "status": "ok"},
        execution_time_ms=12.45,
        retries_attempted=1,
    )

    payload = MsgpackSerializer.serialize_result(result)
    restored = MsgpackSerializer.deserialize_result(payload)

    assert restored.task_id == "abc-123"
    assert restored.name == "compute.stats"
    assert restored.status == TaskStatus.SUCCESS
    assert restored.return_value == {"count": 42, "status": "ok"}
    assert restored.execution_time_ms == 12.45
    assert restored.retries_attempted == 1


def test_json_dump_utility() -> None:
    env = TaskEnvelope(name="demo")
    json_str = MsgpackSerializer.to_json(env)
    assert '"name": "demo"' in json_str
