from __future__ import annotations

import json
from typing import Any, cast

import msgpack  # type: ignore[import-untyped]

from pulsequeue.core.models import TaskEnvelope, TaskResult


class MsgpackSerializer:
    """Safe, high-throughput binary serialization using msgpack.

    Security note: Unlike Python's standard `pickle`, msgpack formats
    cannot execute arbitrary bytecode payloads upon deserialization.
    """

    @staticmethod
    def _default_encoder(obj: Any) -> Any:
        if isinstance(obj, (TaskEnvelope, TaskResult)):
            return obj.model_dump()
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        if hasattr(obj, "__str__"):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON/msgpack serializable")

    @classmethod
    def serialize_envelope(cls, envelope: TaskEnvelope) -> bytes:
        data = envelope.model_dump()
        return cast(bytes, msgpack.packb(data, default=cls._default_encoder, use_bin_type=True))

    @classmethod
    def deserialize_envelope(cls, payload: bytes | str) -> TaskEnvelope:
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        raw = msgpack.unpackb(payload, raw=False)
        return TaskEnvelope.model_validate(raw)

    @classmethod
    def serialize_result(cls, result: TaskResult) -> bytes:
        data = result.model_dump()
        return cast(bytes, msgpack.packb(data, default=cls._default_encoder, use_bin_type=True))

    @classmethod
    def deserialize_result(cls, payload: bytes | str) -> TaskResult:
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        raw = msgpack.unpackb(payload, raw=False)
        return TaskResult.model_validate(raw)

    @classmethod
    def to_json(cls, obj: TaskEnvelope | TaskResult | dict[str, Any]) -> str:
        if hasattr(obj, "model_dump_json"):
            return obj.model_dump_json(indent=2)
        return json.dumps(obj, indent=2, default=str)
