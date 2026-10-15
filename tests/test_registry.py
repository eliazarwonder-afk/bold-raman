from __future__ import annotations

import asyncio

import pytest

from pulsequeue.core.registry import TaskRegistry


@pytest.mark.asyncio
async def test_registry_sync_function() -> None:
    registry = TaskRegistry()

    def sync_multiply(a: int, b: int) -> int:
        return a * b

    defn = registry.register(sync_multiply, name="math.multiply")
    assert defn.name == "math.multiply"
    assert defn.is_async is False

    res = await defn.execute(4, 5)
    assert res == 20


@pytest.mark.asyncio
async def test_registry_async_function() -> None:
    registry = TaskRegistry()

    async def async_greet(name: str) -> str:
        await asyncio.sleep(0.01)
        return f"Hello, {name}!"

    defn = registry.register(async_greet, name="greeter")
    assert defn.is_async is True

    res = await defn.execute("Ada")
    assert res == "Hello, Ada!"


@pytest.mark.asyncio
async def test_registry_timeout() -> None:
    registry = TaskRegistry()

    async def slow_task() -> str:
        await asyncio.sleep(0.2)
        return "done"

    defn = registry.register(slow_task, name="slow", timeout_seconds=0.05)
    with pytest.raises((TimeoutError, asyncio.TimeoutError)):
        await defn.execute()
