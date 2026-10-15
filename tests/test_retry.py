from __future__ import annotations

from pulsequeue.worker.retry import calculate_backoff, decorrelated_jitter


def test_calculate_backoff_no_jitter() -> None:
    # Deterministic check with jitter disabled
    assert calculate_backoff(0, base=1.0, max_backoff=60.0, jitter=False) == 1.0
    assert calculate_backoff(1, base=1.0, max_backoff=60.0, jitter=False) == 2.0
    assert calculate_backoff(2, base=1.0, max_backoff=60.0, jitter=False) == 4.0
    assert calculate_backoff(3, base=1.0, max_backoff=60.0, jitter=False) == 8.0


def test_calculate_backoff_with_jitter() -> None:
    for attempt in range(5):
        val = calculate_backoff(attempt, base=1.0, max_backoff=60.0, jitter=True)
        max_possible = min(60.0, 1.0 * (2.0**attempt))
        assert 0.0 <= val <= max_possible


def test_calculate_backoff_capped_at_max() -> None:
    # Attempt 20 should cap at max_backoff (e.g. 30.0)
    val = calculate_backoff(20, base=1.0, max_backoff=30.0, jitter=False)
    assert val == 30.0


def test_decorrelated_jitter() -> None:
    val = decorrelated_jitter(previous_delay=2.0, base=1.0, max_backoff=30.0)
    assert 1.0 <= val <= 30.0
