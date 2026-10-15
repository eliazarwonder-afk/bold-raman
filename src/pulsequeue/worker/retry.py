from __future__ import annotations

import random


def calculate_backoff(
    attempt: int,
    base: float = 1.0,
    max_backoff: float = 60.0,
    jitter: bool = True,
) -> float:
    """Calculate exponential backoff delay with Full Jitter.

    Formula:
        ceiling = min(max_backoff, base * (2 ** attempt))
        delay = uniform(0, ceiling) if jitter else ceiling

    Reference:
        Marc Brooker (AWS Architecture Blog), "Exponential Backoff And Jitter"
        Full Jitter prevents 'thundering herd' spikes when recovering from
        transient network or database outages.
    """
    if attempt < 0:
        attempt = 0

    # Cap exponent to avoid overflow with large attempt numbers
    capped_attempt = min(attempt, 30)
    ceiling = min(max_backoff, base * (2.0**capped_attempt))

    if jitter:
        return random.uniform(0.0, ceiling)
    return ceiling


def decorrelated_jitter(
    previous_delay: float,
    base: float = 1.0,
    max_backoff: float = 60.0,
) -> float:
    """Calculate Decorrelated Jitter:

    sleep = min(max_backoff, uniform(base, previous_delay * 3))
    """
    return min(max_backoff, random.uniform(base, max(base, previous_delay * 3.0)))
