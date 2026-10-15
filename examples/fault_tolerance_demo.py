"""Example: Fault tolerance, Exponential Backoff with Jitter, and Dead Letter Queue (DLQ)."""

import asyncio
from rich import print as rprint
from pulsequeue import PulseQueue

app = PulseQueue(broker_url="memory://")

attempts = 0


@app.task(max_retries=3, backoff_base=0.1, backoff_jitter=True)
async def process_payment(amount_cents: int, card_token: str) -> dict[str, str]:
    global attempts
    attempts += 1
    rprint(f"[yellow][Attempt {attempts}][/yellow] Charging card {card_token[:8]}... for ${amount_cents / 100:.2f}")

    if card_token.startswith("tok_decline"):
        # Permanent failure - will retry up to max_retries then land in DLQ
        raise ValueError(f"Card {card_token} was declined by issuer bank (Fraud suspicion)")

    return {"status": "paid", "amount": str(amount_cents)}


async def main() -> None:
    await app.connect()
    worker = app.worker(concurrency=2)
    worker_task = asyncio.create_task(worker.start())

    rprint("\n[bold cyan]1. Dispatching a doomed task (declining card)...[/bold cyan]")
    task_id = await process_payment.delay(19900, "tok_decline_fraud_9988")
    rprint(f"  -> Task dispatched: [dim]{task_id}[/dim]")

    rprint("\n[bold cyan]2. Observing retries and DLQ quarantine...[/bold cyan]")
    # Wait for all retries to exhaust
    for _ in range(25):
        dlq_depth = await app.broker.get_dlq_depth("default")
        if dlq_depth > 0:
            break
        await asyncio.sleep(0.1)

    dlq_tasks = await app.broker.get_dlq_tasks("default")
    rprint(f"  [red][DLQ] Tasks quarantined in Dead Letter Queue: {len(dlq_tasks)}[/red]")
    if dlq_tasks:
        failed = dlq_tasks[0]
        rprint(f"  [dim]Task ID:[/dim]  {failed.task_id}")
        rprint(f"  [dim]Retries:[/dim]  {failed.retries} attempts made")
        rprint(f"  [dim]Error:[/dim]    [red]{failed.error}[/red]")

    rprint("\n[bold cyan]3. Simulating operator recovery: Re-queueing task from DLQ...[/bold cyan]")
    success = await app.broker.requeue_dlq(task_id, queue="default")
    rprint(f"  [green][OK] Task {task_id[:8]}... requeued back into active queue: {success}[/green]")
    rprint(f"  -> Remaining in DLQ: {await app.broker.get_dlq_depth('default')}")
    rprint(f"  -> Ready in Queue:   {await app.broker.get_queue_depth('default')}")

    await worker.stop()
    await worker_task
    await app.close()
    rprint("\n[bold green]Fault tolerance flow verified![/bold green]")


if __name__ == "__main__":
    asyncio.run(main())
