import asyncio

from rich import print as rprint

from pulsequeue import PulseQueue

# Initialize PulseQueue with in-memory broker (or "redis://localhost:6379/0")
app = PulseQueue(broker_url="memory://")


@app.task
async def send_welcome_email(user_id: int, email: str) -> dict[str, str]:
    rprint(f"[cyan][Task Started][/cyan] Sending welcome email to user #{user_id} ({email})...")
    await asyncio.sleep(0.5)  # Simulate network latency
    rprint(f"[green][Task Finished][/green] Email delivered to {email}")
    return {"status": "delivered", "user_id": str(user_id)}


@app.task
def compute_fibonacci(n: int) -> int:
    # Synchronous CPU-bound function executed in worker threadpool
    def _fib(x: int) -> int:
        if x <= 1:
            return x
        return _fib(x - 1) + _fib(x - 2)

    return _fib(n)


async def main() -> None:
    await app.connect()

    # Start a worker in the background
    worker = app.worker(concurrency=4)
    worker_task = asyncio.create_task(worker.start())

    rprint("\n[bold]1. Enqueueing tasks...[/bold]")
    # Asynchronously dispatch email task
    task_id1 = await send_welcome_email.delay(101, "ada.lovelace@example.com")
    rprint(f"  -> Dispatched email task: [dim]{task_id1}[/dim]")

    # Asynchronously dispatch fibonacci task
    task_id2 = await compute_fibonacci.delay(20)
    rprint(f"  -> Dispatched CPU task: [dim]{task_id2}[/dim]")

    rprint("\n[bold]2. Waiting for results...[/bold]")
    res1 = await app.get_result(task_id1, timeout=3.0)
    rprint(
        f"  [green][OK] Email Result:[/green] {res1.return_value if res1 else 'timeout'} (status: {res1.status.value if res1 else 'N/A'})"
    )

    res2 = await app.get_result(task_id2, timeout=3.0)
    rprint(
        f"  [green][OK] Fibonacci Result:[/green] {res2.return_value if res2 else 'timeout'} (status: {res2.status.value if res2 else 'N/A'})"
    )

    # Graceful shutdown
    rprint("\n[bold]3. Shutting down worker...[/bold]")
    await worker.stop()
    await worker_task
    await app.close()
    rprint("[bold green]Done![/bold green]")


if __name__ == "__main__":
    asyncio.run(main())
