from __future__ import annotations

import asyncio
import importlib

import typer
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from pulsequeue.app import PulseQueue
from pulsequeue.broker.base import AbstractBroker

app = typer.Typer(
    name="pulsequeue",
    help="⚡ PulseQueue: High-Performance Distributed Task Queue & Worker Engine",
    add_completion=False,
)
dlq_app = typer.Typer(name="dlq", help="Dead-Letter Queue management and inspection")
app.add_typer(dlq_app, name="dlq")

console = Console()


def _get_broker(broker_url: str) -> AbstractBroker:
    pq = PulseQueue(broker_url=broker_url)
    return pq.broker


@app.command()
def worker(
    broker: str = typer.Option("memory://", "--broker", "-b", help="Broker URL (memory:// or redis://...)"),
    queues: list[str] = typer.Option(["default"], "--queue", "-q", help="Queue names to process"),
    concurrency: int = typer.Option(10, "--concurrency", "-c", help="Concurrent task execution limit"),
    module: str | None = typer.Option(None, "--module", "-m", help="Python module to import tasks from"),
) -> None:
    """Start an async worker process listening on one or more queues."""
    if module:
        try:
            importlib.import_module(module)
            console.print(f"[green][OK][/green] Imported tasks module: [bold]{module}[/bold]")
        except Exception as e:
            console.print(f"[red]Error importing module {module}: {e}[/red]")
            raise typer.Exit(1) from e

    pq = PulseQueue(broker_url=broker)
    w = pq.worker(queues=queues, concurrency=concurrency)

    console.print(
        Panel.fit(
            f"[bold cyan]>> PulseQueue Worker Started[/bold cyan]\n"
            f"[dim]Worker ID:[/dim] {w.worker_id}\n"
            f"[dim]Broker:[/dim]    {broker}\n"
            f"[dim]Queues:[/dim]    {', '.join(queues)}\n"
            f"[dim]Concurrency:[/dim] {concurrency}",
            border_style="cyan",
        )
    )

    try:
        asyncio.run(w.start())
    except (KeyboardInterrupt, SystemExit):
        console.print("[yellow]Worker shutdown requested. Exiting cleanly...[/yellow]")


@app.command()
def top(
    broker: str = typer.Option("memory://", "--broker", "-b", help="Broker URL"),
    queue: str = typer.Option("default", "--queue", "-q", help="Queue name to inspect"),
    refresh_rate: float = typer.Option(1.0, "--refresh", "-r", help="Refresh interval in seconds"),
) -> None:
    """Real-time terminal dashboard of workers, queue depths, and DLQ status."""
    b = _get_broker(broker)

    async def _render_dashboard() -> None:
        await b.connect()
        try:
            with Live(console=console, refresh_per_second=int(1 / refresh_rate)) as live:
                while True:
                    q_depth = await b.get_queue_depth(queue)
                    dlq_depth = await b.get_dlq_depth(queue)
                    active_workers = await b.get_active_workers()

                    table = Table(title=f"PulseQueue Cluster Status [Queue: {queue}]", border_style="cyan")
                    table.add_column("Metric", style="bold white")
                    table.add_column("Value", style="green")

                    table.add_row("Broker URL", broker)
                    table.add_row("Ready Queue Depth", str(q_depth))
                    table.add_row(
                        "Dead Letter Queue (DLQ)",
                        f"[bold red]{dlq_depth}[/bold red]" if dlq_depth > 0 else "0",
                    )
                    table.add_row("Live Workers", str(len(active_workers)))

                    if active_workers:
                        w_table = Table(title="Active Worker Nodes", border_style="dim")
                        w_table.add_column("Worker ID", style="cyan")
                        w_table.add_column("Concurrency", justify="right")
                        w_table.add_column("Processed", justify="right")
                        w_table.add_column("Success", justify="right")
                        w_table.add_column("Failed", justify="right")
                        w_table.add_column("Last Seen", justify="right")

                        for w_id, meta in active_workers.items():
                            metrics = meta.get("metrics", {})
                            w_table.add_row(
                                w_id,
                                str(meta.get("concurrency", "-")),
                                str(metrics.get("processed_total", 0)),
                                str(metrics.get("succeeded_total", 0)),
                                str(metrics.get("failed_total", 0)),
                                f"{meta.get('last_seen_seconds_ago', 0)}s ago",
                            )
                        live.update(Panel.fit(table))
                    else:
                        live.update(table)

                    await asyncio.sleep(refresh_rate)
        finally:
            await b.close()

    try:
        asyncio.run(_render_dashboard())
    except (KeyboardInterrupt, SystemExit):
        console.print("\n[dim]Dashboard closed.[/dim]")


@dlq_app.command("list")
def dlq_list(
    broker: str = typer.Option("memory://", "--broker", "-b", help="Broker URL"),
    queue: str = typer.Option("default", "--queue", "-q", help="Queue name"),
    limit: int = typer.Option(20, "--limit", "-l", help="Max tasks to show"),
) -> None:
    """Display tasks quarantined inside the Dead Letter Queue."""
    b = _get_broker(broker)

    async def _fetch() -> None:
        await b.connect()
        try:
            tasks = await b.get_dlq_tasks(queue=queue, limit=limit)
            if not tasks:
                console.print(f"[green][OK] Dead letter queue for '{queue}' is empty. No failed tasks![/green]")
                return

            table = Table(
                title=f"[DLQ] Dead Letter Queue for '{queue}' (Total: {len(tasks)})",
                border_style="red",
            )
            table.add_column("Task ID", style="dim")
            table.add_column("Task Name", style="bold yellow")
            table.add_column("Retries", justify="right")
            table.add_column("Error Message", style="red")

            for t in tasks:
                table.add_row(t.task_id[:8] + "...", t.name, str(t.retries), (t.error or "")[:60])
            console.print(table)
        finally:
            await b.close()

    asyncio.run(_fetch())


@dlq_app.command("retry")
def dlq_retry(
    task_id: str = typer.Argument(..., help="ID of the dead-lettered task to requeue"),
    broker: str = typer.Option("memory://", "--broker", "-b", help="Broker URL"),
    queue: str = typer.Option("default", "--queue", "-q", help="Queue name"),
) -> None:
    """Requeue a quarantined task from the DLQ back to the active execution queue."""
    b = _get_broker(broker)

    async def _run() -> None:
        await b.connect()
        try:
            ok = await b.requeue_dlq(task_id, queue=queue)
            if ok:
                console.print(f"[green][OK] Task {task_id} successfully re-enqueued to '{queue}'![/green]")
            else:
                console.print(f"[red][FAIL] Task {task_id} not found in DLQ for queue '{queue}'[/red]")
        finally:
            await b.close()

    asyncio.run(_run())


@dlq_app.command("purge")
def dlq_purge(
    broker: str = typer.Option("memory://", "--broker", "-b", help="Broker URL"),
    queue: str = typer.Option("default", "--queue", "-q", help="Queue name"),
) -> None:
    """Purge all tasks from the Dead Letter Queue."""
    b = _get_broker(broker)

    async def _run() -> None:
        await b.connect()
        try:
            purged = await b.purge_dlq(queue=queue)
            console.print(f"[yellow][OK] Purged {purged} tasks from DLQ for queue '{queue}'[/yellow]")
        finally:
            await b.close()

    asyncio.run(_run())


@app.command()
def demo(
    broker: str = typer.Option("memory://", "--broker", "-b", help="Broker URL (memory:// or redis://...)"),
) -> None:
    """Run an end-to-end interactive demo showcasing async dispatch, retries, and results."""

    async def _demo_flow() -> None:
        console.print(Panel.fit("[bold cyan]>> PulseQueue Interactive Demo[/bold cyan]", border_style="cyan"))
        pq = PulseQueue(broker_url=broker)
        await pq.connect()

        # Define sample tasks
        @pq.task(max_retries=2, backoff_base=0.1)
        async def add(x: int, y: int) -> int:
            await asyncio.sleep(0.05)
            return x + y

        attempts = 0

        @pq.task(max_retries=2, backoff_base=0.1)
        async def flaky_api_call(service: str) -> str:
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                raise ConnectionError(f"Temporary outage contacting {service} (attempt {attempts})")
            return f"Connected to {service} on attempt {attempts}!"

        # Start background worker
        w = pq.worker(concurrency=5)
        worker_task = asyncio.create_task(w.start())

        console.print("[dim]1. Dispatching tasks...[/dim]")
        tid1 = await add.delay(25, 75)
        console.print(f"  -> Dispatched [bold]add(25, 75)[/bold] (Task ID: {tid1[:8]}...)")

        tid2 = await flaky_api_call.delay("StripePaymentGateway")
        console.print(f"  -> Dispatched [bold]flaky_api_call('StripePaymentGateway')[/bold] (Task ID: {tid2[:8]}...)")

        console.print("[dim]2. Awaiting results...[/dim]")
        res1 = await pq.get_result(tid1, timeout=3.0)
        if res1 and res1.status == "SUCCESS":
            console.print(f"  [green][OK] add Result:[/green] {res1.return_value} (took {res1.execution_time_ms}ms)")

        res2 = await pq.get_result(tid2, timeout=5.0)
        if res2 and res2.status == "SUCCESS":
            console.print(
                f"  [green][OK] flaky_api_call Result:[/green] {res2.return_value} (retried {res2.retries_attempted} times)"
            )

        await w.stop()
        await worker_task
        await pq.close()
        console.print("[bold green]Demo completed successfully![/bold green]")

    asyncio.run(_demo_flow())


if __name__ == "__main__":
    app()
