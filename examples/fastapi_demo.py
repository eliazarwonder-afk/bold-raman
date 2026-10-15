"""Example: Integrating PulseQueue into FastAPI web services for asynchronous background processing."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from pulsequeue import PulseQueue

# Initialize PulseQueue application
pq = PulseQueue(broker_url="memory://")


@pq.task(max_retries=3, timeout_seconds=30.0)
async def generate_pdf_report(report_id: str, client_id: str) -> dict[str, str]:
    import asyncio

    # Simulate heavy PDF generation / rasterization
    await asyncio.sleep(1.0)
    return {"report_id": report_id, "url": f"https://s3.amazonaws.com/reports/{report_id}.pdf"}


# In a real FastAPI application, you can attach PulseQueue lifecycle to FastAPI's lifespan:
@asynccontextmanager
async def lifespan(app: Any) -> AsyncGenerator[None, None]:
    # Startup: connect broker & spawn worker pool
    await pq.connect()
    worker = pq.worker(concurrency=8)
    worker_task = asyncio.create_task(worker.start())
    yield
    # Shutdown: gracefully drain workers
    await worker.stop()
    await worker_task
    await pq.close()


# Example FastAPI endpoints (pseudo-code):
#
# @app.post("/reports")
# async def create_report(client_id: str):
#     report_id = f"rep_{uuid.uuid4().hex[:8]}"
#     task_id = await generate_pdf_report.delay(report_id, client_id)
#     return {"status": "queued", "task_id": task_id, "report_id": report_id}
#
# @app.get("/reports/status/{task_id}")
# async def get_report_status(task_id: str):
#     result = await pq.get_result(task_id)
#     if not result:
#         return {"status": "IN_PROGRESS"}
#     return {"status": result.status.value, "result": result.return_value}


if __name__ == "__main__":
    import asyncio

    async def _test_flow() -> None:
        async with lifespan(None):
            print("Dispatching background report generation...")
            task_id = await generate_pdf_report.delay("rep_9918", "client_oracle")
            print(f"Task dispatched with ID: {task_id}")
            res = await pq.get_result(task_id, timeout=3.0)
            print(f"Task completed: {res.return_value if res else 'N/A'}")

    asyncio.run(_test_flow())
