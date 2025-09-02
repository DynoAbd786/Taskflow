import asyncio
import time
from taskflow.core.engine import task

@task(name="add_numbers", queue="default")
async def add_numbers(a, b):
    return a + b

@task(name="multiply_numbers", queue="default") 
async def multiply_numbers(a, b):
    return a * b

@task(name="process_text", queue="data")
async def process_text(text):
    await asyncio.sleep(1)
    return text.upper()

@task(name="minimal_task", queue="default")
async def minimal_task():
    return "success"

@task(name="slow_task", queue="default")
async def slow_task(duration=10):
    for i in range(duration):
        await asyncio.sleep(1)
    return f"Completed slow task after {duration} seconds"

@task(name="cpu_intensive_task", queue="default")
async def cpu_intensive_task(iterations=1000000):
    start_time = time.time()
    total = 0
    for i in range(iterations):
        total += i * i
        if i % 100000 == 0:
            await asyncio.sleep(0)
    end_time = time.time()
    duration = end_time - start_time
    return {
        "total": total,
        "iterations": iterations, 
        "duration": duration,
        "rate": int(iterations / duration) if duration > 0 else 0,
    }
