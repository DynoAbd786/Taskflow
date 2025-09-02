#!/usr/bin/env python3
"""
Basic usage examples for TaskFlow.
Demonstrates task creation, submission, and monitoring.
"""

import asyncio
import time
from datetime import datetime

from taskflow.core.engine import TaskEngine, task
from taskflow.core.task import TaskPriority


# Define some example tasks
@task(name="add_numbers", queue="math", timeout=30)
def add_numbers(a: int, b: int) -> int:
    """Add two numbers together."""
    print(f"Adding {a} + {b}")
    time.sleep(1)  # Simulate work
    return a + b


@task(name="process_data", queue="data", priority=TaskPriority.HIGH)
async def process_data(data: dict) -> dict:
    """Process data asynchronously."""
    print(f"Processing data: {data}")
    await asyncio.sleep(2)  # Simulate async work

    result = {
        "processed_at": datetime.now().isoformat(),
        "input_size": len(str(data)),
        "status": "completed",
    }

    return result


@task(name="send_email", queue="notifications")
def send_email(to: str, subject: str, body: str) -> bool:
    """Send an email (simulated)."""
    print(f"Sending email to {to}: {subject}")
    time.sleep(0.5)  # Simulate email sending
    return True


@task(name="failing_task", max_retries=2)
def failing_task(should_fail: bool = True) -> str:
    """A task that demonstrates failure and retry behavior."""
    if should_fail:
        raise ValueError("This task is designed to fail")
    return "Success!"


async def main():
    """Main example function."""
    print("TaskFlow Basic Usage Example")
    print("=" * 40)

    # Initialize TaskFlow engine
    engine = TaskEngine()
    await engine.start()

    try:
        # Example 1: Submit simple math task
        print("\n1. Submitting math task...")
        task_id1 = await engine.submit_task("add_numbers", [10, 20])
        print(f"   Task submitted: {task_id1}")

        # Example 2: Submit data processing task
        print("\n2. Submitting data processing task...")
        sample_data = {"name": "John Doe", "age": 30, "city": "New York"}
        task_id2 = await engine.submit_task("process_data", [sample_data])
        print(f"   Task submitted: {task_id2}")

        # Example 3: Submit notification task
        print("\n3. Submitting notification task...")
        task_id3 = await engine.submit_task(
            "send_email", ["user@example.com", "Welcome!", "Welcome to TaskFlow!"]
        )
        print(f"   Task submitted: {task_id3}")

        # Example 4: Submit failing task to demonstrate retries
        print("\n4. Submitting failing task...")
        task_id4 = await engine.submit_task("failing_task", [True])
        print(f"   Task submitted: {task_id4}")

        # Example 5: Batch task submission
        print("\n5. Submitting batch of tasks...")
        batch_tasks = []
        for i in range(5):
            task_id = await engine.submit_task("add_numbers", [i, i * 2])
            batch_tasks.append(task_id)
            print(f"   Batch task {i+1} submitted: {task_id}")

        # Wait a bit for tasks to be processed
        print("\n6. Waiting for tasks to complete...")
        await asyncio.sleep(5)

        # Check task statuses
        print("\n7. Checking task statuses...")
        all_task_ids = [task_id1, task_id2, task_id3, task_id4] + batch_tasks

        for task_id in all_task_ids:
            task = await engine.get_task(task_id)
            if task:
                print(f"   Task {task_id[:8]}: {task.status}")
                if task.result and task.result.result:
                    print(f"      Result: {task.result.result}")
                if task.result and task.result.error:
                    print(f"      Error: {task.result.error}")
            else:
                print(f"   Task {task_id[:8]}: Not found")

        # Show queue statistics
        print("\n8. Queue statistics:")
        queues = ["math", "data", "notifications", "default"]
        for queue_name in queues:
            try:
                stats = await engine.get_queue_stats(queue_name)
                print(
                    f"   {queue_name}: {stats['pending_count']} pending, "
                    f"{stats['completed_count']} completed, {stats['failed_count']} failed"
                )
            except:
                print(f"   {queue_name}: No statistics available")

        # System health check
        print("\n9. System health check:")
        health = await engine.health_check()
        print(f"   Status: {health['status']}")
        print(f"   Active queues: {health.get('active_queues', 0)}")
        print(f"   Running tasks: {health.get('running_tasks', 0)}")

    finally:
        # Cleanup
        await engine.shutdown()
        print("\nTaskFlow engine shut down.")


if __name__ == "__main__":
    # Run the example
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExample interrupted by user.")
    except Exception as e:
        print(f"\nExample failed with error: {e}")
        sys.exit(1)
