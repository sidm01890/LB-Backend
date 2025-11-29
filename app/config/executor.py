"""
Executor configuration for parallel task processing
This module provides a dedicated thread pool executor for CPU-bound tasks
to prevent blocking the main event loop.
"""
import concurrent.futures
import logging
from typing import Optional
import os

logger = logging.getLogger(__name__)

# Global thread pool executor
_task_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None


def create_task_executor(max_workers: int = None) -> concurrent.futures.ThreadPoolExecutor:
    """
    Create a thread pool executor for background tasks.
    
    Args:
        max_workers: Maximum number of worker threads. If None, uses:
                    min(32, (os.cpu_count() or 1) + 4)
    
    Returns:
        ThreadPoolExecutor instance
    """
    global _task_executor
    
    if max_workers is None:
        # Default: Use CPU count + 4, but cap at 32 for I/O-bound tasks
        max_workers = min(32, (os.cpu_count() or 1) + 4)
    
    if _task_executor is None:
        _task_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="task_worker"
        )
        logger.info(f"✅ Task executor created with {max_workers} worker threads")
    
    return _task_executor


def get_task_executor() -> concurrent.futures.ThreadPoolExecutor:
    """
    Get the global task executor, creating it if it doesn't exist.
    
    Returns:
        ThreadPoolExecutor instance
    """
    global _task_executor
    
    if _task_executor is None:
        # Default to 10 workers for summary sheet generation
        # This allows multiple reports to be generated in parallel
        max_workers = int(os.getenv("TASK_EXECUTOR_WORKERS", "10"))
        _task_executor = create_task_executor(max_workers=max_workers)
    
    return _task_executor


async def shutdown_task_executor():
    """
    Shutdown the task executor gracefully.
    Waits for all running tasks to complete before shutting down.
    """
    global _task_executor
    
    if _task_executor is not None:
        logger.info("Shutting down task executor...")
        # Python's ThreadPoolExecutor.shutdown does not support timeout parameter.
        # Wait=True ensures we block until current tasks finish.
        _task_executor.shutdown(wait=True)
        _task_executor = None
        logger.info("Task executor shut down successfully")


def run_in_executor(func, *args, **kwargs):
    """
    Run a function in the task executor.
    
    Args:
        func: Function to execute
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function
    
    Returns:
        Future object representing the task
    """
    executor = get_task_executor()
    return executor.submit(func, *args, **kwargs)

