"""Parallel execution utilities using concurrent.futures."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable


def threaded_map(
    fn: Callable,
    items: Iterable[Any],
    max_workers: int | None = None,
) -> list[Any]:
    """
    Execute a function in parallel across items using a thread pool.

    Args:
        fn: Function to apply to each item
        items: Iterable of items to process
        max_workers: Maximum number of threads (defaults to ThreadPoolExecutor default)

    Returns:
        List of results in the same order as inputs
    """
    items_list = list(items)
    if not items_list:
        return []

    results = [None] * len(items_list)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(fn, item): idx for idx, item in enumerate(items_list)
        }

        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            results[idx] = future.result()

    return results
