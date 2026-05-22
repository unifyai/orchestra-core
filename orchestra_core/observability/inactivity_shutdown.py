"""
Inactivity-based automatic shutdown for local development.

When ORCHESTRA_INACTIVITY_TIMEOUT_SECONDS is set, the server will automatically
shut down after that many seconds without receiving any API requests. This prevents
the server from quietly consuming CPU resources when forgotten.

In multi-worker mode (workers > 1), activity is tracked via a shared file so that
requests handled by ANY worker keep the entire server alive. Shutdown signals the
main uvicorn process to gracefully terminate all workers.
"""

import asyncio
import logging
import os
import signal
import time
from pathlib import Path
from typing import Optional

from orchestra_core.settings import settings

logger = logging.getLogger(__name__)

# Shared activity file path (used when workers > 1 for cross-process coordination)
_activity_file: Optional[Path] = None

# Fallback: per-process monotonic timestamp (used when file operations fail or workers=1)
_last_activity_time: float = time.monotonic()

# Reference to the monitor task for cleanup
_monitor_task: Optional[asyncio.Task] = None


def _get_activity_file() -> Path:
    """Get the path to the shared activity file for this server instance."""
    global _activity_file
    if _activity_file is None:
        # Use port to ensure uniqueness per server instance
        _activity_file = Path(f"/tmp/orchestra-{settings.port}-activity.txt")
    return _activity_file


def _write_activity_to_file() -> bool:
    """Write current timestamp to shared activity file. Returns True on success."""
    try:
        activity_file = _get_activity_file()
        activity_file.write_text(str(time.time()))
        return True
    except (OSError, IOError) as e:
        logger.debug(f"Failed to write activity file: {e}")
        return False


def _read_activity_from_file() -> Optional[float]:
    """Read last activity timestamp from shared file. Returns None on failure."""
    try:
        activity_file = _get_activity_file()
        if activity_file.exists():
            content = activity_file.read_text().strip()
            return float(content)
    except (OSError, IOError, ValueError) as e:
        logger.debug(f"Failed to read activity file: {e}")
    return None


def record_activity() -> None:
    """
    Record that API activity occurred.

    Call this from middleware when a real API request is processed.
    In multi-worker mode, writes to a shared file so all workers see the activity.
    """
    global _last_activity_time
    _last_activity_time = time.monotonic()

    # In multi-worker mode, also write to shared file for cross-process coordination
    if settings.workers_count > 1:
        _write_activity_to_file()


def get_seconds_since_activity() -> float:
    """
    Return seconds elapsed since last recorded activity.

    In multi-worker mode, reads from shared file to see activity from ALL workers.
    Falls back to per-process tracking if file operations fail.
    """
    # In multi-worker mode, prefer shared file for cross-process coordination
    if settings.workers_count > 1:
        file_timestamp = _read_activity_from_file()
        if file_timestamp is not None:
            return time.time() - file_timestamp

    # Fallback to per-process tracking
    return time.monotonic() - _last_activity_time


async def _inactivity_monitor_loop(timeout_seconds: int) -> None:
    """
    Background loop that checks for inactivity and triggers shutdown.

    Checks every 30 seconds (or timeout/4, whichever is smaller) whether
    the inactivity threshold has been exceeded.
    """
    check_interval = min(30, timeout_seconds / 4)
    workers = settings.workers_count
    mode = "multi-worker (shared file)" if workers > 1 else "single-worker"
    logger.info(
        f"Inactivity monitor started ({mode}): will shut down after {timeout_seconds}s "
        f"of no API requests (checking every {check_interval:.0f}s)",
    )

    # Initialize activity file with current time (in multi-worker mode)
    if workers > 1:
        _write_activity_to_file()

    while True:
        await asyncio.sleep(check_interval)

        elapsed = get_seconds_since_activity()
        if elapsed >= timeout_seconds:
            logger.warning(
                f"No API requests for {elapsed:.0f}s (threshold: {timeout_seconds}s). "
                "Initiating shutdown due to inactivity.",
            )
            # In multi-worker mode, signal the main uvicorn process (parent).
            # In single-worker mode, signal ourselves (we ARE the main process).
            if workers > 1:
                target_pid = os.getppid()
                logger.info(f"Signaling main process (PID {target_pid}) for shutdown")
            else:
                target_pid = os.getpid()

            os.kill(target_pid, signal.SIGTERM)
            return


def start_inactivity_monitor() -> None:
    """
    Start the inactivity monitor background task if configured.

    Does nothing if ORCHESTRA_INACTIVITY_TIMEOUT_SECONDS is not set.
    Safe to call multiple times; only starts one monitor.
    """
    global _monitor_task

    timeout = settings.inactivity_timeout_seconds
    if timeout is None:
        return

    if _monitor_task is not None and not _monitor_task.done():
        logger.debug("Inactivity monitor already running")
        return

    loop = asyncio.get_event_loop()
    _monitor_task = loop.create_task(_inactivity_monitor_loop(timeout))


def stop_inactivity_monitor() -> None:
    """Cancel the inactivity monitor task if running and clean up shared state."""
    global _monitor_task

    if _monitor_task is not None and not _monitor_task.done():
        _monitor_task.cancel()
        _monitor_task = None

    # Clean up the shared activity file
    try:
        activity_file = _get_activity_file()
        if activity_file.exists():
            activity_file.unlink()
    except (OSError, IOError):
        pass  # Best effort cleanup
