"""Entrypoint for orchestra-core: `python -m orchestra_core`."""

import os
import shutil

import uvicorn

from orchestra_core.settings import settings


def set_multiproc_dir() -> None:
    """Prepare the directory used by prometheus-client for multi-process mode."""
    shutil.rmtree(settings.prometheus_dir, ignore_errors=True)
    os.makedirs(settings.prometheus_dir, exist_ok=True)
    os.environ["prometheus_multiproc_dir"] = str(
        settings.prometheus_dir.expanduser().absolute(),
    )
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = str(
        settings.prometheus_dir.expanduser().absolute(),
    )


def main() -> None:
    set_multiproc_dir()
    uvicorn.run(
        "orchestra_core.web.application:get_app",
        workers=settings.workers_count,
        host=settings.host,
        port=settings.port,
        reload=settings.reload,
        log_level=settings.log_level.value.lower(),
        timeout_keep_alive=settings.timeout_keep_alive,
        factory=True,
    )


if __name__ == "__main__":
    main()
