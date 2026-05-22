"""orchestra-core kernel models."""

import pkgutil
from pathlib import Path


def load_all_models() -> None:
    """Load all kernel model modules so SQLAlchemy registers their classes."""
    package_dir = Path(__file__).resolve().parent
    modules = pkgutil.walk_packages(
        path=[str(package_dir)],
        prefix="orchestra_core.db.models.",
    )
    for module in modules:
        __import__(module.name)  # noqa: WPS421
