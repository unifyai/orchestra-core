import re
from dataclasses import dataclass
from typing import Any, List, Optional, Type

from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from orchestra_core.settings import settings


def create_database(worker_id=None) -> None:
    """Create a database."""
    url = str(settings.db_url.with_path("/postgres"))
    datname = settings.db_base
    if worker_id:
        url = url.replace("orchestra_test", f"orchestra_test_{worker_id}")
        datname += f"_{worker_id}"
    db_url = make_url(url)
    engine = create_engine(db_url, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        database_existance = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": datname},
        )
        database_exists = database_existance.scalar() == 1

    if database_exists:
        drop_database(worker_id)

    for attempt in range(2):
        try:
            with engine.connect() as conn:  # noqa: WPS440
                conn.execute(
                    text(
                        f'CREATE DATABASE "{datname}" ENCODING "utf8" TEMPLATE template1',  # noqa: E501
                    ),
                )
            return
        except IntegrityError:
            if attempt == 1:
                raise
            drop_database(worker_id)


def drop_database(worker_id=None) -> None:
    """Drop current database."""
    url = str(settings.db_url.with_path("/postgres"))
    datname = settings.db_base
    if worker_id:
        url = url.replace("orchestra_test", f"orchestra_test_{worker_id}")
        datname += f"_{worker_id}"
    db_url = make_url(url)
    engine = create_engine(db_url, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pg_stat_activity.pid) "
                "FROM pg_stat_activity "
                "WHERE pg_stat_activity.datname = :name "
                "AND pid <> pg_backend_pid()",
            ),
            {"name": datname},
        )
        conn.execute(text(f'DROP DATABASE IF EXISTS "{datname}"'))


def get_next_order_value(
    session: Session,
    model_class: Type,
    order: Optional[int] = None,
    where_conditions: Optional[List[Any]] = None,
) -> int:
    """
    Get the next order value for a model, either using the provided order
    or auto-incrementing based on the current maximum order value.

    Args:
        session: SQLAlchemy session
        model_class: The model class (e.g., Project, Interface, Tab)
        order: Explicit order value to use, or None for auto-increment
        where_conditions: List of SQLAlchemy where conditions to apply when finding max order

    Returns:
        The order value to use (either the provided order or next auto-increment value)
    """
    if order is not None:
        return order

    # Build query to find maximum order value
    max_order_query = select(func.max(model_class.order))

    # Apply any where conditions
    if where_conditions:
        for condition in where_conditions:
            max_order_query = max_order_query.where(condition)

    # Execute query and calculate next order value
    max_order = session.execute(max_order_query).scalar_one()
    return (max_order or -1) + 1


@dataclass
class PathSegment:
    """Represents one segment of a FK path.

    Examples:
        PathSegment("images", is_array=True, is_wildcard=True)  # images[*]
        PathSegment("user_id", is_array=False)                  # user_id
        PathSegment("items", is_array=True, is_wildcard=False)  # items[0]
    """

    name: str
    is_array: bool = False
    is_wildcard: bool = False  # True for [*], False for [0], [1], etc.
    array_index: Optional[int] = None  # Specific index if not wildcard


class FKPathParser:
    """Parse and extract values from nested FK paths."""

    # Regex patterns for path parsing
    ARRAY_PATTERN = re.compile(
        r"^(.+?)\[(\*|\d+)\]$",
    )  # Matches "field[*]" or "field[0]"

    @classmethod
    def parse(cls, path: str) -> List[PathSegment]:
        """Parse a path string into segments.

        Args:
            path: FK path string like "images[*].image_id" or "metadata.user.user_id"

        Returns:
            List of PathSegment objects representing the path structure

        Examples:
            >>> parse("images[*].image_id")
            [PathSegment("images", is_array=True, is_wildcard=True),
             PathSegment("image_id", is_array=False)]

            >>> parse("metadata.user.user_id")
            [PathSegment("metadata"), PathSegment("user"), PathSegment("user_id")]

        Raises:
            ValueError: If path syntax is invalid
        """
        if not path or not isinstance(path, str):
            raise ValueError("Path must be a non-empty string")

        # Split by dots first
        parts = path.split(".")
        segments = []

        for part in parts:
            if not part:
                raise ValueError(f"Invalid path '{path}': empty segment")

            # Check if this part has array notation
            array_match = cls.ARRAY_PATTERN.match(part)

            if array_match:
                field_name = array_match.group(1)
                index_str = array_match.group(2)

                if index_str == "*":
                    # Wildcard array access
                    segments.append(
                        PathSegment(
                            name=field_name,
                            is_array=True,
                            is_wildcard=True,
                        ),
                    )
                else:
                    # Specific index
                    segments.append(
                        PathSegment(
                            name=field_name,
                            is_array=True,
                            is_wildcard=False,
                            array_index=int(index_str),
                        ),
                    )
            else:
                # Simple field access
                segments.append(PathSegment(name=part, is_array=False))

        return segments

    @classmethod
    def has_wildcard(cls, segments: List[PathSegment]) -> bool:
        """Check if any segment in the path has a wildcard.

        Args:
            segments: List of PathSegment objects from parse()

        Returns:
            True if path contains [*], False otherwise

        Examples:
            >>> segments = parse("image_ids[*]")
            >>> has_wildcard(segments)
            True

            >>> segments = parse("metadata.author.user_id")
            >>> has_wildcard(segments)
            False

            >>> segments = parse("teams[*].members[*].user_id")
            >>> has_wildcard(segments)
            True
        """
        return any(seg.is_wildcard for seg in segments)

    @classmethod
    def extract_values(
        cls,
        data: Any,
        segments: List[PathSegment],
    ) -> List[Any]:
        """Extract all values at the given path from data structure.

        For array wildcards, returns multiple values (one per array element).
        For simple paths, returns a single-item list (or empty if path not found).

        Args:
            data: Dictionary or nested structure to extract from
            segments: Parsed path segments from parse()

        Returns:
            List of values found at the path. Empty list if path doesn't exist.

        Examples:
            >>> data = {"images": [{"id": 1}, {"id": 2}]}
            >>> segments = parse("images[*].id")
            >>> extract_values(data, segments)
            [1, 2]

            >>> data = {"image_ids": [1, 2, 3]}
            >>> segments = parse("image_ids[*]")
            >>> extract_values(data, segments)
            [1, 2, 3]

            >>> data = {"metadata": {"user": {"user_id": 5}}}
            >>> segments = parse("metadata.user.user_id")
            >>> extract_values(data, segments)
            [5]
        """
        # Start with data wrapped in a list (for consistent handling)
        current_values = [data]

        for segment in segments:
            next_values = []

            for value in current_values:
                # Don't skip None values - let validation layer handle filtering
                # This allows proper tracking of which values are None

                if segment.is_array:
                    # Array access
                    if not isinstance(value, dict):
                        continue  # Can't navigate further

                    arr = value.get(segment.name)
                    if arr is None:
                        continue

                    if not isinstance(arr, list):
                        continue  # Expected array but got something else

                    if segment.is_wildcard:
                        # Wildcard: extract from all array elements
                        next_values.extend(arr)
                    else:
                        # Specific index
                        if 0 <= segment.array_index < len(arr):
                            next_values.append(arr[segment.array_index])
                else:
                    # Simple field access
                    if isinstance(value, dict):
                        # Check if key exists (not just if value is not None)
                        if segment.name in value:
                            field_value = value.get(segment.name)
                            # Include None values - validation layer will filter them
                            next_values.append(field_value)

            current_values = next_values

            # Early exit if we lost all values
            if not current_values:
                return []

        return current_values

    @classmethod
    def is_nested_path(cls, path: str) -> bool:
        """Check if a path contains nesting indicators.

        Args:
            path: FK path string

        Returns:
            True if path has dots (.) or brackets ([])

        Examples:
            >>> is_nested_path("department_id")
            False
            >>> is_nested_path("images[*].image_id")
            True
            >>> is_nested_path("metadata.user.user_id")
            True
        """
        return "." in path or "[" in path

    @classmethod
    def get_root_field(cls, path: str) -> str:
        """Extract the root field name from a path.

        Args:
            path: FK path string

        Returns:
            Root field name (before first . or [)

        Examples:
            >>> get_root_field("images[*].image_id")
            'images'
            >>> get_root_field("metadata.user.user_id")
            'metadata'
            >>> get_root_field("department_id")
            'department_id'
        """
        # Find first occurrence of . or [
        dot_pos = path.find(".")
        bracket_pos = path.find("[")

        # Find which comes first (or only one exists)
        if dot_pos == -1 and bracket_pos == -1:
            # No separators - return whole path
            return path
        elif dot_pos == -1:
            # Only bracket
            return path[:bracket_pos]
        elif bracket_pos == -1:
            # Only dot
            return path[:dot_pos]
        else:
            # Both exist - return up to whichever comes first
            return path[: min(dot_pos, bracket_pos)]

    @classmethod
    def validate_path_syntax(cls, path: str) -> None:
        """Validate path syntax and raise descriptive errors.

        Args:
            path: FK path string to validate

        Raises:
            ValueError: If path syntax is invalid with descriptive message
        """
        if not path or not isinstance(path, str):
            raise ValueError("FK path must be a non-empty string")

        # Check for invalid characters
        invalid_chars = set(path) - set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.[]*",
        )
        if invalid_chars:
            raise ValueError(
                f"FK path '{path}' contains invalid characters: {invalid_chars}. "
                f"Only letters, numbers, underscore, dots, and brackets are allowed.",
            )

        # Check for consecutive dots
        if ".." in path:
            raise ValueError(f"FK path '{path}' has consecutive dots (..)")

        # Check for empty brackets
        if "[]" in path:
            raise ValueError(f"FK path '{path}' has empty brackets []")

        # Try to parse it
        try:
            segments = cls.parse(path)
        except Exception as e:
            raise ValueError(f"FK path '{path}' has invalid syntax: {e}")

        # Check for reasonable depth (prevent DoS)
        max_depth = 10
        if len(segments) > max_depth:
            raise ValueError(
                f"FK path '{path}' exceeds maximum nesting depth of {max_depth}",
            )
