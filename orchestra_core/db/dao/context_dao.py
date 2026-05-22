import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from orchestra_core.db.models.core_models import (
    ActiveDerivedLog,
    Context,
    ContextVersion,
    Embedding,
    EmbeddingQueue,
    LogEvent,
    LogEventContext,
    LogEventVersion,
    LogUniqueConstraint,
    ProjectVersion,
)
from orchestra_core.db.utils import FKPathParser, PathSegment


def delete_orphaned_log_events(
    session: Session,
    project_id: int,
    skip_embedding_cleanup: bool = False,
    log_event_ids: Optional[List[int]] = None,
) -> None:
    from orchestra_core.db.dao.embedding_dao import EmbeddingDAO

    if log_event_ids is not None:
        # Scoped scan: only check the provided log IDs for orphan status.
        # Much faster than a project-wide scan when the candidate set is known
        # (e.g. the logs that belonged to the just-deleted context).
        if not log_event_ids:
            return
        orphaned_log_event_ids = session.execute(
            text(
                """
            SELECT le.id
            FROM log_event le
            WHERE le.id = ANY(:log_event_ids)
              AND NOT EXISTS (
                SELECT 1
                FROM log_event_context lec
                WHERE lec.log_event_id = le.id
              );
            """,
            ),
            {"log_event_ids": log_event_ids},
        ).fetchall()
    else:
        # Project-wide fallback: scans all logs in the project.
        orphaned_log_event_ids = session.execute(
            text(
                """
            SELECT le.id
            FROM log_event le
            WHERE le.project_id = :project_id
              AND NOT EXISTS (
                SELECT 1
                FROM log_event_context lec
                WHERE lec.log_event_id = le.id
              );
            """,
            ),
            {"project_id": project_id},
        ).fetchall()

    if not orphaned_log_event_ids:
        return

    orphaned_ids = [row[0] for row in orphaned_log_event_ids]

    # Clean up embeddings for orphaned log events before hard-deleting them.
    # Skipped when called from a higher-level deletion (e.g. ProjectDAO.delete)
    # that already handles embedding cleanup at project scope.
    if not skip_embedding_cleanup:
        embedding_dao = EmbeddingDAO(session)
        embedding_dao.cancel_queue(
            log_event_ids=orphaned_ids,
            reason="Context deleted",
        )
        embedding_dao.soft_delete(log_event_ids=orphaned_ids)

    session.execute(
        text("DELETE FROM log_event WHERE id = ANY(:log_event_ids)"),
        {"log_event_ids": orphaned_ids},
    )


def cleanup_orphaned_field_types(session: Session, context_id: int) -> None:
    """
    Delete FieldType records for fields that no longer exist in any log events
    for the given context.

    This is called after rollback to clean up field metadata for fields that
    were created after the rolled-back commit point.
    """
    # Get all field names that currently exist in log events for this context
    existing_fields_result = session.execute(
        text(
            """
            SELECT DISTINCT jsonb_object_keys(le.data) AS field_name
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id
            """,
        ),
        {"context_id": context_id},
    ).fetchall()

    existing_field_names = {row[0] for row in existing_fields_result}

    # Delete FieldType records for fields that no longer exist
    # Only delete context-specific field types (context_id is not NULL)
    if existing_field_names:
        session.execute(
            text(
                """
                DELETE FROM field_type
                WHERE context_id = :context_id
                  AND field_name NOT IN :existing_fields
                """,
            ),
            {"context_id": context_id, "existing_fields": tuple(existing_field_names)},
        )
    else:
        # No fields exist - delete all field types for this context
        session.execute(
            text(
                """
                DELETE FROM field_type
                WHERE context_id = :context_id
                """,
            ),
            {"context_id": context_id},
        )


def cleanup_orphaned_derived_log_templates(session: Session, context_id: int) -> None:
    """
    Delete ActiveDerivedLog templates for derived fields that no longer exist
    in any log events for the given context.

    This is called after rollback to clean up derived field templates that were
    created after the rolled-back commit point.
    """
    # Get all field names that currently exist in log events for this context
    existing_fields_result = session.execute(
        text(
            """
            SELECT DISTINCT jsonb_object_keys(le.data) AS field_name
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id
            """,
        ),
        {"context_id": context_id},
    ).fetchall()

    existing_field_names = {row[0] for row in existing_fields_result}

    # Delete ActiveDerivedLog templates for derived fields that no longer exist
    if existing_field_names:
        session.execute(
            text(
                """
                DELETE FROM active_derived_log_template
                WHERE context_id = :context_id
                  AND key NOT IN :existing_fields
                """,
            ),
            {"context_id": context_id, "existing_fields": tuple(existing_field_names)},
        )
    else:
        # No fields exist - delete all derived log templates for this context
        session.execute(
            text(
                """
                DELETE FROM active_derived_log_template
                WHERE context_id = :context_id
                """,
            ),
            {"context_id": context_id},
        )


def cleanup_plots_created_after_commit(
    session: Session,
    project_id: int,
    context_name: str,
    commit_timestamp: datetime,
) -> int:
    """
    Delete Plot records that reference the given context and were created
    after the commit timestamp.

    This is called after rollback to clean up plots that were created after
    the rolled-back commit point.

    Returns:
        Number of plots deleted.
    """
    result = session.execute(
        text(
            """
            DELETE FROM plot
            WHERE project_id = :project_id
              AND project_config->>'context' = :context_name
              AND created_at > :commit_timestamp
            RETURNING id
            """,
        ),
        {
            "project_id": project_id,
            "context_name": context_name,
            "commit_timestamp": commit_timestamp,
        },
    )
    deleted_count = len(result.fetchall())
    if deleted_count > 0:
        logger.info(
            f"Deleted {deleted_count} plots created after rollback point for "
            f"context '{context_name}' in project {project_id}",
        )
    return deleted_count


def cleanup_table_views_created_after_commit(
    session: Session,
    project_id: int,
    context_name: str,
    commit_timestamp: datetime,
) -> int:
    """
    Delete TableView records that reference the given context and were created
    after the commit timestamp.

    This is called after rollback to clean up table views that were created after
    the rolled-back commit point.

    Returns:
        Number of table views deleted.
    """
    result = session.execute(
        text(
            """
            DELETE FROM table_view
            WHERE project_id = :project_id
              AND project_config->>'context' = :context_name
              AND created_at > :commit_timestamp
            RETURNING id
            """,
        ),
        {
            "project_id": project_id,
            "context_name": context_name,
            "commit_timestamp": commit_timestamp,
        },
    )
    deleted_count = len(result.fetchall())
    if deleted_count > 0:
        logger.info(
            f"Deleted {deleted_count} table views created after rollback point for "
            f"context '{context_name}' in project {project_id}",
        )
    return deleted_count


class ContextDAO:
    def __init__(self, session: Session):
        self.session = session

    def _validate_description(self, description: Optional[str]) -> None:
        """Validate description length."""
        if description is not None and len(description) > 256:
            raise ValueError("Description cannot exceed 256 characters")

    def _validate_foreign_keys_config(
        self,
        project_id: int,
        context_name: str,
        foreign_keys: List[Dict[str, Any]],
    ) -> None:
        """Validate foreign keys configuration at context creation time."""
        for fk in foreign_keys:
            # Parse the reference
            ref_parts = fk["references"].split(".")
            if len(ref_parts) != 2:
                raise ValueError(
                    f"Foreign key reference '{fk['references']}' must be in format 'ContextName.column_name'",
                )

            ref_context_name, ref_column_name = ref_parts

            # Note: No referenced context existence check to allow mutually-referencing contexts.
            # (e.g., FunctionManager references GuidanceManager and vice versa).
            # FK validation in place when inserting/updating logs.

            # Validate SET DEFAULT has a default value
            # DISABLED: SET DEFAULT is not currently supported
            # on_delete = fk.get("on_delete", "NO ACTION")
            # on_update = fk.get("on_update", "NO ACTION")
            # default = fk.get("default")
            #
            # if on_delete == "SET DEFAULT" or on_update == "SET DEFAULT":
            #     if default is None:
            #         raise ValueError(
            #             f"Foreign key '{fk['name']}' uses SET DEFAULT action "
            #             f"but no default value is specified. "
            #             f"Add a 'default' field with the value to use or change the action to SET NULL.",
            #         )

            # Note: We don't validate if the column exists yet because it might be created
            # later. The actual validation happens when inserting/updating logs.

        # Check for circular CASCADE dependencies
        cycle = self._detect_circular_references(
            project_id=project_id,
            new_context_name=context_name,
            new_foreign_keys=foreign_keys,
        )

        if cycle:
            # cycle is now List[Tuple[str, str]] - format as "Context.field"
            cycle_path = " → ".join([f"{ctx}.{field}" for ctx, field in cycle])
            raise ValueError(
                f"Circular foreign key dependency detected: {cycle_path}. "
                f"Non-wildcard CASCADE actions would cause an infinite loop when "
                f"deleting or updating records. Use SET NULL or wildcard array FKs "
                f"([*]) to break the cycle.",
            )

    def _build_fk_graph(
        self,
        project_id: int,
        new_context_name: str,
        new_foreign_keys: List[Dict[str, Any]],
    ) -> Dict[tuple[str, str], List[tuple[str, str]]]:
        """Build a directed graph of CASCADE propagation dependencies.

        Now tracks field-level dependencies (context, field) instead of just contexts.
        Wildcard FKs are excluded as they only pop array elements, not delete records.

        The graph represents CASCADE propagation direction, NOT FK reference direction.
        If ContextA.field_x references ContextB.field_y with CASCADE:
        - FK direction: A.field_x → B.field_y (A references B)
        - CASCADE propagation: (B, field_y) → (A, field_x) (deleting B.field_y cascades to A.field_x)

        Args:
            project_id: The project ID
            new_context_name: Name of the context being created
            new_foreign_keys: Foreign keys for the new context

        Returns:
            Graph as adjacency list: {(context, field): [(context, field), ...]}
            Only includes non-wildcard CASCADE relationships
        """
        graph = {}

        # Get all existing contexts with foreign keys in this project
        existing_contexts = (
            self.session.query(Context)
            .filter(
                Context.project_id == project_id,
                Context.foreign_keys != None,  # noqa: E711
                Context.foreign_keys != text("'[]'::jsonb"),
            )
            .all()
        )

        # Add edges representing CASCADE propagation from existing contexts
        for context in existing_contexts:
            context_name = context.name

            if context.foreign_keys:
                for fk in context.foreign_keys:
                    # Only CASCADE actions can cause infinite loops
                    on_delete = fk.get("on_delete", "NO ACTION")
                    on_update = fk.get("on_update", "NO ACTION")

                    if on_delete != "CASCADE" and on_update != "CASCADE":
                        continue

                    fk_name = fk["name"]

                    # Check if this is a wildcard FK (only pops elements, doesn't delete records)
                    is_nested = fk.get("is_nested", False)
                    if is_nested:
                        path_segments_data = fk.get("path_segments", [])
                        path_segments = [
                            PathSegment(
                                name=s["name"],
                                is_array=s["is_array"],
                                is_wildcard=s["is_wildcard"],
                                array_index=s.get("array_index"),
                            )
                            for s in path_segments_data
                        ]
                        # Skip wildcard FKs - they only pop array elements, can't cause infinite loops
                        if FKPathParser.has_wildcard(path_segments):
                            continue

                    # Parse reference: "ContextName.column_name"
                    ref_parts = fk["references"].split(".")
                    if len(ref_parts) == 2:
                        ref_context_name, ref_field_name = ref_parts
                        # CASCADE propagation: deleting (ref_context, ref_field) cascades to (context, fk_name)
                        # Add edge: (ref_context, ref_field) → (context, fk_name)
                        key = (ref_context_name, ref_field_name)
                        if key not in graph:
                            graph[key] = []
                        graph[key].append((context_name, fk_name))

        # Add edges representing CASCADE propagation from new context's FKs
        for fk in new_foreign_keys:
            on_delete = fk.get("on_delete", "NO ACTION")
            on_update = fk.get("on_update", "NO ACTION")

            if on_delete != "CASCADE" and on_update != "CASCADE":
                continue

            fk_name = fk["name"]

            # Check if this is a wildcard FK
            is_nested = fk.get("is_nested", False)
            if is_nested:
                path_segments_data = fk.get("path_segments", [])
                path_segments = [
                    PathSegment(
                        name=s["name"],
                        is_array=s["is_array"],
                        is_wildcard=s["is_wildcard"],
                        array_index=s.get("array_index"),
                    )
                    for s in path_segments_data
                ]
                # Skip wildcard FKs
                if FKPathParser.has_wildcard(path_segments):
                    continue

            # Parse reference
            ref_parts = fk["references"].split(".")
            if len(ref_parts) == 2:
                ref_context_name, ref_field_name = ref_parts
                # CASCADE propagation: deleting (ref_context, ref_field) cascades to (new_context, fk_name)
                # Add edge: (ref_context, ref_field) → (new_context, fk_name)
                key = (ref_context_name, ref_field_name)
                if key not in graph:
                    graph[key] = []
                graph[key].append((new_context_name, fk_name))

        return graph

    def _dfs_detect_cycle(
        self,
        graph: Dict[tuple[str, str], List[tuple[str, str]]],
        start_node: tuple[str, str],
    ) -> Optional[List[tuple[str, str]]]:
        """Use DFS with color tracking to detect cycles in FK graph.

        Args:
            graph: Adjacency list of FK dependencies {(context, field): [(context, field), ...]}
            start_node: (context, field) tuple to start search from

        Returns:
            None if no cycle, or list of (context, field) tuples forming the cycle path
        """
        # Color states for cycle detection
        WHITE = 0  # Not visited
        GRAY = 1  # Currently exploring (on recursion stack)
        BLACK = 2  # Fully explored

        colors = {node: WHITE for node in graph}

        def dfs(
            node: tuple[str, str],
            path: List[tuple[str, str]],
        ) -> Optional[List[tuple[str, str]]]:
            """Recursive DFS helper."""
            if node not in graph:
                # Node doesn't exist in graph (shouldn't happen, but handle gracefully)
                return None

            if colors[node] == GRAY:
                # Back edge detected - cycle found!
                # Reconstruct the cycle from where we've seen this node before
                try:
                    cycle_start_idx = path.index(node)
                    return path[cycle_start_idx:] + [node]
                except ValueError:
                    # Node not in path (shouldn't happen)
                    return [node, node]

            if colors[node] == BLACK:
                # Already fully explored this node
                return None

            # Mark as currently exploring
            colors[node] = GRAY
            current_path = path + [node]

            # Visit all neighbors
            for neighbor in graph[node]:
                cycle = dfs(neighbor, current_path)
                if cycle:
                    return cycle

            # Mark as fully explored
            colors[node] = BLACK
            return None

        return dfs(start_node, [])

    def _detect_circular_references(
        self,
        project_id: int,
        new_context_name: str,
        new_foreign_keys: List[Dict[str, Any]],
    ) -> Optional[List[tuple[str, str]]]:
        """Detect circular CASCADE dependencies that would cause infinite loops.

        Now tracks field-level cycles. Wildcard FKs are excluded as they only pop
        array elements without deleting records, thus cannot cause infinite loops.

        Args:
            project_id: The project ID
            new_context_name: Name of context being created
            new_foreign_keys: Foreign keys for the new context

        Returns:
            None if no cycle, or list of (context, field) tuples forming the cycle
        """
        if not new_foreign_keys:
            return None

        # Build the FK dependency graph (field-level, excludes wildcard FKs)
        graph = self._build_fk_graph(project_id, new_context_name, new_foreign_keys)

        # Check for cycles from ALL nodes, since adding the new context might
        # complete a cycle that doesn't necessarily start from the new context
        for node in graph:
            if graph[node]:  # Only check nodes with outgoing edges
                cycle = self._dfs_detect_cycle(graph, node)
                if cycle:
                    return cycle

        return None

    def validate_foreign_key_references(
        self,
        project_id: int,
        context_id: int,
        entries: Dict[str, Any],
    ) -> None:
        """Validate that foreign key values exist in referenced contexts.

        This is called when creating or updating logs to ensure referential integrity.
        Supports both simple column FKs and nested path FKs.
        """
        # Get the context with its foreign keys
        context = self.session.query(Context).filter_by(id=context_id).one_or_none()
        if not context or not context.foreign_keys:
            return  # No foreign keys to validate

        for fk in context.foreign_keys:
            # Check if this is a nested FK
            is_nested = fk.get("is_nested", False)

            if is_nested:
                # Handle nested path FK
                self._validate_nested_fk_reference(
                    fk=fk,
                    entries=entries,
                    project_id=project_id,
                )
            else:
                # Handle simple column FK (existing logic)
                self._validate_simple_fk_reference(
                    fk=fk,
                    entries=entries,
                    project_id=project_id,
                )

    def _validate_simple_fk_reference(
        self,
        fk: Dict[str, Any],
        entries: Dict[str, Any],
        project_id: int,
    ) -> None:
        """Validate a simple column FK reference (existing logic)."""
        fk_column = fk["name"]

        # Skip if this foreign key column is not in the entries
        if fk_column not in entries:
            return

        fk_value = entries[fk_column]

        # Skip NULL values (allowed unless we add NOT NULL constraint)
        if fk_value is None:
            return

        # Parse the reference
        ref_parts = fk["references"].split(".")
        ref_context_name, ref_column_name = ref_parts

        # Get the referenced context
        ref_context = self.filter(project_id=project_id, name=ref_context_name)
        if not ref_context:
            raise ValueError(
                f"Foreign key constraint violation: Referenced context '{ref_context_name}' does not exist",
            )

        ref_context_id = ref_context[0][0].id

        # Check if the referenced value exists
        json_str = json.dumps(fk_value)

        query = text(
            """
            SELECT COUNT(*)
            FROM log l
            JOIN log_event_log lel ON l.id = lel.log_id
            JOIN log_event_context lec ON lel.log_event_id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND l.key = :column_name
              AND l.value = CAST(:json_str AS jsonb)
        """,
        )

        result = self.session.execute(
            query,
            {
                "context_id": ref_context_id,
                "column_name": ref_column_name,
                "json_str": json_str,
            },
        )
        count = result.scalar()

        if count == 0:
            raise ValueError(
                f"Foreign key constraint violation: Value '{fk_value}' does not exist in "
                f"{ref_context_name}.{ref_column_name}",
            )

    def _validate_nested_fk_reference(
        self,
        fk: Dict[str, Any],
        entries: Dict[str, Any],
        project_id: int,
    ) -> None:
        """Validate a nested path FK reference (new logic for nested FKs)."""
        from orchestra_core.db.utils import FKPathParser, PathSegment

        fk_path = fk["name"]
        path_segments_data = fk.get("path_segments", [])

        # Reconstruct PathSegment objects
        path_segments = [
            PathSegment(
                name=s["name"],
                is_array=s["is_array"],
                is_wildcard=s["is_wildcard"],
                array_index=s.get("array_index"),
            )
            for s in path_segments_data
        ]

        # Get root field name
        root_field = FKPathParser.get_root_field(fk_path)

        # Skip if root field is not in entries
        if root_field not in entries:
            return

        # Extract all values at the nested path
        values = FKPathParser.extract_values(entries, path_segments)

        # Filter out None values
        values = [v for v in values if v is not None]

        if not values:
            return  # No non-null values to validate

        # Parse the reference
        ref_parts = fk["references"].split(".")
        ref_context_name, ref_column_name = ref_parts

        # Get the referenced context
        ref_context = self.filter(project_id=project_id, name=ref_context_name)
        if not ref_context:
            raise ValueError(
                f"Foreign key constraint violation: Referenced context '{ref_context_name}' does not exist",
            )

        ref_context_id = ref_context[0][0].id

        # Validate all extracted values exist in referenced table
        json_values = [json.dumps(v) for v in values]
        placeholders = ", ".join([f":val_{i}" for i in range(len(json_values))])

        query = text(
            f"""
            SELECT DISTINCT l.value
            FROM log l
            JOIN log_event_log lel ON l.id = lel.log_id
            JOIN log_event_context lec ON lel.log_event_id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND l.key = :column_name
              AND l.value::text IN ({placeholders})
        """,
        )

        params = {
            "context_id": ref_context_id,
            "column_name": ref_column_name,
        }
        for i, json_val in enumerate(json_values):
            params[f"val_{i}"] = json_val

        result = self.session.execute(query, params)
        valid_values = set(row[0] for row in result.fetchall())

        # Check for invalid values
        invalid_values = set(values) - valid_values
        if invalid_values:
            # Format invalid values for error message
            invalid_str = ", ".join([str(v) for v in list(invalid_values)[:3]])
            if len(invalid_values) > 3:
                invalid_str += f" (and {len(invalid_values) - 3} more)"

            raise ValueError(
                f"Foreign key constraint violation at path '{fk_path}': "
                f"Values [{invalid_str}] do not exist in {ref_context_name}.{ref_column_name}",
            )

    def batch_validate_foreign_key_references(
        self,
        project_id: int,
        context_id: int,
        batch_entries: List[Dict[str, Any]],
    ) -> Dict[int, str]:
        """Validate FK references for multiple logs in a single batch.

        This method optimizes FK validation by collecting all FK values across
        all logs and validating them with a single query per unique FK, rather
        than one query per log per FK.

        Now supports both simple column FKs and nested path FKs.

        Args:
            project_id: The project ID
            context_id: The context ID
            batch_entries: List of entry dictionaries, one per log

        Returns:
            Dict mapping log index to error message for failed validations.
            Empty dict if all validations pass.

        Example:
            failed = context_dao.batch_validate_foreign_key_references(
                project_id=1,
                context_id=2,
                batch_entries=[
                    {"department_id": 1, "name": "Alice"},
                    {"department_id": 2, "name": "Bob"},
                    {"department_id": 999, "name": "Charlie"},  # Invalid
                ]
            )
            # Returns: {2: "Foreign key constraint violation: ..."}
        """
        from collections import defaultdict

        # Get the context with its foreign keys
        context = self.session.query(Context).filter_by(id=context_id).one_or_none()
        if not context or not context.foreign_keys:
            return {}  # No foreign keys to validate

        # Step 1: Collect all FK values grouped by FK definition
        # Structure: {(ref_context_name, ref_column, fk_path, is_nested): {values}}
        fk_values_by_ref = defaultdict(set)

        for entries in batch_entries:
            for fk in context.foreign_keys:
                fk_path = fk["name"]
                is_nested = fk.get("is_nested", False)

                if is_nested:
                    # Extract values from nested path
                    from orchestra_core.db.utils import FKPathParser, PathSegment

                    path_segments_data = fk.get("path_segments", [])
                    path_segments = [
                        PathSegment(
                            name=s["name"],
                            is_array=s["is_array"],
                            is_wildcard=s["is_wildcard"],
                            array_index=s.get("array_index"),
                        )
                        for s in path_segments_data
                    ]

                    root_field = FKPathParser.get_root_field(fk_path)
                    if root_field not in entries:
                        continue

                    values = FKPathParser.extract_values(entries, path_segments)
                    # Filter out None values
                    values = [v for v in values if v is not None]

                    # Parse the reference
                    ref_parts = fk["references"].split(".")
                    if len(ref_parts) != 2:
                        continue

                    ref_context_name, ref_column = ref_parts
                    key = (ref_context_name, ref_column, fk_path, True)

                    for value in values:
                        fk_values_by_ref[key].add(value)
                else:
                    # Simple column FK (existing logic)
                    if fk_path not in entries:
                        continue

                    value = entries[fk_path]

                    # Skip NULL values (allowed)
                    if value is None:
                        continue

                    # Parse the reference
                    ref_parts = fk["references"].split(".")
                    if len(ref_parts) != 2:
                        continue

                    ref_context_name, ref_column = ref_parts
                    key = (ref_context_name, ref_column, fk_path, False)
                    fk_values_by_ref[key].add(value)

        # If no FK values to validate, return early
        if not fk_values_by_ref:
            return {}

        # Step 2: Query valid values for each unique FK in a single query
        # Structure: {(ref_context_name, ref_column, fk_path, is_nested): set of valid values}
        valid_fk_values = {}

        for (
            ref_context_name,
            ref_column,
            fk_path,
            is_nested,
        ), values in fk_values_by_ref.items():
            # Get the referenced context
            ref_context = self.filter(project_id=project_id, name=ref_context_name)
            if not ref_context:
                # Referenced context doesn't exist - mark all logs using this FK as failed
                valid_fk_values[(ref_context_name, ref_column, fk_path, is_nested)] = (
                    set()
                )
                continue

            ref_context_id = ref_context[0][0].id

            # Convert values to JSON strings for query
            json_values = [json.dumps(v) for v in values]

            # Build SQL with proper parameter binding for array
            # Create placeholders for each value
            placeholders = ", ".join([f":val_{i}" for i in range(len(json_values))])

            query_str = f"""
                SELECT DISTINCT l.value
                FROM log l
                JOIN log_event_log lel ON l.id = lel.log_id
                JOIN log_event_context lec ON lel.log_event_id = lec.log_event_id
                WHERE lec.context_id = :context_id
                  AND l.key = :column_name
                  AND l.value::text IN ({placeholders})
            """

            query = text(query_str)

            # Build parameters dict with all values
            params = {
                "context_id": ref_context_id,
                "column_name": ref_column,
            }
            for i, json_val in enumerate(json_values):
                params[f"val_{i}"] = json_val

            result = self.session.execute(query, params)

            # Store set of valid values (convert from jsonb back to Python objects)
            valid_values = set()
            for row in result.fetchall():
                try:
                    # row[0] is already a Python object from jsonb
                    valid_values.add(row[0])
                except (TypeError, ValueError):
                    # If conversion fails, skip this value
                    pass

            valid_fk_values[(ref_context_name, ref_column, fk_path, is_nested)] = (
                valid_values
            )

        # Step 3: Check each log's FK values against valid sets
        failed_validations = {}

        for idx, entries in enumerate(batch_entries):
            for fk in context.foreign_keys:
                fk_path = fk["name"]
                is_nested = fk.get("is_nested", False)

                # For nested FKs, extract values to check
                if is_nested:
                    from orchestra_core.db.utils import FKPathParser, PathSegment

                    path_segments_data = fk.get("path_segments", [])
                    path_segments = [
                        PathSegment(
                            name=s["name"],
                            is_array=s["is_array"],
                            is_wildcard=s["is_wildcard"],
                            array_index=s.get("array_index"),
                        )
                        for s in path_segments_data
                    ]

                    root_field = FKPathParser.get_root_field(fk_path)
                    if root_field not in entries:
                        continue

                    values_to_check = FKPathParser.extract_values(
                        entries,
                        path_segments,
                    )
                    # Filter out None values
                    values_to_check = [v for v in values_to_check if v is not None]

                    # Skip if no values to validate (e.g., empty arrays)
                    if not values_to_check:
                        continue
                else:
                    # Simple column FK
                    if fk_path not in entries:
                        continue

                    value = entries[fk_path]

                    # Skip NULL values
                    if value is None:
                        continue

                    values_to_check = [value]

                # Skip if no values to validate (defensive check)
                if not values_to_check:
                    continue

                # Parse the reference
                ref_parts = fk["references"].split(".")
                if len(ref_parts) != 2:
                    continue

                ref_context_name, ref_column = ref_parts
                key = (ref_context_name, ref_column, fk_path, is_nested)

                # Check if this FK was validated
                if key not in valid_fk_values:
                    # Referenced context doesn't exist
                    failed_validations[idx] = (
                        f"Foreign key constraint violation: Referenced context '{ref_context_name}' does not exist"
                    )
                    break  # Stop checking this log's other FKs

                # Check if all values are in valid set
                invalid_values = [
                    v for v in values_to_check if v not in valid_fk_values[key]
                ]
                if invalid_values:
                    if is_nested:
                        invalid_str = ", ".join([str(v) for v in invalid_values[:3]])
                        if len(invalid_values) > 3:
                            invalid_str += f" (and {len(invalid_values) - 3} more)"
                        failed_validations[idx] = (
                            f"Foreign key constraint violation at path '{fk_path}': "
                            f"Values [{invalid_str}] do not exist in {ref_context_name}.{ref_column}"
                        )
                    else:
                        failed_validations[idx] = (
                            f"Foreign key constraint violation: Value '{invalid_values[0]}' does not exist in "
                            f"{ref_context_name}.{ref_column}"
                        )
                    break  # Stop checking this log's other FKs

        return failed_validations

    # DISABLED: RESTRICT and NO ACTION are not currently supported
    # def check_restrict_constraints(
    #     self,
    #     project_id: int,
    #     context_id: int,
    #     columns_values: Dict[str, List[Any]],
    #     action: str = "DELETE",
    # ) -> List[Dict[str, Any]]:
    #     """Check if deleting/updating values would violate RESTRICT constraints.
    #
    #     Args:
    #         project_id: The project ID
    #         context_id: The context being modified (where values are being deleted/updated)
    #         columns_values: Dict mapping column names to lists of values to check
    #                        e.g., {"id": [1, 2, 3], "code": ["A", "B"]}
    #         action: Either "DELETE" or "UPDATE"
    #
    #     Returns:
    #         List of violations, each containing:
    #         - context: The context being modified
    #         - column: The column being deleted/updated
    #         - value: The specific value
    #         - referencing_context: Context with the FK
    #         - fk_column: FK column name
    #         - count: Number of referencing rows
    #         - fk_action: The FK action type ("on_delete" or "on_update")
    #     """
    #     if not columns_values:
    #         return []
    #
    #     violations = []
    #
    #     # Get the context name for the context being modified
    #     context = self.session.query(Context).filter_by(id=context_id).one_or_none()
    #     if not context:
    #         return []
    #     context_name = context.name
    #
    #     # Find all contexts in this project that have foreign keys
    #     all_contexts = (
    #         self.session.query(Context)
    #         .filter(
    #             Context.project_id == project_id,
    #             Context.foreign_keys != None,  # noqa: E711
    #             Context.foreign_keys != text("'[]'::jsonb"),
    #         )
    #         .all()
    #     )
    #
    #     # Check each context for FKs that reference this context
    #     for ref_context in all_contexts:
    #         if not ref_context.foreign_keys:
    #             continue
    #
    #         for fk in ref_context.foreign_keys:
    #             # Parse the reference: "ContextName.column_name"
    #             ref_parts = fk["references"].split(".")
    #             if len(ref_parts) != 2:
    #                 continue
    #
    #             ref_context_name, ref_column_name = ref_parts
    #
    #             # Check if this FK references our context
    #             if ref_context_name != context_name:
    #                 continue
    #
    #             # Check if the column being deleted/updated is referenced
    #             if ref_column_name not in columns_values:
    #                 continue
    #
    #             # Check the FK action
    #             fk_action_type = "on_delete" if action == "DELETE" else "on_update"
    #             fk_action = fk.get(fk_action_type, "NO ACTION")
    #
    #             # Only enforce RESTRICT and NO ACTION
    #             if fk_action not in ("RESTRICT", "NO ACTION"):
    #                 continue
    #
    #             # Get the FK column name
    #             fk_column = fk["name"]
    #
    #             # For each value being deleted/updated, check if it's referenced
    #             for value in columns_values[ref_column_name]:
    #                 # Skip NULL values
    #                 if value is None:
    #                     continue
    #
    #                 # Convert value to JSON for comparison
    #                 json_str = json.dumps(value)
    #
    #                 # Count how many rows reference this value
    #                 query = text(
    #                     """
    #                     SELECT COUNT(*)
    #                     FROM log l
    #                     JOIN log_event_log lel ON l.id = lel.log_id
    #                     JOIN log_event_context lec ON lel.log_event_id = lec.log_event_id
    #                     WHERE lec.context_id = :context_id
    #                       AND l.key = :fk_column
    #                       AND l.value = CAST(:json_str AS jsonb)
    #                 """,
    #                 )
    #
    #                 result = self.session.execute(
    #                     query,
    #                     {
    #                         "context_id": ref_context.id,
    #                         "fk_column": fk_column,
    #                         "json_str": json_str,
    #                     },
    #                 )
    #                 count = result.scalar()
    #
    #                 if count > 0:
    #                     violations.append(
    #                         {
    #                             "context": context_name,
    #                             "column": ref_column_name,
    #                             "value": value,
    #                             "referencing_context": ref_context.name,
    #                             "fk_column": fk_column,
    #                             "count": count,
    #                             "fk_action": fk_action,
    #                         },
    #                     )
    #
    #     return violations

    def apply_fk_actions(
        self,
        project_id: int,
        context_id: int,
        columns_values: Dict[str, List[Any]],
        action: str = "DELETE",
        new_values: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Apply CASCADE, SET NULL, or SET DEFAULT actions for FK constraints.

        Args:
            project_id: The project ID
            context_id: The context being modified (referenced context)
            columns_values: Dict mapping column names to lists of old values
                           e.g., {"id": [1, 2, 3], "code": ["A", "B"]}
            action: Either "DELETE" or "UPDATE"
            new_values: For UPDATE, the new values being set (optional)

        Returns:
            Statistics about actions taken: {
                "cascaded_deletes": int,
                "cascaded_updates": int,
                "set_null": int,
                "set_default": int,
            }
        """
        if not columns_values:
            return {
                "cascaded_deletes": 0,
                "cascaded_updates": 0,
                "set_null": 0,
                "set_default": 0,
            }

        stats = {
            "cascaded_deletes": 0,
            "cascaded_updates": 0,
            "set_null": 0,
            "set_default": 0,
        }

        # Get the context name for the context being modified
        context = self.session.query(Context).filter_by(id=context_id).one_or_none()
        if not context:
            return stats
        context_name = context.name

        # Find all contexts in this project that have foreign keys
        all_contexts = (
            self.session.query(Context)
            .filter(
                Context.project_id == project_id,
                Context.foreign_keys != None,  # noqa: E711
                Context.foreign_keys != text("'[]'::jsonb"),
            )
            .all()
        )

        # Process each context for FKs that reference this context
        for ref_context in all_contexts:
            if not ref_context.foreign_keys:
                continue

            for fk in ref_context.foreign_keys:
                # Parse the reference: "ContextName.column_name"
                ref_parts = fk["references"].split(".")
                if len(ref_parts) != 2:
                    continue

                ref_context_name, ref_column_name = ref_parts

                # Check if this FK references our context
                if ref_context_name != context_name:
                    continue

                # Check if the column being deleted/updated is referenced
                if ref_column_name not in columns_values:
                    continue

                # Get the FK action
                fk_action_type = "on_delete" if action == "DELETE" else "on_update"
                fk_action = fk.get(fk_action_type, "NO ACTION")

                # Skip RESTRICT and NO ACTION (already handled in check phase)
                if fk_action in ("RESTRICT", "NO ACTION"):
                    continue

                # Get the FK column name or path
                fk_column = fk["name"]
                is_nested = fk.get("is_nested", False)

                # OPTIMIZATION: Batch processing - collect non-NULL values and process together
                non_null_values = [
                    v for v in columns_values[ref_column_name] if v is not None
                ]

                if not non_null_values:
                    continue  # No values to process

                # Branch logic based on whether FK is nested or simple
                if is_nested:
                    # Nested FK: Use JSONB path operations
                    from orchestra_core.db.utils import PathSegment

                    path_segments_data = fk.get("path_segments", [])
                    path_segments = [
                        PathSegment(
                            name=s["name"],
                            is_array=s["is_array"],
                            is_wildcard=s["is_wildcard"],
                            array_index=s.get("array_index"),
                        )
                        for s in path_segments_data
                    ]

                    if fk_action == "CASCADE" and action == "DELETE":
                        # CASCADE DELETE: Delete entire log events containing the nested FK value
                        for old_value in non_null_values:
                            json_str = json.dumps(old_value)
                            stats["cascaded_deletes"] += self._cascade_delete_nested(
                                ref_context.id,
                                fk_column,
                                path_segments,
                                json_str,
                            )
                    elif fk_action == "CASCADE" and action == "UPDATE":
                        # CASCADE UPDATE: Update nested values
                        if new_values and ref_column_name in new_values:
                            new_value = new_values[ref_column_name]
                            json_values = [json.dumps(v) for v in non_null_values]
                            update_count = self._cascade_update_nested_batch(
                                ref_context.id,
                                fk_column,
                                path_segments,
                                json_values,
                                new_value,
                            )
                            stats["cascaded_updates"] += update_count
                    elif fk_action == "SET NULL":
                        # SET NULL: Set nested values to null
                        json_values = [json.dumps(v) for v in non_null_values]
                        stats["set_null"] += self._set_null_nested_batch(
                            ref_context.id,
                            fk_column,
                            path_segments,
                            json_values,
                        )
                else:
                    # Simple FK: Use existing methods
                    if fk_action == "CASCADE" and action == "DELETE":
                        # CASCADE DELETE requires per-value processing for recursive cascading
                        for old_value in non_null_values:
                            json_str = json.dumps(old_value)
                            stats["cascaded_deletes"] += self._cascade_delete(
                                ref_context.id,
                                fk_column,
                                json_str,
                            )
                    elif fk_action == "CASCADE" and action == "UPDATE":
                        # CASCADE UPDATE: Batch update all values at once
                        if new_values and ref_column_name in new_values:
                            new_value = new_values[ref_column_name]
                            json_values = [json.dumps(v) for v in non_null_values]
                            stats["cascaded_updates"] += self._cascade_update_batch(
                                ref_context.id,
                                fk_column,
                                json_values,
                                new_value,
                            )
                    elif fk_action == "SET NULL":
                        # SET NULL: Batch delete all FK column entries at once
                        json_values = [json.dumps(v) for v in non_null_values]
                        stats["set_null"] += self._set_null_batch(
                            ref_context.id,
                            fk_column,
                            json_values,
                        )

                    # DISABLED: SET DEFAULT is not currently supported
                    # elif fk_action == "SET DEFAULT":
                    #     # SET DEFAULT: Update FK column to default value from FK definition
                    #     default_value = fk.get("default")
                    #
                    #     if default_value is None:
                    #         # Raise error instead of falling back to SET NULL
                    #         raise ValueError(
                    #             f"Foreign key '{fk_column}' in context '{ref_context.name}' "
                    #             f"has SET DEFAULT action but no default value specified. "
                    #             f"Add a 'default' field to the foreign key definition or use SET NULL action.",
                    #         )
                    #
                    #     stats["set_default"] += self._set_default(
                    #         ref_context.id,
                    #         fk_column,
                    #         json_str,
                    #         default_value,
                    #     )

        return stats

    def _cascade_delete(
        self,
        context_id: int,
        fk_column: str,
        old_value_json: str,
    ) -> int:
        """Delete all log events where FK column matches old value."""
        return self._cascade_delete(context_id, fk_column, old_value_json)

    def _cascade_delete(
        self,
        context_id: int,
        fk_column: str,
        old_value_json: str,
    ) -> int:
        """JSONB mode: Delete all log events where FK column matches old value."""
        # Find all log_event_ids that reference this value in LogEvent.data
        query = text(
            """
            SELECT DISTINCT le.id, le.project_id, le.data
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND le.data @> jsonb_build_object(:fk_column, CAST(:json_str AS jsonb))
        """,
        )

        result = self.session.execute(
            query,
            {
                "context_id": context_id,
                "fk_column": fk_column,
                "json_str": old_value_json,
            },
        )
        rows = result.fetchall()

        if not rows:
            return 0

        log_event_ids = [row[0] for row in rows]
        project_id = rows[0][1]  # All should have same project_id

        # Before deleting, collect all column values from JSONB data
        # to trigger cascading deletes recursively
        columns_values: Dict[str, List[Any]] = {}
        for _, _, data in rows:
            if data:
                for key, value in data.items():
                    if value is not None:
                        if key not in columns_values:
                            columns_values[key] = []
                        columns_values[key].append(value)

        # Recursively apply FK actions for the context being deleted
        if columns_values:
            self.apply_fk_actions(
                project_id=project_id,
                context_id=context_id,
                columns_values=columns_values,
                action="DELETE",
            )

        # Now delete the log events
        from orchestra.db.dao.log_event_dao import LogEventDAO

        log_event_dao = LogEventDAO(self.session)
        log_event_dao.delete(log_event_ids)

        return len(log_event_ids)

    def _cascade_update(
        self,
        context_id: int,
        fk_column: str,
        old_value_json: str,
        new_value: Any,
    ) -> int:
        """Update all FK column values from old to new."""
        return self._cascade_update(
            context_id,
            fk_column,
            old_value_json,
            new_value,
        )

    def _cascade_update(
        self,
        context_id: int,
        fk_column: str,
        old_value_json: str,
        new_value: Any,
    ) -> int:
        """JSONB mode: Update FK column values from old to new in LogEvent.data."""
        new_value_json = json.dumps(new_value)

        # Update LogEvent.data using jsonb_set
        query = text(
            """
            UPDATE log_event
            SET data = jsonb_set(data, ARRAY[:fk_column], CAST(:new_value AS jsonb))
            WHERE id IN (
                SELECT le.id
                FROM log_event le
                JOIN log_event_context lec ON le.id = lec.log_event_id
                WHERE lec.context_id = :context_id
                  AND le.data @> jsonb_build_object(:fk_column, CAST(:old_value AS jsonb))
            )
        """,
        )

        result = self.session.execute(
            query,
            {
                "context_id": context_id,
                "fk_column": fk_column,
                "old_value": old_value_json,
                "new_value": new_value_json,
            },
        )

        return result.rowcount

    def _set_null(self, context_id: int, fk_column: str, old_value_json: str) -> int:
        """Delete FK column entries (effectively setting to NULL)."""
        return self._set_null(context_id, fk_column, old_value_json)

    def _set_null(
        self,
        context_id: int,
        fk_column: str,
        old_value_json: str,
    ) -> int:
        """JSONB mode: Remove FK column from LogEvent.data (effectively setting to NULL)."""
        # Remove the FK field from LogEvent.data using the - operator
        query = text(
            """
            UPDATE log_event
            SET data = data - :fk_column
            WHERE id IN (
                SELECT le.id
                FROM log_event le
                JOIN log_event_context lec ON le.id = lec.log_event_id
                WHERE lec.context_id = :context_id
                  AND le.data @> jsonb_build_object(:fk_column, CAST(:json_str AS jsonb))
            )
        """,
        )

        result = self.session.execute(
            query,
            {
                "context_id": context_id,
                "fk_column": fk_column,
                "json_str": old_value_json,
            },
        )

        return result.rowcount

    def _cascade_update_batch(
        self,
        context_id: int,
        fk_column: str,
        old_values_json: List[str],
        new_value: Any,
    ) -> int:
        """Update FK column values for multiple old values in a single query.

        This is an optimized version that processes multiple values at once
        using a CTE to update both log and json_log tables in a single query.

        Args:
            context_id: Context ID where FKs are defined
            fk_column: FK column name to update
            old_values_json: List of JSON-serialized old values to find
            new_value: New value to set

        Returns:
            Total number of rows updated across both tables
        """
        if not old_values_json:
            return 0

        return self._cascade_update_batch(
            context_id,
            fk_column,
            old_values_json,
            new_value,
        )

    def _cascade_update_batch(
        self,
        context_id: int,
        fk_column: str,
        old_values_json: List[str],
        new_value: Any,
    ) -> int:
        """JSONB mode: Update FK column values for multiple old values.

        Uses JSONB containment (@>) semantics for type-safe comparison,
        avoiding text conversion issues with booleans and numeric types.
        """
        new_value_json = json.dumps(new_value)

        # Build JSONB array from old values for unnest
        # Each old_val is already a JSON string (e.g., "1", '"hello"', "true")
        array_elements = ", ".join(
            [f"CAST(:old_val_{i} AS jsonb)" for i in range(len(old_values_json))],
        )

        # Use CTE with unnest to check JSONB containment for each old value
        # This avoids large OR chains while maintaining JSONB type semantics
        query_str = f"""
            UPDATE log_event
            SET data = jsonb_set(data, ARRAY[:fk_column], CAST(:new_value AS jsonb))
            WHERE id IN (
                SELECT DISTINCT le.id
                FROM log_event le
                JOIN log_event_context lec ON le.id = lec.log_event_id
                CROSS JOIN unnest(ARRAY[{array_elements}]) AS old_val(v)
                WHERE lec.context_id = :context_id
                  AND le.data @> jsonb_build_object(:fk_column, old_val.v)
            )
        """

        query = text(query_str)

        # Build parameters dict - pass JSON strings directly without transformation
        params = {
            "context_id": context_id,
            "fk_column": fk_column,
            "new_value": new_value_json,
        }
        for i, old_val in enumerate(old_values_json):
            params[f"old_val_{i}"] = old_val

        result = self.session.execute(query, params)
        return result.rowcount

    def _set_null_batch(
        self,
        context_id: int,
        fk_column: str,
        old_values_json: List[str],
    ) -> int:
        """Delete FK column entries for multiple values in a single query.

        Args:
            context_id: Context ID where FKs are defined
            fk_column: FK column name to delete
            old_values_json: List of JSON-serialized old values to find

        Returns:
            Total number of rows deleted
        """
        if not old_values_json:
            return 0

        return self._set_null_batch(context_id, fk_column, old_values_json)

    def _set_null_batch(
        self,
        context_id: int,
        fk_column: str,
        old_values_json: List[str],
    ) -> int:
        """JSONB mode: Remove FK column from LogEvent.data for multiple values.

        Uses JSONB containment (@>) semantics for type-safe comparison,
        avoiding text conversion issues with booleans and numeric types.
        """
        # Build JSONB array from old values for unnest
        # Each old_val is already a JSON string (e.g., "1", '"hello"', "true")
        array_elements = ", ".join(
            [f"CAST(:old_val_{i} AS jsonb)" for i in range(len(old_values_json))],
        )

        # Use CTE with unnest to check JSONB containment for each old value
        # This avoids large OR chains while maintaining JSONB type semantics
        query_str = f"""
            UPDATE log_event
            SET data = data - :fk_column
            WHERE id IN (
                SELECT DISTINCT le.id
                FROM log_event le
                JOIN log_event_context lec ON le.id = lec.log_event_id
                CROSS JOIN unnest(ARRAY[{array_elements}]) AS old_val(v)
                WHERE lec.context_id = :context_id
                  AND le.data @> jsonb_build_object(:fk_column, old_val.v)
            )
        """

        query = text(query_str)

        # Build parameters dict - pass JSON strings directly without transformation
        params = {
            "context_id": context_id,
            "fk_column": fk_column,
        }
        for i, old_val in enumerate(old_values_json):
            params[f"old_val_{i}"] = old_val

        result = self.session.execute(query, params)
        return result.rowcount

    def _cascade_delete_nested_remove_elements(
        self,
        context_id: int,
        fk_path: str,
        path_segments: List,
        old_values_json: List[str],
    ) -> int:
        """Remove matching elements from nested arrays (wildcard paths).

        This handles CASCADE DELETE for wildcard paths like:
        - image_ids[*]: Remove matching primitive values from array
        - images[*].image_id: Remove matching objects from array
        - teams[*].members[*].user_id: Remove matching nested objects

        Args:
            context_id: Context ID where FKs are defined
            fk_path: Full path string (e.g., 'image_ids[*]')
            path_segments: Parsed path segments from FKPathParser
            old_values_json: List of JSON-serialized values to remove

        Returns:
            Number of log entries updated
        """
        from orchestra_core.db.utils import FKPathParser

        if not old_values_json:
            return 0

        # Parse old values
        old_values = [json.loads(v) for v in old_values_json]
        old_values_set = set(old_values)

        # Get root field name
        root_field = FKPathParser.get_root_field(fk_path)

        # Find all logs that need updating
        query = text(
            """
            SELECT l.id, l.value, lec.log_event_id
            FROM log l
            JOIN log_event_log lel ON l.id = lel.log_id
            JOIN log_event_context lec ON lel.log_event_id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND l.key = :root_field
              AND l.value IS NOT NULL
        """,
        )

        result = self.session.execute(
            query,
            {
                "context_id": context_id,
                "root_field": root_field,
            },
        )

        # Process each log and remove matching array elements
        updates_log = []  # (log_id, new_jsonb_value)
        updates_json_log = []  # (log_event_id, new_json_value)

        all_rows = result.fetchall()
        for row in all_rows:
            log_id = row[0]
            root_data = row[1]  # JSONB value, already Python object
            log_event_id = row[2]

            # Make a deep copy to modify
            import copy

            modified_data = copy.deepcopy(root_data)

            # CRITICAL: Wrap root_data in a dict with the root field name
            # The path segments expect {"images": [...]} not just [...]
            wrapped_data = {root_field: modified_data}

            # Remove matching array elements
            removed = self._remove_matching_array_elements(
                wrapped_data,
                path_segments,
                old_values_set,
            )

            if removed:
                # Extract the updated root field value
                updated_root_data = wrapped_data[root_field]
                updates_log.append((log_id, updated_root_data))
                updates_json_log.append((log_event_id, updated_root_data))

        if not updates_log:
            return 0

        # OPTIMIZATION: Perform bulk updates using PostgreSQL unnest
        # Instead of N individual UPDATE queries, we use 2 bulk queries
        update_count = 0

        # Bulk update log table (1 query instead of N)
        log_ids = [log_id for log_id, _ in updates_log]
        log_values = [json.dumps(new_data) for _, new_data in updates_log]

        bulk_log_update = text(
            """
            UPDATE log l
            SET value = CAST(v.new_value AS jsonb)
            FROM (
                SELECT unnest(CAST(:log_ids AS bigint[])) as id,
                       unnest(CAST(:new_values AS text[])) as new_value
            ) v
            WHERE l.id = v.id
        """,
        )
        result = self.session.execute(
            bulk_log_update,
            {
                "log_ids": log_ids,
                "new_values": log_values,
            },
        )
        update_count += result.rowcount

        # Bulk update json_log table (1 query instead of N)
        json_log_event_ids = [log_event_id for log_event_id, _ in updates_json_log]
        json_log_values = [json.dumps(new_data) for _, new_data in updates_json_log]

        bulk_json_log_update = text(
            """
            UPDATE json_log jl
            SET value = CAST(v.new_value AS json)
            FROM (
                SELECT unnest(CAST(:log_event_ids AS bigint[])) as log_event_id,
                       unnest(CAST(:new_values AS text[])) as new_value
            ) v
            JOIN log_event_json_log lejl ON lejl.log_event_id = v.log_event_id
            WHERE jl.id = lejl.json_log_id
              AND jl.key = :root_field
        """,
        )
        result = self.session.execute(
            bulk_json_log_update,
            {
                "log_event_ids": json_log_event_ids,
                "new_values": json_log_values,
                "root_field": root_field,
            },
        )
        update_count += result.rowcount

        return update_count

    def _cascade_delete_nested(
        self,
        context_id: int,
        fk_path: str,
        path_segments: List,
        old_value_json: str,
    ) -> int:
        """Handle CASCADE DELETE for nested paths.

        Behavior depends on whether the path contains wildcards:

        - Wildcard paths (image_ids[*], images[*].image_id):
          Remove matching elements from arrays, keep the log

        - Non-wildcard paths (metadata.author.user_id):
          Delete the entire log event (standard CASCADE behavior)

        Args:
            context_id: Context ID where FKs are defined
            fk_path: Full path string (e.g., 'images[*].image_id')
            path_segments: Parsed path segments from FKPathParser
            old_value_json: JSON-serialized value to find

        Returns:
            Number of log events deleted or updated
        """
        return self._cascade_delete_nested(
            context_id,
            fk_path,
            path_segments,
            old_value_json,
        )

    def _cascade_delete_nested(
        self,
        context_id: int,
        fk_path: str,
        path_segments: List,
        old_value_json: str,
    ) -> int:
        """Handle CASCADE DELETE for nested paths.

        Behavior depends on whether the path contains wildcards:
        - Wildcard paths: Remove matching elements from arrays
        - Non-wildcard paths: Delete entire log events
        """
        from orchestra_core.db.utils import FKPathParser

        has_wildcard = FKPathParser.has_wildcard(path_segments)

        if has_wildcard:
            # Wildcard path: Remove matching elements from arrays
            return self._cascade_delete_nested_remove_elements(
                context_id,
                fk_path,
                path_segments,
                [old_value_json],  # Wrap in list for batch method
            )

        # Non-wildcard nested path: Delete entire log events
        root_field = FKPathParser.get_root_field(fk_path)
        old_value = json.loads(old_value_json)

        # Find all log_event_ids that have this value at the nested path
        # Query LogEvent.data directly
        query = text(
            """
            SELECT DISTINCT le.id, le.data, le.project_id
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND le.data ? :root_field
        """,
        )

        result = self.session.execute(
            query,
            {
                "context_id": context_id,
                "root_field": root_field,
            },
        )

        # Check each log's JSONB data to find matching values
        matching_log_event_ids = []
        project_id = None
        columns_values: Dict[str, List[Any]] = {}

        for row in result.fetchall():
            log_event_id = row[0]
            data = row[1]  # JSONB data, already Python dict
            if project_id is None:
                project_id = row[2]

            # Extract values from this log's data
            try:
                extracted_values = FKPathParser.extract_values(
                    data,
                    path_segments,
                )
                if old_value in extracted_values:
                    matching_log_event_ids.append(log_event_id)
                    # Collect column values for recursive FK actions
                    if data:
                        for key, value in data.items():
                            if value is not None:
                                if key not in columns_values:
                                    columns_values[key] = []
                                columns_values[key].append(value)
            except Exception:
                continue

        if not matching_log_event_ids:
            return 0

        # Recursively apply FK actions for the context being deleted
        if columns_values:
            self.apply_fk_actions(
                project_id=project_id,
                context_id=context_id,
                columns_values=columns_values,
                action="DELETE",
            )

        # Now delete the log events (this will cascade to logs via DB constraints)
        from orchestra.db.dao.log_event_dao import LogEventDAO

        log_event_dao = LogEventDAO(self.session)
        log_event_dao.delete(matching_log_event_ids)

        return len(matching_log_event_ids)

    def _cascade_delete_nested_remove_elements(
        self,
        context_id: int,
        fk_path: str,
        path_segments: List,
        old_values_json: List[str],
    ) -> int:
        """JSONB mode: Remove matching elements from nested arrays (wildcard paths).

        Handles paths like:
        - image_ids[*]: Remove matching primitive values
        - images[*].image_id: Remove matching objects
        - teams[*].members[*].user_id: Remove matching nested objects

        Implementation Notes:
        ---------------------
        This method uses Python-level array manipulation (_remove_matching_array_elements)
        combined with bulk SQL updates that fully replace LogEvent.data. This approach:
        - Reuses tested Python helpers for complex nested path traversal
        - Is correct but rewrites the entire JSONB document on each update

        Performance Considerations:
        - For large JSONB payloads, full document replacement may be costly
        - If profiling shows this as a bottleneck, refactor to use jsonb_set with
          computed path arrays to update only affected nested keys
        - Consider jsonb_path_query for PostgreSQL 12+ to handle wildcards in SQL
        - Keep method signatures unchanged for caller/test compatibility
        """
        from orchestra_core.db.utils import FKPathParser

        if not old_values_json:
            return 0

        # Parse old values
        old_values = [json.loads(v) for v in old_values_json]
        old_values_set = set(old_values)

        # Get root field name
        root_field = FKPathParser.get_root_field(fk_path)

        # Find all logs that need updating
        # Query LogEvent.data directly
        query = text(
            """
            SELECT le.id, le.data
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND le.data ? :root_field
        """,
        )

        result = self.session.execute(
            query,
            {
                "context_id": context_id,
                "root_field": root_field,
            },
        )

        # Process each log and remove matching array elements
        import copy

        updates = []  # (log_event_id, new_data)

        for row in result.fetchall():
            log_event_id = row[0]
            data = row[1]  # JSONB value, already Python object

            if not data or root_field not in data:
                continue

            # Make a deep copy to modify
            modified_data = copy.deepcopy(data)

            # Remove matching array elements using existing helper
            removed = self._remove_matching_array_elements(
                modified_data,
                path_segments,
                old_values_set,
            )

            if removed:
                updates.append((log_event_id, modified_data))

        if not updates:
            return 0

        # OPTIMIZATION: Bulk update using PostgreSQL unnest
        # Single query instead of N individual UPDATEs
        log_event_ids = [log_event_id for log_event_id, _ in updates]
        data_values = [json.dumps(new_data) for _, new_data in updates]

        bulk_update = text(
            """
            UPDATE log_event
            SET data = CAST(v.new_data AS jsonb)
            FROM (
                SELECT unnest(CAST(:log_event_ids AS bigint[])) as id,
                       unnest(CAST(:new_data_values AS text[])) as new_data
            ) v
            WHERE log_event.id = v.id
        """,
        )

        result = self.session.execute(
            bulk_update,
            {
                "log_event_ids": log_event_ids,
                "new_data_values": data_values,
            },
        )

        return result.rowcount

    def _cascade_update_nested_batch(
        self,
        context_id: int,
        fk_path: str,
        path_segments: List,
        old_values_json: List[str],
        new_value: Any,
    ) -> int:
        """Update nested FK values in multiple logs.

        This handles nested FKs like 'images[*].image_id' where we need to find
        all logs that have any of the old values at the nested path and update
        them to the new value.

        For array paths with [*], this updates ALL matching occurrences within each array.

        Args:
            context_id: Context ID where FKs are defined
            fk_path: Full path string (e.g., 'images[*].image_id')
            path_segments: Parsed path segments from FKPathParser
            old_values_json: List of JSON-serialized old values to find
            new_value: New value to set

        Returns:
            Number of log entries updated (log + json_log)
        """
        # Legacy mode removed; only current storage is supported
        return self._cascade_update_nested_batch(
            context_id,
            fk_path,
            path_segments,
            old_values_json,
            new_value,
        )

    def _cascade_update_nested_batch(
        self,
        context_id: int,
        fk_path: str,
        path_segments: List,
        old_values_json: List[str],
        new_value: Any,
    ) -> int:
        """JSONB mode: Update nested FK values in multiple logs.

        Handles nested FKs like 'images[*].image_id' where we need to find
        all logs that have any of the old values at the nested path and update
        them to the new value.

        For array paths with [*], updates ALL matching occurrences within each array.

        Implementation Notes:
        ---------------------
        This method uses Python-level value updates (_update_nested_value) combined
        with bulk SQL updates that fully replace LogEvent.data. This approach:
        - Reuses tested Python helpers for complex nested path traversal
        - Is correct but rewrites the entire JSONB document on each update

        Performance Considerations:
        - For large JSONB payloads, full document replacement may be costly
        - If profiling shows this as a bottleneck, refactor to use jsonb_set with
          computed path arrays to update only affected nested keys
        - Consider jsonb_path_query for PostgreSQL 12+ to handle wildcards in SQL
        - Keep method signatures unchanged for caller/test compatibility
        """
        from orchestra_core.db.utils import FKPathParser

        if not old_values_json:
            return 0

        # Parse old values
        old_values = [json.loads(v) for v in old_values_json]
        old_values_set = set(old_values)

        # Get root field name
        root_field = FKPathParser.get_root_field(fk_path)

        # Find all logs that need updating
        query = text(
            """
            SELECT le.id, le.data
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND le.data ? :root_field
        """,
        )

        result = self.session.execute(
            query,
            {
                "context_id": context_id,
                "root_field": root_field,
            },
        )

        # Process each log and update nested values
        import copy

        updates = []  # (log_event_id, new_data)

        for row in result.fetchall():
            log_event_id = row[0]
            data = row[1]  # JSONB value, already Python object

            if not data:
                continue

            # Make a deep copy to modify
            modified_data = copy.deepcopy(data)

            # Update nested values using existing helper
            updated = self._update_nested_value(
                modified_data,
                path_segments,
                old_values_set,
                new_value,
            )

            if updated:
                updates.append((log_event_id, modified_data))

        if not updates:
            return 0

        # OPTIMIZATION: Bulk update using PostgreSQL unnest
        log_event_ids = [log_event_id for log_event_id, _ in updates]
        data_values = [json.dumps(new_data) for _, new_data in updates]

        bulk_update = text(
            """
            UPDATE log_event
            SET data = CAST(v.new_data AS jsonb)
            FROM (
                SELECT unnest(CAST(:log_event_ids AS bigint[])) as id,
                       unnest(CAST(:new_data_values AS text[])) as new_data
            ) v
            WHERE log_event.id = v.id
        """,
        )

        result = self.session.execute(
            bulk_update,
            {
                "log_event_ids": log_event_ids,
                "new_data_values": data_values,
            },
        )

        return result.rowcount

    def _set_null_nested_batch(
        self,
        context_id: int,
        fk_path: str,
        path_segments: List,
        old_values_json: List[str],
    ) -> int:
        """Set nested FK values to null in multiple logs.

        This handles nested FKs like 'images[*].image_id' where we need to find
        all logs that have any of the old values at the nested path and set
        them to null.

        For array paths with [*], this sets ALL matching occurrences to null.

        Args:
            context_id: Context ID where FKs are defined
            fk_path: Full path string (e.g., 'images[*].image_id')
            path_segments: Parsed path segments from FKPathParser
            old_values_json: List of JSON-serialized old values to find

        Returns:
            Number of log entries updated (log + json_log)
        """
        # Legacy mode removed; only current storage is supported
        return self._set_null_nested_batch(
            context_id,
            fk_path,
            path_segments,
            old_values_json,
        )

    def _set_null_nested_batch(
        self,
        context_id: int,
        fk_path: str,
        path_segments: List,
        old_values_json: List[str],
    ) -> int:
        """JSONB mode: Set nested FK values to null in multiple logs.

        Handles nested FKs like 'images[*].image_id' where we need to find
        all logs that have any of the old values at the nested path and set
        them to null.

        For array paths with [*], sets ALL matching occurrences to null.

        Implementation Notes:
        ---------------------
        This method uses Python-level value updates (_update_nested_value with None)
        combined with bulk SQL updates that fully replace LogEvent.data. This approach:
        - Reuses tested Python helpers for complex nested path traversal
        - Is correct but rewrites the entire JSONB document on each update

        Performance Considerations:
        - For large JSONB payloads, full document replacement may be costly
        - If profiling shows this as a bottleneck, refactor to use jsonb_set with
          computed path arrays to update only affected nested keys
        - Consider jsonb_path_query for PostgreSQL 12+ to handle wildcards in SQL
        - Keep method signatures unchanged for caller/test compatibility
        """
        from orchestra_core.db.utils import FKPathParser

        if not old_values_json:
            return 0

        # Parse old values
        old_values = [json.loads(v) for v in old_values_json]
        old_values_set = set(old_values)

        # Get root field name
        root_field = FKPathParser.get_root_field(fk_path)

        # Find all logs that need updating
        query = text(
            """
            SELECT le.id, le.data
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND le.data ? :root_field
        """,
        )

        result = self.session.execute(
            query,
            {
                "context_id": context_id,
                "root_field": root_field,
            },
        )

        # Process each log and set nested values to null
        import copy

        updates = []  # (log_event_id, new_data)

        for row in result.fetchall():
            log_event_id = row[0]
            data = row[1]  # JSONB value, already Python object

            if not data:
                continue

            # Make a deep copy to modify
            modified_data = copy.deepcopy(data)

            # Set nested values to null using existing helper
            updated = self._update_nested_value(
                modified_data,
                path_segments,
                old_values_set,
                None,  # Set to null
            )

            if updated:
                updates.append((log_event_id, modified_data))

        if not updates:
            return 0

        # OPTIMIZATION: Bulk update using PostgreSQL unnest
        log_event_ids = [log_event_id for log_event_id, _ in updates]
        data_values = [json.dumps(new_data) for _, new_data in updates]

        bulk_update = text(
            """
            UPDATE log_event
            SET data = CAST(v.new_data AS jsonb)
            FROM (
                SELECT unnest(CAST(:log_event_ids AS bigint[])) as id,
                       unnest(CAST(:new_data_values AS text[])) as new_data
            ) v
            WHERE log_event.id = v.id
        """,
        )

        result = self.session.execute(
            bulk_update,
            {
                "log_event_ids": log_event_ids,
                "new_data_values": data_values,
            },
        )

        return result.rowcount

    def _remove_matching_array_elements(
        self,
        data: Any,
        path_segments: List,
        values_to_remove: set,
    ) -> bool:
        """Remove matching elements from arrays in nested structure.

        This handles CASCADE DELETE for wildcard paths by removing array elements
        rather than deleting the entire log.

        Examples:
            Flat arrays (image_ids[*]):
                [1, 2, 3] → [1, 3] (removes 2)

            Nested arrays (images[*].image_id):
                [{id:1}, {id:2}, {id:3}] → [{id:1}, {id:3}]

            Deep nesting (teams[*].members[*].user_id):
                Removes member objects where user_id matches

        Args:
            data: Root data structure (dict)
            path_segments: Path to traverse
            values_to_remove: Set of values to remove

        Returns:
            True if any elements were removed
        """
        if not path_segments:
            return False

        removed = False
        segment = path_segments[0]
        remaining = path_segments[1:]

        if segment.is_array:
            # Handle array segment
            if not isinstance(data, dict) or segment.name not in data:
                return False

            arr = data[segment.name]
            if not isinstance(arr, list):
                return False

            if segment.is_wildcard:
                # Wildcard array - need to check if this is the final wildcard
                if not remaining:
                    # Final wildcard - flat array of primitives (e.g., image_ids[*])
                    # Remove matching primitive values
                    original_len = len(arr)
                    arr[:] = [item for item in arr if item not in values_to_remove]
                    removed = len(arr) < original_len
                else:
                    # More segments after wildcard - check if next segment is also wildcard
                    # Need to determine if we should remove entire objects or recurse deeper

                    # Check if any remaining segment has a wildcard
                    has_nested_wildcard = any(s.is_wildcard for s in remaining)

                    if has_nested_wildcard:
                        # Deep nesting (e.g., teams[*].members[*].user_id)
                        # Recurse into each array element
                        for item in arr:
                            if self._remove_matching_array_elements(
                                item,
                                remaining,
                                values_to_remove,
                            ):
                                removed = True
                    else:
                        # Single wildcard with nested field (e.g., images[*].image_id)
                        # Remove entire objects where nested value matches
                        original_len = len(arr)
                        arr[:] = [
                            item
                            for item in arr
                            if not self._object_contains_value(
                                item,
                                remaining,
                                values_to_remove,
                            )
                        ]
                        removed = len(arr) < original_len
            else:
                # Specific index
                idx = segment.array_index
                if idx is not None and 0 <= idx < len(arr):
                    if remaining:
                        if self._remove_matching_array_elements(
                            arr[idx],
                            remaining,
                            values_to_remove,
                        ):
                            removed = True
        else:
            # Handle dict segment
            if not isinstance(data, dict) or segment.name not in data:
                return False

            if remaining:
                # Recurse deeper
                if self._remove_matching_array_elements(
                    data[segment.name],
                    remaining,
                    values_to_remove,
                ):
                    removed = True

        return removed

    def _object_contains_value(
        self,
        obj: Any,
        path_segments: List,
        values_to_check: set,
    ) -> bool:
        """Check if an object contains a specific value at a given path.

        Used by _remove_matching_array_elements to determine which array
        elements should be removed.

        Args:
            obj: Object to check (dict or primitive)
            path_segments: Remaining path segments to traverse
            values_to_check: Set of values to look for

        Returns:
            True if the object contains any of the values at the path
        """
        if not path_segments:
            return False

        segment = path_segments[0]
        remaining = path_segments[1:]

        if segment.is_array:
            # Shouldn't happen in this context, but handle gracefully
            return False

        if not isinstance(obj, dict) or segment.name not in obj:
            return False

        current_value = obj[segment.name]

        if not remaining:
            # Final segment - check the value
            return current_value in values_to_check
        else:
            # Recurse deeper
            return self._object_contains_value(
                current_value,
                remaining,
                values_to_check,
            )

    def _update_nested_value(
        self,
        data: Any,
        path_segments: List,
        old_values_set: set,
        new_value: Any,
    ) -> bool:
        """Recursively update nested values in a data structure.

        This helper method traverses a data structure following the path segments
        and replaces any values in old_values_set with new_value.

        Args:
            data: The data structure to modify (dict or list)
            path_segments: List of PathSegment objects defining the path
            old_values_set: Set of old values to replace
            new_value: New value to set (can be None for SET NULL)

        Returns:
            True if any values were updated, False otherwise
        """
        if not path_segments:
            return False

        updated = False
        segment = path_segments[0]
        remaining = path_segments[1:]

        if segment.is_array:
            # Handle array segment
            if not isinstance(data, dict) or segment.name not in data:
                return False

            arr = data[segment.name]
            if not isinstance(arr, list):
                return False

            if segment.is_wildcard:
                # Process all array elements
                for i, item in enumerate(arr):
                    if remaining:
                        # Recurse into nested structure (e.g., images[*].image_id)
                        if self._update_nested_value(
                            item,
                            remaining,
                            old_values_set,
                            new_value,
                        ):
                            updated = True
                    else:
                        # Flat array wildcard (e.g., image_ids[*])
                        # Check if this primitive value matches and update in-place
                        if item in old_values_set:
                            arr[i] = new_value
                            updated = True
            else:
                # Process specific index
                idx = segment.array_index
                if idx is not None and 0 <= idx < len(arr):
                    if remaining:
                        if self._update_nested_value(
                            arr[idx],
                            remaining,
                            old_values_set,
                            new_value,
                        ):
                            updated = True
        else:
            # Handle dict segment
            if not isinstance(data, dict) or segment.name not in data:
                return False

            if remaining:
                # Recurse deeper
                if self._update_nested_value(
                    data[segment.name],
                    remaining,
                    old_values_set,
                    new_value,
                ):
                    updated = True
            else:
                # Final segment - check and update value
                current_value = data[segment.name]
                if current_value in old_values_set:
                    data[segment.name] = new_value
                    updated = True

        return updated

    # DISABLED: SET DEFAULT is not currently supported
    # def _set_default(
    #     self,
    #     context_id: int,
    #     fk_column: str,
    #     old_value_json: str,
    #     default_value: Any,
    # ) -> int:
    #     """Update FK column to default value."""
    #     # This is similar to CASCADE UPDATE but uses default value
    #     return self._cascade_update(
    #         context_id,
    #         fk_column,
    #         old_value_json,
    #         default_value,
    #     )
    #
    # def _get_default_value(self, context: Context, fk_column: str) -> Optional[Any]:
    #     """
    #     DEPRECATED: Get default value for FK column from context definition.
    #
    #     This method is deprecated. Default values should now be specified
    #     in the foreign key definition's 'default' field.
    #
    #     Args:
    #         context: The context object
    #         fk_column: The foreign key column name
    #
    #     Returns:
    #         None (deprecated functionality)
    #     """
    #     # This method is no longer used as of the new FK default field implementation
    #     # Kept for backwards compatibility but returns None
    #     return None

    def create(
        self,
        project_id: int,
        name: str,
        description: Optional[str] = None,
        is_versioned: bool = False,
        allow_duplicates: bool = True,
        unique_keys: Optional[Dict[str, str]] = None,
        auto_counting: Optional[Dict[str, Optional[str]]] = None,
        foreign_keys: Optional[List[Dict[str, Any]]] = None,
    ) -> int:
        """Create a new context using upsert to handle race conditions."""
        from orchestra_core.db.dao.field_type_dao import FieldTypeDAO

        ts = datetime.now(timezone.utc)

        self._validate_description(description)

        # Validate foreign keys if provided
        if foreign_keys:
            self._validate_foreign_keys_config(project_id, name, foreign_keys)

        # Extract names and types from unique_keys dict
        unique_key_names = list(unique_keys.keys()) if unique_keys else []
        unique_key_types = list(unique_keys.values()) if unique_keys else []

        # Convert foreign_keys list to proper format for storage
        foreign_keys_json = foreign_keys if foreign_keys else []

        stmt = pg_insert(Context).values(
            project_id=project_id,
            name=name,
            description=description,
            created_at=ts,
            updated_at=ts,
            is_versioned=is_versioned,
            allow_duplicates=allow_duplicates,
            unique_key_names=unique_key_names,
            unique_key_types=unique_key_types,
            auto_counting=auto_counting or {},
            foreign_keys=foreign_keys_json,
        )

        # On conflict, do nothing and return the existing context's id
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["project_id", "name"],
        ).returning(Context.id)

        result = self.session.execute(stmt)
        context_id = result.scalar()

        if context_id is None:
            # If insert failed due to conflict, retrieve the existing context
            context_raw = self.filter(project_id=project_id, name=name)
            if context_raw:
                context_id = context_raw[0][0].id
            else:
                raise ValueError(f"Failed to create or retrieve context {name}")

        # If unique_keys is provided, ensure the FieldType exists for each column
        if unique_keys:
            field_type_dao = FieldTypeDAO(self.session)

            # Get the context to access the preserved order
            context_obj = self.session.query(Context).filter_by(id=context_id).one()
            ordered_columns = context_obj.unique_key_names or list(unique_keys.keys())

            # Ensure we iterate in the correct order
            for col_name in ordered_columns:
                if col_name not in unique_keys:
                    continue
                col_type = unique_keys[col_name]
                field_type = field_type_dao.get_by_name_and_context(
                    project_id,
                    col_name,
                    context_id,
                )
                if not field_type:
                    # Get initial value based on type
                    from orchestra.web.api.log.python2SQL.constants import (
                        get_default_value_for_type,
                    )

                    initial_value = get_default_value_for_type(col_type)

                    # Create the field type
                    # Set unique=True only for single unique keys
                    is_unique = len(unique_keys) == 1
                    field_type_dao.create_field_type_if_absent(
                        project_id=project_id,
                        field_name=col_name,
                        value=initial_value,
                        context_id=context_id,
                        field_category="entry",
                        mutable=False,  # Unique key fields should be immutable
                        unique=is_unique,  # Only set True for single unique keys
                        description=f"{'Unique' if is_unique else 'Composite unique'} key component ({col_type}).",
                        field_type=col_type,
                    )
        self.session.commit()
        return context_id

    def bulk_create(
        self,
        project_id: int,
        contexts: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Create multiple contexts in a single database transaction.

        Args:
            project_id: ID of the project to create contexts in
            contexts: List of dictionaries with context data:
                - name: str (required)
                - description: Optional[str]
                - is_versioned: bool (default False)
                - allow_duplicates: bool (default True)
                - unique_keys: Optional[Dict[str, str]]
                - auto_counting: Optional[Dict[str, Optional[str]]]

        Returns:
            Dictionary with:
                - created: List of successfully created context names
                - errors: List of errors with index, name, and error message
        """
        if not contexts:
            return {"created": [], "errors": []}

        created_contexts = []
        errors = []

        try:
            # Validate all contexts first
            for idx, context_data in enumerate(contexts):
                try:
                    name = context_data.get("name")
                    if name is None:
                        errors.append(
                            {
                                "index": idx,
                                "name": "unknown",
                                "error": "Context name is required",
                            },
                        )
                        continue

                    # Normalize name: remove leading slash to treat '/exp1/name1' the same as 'exp1/name1'
                    name = name.lstrip("/")

                    # Validate name format
                    if not re.match(r"^[a-zA-Z0-9\_\-/]+$", name) or "//" in name:
                        errors.append(
                            {
                                "index": idx,
                                "name": name,
                                "error": "Invalid context name. Names can only contain alphanumeric characters, underscores, dashes, and forward slashes. Consecutive slashes are not allowed.",
                            },
                        )
                        continue

                    # Validate description length
                    description = context_data.get("description")
                    if description is not None:
                        try:
                            self._validate_description(description)
                        except ValueError as e:
                            errors.append(
                                {
                                    "index": idx,
                                    "name": name,
                                    "error": str(e),
                                },
                            )
                            continue

                    # Check if context already exists
                    existing = self.filter(project_id=project_id, name=name)
                    if existing:
                        errors.append(
                            {
                                "index": idx,
                                "name": name,
                                "error": "A context with this name already exists in the project.",
                            },
                        )
                        continue

                except Exception as e:
                    errors.append(
                        {
                            "index": idx,
                            "name": context_data.get("name", "unknown"),
                            "error": str(e),
                        },
                    )
                    continue

            # Create all valid contexts
            for idx, context_data in enumerate(contexts):
                try:
                    name = context_data.get("name", "").lstrip("/")

                    # Skip if we already recorded an error for this context
                    if any(e["index"] == idx for e in errors):
                        continue

                    # Create the context
                    self.create(
                        project_id=project_id,
                        name=name,
                        description=context_data.get("description"),
                        is_versioned=context_data.get("is_versioned", False),
                        allow_duplicates=context_data.get("allow_duplicates", True),
                        unique_keys=context_data.get("unique_keys"),
                        auto_counting=context_data.get("auto_counting"),
                        foreign_keys=context_data.get("foreign_keys"),
                    )
                    created_contexts.append(name)

                except Exception as e:
                    # If creation fails, add to errors
                    errors.append(
                        {
                            "index": idx,
                            "name": name,
                            "error": str(e),
                        },
                    )
                    # Rollback the transaction to maintain consistency
                    self.session.rollback()
                    # Re-add successfully created contexts in this transaction
                    for created_name in created_contexts:
                        try:
                            # Check if it still exists (wasn't rolled back)
                            existing = self.filter(
                                project_id=project_id,
                                name=created_name,
                            )
                            if not existing:
                                # Re-create it
                                matching_context = next(
                                    (
                                        c
                                        for c in contexts
                                        if c.get("name", "").lstrip("/") == created_name
                                    ),
                                    None,
                                )
                                if matching_context:
                                    self.create(
                                        project_id=project_id,
                                        name=created_name,
                                        description=matching_context.get("description"),
                                        is_versioned=matching_context.get(
                                            "is_versioned",
                                            False,
                                        ),
                                        allow_duplicates=matching_context.get(
                                            "allow_duplicates",
                                            True,
                                        ),
                                        unique_keys=matching_context.get("unique_keys"),
                                        auto_counting=matching_context.get(
                                            "auto_counting",
                                        ),
                                        foreign_keys=matching_context.get(
                                            "foreign_keys",
                                        ),
                                    )
                        except:
                            # If re-creation fails, remove from created list
                            created_contexts.remove(created_name)

        except Exception as e:
            self.session.rollback()
            raise ValueError(f"Failed to bulk create contexts: {str(e)}")

        return {
            "created": created_contexts,
            "errors": errors,
        }

    def filter(
        self,
        id: Optional[int] = None,
        project_id: Optional[int] = None,
        name: Optional[str] = None,
    ) -> List[Context]:
        query = select(Context)

        if id:
            query = query.where(Context.id == id)
        if project_id:
            query = query.where(Context.project_id == project_id)
        if name is not None:
            query = query.where(Context.name == name)

        rows = self.session.execute(query)
        return rows.fetchall()

    def update(
        self,
        id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        query = select(Context)
        query = query.where(Context.id == id)
        raw = self.session.execute(query)
        entry = raw.scalars().first()

        if entry is not None:
            if name is not None:
                # check if name is valid
                if not re.match(r"^[a-zA-Z0-9_/]+$", name):
                    raise ValueError(
                        "Context name must contain only alphanumeric characters and '/'",
                    )
                setattr(entry, "name", name)
            if description is not None:  # Allow setting description to None
                setattr(entry, "description", description)
            self.session.commit()
        else:
            raise ValueError(f"Context with id {id} not found")

    def rename_with_children(
        self,
        project_id: int,
        old_prefix: str,
        new_prefix: str,
    ) -> int:
        """Rename a context and all its children by replacing the name prefix.

        Returns the number of contexts renamed.
        """
        if not re.match(r"^[a-zA-Z0-9_/]+$", new_prefix):
            raise ValueError(
                "Context name must contain only alphanumeric characters and '/'",
            )
        old_len = len(old_prefix)
        stmt = (
            update(Context)
            .where(
                Context.project_id == project_id,
                Context.name.like(f"{old_prefix}/%"),
            )
            .values(
                name=func.concat(new_prefix, func.substring(Context.name, old_len + 1)),
            )
        )
        child_result = self.session.execute(stmt)

        parent_stmt = (
            update(Context)
            .where(
                Context.project_id == project_id,
                Context.name == old_prefix,
            )
            .values(name=new_prefix)
        )
        parent_result = self.session.execute(parent_stmt)

        total = child_result.rowcount + parent_result.rowcount
        self.session.commit()
        return total

    def delete(self, id: int, skip_embedding_cleanup: bool = False) -> None:
        """
        Delete a context and clean up associated data in phases.

        Phase 0: Collect the context's log_event_ids (used by all later phases)
        Phase 1: Sibling cleanup for Assistants/UnityTests (batched)
        Phase 2: GCS media cleanup (must happen while log data exists)
        Phase 3: Delete context row (cascades log_event_context)
        Phase 4: Scoped orphan detection + embedding cleanup + log deletion
        """
        from orchestra_core.db.dao.embedding_dao import EmbeddingDAO
        from orchestra.db.dao.log_event_dao import LogEventDAO
        from orchestra.db.dao.plot_dao import PlotDAO
        from orchestra_core.db.dao.sibling_context_cleanup import (
            get_assistants_sibling_context_info,
            remove_logs_from_sibling_contexts,
        )
        from orchestra.db.dao.table_view_dao import TableViewDAO

        try:
            context = self.session.query(Context).filter_by(id=id).one()
            project = context.project
            context_name = context.name
            project_id = project.id

            logger.info(
                f"Starting phased deletion of context {id} ('{context_name}') "
                f"in project {project_id}",
            )

            # ── Phase 0: Collect log_event_ids once, reuse everywhere ──
            log_event_ids = [
                row[0]
                for row in self.session.execute(
                    text(
                        "SELECT log_event_id FROM log_event_context "
                        "WHERE context_id = :ctx_id",
                    ),
                    {"ctx_id": id},
                ).fetchall()
            ]

            # Delete plots that reference this context
            # Plots store context as a string in project_config JSONB, not as a FK
            plot_dao = PlotDAO(self.session)
            deleted_plots = plot_dao.delete_by_project(
                project_id=project_id,
                context=context_name,
            )
            if deleted_plots > 0:
                logger.info(
                    f"Deleted {deleted_plots} plots for context '{context_name}' "
                    f"in project {project_id}",
                )

            # Delete table views that reference this context
            # Table views store context as a string in project_config JSONB, not as FK
            table_view_dao = TableViewDAO(self.session)
            deleted_table_views = table_view_dao.delete_by_project(
                project_id=project_id,
                context=context_name,
            )
            if deleted_table_views > 0:
                logger.info(
                    f"Deleted {deleted_table_views} table views for context "
                    f"'{context_name}' in project {project_id}",
                )

            # ── Phase 1: Sibling cleanup (batched queries) ──
            # For Assistants/UnityTests projects, clean up sibling contexts first.
            # This must happen BEFORE deleting the context while associations exist.
            is_assistants_project = project.name in ("Assistants", "UnityTests")

            if is_assistants_project and "/" in context_name and log_event_ids:
                sibling_map = get_assistants_sibling_context_info(
                    session=self.session,
                    project_id=project_id,
                    context_id=id,
                    context_name=context_name,
                    log_event_ids=log_event_ids,
                    context_dao=self,
                )

                if sibling_map:
                    removed = remove_logs_from_sibling_contexts(
                        self.session,
                        sibling_map,
                    )
                    self.session.flush()
                    logger.info(
                        f"Phase 1: Removed {removed} sibling context associations",
                    )

            # ── Phase 2: GCS media cleanup ──
            # Delete associated GCS media BEFORE deleting the context,
            # because log_event data is needed to locate GCS objects.
            if log_event_ids:
                log_event_dao = LogEventDAO(self.session, self)
                log_event_dao._bulk_delete_gcs_media(log_event_ids, project_id)

            # ── Phase 3: Delete context row (cascades log_event_context) ──
            # Proceed with deleting the context from the database.
            # This CASCADE-deletes from log_event_context, severing log
            # associations and allowing the Phase 4 orphan query to work.
            self.session.delete(context)
            self.session.flush()  # Ensure the context deletion cascades

            # ── Phase 4: Scoped orphan cleanup ──
            # Uses scoped query on this context's log_event_ids instead of
            # scanning all logs in the project (23ms vs 632ms on prod).
            if log_event_ids:
                if not skip_embedding_cleanup:
                    # Find which of our logs are now orphaned (no context left)
                    orphaned_ids = [
                        row[0]
                        for row in self.session.execute(
                            text(
                                "SELECT le.id FROM log_event le "
                                "WHERE le.id = ANY(:ids) "
                                "AND NOT EXISTS ("
                                "  SELECT 1 FROM log_event_context lec "
                                "  WHERE lec.log_event_id = le.id"
                                ")",
                            ),
                            {"ids": log_event_ids},
                        ).fetchall()
                    ]

                    if orphaned_ids:
                        embedding_dao = EmbeddingDAO(self.session)
                        cancelled = embedding_dao.cancel_queue(
                            log_event_ids=orphaned_ids,
                            reason="Context deleted",
                        )
                        soft_deleted = embedding_dao.soft_delete(
                            log_event_ids=orphaned_ids,
                        )

                        if soft_deleted > 0 or cancelled > 0:
                            logger.info(
                                f"Phase 4a: Cancelled {cancelled} queue items, "
                                f"soft-deleted {soft_deleted} embeddings for "
                                f"{len(orphaned_ids)} orphaned logs",
                            )

                        # Batched orphan log deletion with SKIP LOCKED.
                        # SKIP LOCKED avoids blocking on rows locked by
                        # concurrent embedding workers, preventing deadlocks.
                        batch_size = 5000
                        total_deleted = 0
                        while True:
                            result = self.session.execute(
                                text(
                                    "WITH batch AS ("
                                    "  SELECT id FROM log_event"
                                    "  WHERE id = ANY(:ids)"
                                    "  LIMIT :batch_size"
                                    "  FOR UPDATE SKIP LOCKED"
                                    ") "
                                    "DELETE FROM log_event "
                                    "WHERE id IN (SELECT id FROM batch)",
                                ),
                                {
                                    "ids": orphaned_ids,
                                    "batch_size": batch_size,
                                },
                            )
                            deleted = result.rowcount
                            self.session.commit()
                            if deleted == 0:
                                break
                            total_deleted += deleted

                        if total_deleted > 0:
                            logger.info(
                                f"Phase 4b: Deleted {total_deleted} orphaned "
                                f"log events in batches",
                            )
                else:
                    # Called from higher-level deletion (e.g. ProjectDAO) that
                    # handles embedding cleanup at project scope -- just find
                    # and delete orphan logs.
                    delete_orphaned_log_events(
                        self.session,
                        project_id,
                        skip_embedding_cleanup=True,
                        log_event_ids=log_event_ids,
                    )

            self.session.commit()

            logger.info(
                f"Context {id} ('{context_name}') deleted successfully",
            )
        except Exception as e:
            self.session.rollback()
            raise ValueError(f"Failed to delete context with id {id}: {e}")

    def get_or_create(
        self,
        project_id: int,
        name: str,
        description: Optional[str] = None,
        is_versioned: bool = False,
        allow_duplicates: bool = True,
        unique_keys: Optional[Dict[str, str]] = None,
        auto_counting: Optional[Dict[str, Optional[str]]] = None,
        foreign_keys: Optional[List[Dict[str, Any]]] = None,
    ) -> int:
        """
        Get or create a context using upsert.

        If the context doesn't exist, it will be created with the provided parameters.
        This method ensures a context is always returned, creating one implicitly if needed.

        Args:
            project_id: ID of the project to associate the context with
            name: Name of the context
            description: Optional description of the context
            is_versioned: Whether the context should be versioned

        Returns:
            The ID of the existing or newly created context
        """
        try:
            self._validate_description(description)
            # First try to find the context
            contexts = self.filter(project_id=project_id, name=name)
            if contexts:
                # Context exists, return its ID
                return contexts[0][0].id

            # Context doesn't exist, create it
            ts = datetime.now(timezone.utc)

            # Use description if provided, otherwise use a default
            actual_description = (
                description if description is not None else "default context"
            )

            # Extract names and types from unique_keys dict
            unique_key_names = list(unique_keys.keys()) if unique_keys else []
            unique_key_types = list(unique_keys.values()) if unique_keys else []

            # Convert foreign_keys list to proper format for storage
            foreign_keys_json = foreign_keys if foreign_keys else []

            # Create the context
            stmt = pg_insert(Context).values(
                project_id=project_id,
                name=name,
                description=actual_description,
                created_at=ts,
                updated_at=ts,
                is_versioned=is_versioned,
                allow_duplicates=allow_duplicates,
                unique_key_names=unique_key_names,
                unique_key_types=unique_key_types,
                auto_counting=auto_counting or {},
                foreign_keys=foreign_keys_json,
            )

            # On conflict, do nothing and return the existing context's id
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["project_id", "name"],
            ).returning(Context.id)

            result = self.session.execute(stmt)
            context_id = result.scalar()

            if context_id is None:
                # If insert failed due to conflict, retrieve the existing context
                # This handles race conditions where the context was created between our check and insert
                contexts = self.filter(project_id=project_id, name=name)
                if contexts:
                    context_id = contexts[0][0].id
                else:
                    # This should rarely happen, but we'll create a default context as a fallback
                    fallback_stmt = (
                        pg_insert(Context)
                        .values(
                            project_id=project_id,
                            name=name,
                            description="default context",
                            created_at=ts,
                            updated_at=ts,
                            is_versioned=False,
                            allow_duplicates=allow_duplicates,
                            unique_key_names=unique_key_names,
                            unique_key_types=unique_key_types,
                            auto_counting=auto_counting or {},
                            foreign_keys=foreign_keys_json,
                        )
                        .returning(Context.id)
                    )

                    fallback_result = self.session.execute(fallback_stmt)
                    context_id = fallback_result.scalar()

                    if context_id is None:
                        raise ValueError(f"Failed to create or retrieve context {name}")

            self.session.commit()
            return context_id

        except Exception as e:
            self.session.rollback()
            # As a last resort, try to create the default context
            try:
                return self.create(
                    project_id=project_id,
                    name=name,
                    description="default context",
                    is_versioned=False,
                    allow_duplicates=allow_duplicates,
                    unique_keys=unique_keys,
                )
            except Exception:
                raise ValueError(
                    f"Failed to create or retrieve context {name}: {str(e)}",
                )

    def add_logs(self, context_id: int, log_ids: List[int]) -> None:
        """Associate LogEvent instances with the specified context.

        Args:
            context_id: ID of the context to associate logs with
            log_ids: List of log event IDs to associate with the context

        Raises:
            ValueError: If context_id doesn't exist or any log_ids don't exist
            ValueError: If duplicates are found and context doesn't allow duplicates
        """
        try:
            # Get the context to check if duplicates are allowed
            context = self.session.query(Context).filter_by(id=context_id).one_or_none()
            if not context:
                raise ValueError(f"Context with id {context_id} not found")

            # Get all log events
            log_events = (
                self.session.query(LogEvent).filter(LogEvent.id.in_(log_ids)).all()
            )
            found_ids = {log.id for log in log_events}
            missing_ids = set(log_ids) - found_ids

            if missing_ids:
                raise ValueError(f"Log events with ids {missing_ids} not found")

            # Check for duplicates if the context doesn't allow them
            if not context.allow_duplicates:
                for log_event in log_events:
                    if self.check_for_duplicates(context_id, log_event.id):
                        raise ValueError(
                            f"Duplicate log entry detected. Context '{context.name}' does not allow duplicates.",
                        )

            # Create associations between log events and context
            for log_event in log_events:
                association = LogEventContext(
                    log_event_id=log_event.id,
                    context_id=context_id,
                )
                self.session.add(association)

            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def is_versioned(self, context_id: int) -> bool:
        context = self.session.query(Context).filter_by(id=context_id).one_or_none()
        return context and context.is_versioned

    def get_context_id(self, project_id: int, body):
        if body:
            allow_duplicates = getattr(body, "allow_duplicates", True)
            unique_keys = getattr(body, "unique_keys", None)
            return self.get_or_create(
                project_id=project_id,
                name=body.name,
                description=body.description,
                is_versioned=body.is_versioned,
                allow_duplicates=allow_duplicates,
                unique_keys=unique_keys,
            )
        else:
            # Create or get default context using upsert
            return self.get_or_create(
                project_id=project_id,
                name="",
                description="default context",
                is_versioned=False,
                unique_keys=None,
            )

    def check_for_duplicates(self, context_id: int, log_event_id: int) -> bool:
        """
        Check if a log event would create duplicates in the context using a single SQL query.

        Args:
            context_id: ID of the context to check
            log_event_id: ID of the log event to check for duplicates

        Returns:
            True if duplicates are found, False otherwise
        """
        query = """
        WITH new_log_pairs AS (
            SELECT l.key, l.value
            FROM log l
            JOIN log_event_log lel ON l.id = lel.log_id
            WHERE lel.log_event_id = :log_event_id
        ),
        context_log_events AS (
            SELECT le.id
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id AND le.id != :log_event_id
        ),
        potential_duplicates AS (
            SELECT
                cle.id,
                COUNT(*) as pair_count
            FROM context_log_events cle
            JOIN log_event_log lel ON cle.id = lel.log_event_id
            JOIN log l ON lel.log_id = l.id
            GROUP BY cle.id
            HAVING COUNT(*) = (SELECT COUNT(*) FROM new_log_pairs)
        ),
        matching_pairs AS (
            SELECT
                pd.id,
                COUNT(*) as matching_count
            FROM potential_duplicates pd
            JOIN log_event_log lel ON pd.id = lel.log_event_id
            JOIN log l ON lel.log_id = l.id
            JOIN new_log_pairs nlp ON l.key = nlp.key AND l.value = nlp.value
            GROUP BY pd.id
        )
        SELECT EXISTS (
            SELECT 1 FROM matching_pairs mp
            JOIN potential_duplicates pd ON mp.id = pd.id
            WHERE mp.matching_count = pd.pair_count
        ) as has_duplicate
        """
        result = self.session.execute(
            text(query),
            {"context_id": context_id, "log_event_id": log_event_id},
        )
        return result.scalar()

    def check_for_duplicates_subset(
        self,
        context_id: int,
        log_event_id: int,
        keys_to_check: List[str],
    ) -> bool:
        """
        Check for duplicates based only on a subset of keys.

        Returns True if there exists another log_event in the same context whose
        values for keys_to_check match the updated log_event's values for those keys.

        Note: For batch operations, use `check_for_duplicates_subset_batch` to avoid
        N+1 queries. This method executes one query per call.
        """
        if not keys_to_check:
            return False

        query = """
        WITH updated_pairs AS (
            SELECT l.key, l.value
            FROM log l
            JOIN log_event_log lel ON l.id = lel.log_id
            WHERE lel.log_event_id = :log_event_id AND l.key = ANY(:keys)
        ),
        context_other_events AS (
            SELECT le.id
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id AND le.id != :log_event_id
        ),
        matching_other AS (
            SELECT cle.id, COUNT(*) AS match_count
            FROM context_other_events cle
            JOIN log_event_log lel ON cle.id = lel.log_event_id
            JOIN log l ON lel.log_id = l.id
            JOIN updated_pairs up ON up.key = l.key AND up.value = l.value
            WHERE l.key = ANY(:keys)
            GROUP BY cle.id
        )
        SELECT EXISTS (
            SELECT 1 FROM matching_other WHERE match_count = :num_keys
        ) AS has_duplicate
        """
        result = self.session.execute(
            text(query),
            {
                "context_id": context_id,
                "log_event_id": log_event_id,
                "keys": keys_to_check,
                "num_keys": len(keys_to_check),
            },
        )
        return result.scalar()

    def check_for_duplicates_subset_batch(
        self,
        context_id: int,
        log_event_ids: List[int],
        keys_to_check: List[str],
    ) -> List[int]:
        """
        Batch check for duplicates based on a subset of keys for multiple log events.

        This method checks which log_event_ids have duplicates in the context by
        comparing only the specified keys in LogEvent.data JSONB columns. It uses
        a single efficient SQL query instead of N individual queries.

        Args:
            context_id: ID of the context to check
            log_event_ids: List of log event IDs to check for duplicates
            keys_to_check: List of field keys to compare for duplicate detection

        Returns:
            List of log_event_ids that have duplicates (should be marked as failed)
        """
        if not log_event_ids or not keys_to_check:
            return []

        # Build the key extraction for JSONB comparison
        # We extract the specified keys from LogEvent.data and compare them
        query = """
        WITH check_logs AS (
            SELECT le.id, le.data
            FROM log_event le
            WHERE le.id = ANY(:log_event_ids)
        ),
        existing_logs AS (
            SELECT le.id, le.data
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND le.id != ALL(:log_event_ids)
        ),
        duplicates AS (
            SELECT DISTINCT cl.id
            FROM check_logs cl
            WHERE EXISTS (
                SELECT 1 FROM existing_logs el
                WHERE (
                    SELECT jsonb_object_agg(k, el.data->k)
                    FROM unnest(:keys) AS k
                    WHERE el.data ? k
                ) = (
                    SELECT jsonb_object_agg(k, cl.data->k)
                    FROM unnest(:keys) AS k
                    WHERE cl.data ? k
                )
            )
        )
        SELECT id FROM duplicates
        """
        result = self.session.execute(
            text(query),
            {
                "context_id": context_id,
                "log_event_ids": log_event_ids,
                "keys": keys_to_check,
            },
        )
        return [row[0] for row in result.fetchall()]

    def check_for_duplicates_batch(
        self,
        context_id: int,
        log_event_ids: List[int],
    ) -> List[int]:
        """
        Check for duplicates in JSONB mode for a batch of log events.

        This method checks which log_event_ids have duplicates in the context by
        comparing LogEvent.data JSONB columns. It uses a single efficient SQL query
        instead of N individual queries.

        Args:
            context_id: ID of the context to check
            log_event_ids: List of log event IDs to check for duplicates

        Returns:
            List of log_event_ids that have duplicates (should be deleted/rejected)
        """
        if not log_event_ids:
            return []

        # Use a single SQL query to find all duplicates in the batch
        # This compares LogEvent.data JSONB columns for exact matches
        query = """
        WITH new_logs AS (
            SELECT le.id, le.data
            FROM log_event le
            WHERE le.id = ANY(:log_event_ids)
        ),
        existing_logs AS (
            SELECT le.id, le.data
            FROM log_event le
            JOIN log_event_context lec ON le.id = lec.log_event_id
            WHERE lec.context_id = :context_id
              AND le.id != ALL(:log_event_ids)
        ),
        duplicates AS (
            SELECT DISTINCT nl.id
            FROM new_logs nl
            WHERE EXISTS (
                SELECT 1 FROM existing_logs el
                WHERE el.data = nl.data
            )
        )
        SELECT id FROM duplicates
        """
        result = self.session.execute(
            text(query),
            {"context_id": context_id, "log_event_ids": log_event_ids},
        )
        return [row[0] for row in result.fetchall()]

    def add_logs_copy(self, context_id: int, log_ids: List[int]) -> None:
        """Associate copies of LogEvent instances with the specified context.

        This method creates new copies of the specified log events and associates
        these copies with the context.

        Copies LogEvent.data and LogEvent.key_order JSONB fields.

        Args:
            context_id: ID of the context to associate logs with
            log_ids: List of log event IDs to copy and associate with the context

        Raises:
            ValueError: If context_id doesn't exist or any log_ids don't exist
            ValueError: If duplicates are found and context doesn't allow duplicates
        """

        try:
            # Get the context to check if duplicates are allowed
            context = self.session.query(Context).filter_by(id=context_id).one_or_none()
            if not context:
                raise ValueError(f"Context with id {context_id} not found")

            # Get current timestamp for all new records
            current_time = datetime.now(timezone.utc)

            # Process each log event
            for original_log_id in log_ids:
                # Query the original LogEvent
                original_log_event = (
                    self.session.query(LogEvent)
                    .filter_by(id=original_log_id)
                    .one_or_none()
                )
                if not original_log_event:
                    raise ValueError(f"Log event with id {original_log_id} not found")

                # Check for duplicates if the context doesn't allow them
                if not context.allow_duplicates:
                    if self.check_for_duplicates(context_id, original_log_event.id):
                        raise ValueError(
                            f"Duplicate log entry detected. Context '{context.name}' does not allow duplicates.",
                        )

                # Create a new LogEvent by copying necessary fields
                new_log_event_data = {
                    "project_id": original_log_event.project_id,
                    "created_at": current_time,
                    "updated_at": current_time,
                    "data": original_log_event.data,
                    "key_order": original_log_event.key_order,
                }

                new_log_event = LogEvent(**new_log_event_data)
                self.session.add(new_log_event)
                self.session.flush()  # Get the new ID

                # Create association between the new log event and context
                association = LogEventContext(
                    log_event_id=new_log_event.id,
                    context_id=context_id,
                )
                self.session.add(association)
            # Commit all changes
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e

    def commit(self, context_id: int, commit_message: Optional[str] = None) -> str:
        """
        Create a new version of a single context.
        """
        context = self.session.query(Context).filter_by(id=context_id).one_or_none()
        if not context or not context.is_versioned:
            raise ValueError("Context is not versioned.")

        # Get the current HEAD commit
        current_head = context.current_commit_hash

        # If context has no commits yet, use the project's current commit as the parent
        if current_head is None and context.project:
            current_head = context.project.current_commit_hash

        # 1. Generate a unique commit hash
        commit_hash = hashlib.sha256(
            f"context_{context_id}{datetime.now(timezone.utc)}".encode(),
        ).hexdigest()

        # 2. Create a snapshot for the context
        self.create_version_snapshot(
            context=context,
            commit_hash=commit_hash,
            commit_message=commit_message,
            project_version=None,  # This is a context-only commit
            prev_commit_hash=current_head,
        )

        # Update the previous version's next_commit_hash array if it exists
        if current_head:
            # Try to find a context version first
            prev_context_version = (
                self.session.query(ContextVersion)
                .filter_by(
                    context_id=context_id,
                    commit_hash=current_head,
                )
                .with_for_update()
                .one_or_none()
            )

            if prev_context_version:
                if commit_hash not in prev_context_version.next_commit_hash:
                    prev_context_version.next_commit_hash = (
                        prev_context_version.next_commit_hash + [commit_hash]
                    )
            else:
                # If not found, it might be a project version
                prev_project_version = (
                    self.session.query(ProjectVersion)
                    .filter_by(
                        project_id=context.project_id,
                        commit_hash=current_head,
                    )
                    .with_for_update()
                    .one_or_none()
                )

                if prev_project_version:
                    # For project versions, we update the context version that was created as part of that project commit
                    context_version_in_project = (
                        self.session.query(ContextVersion)
                        .filter_by(
                            context_id=context_id,
                            project_version_id=prev_project_version.id,
                        )
                        .with_for_update()
                        .one_or_none()
                    )

                    if context_version_in_project:
                        if (
                            commit_hash
                            not in context_version_in_project.next_commit_hash
                        ):
                            context_version_in_project.next_commit_hash = (
                                context_version_in_project.next_commit_hash
                                + [commit_hash]
                            )

        context.updated_at = datetime.now(timezone.utc)

        # Update the context's HEAD pointer
        context.current_commit_hash = commit_hash

        self.session.commit()
        return commit_hash

    def rollback(self, context_id: int, commit_hash: str) -> None:
        """
        Orchestrates the rollback of a context in two phases:
        1. Restore the state from the version snapshot.
        2. Clean up any orphaned data from the previous state.
        This ensures the operation is atomic and safe.
        """
        try:
            context_version = (
                self.session.query(ContextVersion)
                .filter_by(context_id=context_id, commit_hash=commit_hash)
                .one_or_none()
            )
            if not context_version:
                raise ValueError(
                    f"Commit hash {commit_hash} not found for context {context_id}.",
                )

            context = self.session.query(Context).filter_by(id=context_id).one()

            # Step 1: Restore the state
            self.rollback_to_version(context_id, context_version.id)
            context.updated_at = datetime.now(timezone.utc)

            # Move the HEAD pointer to the target commit
            context.current_commit_hash = commit_hash

            self.session.commit()

            # Step 2: Garbage collection in a new transaction
            delete_orphaned_log_events(self.session, context.project_id)
            cleanup_orphaned_field_types(self.session, context_id)
            cleanup_orphaned_derived_log_templates(self.session, context_id)

            # Clean up plots and table views created after the commit point
            cleanup_plots_created_after_commit(
                self.session,
                context.project_id,
                context.name,
                context_version.archived_at,
            )
            cleanup_table_views_created_after_commit(
                self.session,
                context.project_id,
                context.name,
                context_version.archived_at,
            )

            self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e

    def get_commit_history(self, context_id: int) -> List[dict]:
        """
        Retrieves the combined commit history for a versioned context,
        including context-only and project-level commits.
        """
        context = self.session.query(Context).filter_by(id=context_id).one_or_none()
        if not context or not context.is_versioned:
            raise ValueError("Context is not versioned.")

        # Query all versions for this context
        versions = (
            self.session.query(ContextVersion)
            .filter_by(context_id=context_id)
            .order_by(ContextVersion.archived_at.desc())
            .all()
        )

        history = []
        for v in versions:
            history.append(
                {
                    "commit_hash": v.commit_hash,
                    "commit_message": v.commit_message,
                    "created_at": v.archived_at.isoformat(),
                    "type": "project" if v.project_version_id else "context",
                    "prev_commit_hash": v.prev_commit_hash,
                    "next_commit_hash": v.next_commit_hash,
                },
            )

        return history

    def create_version_snapshot(
        self,
        context: Context,
        commit_hash: str,
        commit_message: Optional[str] = None,
        project_version: Optional[ProjectVersion] = None,
        prev_commit_hash: Optional[str] = None,
    ) -> None:
        """Creates a snapshot of the context's current state."""
        return self.create_version_snapshot(
            context=context,
            commit_hash=commit_hash,
            commit_message=commit_message,
            project_version=project_version,
            prev_commit_hash=prev_commit_hash,
        )

    def create_version_snapshot(
        self,
        context: Context,
        commit_hash: str,
        commit_message: Optional[str] = None,
        project_version: Optional[ProjectVersion] = None,
        prev_commit_hash: Optional[str] = None,
    ) -> None:
        """Creates a snapshot of the context's current state.

        This method stores complete JSONB documents in LogEventVersion (one row per event),
        capturing both data and key_order for each log event.
        """
        if not context.is_versioned:
            return

        # 1. Create a ContextVersion record
        context_version = ContextVersion(
            context_id=context.id,
            project_version_id=project_version.id if project_version else None,
            name=context.name,
            description=context.description,
            commit_hash=commit_hash,
            commit_message=commit_message,
            prev_commit_hash=prev_commit_hash,
        )
        self.session.add(context_version)
        self.session.flush()  # Flush to get the context_version.id

        # Update the previous version's next_commit_hash array if it exists
        if prev_commit_hash:
            prev_version = (
                self.session.query(ContextVersion)
                .filter_by(
                    context_id=context.id,
                    commit_hash=prev_commit_hash,
                )
                .with_for_update()
                .one()
            )
            if commit_hash not in prev_version.next_commit_hash:
                prev_version.next_commit_hash = prev_version.next_commit_hash + [
                    commit_hash,
                ]

        # 2. Get all LogEvents for the context with their JSONB data
        log_events = (
            self.session.query(
                LogEvent.id,
                LogEvent.data,
                LogEvent.key_order,
                LogEvent.created_at,
                LogEvent.updated_at,
            )
            .join(LogEventContext, LogEvent.id == LogEventContext.log_event_id)
            .filter(LogEventContext.context_id == context.id)
            .all()
        )

        if not log_events:
            return

        # 3. Create LogEventVersion snapshots
        log_event_versions = [
            LogEventVersion(
                context_version_id=context_version.id,
                log_event_id=le.id,
                data=le.data,
                key_order=le.key_order,
                created_at=le.created_at,
                updated_at=le.updated_at,
            )
            for le in log_events
        ]

        # 4. Bulk insert the log event snapshots for efficiency
        self.session.bulk_save_objects(log_event_versions)

    def rollback_to_version(
        self,
        context_id: int,
        context_version_id: int,
    ) -> None:
        """Restores context state from snapshots.

        This method recreates LogEvent rows with data and key_order copied from
        LogEventVersion snapshots.

        This method only prepares the operations and does NOT commit.
        """
        # 1. Query all LogEventVersion snapshots for the target version
        log_event_versions = (
            self.session.query(LogEventVersion)
            .filter_by(context_version_id=context_version_id)
            .all()
        )

        # 2. Get the context for project_id
        context = self.session.query(Context).filter_by(id=context_id).one()

        # 3. Clear existing context associations
        self.session.query(LogEventContext).filter_by(context_id=context_id).delete(
            synchronize_session=False,
        )

        if not log_event_versions:
            return

        # 4. Bulk insert new LogEvents with RETURNING to get IDs
        stmt = (
            pg_insert(LogEvent)
            .values(
                [
                    {
                        "project_id": context.project_id,
                        "data": lev.data,
                        "key_order": lev.key_order,
                        "created_at": lev.created_at,
                        "updated_at": lev.updated_at,
                    }
                    for lev in log_event_versions
                ],
            )
            .returning(LogEvent.id)
        )
        result = self.session.execute(stmt)
        new_log_event_ids = [row[0] for row in result]

        # 5. Bulk insert LogEventContext associations
        if new_log_event_ids:
            assoc_values = [
                {"log_event_id": le_id, "context_id": context_id}
                for le_id in new_log_event_ids
            ]
            stmt_assoc = pg_insert(LogEventContext).values(assoc_values)
            self.session.execute(stmt_assoc)

    # -------------------------------------------------------------------------
    # Deep-copy helpers (used by admin_copy_context endpoint)
    # -------------------------------------------------------------------------

    def get_log_event_ids(self, context_id: int) -> List[int]:
        """Return ordered list of log event IDs in a context.

        Args:
            context_id: The context to query.

        Returns:
            Sorted list of log event IDs.
        """
        rows = self.session.execute(
            select(LogEventContext.log_event_id)
            .where(LogEventContext.context_id == context_id)
            .order_by(LogEventContext.log_event_id),
        ).fetchall()
        return [row[0] for row in rows]

    def batch_copy_log_events(
        self,
        source_log_event_ids: List[int],
        target_context_id: int,
        target_project_id: int,
        batch_size: int = 10000,
    ) -> Dict[int, int]:
        """Deep-copy log events and associate them with a target context.

        Processes in batches, committing after each batch to keep transactions
        bounded. Returns a mapping of ``{old_id: new_id}`` for downstream use
        (unique constraints, embeddings, etc.).

        Args:
            source_log_event_ids: Ordered list of source LogEvent IDs.
            target_context_id: Context to associate the new log events with.
            target_project_id: Project the new log events belong to.
            batch_size: Number of log events per batch.

        Returns:
            Dictionary mapping old log event IDs to their new copies.
        """
        id_map: Dict[int, int] = {}
        now = datetime.now(timezone.utc)

        for offset in range(0, len(source_log_event_ids), batch_size):
            batch_ids = source_log_event_ids[offset : offset + batch_size]

            source_events = (
                self.session.query(LogEvent)
                .filter(LogEvent.id.in_(batch_ids))
                .order_by(LogEvent.id)
                .all()
            )

            le_values = [
                {
                    "project_id": target_project_id,
                    "data": le.data,
                    "key_order": le.key_order,
                    "created_at": now,
                    "updated_at": now,
                }
                for le in source_events
            ]

            stmt = pg_insert(LogEvent).values(le_values).returning(LogEvent.id)
            new_ids = [row[0] for row in self.session.execute(stmt).fetchall()]

            for i, le in enumerate(source_events):
                id_map[le.id] = new_ids[i]

            lec_values = [
                {"log_event_id": new_id, "context_id": target_context_id}
                for new_id in new_ids
            ]
            self.session.execute(pg_insert(LogEventContext).values(lec_values))

            self.session.commit()

        return id_map

    def copy_derived_templates(
        self,
        source_context_id: int,
        target_context_id: int,
        target_project_id: int,
    ) -> int:
        """Copy active derived-log templates from one context to another.

        Args:
            source_context_id: Context to copy templates from.
            target_context_id: Context to copy templates to.
            target_project_id: Project ID for the target templates.

        Returns:
            Number of templates copied.
        """
        templates = (
            self.session.query(ActiveDerivedLog)
            .filter(ActiveDerivedLog.context_id == source_context_id)
            .all()
        )
        if not templates:
            return 0

        values = [
            {
                "project_id": target_project_id,
                "context_id": target_context_id,
                "key": t.key,
                "equation": t.equation,
                "referenced_logs": t.referenced_logs,
                "filter_expression": t.filter_expression,
                "inferred_type": t.inferred_type,
                "referenced_keys": t.referenced_keys,
                "is_active": t.is_active,
            }
            for t in templates
        ]
        self.session.execute(pg_insert(ActiveDerivedLog).values(values))
        self.session.flush()
        return len(values)

    def copy_unique_constraints(
        self,
        source_context_id: int,
        target_context_id: int,
        id_map: Dict[int, int],
        batch_size: int = 10000,
    ) -> int:
        """Copy unique-constraint lookup rows, remapping log event IDs.

        Args:
            source_context_id: Context to copy constraints from.
            target_context_id: Context to copy constraints to.
            id_map: Mapping of old log event ID → new log event ID.
            batch_size: Number of rows to process per batch.

        Returns:
            Total number of constraint rows copied.
        """
        old_ids = list(id_map.keys())
        total = 0

        for offset in range(0, len(old_ids), batch_size):
            batch_old = old_ids[offset : offset + batch_size]

            rows = (
                self.session.query(LogUniqueConstraint)
                .filter(
                    LogUniqueConstraint.context_id == source_context_id,
                    LogUniqueConstraint.log_event_id.in_(batch_old),
                )
                .all()
            )
            if not rows:
                continue

            values = [
                {
                    "context_id": target_context_id,
                    "field_name": r.field_name,
                    "value_hash": r.value_hash,
                    "log_event_id": id_map[r.log_event_id],
                }
                for r in rows
                if r.log_event_id in id_map
            ]
            if values:
                self.session.execute(pg_insert(LogUniqueConstraint).values(values))
                total += len(values)

        if total:
            self.session.commit()
        return total

    def queue_embedding_copies(
        self,
        id_map: Dict[int, int],
        batch_size: int = 10000,
    ) -> int:
        """Queue copies of embeddings for HNSW-safe insertion.

        Instead of inserting directly into the indexed ``embedding`` table
        (which triggers expensive HNSW graph recomputations), this method
        copies the *pre-generated vectors* into ``embedding_queue`` with
        ``status='vector_ready'``. The existing Stage 2 background worker
        (``/admin/index_ready_embeddings``) then inserts them at a controlled
        rate.

        Args:
            id_map: Mapping of old log event ID → new log event ID.
            batch_size: Chunk size for querying/inserting embeddings.

        Returns:
            Number of embedding-queue rows created.
        """
        if not id_map:
            return 0

        now = datetime.now(timezone.utc)
        old_ids = list(id_map.keys())
        total = 0

        for offset in range(0, len(old_ids), batch_size):
            batch_old = old_ids[offset : offset + batch_size]

            source_embeddings = (
                self.session.query(Embedding)
                .filter(
                    Embedding.ref_id.in_(batch_old),
                    Embedding.is_deleted.is_(False),
                )
                .all()
            )
            if not source_embeddings:
                continue

            values = [
                {
                    "ref_id": id_map[emb.ref_id],
                    "key": emb.key,
                    "text": "[copied]",
                    "model": emb.model,
                    "status": "vector_ready",
                    "generated_vector": emb.vector,
                    "vector_generated_at": now,
                    "created_at": now,
                }
                for emb in source_embeddings
                if emb.ref_id in id_map
            ]

            if values:
                self.session.execute(pg_insert(EmbeddingQueue).values(values))
                total += len(values)

        if total:
            self.session.commit()
        return total
