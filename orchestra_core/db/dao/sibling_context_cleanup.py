"""
Shared sibling context cleanup logic for Assistants/UnityTests projects.

Handles the 3-tier context hierarchy used in Assistants projects:
- Tier 1: All/<SubContext> (global aggregate) - PROTECTED ARCHIVE
- Tier 2: <User>/All/<SubContext> (user aggregate)
- Tier 3: <User>/<Assistant>/<SubContext> (user + assistant specific)

When deleting logs/contexts from one tier, the same logs should be
removed from sibling tiers to maintain consistency.

ARCHIVE PROTECTION:
- Topmost archive contexts (All/*) are protected from cascading deletions
  originating from lower-tier contexts (Tier 2 or Tier 3).
- This preserves historical data for billing and reporting.
- Deleting from All/* itself still cascades to lower tiers normally.
- Intermediate contexts (*/All/*) are NOT protected.
"""

import logging
from typing import TYPE_CHECKING, Dict, List, Set, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from orchestra_core.db.models.core_models import Context, LogEvent, LogEventContext

if TYPE_CHECKING:
    from orchestra_core.db.dao.context_dao import ContextDAO

logger = logging.getLogger(__name__)


def get_assistants_sibling_context_info(
    session: Session,
    project_id: int,
    context_id: int,
    context_name: str,
    log_event_ids: List[int],
    context_dao: "ContextDAO",
) -> Dict[int, List[int]]:
    """
    For Assistants/UnityTests project, find sibling context IDs for each log event.

    Uses a 3-tier context hierarchy:
    - Tier 1: "<prefix>/All/<SubContext>" (global aggregate)
    - Tier 2: "<prefix>/<User>/All/<SubContext>" (user aggregate)
    - Tier 3: "<prefix>/<User>/<Assistant>/<SubContext>" (user + assistant specific)

    Both <prefix> and <SubContext> can have arbitrary depth. We determine them
    dynamically by finding each log's Tier 1 context and locating "All" within it.
    The Tier 1 context is identified as the shortest context containing "All"
    that each log belongs to.

    Deletion cascade rules:
    - From any tier: delete from the other two tiers

    Uses "_user" and "_assistant" fields from logs to construct sibling paths.

    All DB lookups are batched to avoid per-log round trips.

    Args:
        session: Database session
        project_id: Project ID
        context_id: Current context ID being deleted from
        context_name: Name of the current context
        log_event_ids: List of log event IDs being deleted
        context_dao: Context DAO instance (kept for API compatibility)

    Returns:
        Dict mapping log_event_id to list of sibling context_ids.
        Empty dict if no sibling contexts found.
    """
    if not log_event_ids or not context_name:
        return {}

    def _is_topmost_archive(name: str) -> bool:
        """Check if context is a topmost archive (All/* only, NOT */All/*).

        Topmost archives are protected from cascading deletions originating
        from lower-tier contexts.
        """
        return name.startswith("All/")

    def _get_log_field_values(field_name: str) -> Dict[int, str]:
        """Get field values for all log events from LogEvent.data JSONB column.

        Returns:
            Dict mapping log_event_id to field value string.
        """
        values = (
            session.query(
                LogEvent.id,
                LogEvent.data[field_name].astext,
            )
            .filter(
                LogEvent.id.in_(log_event_ids),
                LogEvent.data.has_key(field_name),
            )
            .all()
        )

        result = {}
        for log_event_id, value in values:
            if value:
                if isinstance(value, str):
                    value = value.strip('"')
                result[log_event_id] = value
        return result

    def _get_tier1_context_for_logs() -> Dict[int, str]:
        """Find the Tier 1 context name for each log.

        Tier 1 is identified as the SHORTEST context containing "All" that
        each log belongs to. This works because Tier 2 adds a User component,
        making it longer than Tier 1 for the same prefix/SubContext.

        Returns:
            Dict mapping log_event_id to its Tier 1 context name.
        """
        # Query all contexts that contain these logs
        log_contexts = (
            session.query(LogEventContext.log_event_id, Context.name)
            .join(Context, Context.id == LogEventContext.context_id)
            .filter(
                LogEventContext.log_event_id.in_(log_event_ids),
                Context.project_id == project_id,
            )
            .all()
        )

        # For each log, find its Tier 1 context (shortest one containing "All")
        result: Dict[int, str] = {}
        for log_id, ctx_name in log_contexts:
            if "/All/" not in ctx_name and not ctx_name.startswith("All/"):
                # No "All" in this context - not an aggregation context
                continue

            if log_id not in result or len(ctx_name) < len(result[log_id]):
                # First match or shorter match - this is more likely Tier 1
                result[log_id] = ctx_name

        return result

    def _parse_tier1_context(tier1_ctx: str) -> Tuple[str, str]:
        """Parse a Tier 1 context into (prefix, sub_context).

        Args:
            tier1_ctx: Context name like "<prefix>/All/<SubContext>"

        Returns:
            Tuple of (prefix, sub_context) where prefix may be empty string.
        """
        parts = tier1_ctx.split("/")
        try:
            all_idx = parts.index("All")
            prefix = "/".join(parts[:all_idx]) if all_idx > 0 else ""
            sub_context = (
                "/".join(parts[all_idx + 1 :]) if all_idx < len(parts) - 1 else ""
            )
            return (prefix, sub_context)
        except ValueError:
            return ("", "")

    # ── Step 1: Find Tier 1 context for each log (1 query) ──
    tier1_contexts = _get_tier1_context_for_logs()

    if not tier1_contexts:
        return {}

    # ── Step 2: Get _user and _assistant fields (2 queries) ──
    user_values = _get_log_field_values("_user")
    assistant_values = _get_log_field_values("_assistant")

    current_is_archive = _is_topmost_archive(context_name)

    # ── Step 3: Construct all candidate sibling names (Python-only, no DB) ──
    candidate_names: Set[str] = set()
    log_sibling_names: Dict[int, List[str]] = {}

    for log_id in log_event_ids:
        tier1_ctx = tier1_contexts.get(log_id)
        if not tier1_ctx:
            continue

        prefix, sub_context = _parse_tier1_context(tier1_ctx)
        if not sub_context:
            continue

        user_ctx = user_values.get(log_id)
        assistant_ctx = assistant_values.get(log_id)

        # Construct all three tier context names
        if prefix:
            tier1_name = f"{prefix}/All/{sub_context}"
            tier2_name = f"{prefix}/{user_ctx}/All/{sub_context}" if user_ctx else None
            tier3_name = (
                f"{prefix}/{user_ctx}/{assistant_ctx}/{sub_context}"
                if user_ctx and assistant_ctx
                else None
            )
        else:
            tier1_name = f"All/{sub_context}"
            tier2_name = f"{user_ctx}/All/{sub_context}" if user_ctx else None
            tier3_name = (
                f"{user_ctx}/{assistant_ctx}/{sub_context}"
                if user_ctx and assistant_ctx
                else None
            )

        # Find sibling contexts (excluding the current context)
        siblings_for_log: List[str] = []
        for sibling_name in [tier1_name, tier2_name, tier3_name]:
            if sibling_name and sibling_name != context_name:
                # ARCHIVE PROTECTION: When deleting from a non-archive context,
                # skip cascade to topmost archive (All/*) contexts.
                # This preserves historical data in the archive.
                sibling_is_archive = _is_topmost_archive(sibling_name)
                if not current_is_archive and sibling_is_archive:
                    continue
                siblings_for_log.append(sibling_name)
                candidate_names.add(sibling_name)

        if siblings_for_log:
            log_sibling_names[log_id] = siblings_for_log

    if not candidate_names:
        return {}

    # ── Step 4: Batch-resolve all candidate names → IDs (1 query) ──
    # Replaces the old per-log _find_context_id() which made ~16K individual
    # SELECT queries (one per log per sibling tier). Now a single query
    # resolves all unique sibling context names to their IDs at once.
    name_to_id: Dict[str, int] = {}
    rows = session.execute(
        text(
            "SELECT id, name FROM context "
            "WHERE project_id = :pid AND name = ANY(:names)",
        ),
        {"pid": project_id, "names": list(candidate_names)},
    ).fetchall()
    for row_id, row_name in rows:
        name_to_id[row_name] = row_id

    if not name_to_id:
        # None of the candidate sibling contexts exist in the DB
        return {}

    # ── Step 5: Build candidate (log_id, ctx_id) pairs (Python-only) ──
    # Map resolved context IDs back to each log's candidate siblings,
    # excluding the current context being deleted.
    candidate_pairs: List[Tuple[int, int]] = []
    candidate_ctx_ids: Set[int] = set()
    candidate_log_ids: Set[int] = set()

    for log_id, sibling_names in log_sibling_names.items():
        for sib_name in sibling_names:
            ctx_id = name_to_id.get(sib_name)
            if ctx_id and ctx_id != context_id:
                candidate_pairs.append((log_id, ctx_id))
                candidate_ctx_ids.add(ctx_id)
                candidate_log_ids.add(log_id)

    if not candidate_pairs:
        return {}

    # ── Step 6: Batch-verify which pairs actually exist (1 query) ──
    # Replaces the old per-log _verify_logs_in_context() which made ~16K
    # individual SELECT queries. Now a single query fetches all existing
    # (log_event_id, context_id) associations for our candidate sets.
    # The ANY/ANY filter is a cross-product scan, but with only 2-3 unique
    # context IDs the result set is bounded and well-indexed.
    existing_associations: Set[Tuple[int, int]] = set()
    verify_rows = session.execute(
        text(
            "SELECT log_event_id, context_id FROM log_event_context "
            "WHERE log_event_id = ANY(:log_ids) AND context_id = ANY(:ctx_ids)",
        ),
        {
            "log_ids": list(candidate_log_ids),
            "ctx_ids": list(candidate_ctx_ids),
        },
    ).fetchall()
    for le_id, c_id in verify_rows:
        existing_associations.add((le_id, c_id))

    # ── Step 7: Build verified sibling_map (Python-only) ──
    # Only include pairs that were confirmed to exist in the DB.
    # This is the same output format as the original per-log approach:
    # Dict mapping log_event_id to list of sibling context_ids.
    sibling_map: Dict[int, List[int]] = {}
    for log_id, ctx_id in candidate_pairs:
        if (log_id, ctx_id) in existing_associations:
            if log_id not in sibling_map:
                sibling_map[log_id] = []
            if ctx_id not in sibling_map[log_id]:
                sibling_map[log_id].append(ctx_id)

    return sibling_map


def remove_logs_from_sibling_contexts(
    session: Session,
    sibling_context_map: Dict[int, List[int]],
) -> int:
    """
    Remove log associations from sibling contexts using a single bulk DELETE.

    Args:
        session: Database session
        sibling_context_map: Dict mapping log_event_id to list of sibling context_ids

    Returns:
        Number of associations removed.
    """
    if not sibling_context_map:
        return 0

    # Group log_ids by context_id for efficient bulk DELETEs.
    # Typically only 2-3 unique sibling context IDs exist, so this is
    # 2-3 queries instead of the previous ~16K individual DELETEs.
    ctx_to_logs: Dict[int, List[int]] = {}
    for log_id, sibling_ctx_ids in sibling_context_map.items():
        for ctx_id in sibling_ctx_ids:
            if ctx_id not in ctx_to_logs:
                ctx_to_logs[ctx_id] = []
            ctx_to_logs[ctx_id].append(log_id)

    removed = 0
    for ctx_id, log_ids in ctx_to_logs.items():
        result = session.execute(
            text(
                "DELETE FROM log_event_context "
                "WHERE context_id = :ctx_id "
                "AND log_event_id = ANY(:log_ids)",
            ),
            {"ctx_id": ctx_id, "log_ids": log_ids},
        )
        removed += result.rowcount

    return removed
