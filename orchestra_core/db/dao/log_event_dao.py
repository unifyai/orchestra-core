"""Kernel LogEventDAO: minimal CRUD over the `log_event` table.

Heavy logic lives in orchestra-platform's log_event_dao: GCS media handling,
derived-log recomputation, multi-context counter coordination, etc. The
kernel ships only the operations needed to serve the open-source single-
tenant API surface.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from orchestra_core.db.dao.context_dao import ContextDAO
from orchestra_core.db.models.core_models import (
    Context,
    LogEvent,
    LogEventContext,
)

logger = logging.getLogger(__name__)


class LogEventDAO:
    """Single-tenant LogEventDAO.

    The platform may pass in its own BucketService-equipped DAO instead;
    in the kernel we have no notion of object storage URLs.
    """

    def __init__(
        self,
        session: Session,
        context_dao: Optional[ContextDAO] = None,
    ):
        self.session = session
        self.context_dao = context_dao

    def get(self, id: int) -> Optional[LogEvent]:
        return (
            self.session.execute(select(LogEvent).where(LogEvent.id == id))
            .scalars()
            .first()
        )

    def filter(
        self,
        project_id: Optional[int] = None,
        context_id: Optional[int] = None,
        ids: Optional[List[int]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[LogEvent]:
        query = select(LogEvent)
        if project_id is not None:
            query = query.where(LogEvent.project_id == project_id)
        if ids:
            query = query.where(LogEvent.id.in_(ids))
        if context_id is not None:
            query = query.join(
                LogEventContext,
                LogEventContext.log_event_id == LogEvent.id,
            ).where(LogEventContext.context_id == context_id)
        query = query.order_by(LogEvent.id)
        if limit is not None:
            query = query.limit(limit).offset(offset)
        return self.session.execute(query).scalars().all()

    def create(
        self,
        project_id: int,
        data: Dict[str, Any],
        context_ids: Optional[List[int]] = None,
        key_order: Optional[Dict[str, List[str]]] = None,
    ) -> LogEvent:
        log = LogEvent(project_id=project_id, data=data, key_order=key_order)
        self.session.add(log)
        self.session.flush()
        if context_ids:
            for cid in context_ids:
                self.session.add(
                    LogEventContext(log_event_id=log.id, context_id=cid),
                )
            self.session.flush()
        return log

    def bulk_create(
        self,
        project_id: int,
        rows: List[Dict[str, Any]],
        context_ids: Optional[List[int]] = None,
    ) -> List[LogEvent]:
        logs = [LogEvent(project_id=project_id, data=row) for row in rows]
        self.session.add_all(logs)
        self.session.flush()
        if context_ids:
            for log in logs:
                for cid in context_ids:
                    self.session.add(
                        LogEventContext(log_event_id=log.id, context_id=cid),
                    )
            self.session.flush()
        return logs

    def update(
        self,
        id: int,
        data: Optional[Dict[str, Any]] = None,
        key_order: Optional[Dict[str, List[str]]] = None,
    ) -> Optional[LogEvent]:
        log = self.get(id)
        if log is None:
            return None
        if data is not None:
            log.data = data
        if key_order is not None:
            log.key_order = key_order
        self.session.flush()
        return log

    def delete(self, id: int) -> None:
        log = self.get(id)
        if log is not None:
            self.session.delete(log)
            self.session.flush()

    def bulk_delete(self, ids: List[int]) -> int:
        if not ids:
            return 0
        result = self.session.execute(
            text("DELETE FROM log_event WHERE id = ANY(:ids)"),
            {"ids": ids},
        )
        self.session.flush()
        return result.rowcount or 0

    def add_to_context(self, log_event_id: int, context_id: int) -> None:
        existing = (
            self.session.query(LogEventContext)
            .filter_by(log_event_id=log_event_id, context_id=context_id)
            .first()
        )
        if existing is None:
            self.session.add(
                LogEventContext(log_event_id=log_event_id, context_id=context_id),
            )
            self.session.flush()

    def remove_from_context(self, log_event_id: int, context_id: int) -> None:
        self.session.query(LogEventContext).filter_by(
            log_event_id=log_event_id,
            context_id=context_id,
        ).delete()
        self.session.flush()

    def list_contexts(self, log_event_id: int) -> List[Context]:
        return (
            self.session.query(Context)
            .join(LogEventContext, LogEventContext.context_id == Context.id)
            .filter(LogEventContext.log_event_id == log_event_id)
            .all()
        )
