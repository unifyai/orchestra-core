"""Kernel ProjectDAO.

Single-tenant CRUD over the `project` and `project_version` tables. The
multi-tenant access-control queries (filter_by_user_access,
get_by_user_and_name_any_context) live in orchestra-platform's own DAO and
are not part of the open-source kernel.
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import and_, select, text
from sqlalchemy.orm import Session

from orchestra_core.db.dao.context_dao import ContextDAO
from orchestra_core.db.dao.embedding_dao import EmbeddingDAO
from orchestra_core.db.models.core_models import (
    Context,
    ContextVersion,
    Project,
    ProjectVersion,
)
from orchestra_core.db.utils import get_next_order_value

logger = logging.getLogger(__name__)


class ProjectDAO:
    """Single-tenant ProjectDAO."""

    DEFAULT_DELETE_BATCH_SIZE = 10000
    MIN_DELETE_BATCH_SIZE = 1000
    MAX_DELETE_BATCH_SIZE = 20000

    def __init__(self, session: Session, context_dao: ContextDAO):
        self.session = session
        self.context_dao = context_dao

    def _validate_description(self, description: Optional[str]) -> None:
        if description is not None and len(description) > 256:
            raise ValueError("Description cannot exceed 256 characters")

    def get(self, id: int) -> Optional[Project]:
        return self.session.execute(
            select(Project).where(Project.id == id),
        ).scalars().first()

    def create(
        self,
        name: str,
        user_id: Optional[str] = None,
        organization_id: Optional[int] = None,
        is_versioned: bool = False,
        description: Optional[str] = None,
        icon: Optional[str] = "folder",
        order: Optional[int] = None,
    ) -> None:
        self._validate_description(description)

        where_conditions = []
        if user_id is not None:
            where_conditions.append(Project.user_id == user_id)
        if organization_id is not None:
            where_conditions.append(Project.organization_id == organization_id)

        order_value = get_next_order_value(
            session=self.session,
            model_class=Project,
            order=order,
            where_conditions=where_conditions,
        )

        self.session.add(
            Project(
                name=name,
                user_id=user_id,
                organization_id=organization_id,
                is_versioned=is_versioned,
                description=description,
                icon=icon,
                order=order_value,
            ),
        )

    def filter(
        self,
        id: Optional[int] = None,
        user_id: Optional[str] = None,
        organization_id: Optional[int] = None,
        name: Optional[str] = None,
    ) -> List[Project]:
        query = select(Project)
        if id:
            query = query.where(Project.id == id)
        if user_id:
            query = query.where(Project.user_id == user_id)
        if organization_id:
            query = query.where(Project.organization_id == organization_id)
        if name:
            query = query.where(Project.name == name)
        return self.session.execute(query).fetchall()

    def update(
        self,
        id: int,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        organization_id: Optional[int] = None,
        icon: Optional[str] = None,
        description: Optional[str] = None,
        order: Optional[int] = None,
    ) -> None:
        self._validate_description(description)
        entry = (
            self.session.execute(select(Project).where(Project.id == id))
            .scalars()
            .first()
        )
        if entry is None:
            return
        if name:
            entry.name = name
        if user_id:
            entry.user_id = user_id
        if organization_id:
            entry.organization_id = organization_id
        if description is not None:
            entry.description = description
        if icon is not None:
            entry.icon = icon
        if order is not None:
            entry.order = order

    def rename(
        self,
        user_id: str,
        name: str,
        new_name: str,
        description: Optional[str] = None,
    ) -> None:
        rows = self.filter(user_id=user_id, name=name)
        if not rows:
            raise ValueError(f"No project named {name!r} for user {user_id!r}")
        self.update(id=rows[0][0].id, name=new_name, description=description)

    def get_by_user_and_name(
        self,
        user_id: str,
        name: str,
        organization_id: Optional[int] = None,
    ) -> Optional[Project]:
        query = select(Project).where(Project.name == name)
        if organization_id is not None:
            query = query.where(Project.organization_id == organization_id)
        else:
            query = query.where(
                and_(Project.user_id == user_id, Project.organization_id.is_(None)),
            )
        return self.session.execute(query).scalars().first()

    def delete(self, id: int, batch_size: int = None) -> None:
        """Delete a project and all associated data using batched operations.

        Phase 0: Cancel pending embedding queue items.
        Phase 1: Soft-delete embeddings (no HNSW index surgery).
        Phase 2: Delete log_events in batches (SKIP LOCKED first, then blocking).
        Phase 3: Delete the project (CASCADE handles remaining children).
        """
        if batch_size is None:
            batch_size = self.DEFAULT_DELETE_BATCH_SIZE
        batch_size = max(
            self.MIN_DELETE_BATCH_SIZE,
            min(batch_size, self.MAX_DELETE_BATCH_SIZE),
        )

        try:
            project = self.session.query(Project).filter_by(id=id).one()
            project_name = project.name
            logger.info(
                f"Starting batched deletion of project {id} ('{project_name}') "
                f"with batch_size={batch_size}",
            )

            embedding_dao = EmbeddingDAO(self.session)
            cancelled_count = embedding_dao.cancel_queue(
                project_id=id,
                reason="Project deleted",
            )
            self.session.commit()

            soft_deleted_count = embedding_dao.soft_delete(project_id=id)
            self.session.commit()

            total_log_events_deleted = 0
            while True:
                result = self.session.execute(
                    text(
                        """
                        WITH batch AS (
                            SELECT id FROM log_event
                            WHERE project_id = :project_id
                            LIMIT :batch_size
                            FOR UPDATE SKIP LOCKED
                        )
                        DELETE FROM log_event
                        WHERE id IN (SELECT id FROM batch)
                    """,
                    ),
                    {"project_id": id, "batch_size": batch_size},
                )
                deleted = result.rowcount
                self.session.commit()
                if deleted == 0:
                    break
                total_log_events_deleted += deleted

            remaining = self.session.execute(
                text(
                    "SELECT COUNT(*) FROM log_event WHERE project_id = :project_id",
                ),
                {"project_id": id},
            ).scalar()
            if remaining and remaining > 0:
                while True:
                    result = self.session.execute(
                        text(
                            """
                            WITH batch AS (
                                SELECT id FROM log_event
                                WHERE project_id = :project_id
                                LIMIT :batch_size
                                FOR UPDATE
                            )
                            DELETE FROM log_event
                            WHERE id IN (SELECT id FROM batch)
                        """,
                        ),
                        {"project_id": id, "batch_size": batch_size},
                    )
                    deleted = result.rowcount
                    self.session.commit()
                    if deleted == 0:
                        break
                    total_log_events_deleted += deleted

            project = self.session.query(Project).filter_by(id=id).first()
            if project:
                self.session.delete(project)
                self.session.commit()

            logger.info(
                f"Project {id} ('{project_name}') deleted. "
                f"Cancelled {cancelled_count} queue items, "
                f"removed {total_log_events_deleted} log_events, "
                f"soft-deleted {soft_deleted_count} embeddings.",
            )
        except Exception as e:
            self.session.rollback()
            raise ValueError(f"Failed to delete project with id {id}: {e}")

    def commit(self, project_id: int, commit_message: Optional[str] = None) -> str:
        """Create a new version of a project by snapshotting versioned contexts."""
        project = self.session.query(Project).filter_by(id=project_id).one_or_none()
        if not project or not project.is_versioned:
            raise ValueError("Project is not versioned.")

        current_head = project.current_commit_hash
        commit_hash = hashlib.sha256(
            f"{project_id}{datetime.now(timezone.utc)}".encode(),
        ).hexdigest()

        project_version = ProjectVersion(
            project_id=project_id,
            commit_hash=commit_hash,
            commit_message=commit_message,
            prev_commit_hash=current_head,
        )
        self.session.add(project_version)
        self.session.flush()

        if current_head:
            prev_version = (
                self.session.query(ProjectVersion)
                .filter_by(project_id=project_id, commit_hash=current_head)
                .with_for_update()
                .one()
            )
            if commit_hash not in prev_version.next_commit_hash:
                prev_version.next_commit_hash = prev_version.next_commit_hash + [
                    commit_hash,
                ]

        contexts = (
            self.session.query(Context)
            .filter_by(project_id=project_id, is_versioned=True)
            .all()
        )
        for context in contexts:
            self.context_dao.create_version_snapshot(
                context=context,
                commit_hash=commit_hash,
                commit_message=commit_message,
                project_version=project_version,
                prev_commit_hash=context.current_commit_hash,
            )
        project.updated_at = datetime.now(timezone.utc)
        project.current_commit_hash = commit_hash

        self.session.commit()
        return commit_hash

    def rollback(self, project_id: int, commit_hash: str) -> None:
        """Rollback a project and all its versioned contexts to a specific commit."""
        project = self.session.query(Project).filter_by(id=project_id).one_or_none()
        if not project or not project.is_versioned:
            raise ValueError("Project is not versioned.")

        project_version = (
            self.session.query(ProjectVersion)
            .filter_by(project_id=project_id, commit_hash=commit_hash)
            .one_or_none()
        )
        if not project_version:
            raise ValueError(
                f"Commit hash {commit_hash} not found for project {project_id}.",
            )

        context_versions = (
            self.session.query(ContextVersion)
            .filter_by(project_version_id=project_version.id)
            .all()
        )
        for cv in context_versions:
            self.context_dao.rollback(cv.context_id, cv.commit_hash)

        project.updated_at = datetime.now(timezone.utc)
        project.current_commit_hash = commit_hash

        self.session.commit()

    def get_commit_history(self, project_id: int) -> List[dict]:
        """Retrieve the commit history for a versioned project."""
        project = self.session.query(Project).filter_by(id=project_id).one_or_none()
        if not project or not project.is_versioned:
            raise ValueError("Project is not versioned.")

        versions = (
            self.session.query(ProjectVersion)
            .filter_by(project_id=project_id)
            .order_by(ProjectVersion.created_at.desc())
            .all()
        )
        return [
            {
                "commit_hash": v.commit_hash,
                "commit_message": v.commit_message,
                "created_at": v.created_at.isoformat(),
                "prev_commit_hash": v.prev_commit_hash,
                "next_commit_hash": v.next_commit_hash,
            }
            for v in versions
        ]
