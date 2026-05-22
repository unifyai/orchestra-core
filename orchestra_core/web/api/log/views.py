"""Kernel log router: CRUD over `log_event` and `field_type`.

This is a deliberately minimal subset of the platform's log API. The kernel
ships:

- POST /logs            : create logs (single-tenant)
- GET  /logs            : list/filter logs
- PUT  /logs            : update logs by id
- DELETE /logs          : delete logs by ids
- POST /logs/fields     : create field types
- GET  /logs/fields     : list field types

Heavier operations (derived logs, group queries, joins, atomic updates,
JSONB metric aggregation) live in orchestra-platform's thicker router.
"""

from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from orchestra_core.db.dao.context_dao import ContextDAO
from orchestra_core.db.dao.field_type_dao import FieldTypeDAO
from orchestra_core.db.dao.log_event_dao import LogEventDAO
from orchestra_core.db.dao.project_dao import ProjectDAO
from orchestra_core.db.dependencies import get_db_session
from orchestra_core.db.models.core_models import Context, FieldType, Project
from orchestra_core.web.api.log.schema import (
    CreateFieldRequest,
    CreateLogsRequest,
    CreateLogsResponse,
    DeleteLogsRequest,
    FieldInfo,
    FieldList,
    GetLogsRequest,
    LogInfo,
    LogList,
    RenameFieldRequest,
    UpdateLogsRequest,
)
from orchestra_core.web.api.utils.http_responses import not_found

router = APIRouter()


def _resolve_project(session: Session, user_id: str, name: str) -> Project:
    project_dao = ProjectDAO(session=session, context_dao=ContextDAO(session))
    project = project_dao.get_by_user_and_name(user_id=user_id, name=name)
    if project is None:
        raise not_found("project")
    return project


def _resolve_context_ids(
    session: Session, project: Project, names: List[str]
) -> List[int]:
    if not names:
        return []
    rows = (
        session.execute(
            select(Context).where(
                Context.project_id == project.id,
                Context.name.in_(names),
            ),
        )
        .scalars()
        .all()
    )
    return [r.id for r in rows]


@router.post(
    "/logs", response_model=CreateLogsResponse, status_code=status.HTTP_201_CREATED
)
def create_logs(
    payload: CreateLogsRequest,
    request: Request,
    session: Session = Depends(get_db_session),
) -> CreateLogsResponse:
    project = _resolve_project(session, request.state.user_id, payload.project)
    context_names: List[str] = []
    if payload.context:
        context_names.append(payload.context)
    if payload.contexts:
        context_names.extend(payload.contexts)
    context_ids = _resolve_context_ids(session, project, context_names)

    log_dao = LogEventDAO(session)
    logs = log_dao.bulk_create(
        project_id=project.id,
        rows=payload.rows,
        context_ids=context_ids,
    )
    session.commit()
    return CreateLogsResponse(log_event_ids=[log.id for log in logs])


@router.get("/logs", response_model=LogList)
def get_logs(
    project: str,
    request: Request,
    session: Session = Depends(get_db_session),
    context: str = "",
    limit: int = 0,
    offset: int = 0,
) -> LogList:
    project_row = _resolve_project(session, request.state.user_id, project)
    log_dao = LogEventDAO(session)

    context_id = None
    if context:
        ctx = (
            session.execute(
                select(Context).where(
                    Context.project_id == project_row.id,
                    Context.name == context,
                ),
            )
            .scalars()
            .first()
        )
        if ctx is None:
            raise not_found("context")
        context_id = ctx.id

    rows = log_dao.filter(
        project_id=project_row.id,
        context_id=context_id,
        limit=limit if limit > 0 else None,
        offset=offset,
    )

    out: List[LogInfo] = []
    for log in rows:
        ctx_names = [c.name for c in log_dao.list_contexts(log.id)]
        out.append(
            LogInfo(
                id=log.id,
                project_id=log.project_id,
                data=log.data or {},
                key_order=log.key_order,
                contexts=ctx_names,
            ),
        )
    return LogList(logs=out)


@router.put("/logs")
def update_logs(
    payload: UpdateLogsRequest,
    request: Request,
    session: Session = Depends(get_db_session),
) -> dict:
    project = _resolve_project(session, request.state.user_id, payload.project)
    log_dao = LogEventDAO(session)

    updated: List[int] = []
    for row in payload.rows:
        log_id = row.get(payload.key)
        if log_id is None:
            continue
        log = log_dao.get(int(log_id))
        if log is None or log.project_id != project.id:
            continue
        merged = dict(log.data or {})
        for k, v in row.items():
            if k == payload.key:
                continue
            merged[k] = v
        log_dao.update(log.id, data=merged)
        updated.append(log.id)
    session.commit()
    return {"updated_log_event_ids": updated}


@router.delete("/logs", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_logs(
    payload: DeleteLogsRequest,
    request: Request,
    session: Session = Depends(get_db_session),
):
    project = _resolve_project(session, request.state.user_id, payload.project)
    log_dao = LogEventDAO(session)
    log_dao.bulk_delete(
        [
            i
            for i in payload.ids
            if (log := log_dao.get(i)) and log.project_id == project.id
        ],
    )
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/logs/fields", response_model=FieldInfo, status_code=status.HTTP_201_CREATED
)
def create_field(
    payload: CreateFieldRequest,
    request: Request,
    session: Session = Depends(get_db_session),
) -> FieldInfo:
    project = _resolve_project(session, request.state.user_id, payload.project)
    context_id = None
    if payload.context:
        ctx = (
            session.execute(
                select(Context).where(
                    Context.project_id == project.id,
                    Context.name == payload.context,
                ),
            )
            .scalars()
            .first()
        )
        if ctx is None:
            raise not_found("context")
        context_id = ctx.id

    ft = FieldType(
        project_id=project.id,
        context_id=context_id,
        field_name=payload.field_name,
        field_type=payload.field_type,
        field_category=payload.field_category,
        mutable=payload.mutable,
        unique=payload.unique,
        description=payload.description,
    )
    session.add(ft)
    session.commit()
    session.refresh(ft)
    return FieldInfo(
        id=ft.id,
        field_name=ft.field_name,
        field_type=ft.field_type,
        field_category=ft.field_category,
        mutable=ft.mutable,
        unique=ft.unique,
        description=ft.description,
    )


@router.get("/logs/fields", response_model=FieldList)
def list_fields(
    project: str,
    request: Request,
    session: Session = Depends(get_db_session),
    context: str = "",
) -> FieldList:
    project_row = _resolve_project(session, request.state.user_id, project)
    query = select(FieldType).where(FieldType.project_id == project_row.id)
    if context:
        ctx = (
            session.execute(
                select(Context).where(
                    Context.project_id == project_row.id,
                    Context.name == context,
                ),
            )
            .scalars()
            .first()
        )
        if ctx is None:
            raise not_found("context")
        query = query.where(FieldType.context_id == ctx.id)
    rows = session.execute(query).scalars().all()
    return FieldList(
        fields=[
            FieldInfo(
                id=r.id,
                field_name=r.field_name,
                field_type=r.field_type,
                field_category=r.field_category,
                mutable=r.mutable,
                unique=r.unique,
                description=r.description,
            )
            for r in rows
        ],
    )


@router.post("/logs/rename_field")
def rename_field(
    payload: RenameFieldRequest,
    request: Request,
    session: Session = Depends(get_db_session),
) -> dict:
    project = _resolve_project(session, request.state.user_id, payload.project)
    query = select(FieldType).where(
        FieldType.project_id == project.id,
        FieldType.field_name == payload.old_name,
    )
    if payload.context:
        ctx = (
            session.execute(
                select(Context).where(
                    Context.project_id == project.id,
                    Context.name == payload.context,
                ),
            )
            .scalars()
            .first()
        )
        if ctx is None:
            raise not_found("context")
        query = query.where(FieldType.context_id == ctx.id)
    field = session.execute(query).scalars().first()
    if field is None:
        raise not_found(f"field {payload.old_name}")
    field.field_name = payload.new_name
    session.commit()
    return {"renamed": payload.new_name}
