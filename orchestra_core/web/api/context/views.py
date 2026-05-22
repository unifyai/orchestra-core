"""Kernel context router: CRUD over `context`/`context_version`."""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from orchestra_core.db.dao.context_dao import ContextDAO
from orchestra_core.db.dao.log_event_dao import LogEventDAO
from orchestra_core.db.dao.project_dao import ProjectDAO
from orchestra_core.db.dependencies import get_db_session
from orchestra_core.db.models.core_models import Context
from orchestra_core.web.api.context.schema import (
    AddLogsRequest,
    CommitRequest,
    ContextCreate,
    ContextInfo,
    ContextList,
    RenameRequest,
    RollbackRequest,
)
from orchestra_core.web.api.utils.http_responses import not_found

router = APIRouter()


def _resolve_project(session: Session, user_id: str, project_name: str):
    project_dao = ProjectDAO(session=session, context_dao=ContextDAO(session))
    project = project_dao.get_by_user_and_name(user_id=user_id, name=project_name)
    if project is None:
        raise not_found("project")
    return project


def _to_info(ctx: Context) -> ContextInfo:
    unique_keys: Dict[str, str] = {}
    if ctx.unique_key_names and ctx.unique_key_types:
        unique_keys = dict(zip(ctx.unique_key_names, ctx.unique_key_types))
    return ContextInfo(
        id=ctx.id,
        name=ctx.name,
        description=ctx.description,
        is_versioned=ctx.is_versioned,
        allow_duplicates=ctx.allow_duplicates,
        unique_keys=unique_keys,
        foreign_keys=ctx.foreign_keys or [],
        current_commit_hash=ctx.current_commit_hash,
    )


@router.get("/project/{project_name}/contexts", response_model=ContextList)
def list_contexts(
    project_name: str,
    request: Request,
    session: Session = Depends(get_db_session),
) -> ContextList:
    project = _resolve_project(session, request.state.user_id, project_name)
    rows = (
        session.execute(select(Context).where(Context.project_id == project.id))
        .scalars()
        .all()
    )
    return ContextList(contexts=[_to_info(r) for r in rows])


@router.get(
    "/project/{project_name}/contexts/{name}",
    response_model=ContextInfo,
)
def get_context(
    project_name: str,
    name: str,
    request: Request,
    session: Session = Depends(get_db_session),
) -> ContextInfo:
    project = _resolve_project(session, request.state.user_id, project_name)
    ctx = (
        session.execute(
            select(Context).where(
                Context.project_id == project.id, Context.name == name,
            ),
        )
        .scalars()
        .first()
    )
    if ctx is None:
        raise not_found("context")
    return _to_info(ctx)


@router.post(
    "/project/{project_name}/contexts",
    status_code=status.HTTP_201_CREATED,
    response_model=ContextInfo,
)
def create_context(
    project_name: str,
    payload: ContextCreate,
    request: Request,
    session: Session = Depends(get_db_session),
) -> ContextInfo:
    project = _resolve_project(session, request.state.user_id, project_name)
    unique_key_names: List[str] = []
    unique_key_types: List[str] = []
    if payload.unique_keys:
        for k, v in payload.unique_keys.items():
            unique_key_names.append(k)
            unique_key_types.append(v)
    ctx = Context(
        project_id=project.id,
        name=payload.name,
        description=payload.description,
        is_versioned=payload.is_versioned,
        allow_duplicates=payload.allow_duplicates,
        unique_key_names=unique_key_names,
        unique_key_types=unique_key_types,
        foreign_keys=payload.foreign_keys or [],
    )
    session.add(ctx)
    session.commit()
    session.refresh(ctx)
    return _to_info(ctx)


@router.delete(
    "/project/{project_name}/contexts/{name}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def delete_context(
    project_name: str,
    name: str,
    request: Request,
    session: Session = Depends(get_db_session),
):
    project = _resolve_project(session, request.state.user_id, project_name)
    ctx = (
        session.execute(
            select(Context).where(
                Context.project_id == project.id, Context.name == name,
            ),
        )
        .scalars()
        .first()
    )
    if ctx is None:
        raise not_found("context")
    session.delete(ctx)
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/project/{project_name}/contexts/{name}/rename",
    response_model=ContextInfo,
)
def rename_context(
    project_name: str,
    name: str,
    payload: RenameRequest,
    request: Request,
    session: Session = Depends(get_db_session),
) -> ContextInfo:
    project = _resolve_project(session, request.state.user_id, project_name)
    ctx = (
        session.execute(
            select(Context).where(
                Context.project_id == project.id, Context.name == name,
            ),
        )
        .scalars()
        .first()
    )
    if ctx is None:
        raise not_found("context")
    ctx.name = payload.new_name
    if payload.description is not None:
        ctx.description = payload.description
    session.commit()
    session.refresh(ctx)
    return _to_info(ctx)


@router.post("/project/{project_name}/contexts/{name}/add_logs")
def add_logs_to_context(
    project_name: str,
    name: str,
    payload: AddLogsRequest,
    request: Request,
    session: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    project = _resolve_project(session, request.state.user_id, project_name)
    ctx = (
        session.execute(
            select(Context).where(
                Context.project_id == project.id, Context.name == name,
            ),
        )
        .scalars()
        .first()
    )
    if ctx is None:
        raise not_found("context")
    log_dao = LogEventDAO(session)
    for log_event_id in payload.log_event_ids:
        log_dao.add_to_context(log_event_id=log_event_id, context_id=ctx.id)
    session.commit()
    return {"added": len(payload.log_event_ids)}


@router.post("/project/{project_name}/contexts/{name}/commit")
def commit_context(
    project_name: str,
    name: str,
    payload: CommitRequest,
    request: Request,
    session: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    project = _resolve_project(session, request.state.user_id, project_name)
    ctx = (
        session.execute(
            select(Context).where(
                Context.project_id == project.id, Context.name == name,
            ),
        )
        .scalars()
        .first()
    )
    if ctx is None:
        raise not_found("context")
    if not ctx.is_versioned:
        raise not_found("versioning enabled for context")
    ctx_dao = ContextDAO(session)
    snapshot = ctx_dao.create_version_snapshot(
        context=ctx,
        commit_hash=None,
        commit_message=payload.message,
        project_version=None,
        prev_commit_hash=ctx.current_commit_hash,
    )
    session.commit()
    return {"commit_hash": getattr(snapshot, "commit_hash", None)}


@router.post(
    "/project/{project_name}/contexts/{name}/rollback",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def rollback_context(
    project_name: str,
    name: str,
    payload: RollbackRequest,
    request: Request,
    session: Session = Depends(get_db_session),
):
    project = _resolve_project(session, request.state.user_id, project_name)
    ctx = (
        session.execute(
            select(Context).where(
                Context.project_id == project.id, Context.name == name,
            ),
        )
        .scalars()
        .first()
    )
    if ctx is None:
        raise not_found("context")
    ctx_dao = ContextDAO(session)
    ctx_dao.rollback(ctx.id, payload.commit_hash)
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
