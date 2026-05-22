"""Kernel project router: CRUD over the `project` table.

Single-tenant: all rows scoped to `request.state.user_id` (sentinel `1` in
core). Mirrors the URL surface unity's unify SDK calls.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import Response
from sqlalchemy.orm import Session

from orchestra_core.db.dao.context_dao import ContextDAO
from orchestra_core.db.dao.project_dao import ProjectDAO
from orchestra_core.db.dependencies import get_db_session
from orchestra_core.web.api.project.schema import (
    CommitHistory,
    CommitInfo,
    CommitRequest,
    ProjectCreate,
    ProjectInfo,
    ProjectList,
    ProjectUpdate,
    RenameRequest,
    RollbackRequest,
)
from orchestra_core.web.api.utils.http_responses import not_found

router = APIRouter()


def _build_dao(session: Session) -> ProjectDAO:
    return ProjectDAO(session=session, context_dao=ContextDAO(session))


def _to_info(project) -> ProjectInfo:
    return ProjectInfo(
        id=project.id,
        name=project.name,
        description=project.description,
        icon=project.icon or "folder",
        order=project.order or 0,
        is_versioned=project.is_versioned,
        current_commit_hash=project.current_commit_hash,
    )


@router.get("/projects", response_model=ProjectList)
def list_projects(
    request: Request,
    session: Session = Depends(get_db_session),
) -> ProjectList:
    dao = _build_dao(session)
    rows = dao.filter(user_id=request.state.user_id)
    return ProjectList(projects=[_to_info(row[0]) for row in rows])


@router.post("/project", status_code=status.HTTP_201_CREATED)
def create_project(
    payload: ProjectCreate,
    request: Request,
    session: Session = Depends(get_db_session),
) -> dict:
    dao = _build_dao(session)
    dao.create(
        name=payload.name,
        user_id=request.state.user_id,
        description=payload.description,
        icon=payload.icon or "folder",
        is_versioned=payload.is_versioned,
        order=payload.order,
    )
    session.commit()
    project = dao.get_by_user_and_name(
        user_id=request.state.user_id,
        name=payload.name,
    )
    if project is None:
        raise not_found("project")
    return _to_info(project).model_dump()


@router.get("/project/{name}", response_model=ProjectInfo)
def get_project(
    name: str,
    request: Request,
    session: Session = Depends(get_db_session),
) -> ProjectInfo:
    dao = _build_dao(session)
    project = dao.get_by_user_and_name(user_id=request.state.user_id, name=name)
    if project is None:
        raise not_found("project")
    return _to_info(project)


@router.delete(
    "/project/{name}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def delete_project(
    name: str,
    request: Request,
    session: Session = Depends(get_db_session),
):
    dao = _build_dao(session)
    project = dao.get_by_user_and_name(user_id=request.state.user_id, name=name)
    if project is None:
        raise not_found("project")
    dao.delete(project.id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch("/project/{name}", response_model=ProjectInfo)
def update_project(
    name: str,
    payload: ProjectUpdate,
    request: Request,
    session: Session = Depends(get_db_session),
) -> ProjectInfo:
    dao = _build_dao(session)
    project = dao.get_by_user_and_name(user_id=request.state.user_id, name=name)
    if project is None:
        raise not_found("project")
    dao.update(
        id=project.id,
        name=payload.name,
        description=payload.description,
        icon=payload.icon,
        order=payload.order,
    )
    session.commit()
    project = dao.get(project.id)
    return _to_info(project)


@router.post("/project/{name}/commit")
def commit_project(
    name: str,
    payload: CommitRequest,
    request: Request,
    session: Session = Depends(get_db_session),
) -> dict:
    dao = _build_dao(session)
    project = dao.get_by_user_and_name(user_id=request.state.user_id, name=name)
    if project is None:
        raise not_found("project")
    commit_hash = dao.commit(project.id, payload.message)
    return {"commit_hash": commit_hash}


@router.post(
    "/project/{name}/rollback",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def rollback_project(
    name: str,
    payload: RollbackRequest,
    request: Request,
    session: Session = Depends(get_db_session),
):
    dao = _build_dao(session)
    project = dao.get_by_user_and_name(user_id=request.state.user_id, name=name)
    if project is None:
        raise not_found("project")
    dao.rollback(project.id, payload.commit_hash)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/project/{name}/commits", response_model=CommitHistory)
def list_commits(
    name: str,
    request: Request,
    session: Session = Depends(get_db_session),
) -> CommitHistory:
    dao = _build_dao(session)
    project = dao.get_by_user_and_name(user_id=request.state.user_id, name=name)
    if project is None:
        raise not_found("project")
    return CommitHistory(
        commits=[CommitInfo(**c) for c in dao.get_commit_history(project.id)],
    )


@router.post("/project/{name}/rename", response_model=ProjectInfo)
def rename_project(
    name: str,
    payload: RenameRequest,
    request: Request,
    session: Session = Depends(get_db_session),
) -> ProjectInfo:
    dao = _build_dao(session)
    dao.rename(
        user_id=request.state.user_id,
        name=name,
        new_name=payload.new_name,
        description=payload.description,
    )
    session.commit()
    project = dao.get_by_user_and_name(
        user_id=request.state.user_id,
        name=payload.new_name,
    )
    if project is None:
        raise not_found("project")
    return _to_info(project)
