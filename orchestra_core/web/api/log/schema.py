from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class CreateLogsRequest(BaseModel):
    project: str
    context: Optional[str] = None
    contexts: Optional[List[str]] = None
    rows: List[Dict[str, Any]]


class CreateLogsResponse(BaseModel):
    log_event_ids: List[int]


class GetLogsRequest(BaseModel):
    project: str
    context: Optional[str] = None
    ids: Optional[List[int]] = None
    limit: Optional[int] = None
    offset: int = 0


class LogInfo(BaseModel):
    id: int
    project_id: int
    data: Dict[str, Any]
    key_order: Optional[Dict[str, List[str]]] = None
    contexts: List[str] = Field(default_factory=list)


class LogList(BaseModel):
    logs: List[LogInfo]


class UpdateLogsRequest(BaseModel):
    project: str
    rows: List[Dict[str, Any]]
    key: str = "id"


class DeleteLogsRequest(BaseModel):
    project: str
    ids: List[int]


class CreateFieldRequest(BaseModel):
    project: str
    context: Optional[str] = None
    field_name: str
    field_type: str
    field_category: str = "entry"
    description: Optional[str] = None
    mutable: bool = True
    unique: bool = False


class FieldInfo(BaseModel):
    id: int
    field_name: str
    field_type: str
    field_category: str
    mutable: bool
    unique: bool
    description: Optional[str]


class FieldList(BaseModel):
    fields: List[FieldInfo]


class RenameFieldRequest(BaseModel):
    project: str
    context: Optional[str] = None
    old_name: str
    new_name: str
