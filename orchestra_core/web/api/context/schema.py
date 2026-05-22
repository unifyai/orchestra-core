from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ContextCreate(BaseModel):
    name: str
    description: Optional[str] = None
    is_versioned: bool = False
    allow_duplicates: bool = True
    unique_keys: Optional[Dict[str, str]] = None
    foreign_keys: Optional[List[Dict[str, Any]]] = None


class ContextInfo(BaseModel):
    id: int
    name: str
    description: Optional[str]
    is_versioned: bool
    allow_duplicates: bool
    unique_keys: Dict[str, str] = Field(default_factory=dict)
    foreign_keys: List[Dict[str, Any]] = Field(default_factory=list)
    current_commit_hash: Optional[str]


class ContextList(BaseModel):
    contexts: List[ContextInfo]


class AddLogsRequest(BaseModel):
    log_event_ids: List[int]


class CommitRequest(BaseModel):
    message: Optional[str] = None


class RollbackRequest(BaseModel):
    commit_hash: str


class RenameRequest(BaseModel):
    new_name: str
    description: Optional[str] = None
