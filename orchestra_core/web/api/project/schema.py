from typing import List, Optional

from pydantic import BaseModel, Field


class ProjectCreate(BaseModel):
    name: str
    description: Optional[str] = None
    icon: Optional[str] = "folder"
    is_versioned: bool = False
    order: Optional[int] = None


class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None
    order: Optional[int] = None


class ProjectInfo(BaseModel):
    id: int
    name: str
    description: Optional[str]
    icon: str
    order: int
    is_versioned: bool
    current_commit_hash: Optional[str]


class ProjectList(BaseModel):
    projects: List[ProjectInfo]


class CommitRequest(BaseModel):
    message: Optional[str] = None


class CommitInfo(BaseModel):
    commit_hash: str
    commit_message: Optional[str]
    created_at: str
    prev_commit_hash: Optional[str]
    next_commit_hash: List[str] = Field(default_factory=list)


class CommitHistory(BaseModel):
    commits: List[CommitInfo]


class RollbackRequest(BaseModel):
    commit_hash: str


class RenameRequest(BaseModel):
    new_name: str
    description: Optional[str] = None
