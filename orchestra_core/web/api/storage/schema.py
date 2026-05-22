from typing import Optional

from pydantic import BaseModel


class UploadRequest(BaseModel):
    base64_content: str
    media_type: str


class UploadResponse(BaseModel):
    url: str
    filename: str


class SignedUrlRequest(BaseModel):
    filename: str
    expiration_seconds: Optional[int] = 3600


class SignedUrlResponse(BaseModel):
    url: str
