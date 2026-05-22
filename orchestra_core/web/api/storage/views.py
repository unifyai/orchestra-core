"""Kernel storage router: upload/download/signed-URL serving over local filesystem."""

from __future__ import annotations

import base64
import functools

from fastapi import APIRouter, status
from fastapi.responses import Response

from orchestra_core.services.local_bucket_service import LocalBucketService
from orchestra_core.web.api.storage.schema import (
    SignedUrlRequest,
    SignedUrlResponse,
    UploadRequest,
    UploadResponse,
)
from orchestra_core.web.api.utils.http_responses import not_found

router = APIRouter()


@functools.lru_cache(maxsize=1)
def _bucket() -> LocalBucketService:
    return LocalBucketService()


@router.post(
    "/storage/upload",
    response_model=UploadResponse,
    status_code=status.HTTP_201_CREATED,
)
def upload(payload: UploadRequest) -> UploadResponse:
    url, filename = _bucket().upload_media(payload.base64_content, payload.media_type)
    return UploadResponse(url=url, filename=filename)


@router.post("/storage/signed-url", response_model=SignedUrlResponse)
def signed_url(payload: SignedUrlRequest) -> SignedUrlResponse:
    return SignedUrlResponse(
        url=_bucket().get_signed_url(
            payload.filename,
            payload.expiration_seconds or 3600,
        ),
    )


@router.get("/storage/{filename}")
def download(filename: str) -> Response:
    """Return the raw bytes for `filename`. Used as the public URL prefix.

    Note: 404 is returned via the not_found helper for parity with the
    rest of the kernel.
    """
    content_b64 = _bucket().get_media(filename)
    if content_b64 is None:
        raise not_found("file")
    content = base64.b64decode(content_b64)
    return Response(content=content, media_type="application/octet-stream")


@router.delete(
    "/storage/{filename}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def delete(filename: str):
    if not _bucket().delete_media(filename):
        raise not_found("file")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
