"""Local-filesystem object storage for orchestra-core.

Mirrors the core method surface of orchestra-platform's GCS-backed
BucketService (`upload_media`, `get_media`, `delete_media`, `get_media_url`)
so kernel routers can use either backend interchangeably. URLs returned are
served via the kernel's `/v0/storage/{filename}` endpoint, so they remain
self-contained inside the local install.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import mimetypes
import os
import uuid
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_LOCAL_BUCKET_ROOT = Path.home() / ".orchestra-core" / "buckets" / "default"


class LocalBucketService:
    """Filesystem-backed equivalent of platform's GCS BucketService.

    Files are stored at `<root>/<filename>`. Generated URLs use the public
    URL prefix (configurable via `ORCHESTRA_LOCAL_PUBLIC_URL`, defaulting to
    the locally-served `/v0/storage/` route).
    """

    def __init__(
        self,
        root: Optional[Path] = None,
        public_url_prefix: Optional[str] = None,
    ):
        self.root = Path(
            root
            or os.environ.get("ORCHESTRA_LOCAL_BUCKET_ROOT")
            or DEFAULT_LOCAL_BUCKET_ROOT,
        )
        self.root.mkdir(parents=True, exist_ok=True)

        self.public_url_prefix = (
            public_url_prefix
            or os.environ.get(
                "ORCHESTRA_LOCAL_PUBLIC_URL",
                "http://127.0.0.1:8000/v0/storage",
            )
        ).rstrip("/")

        self.bucket_name = "local"
        self.bucket = None

    def _extension_from_content_type(self, content_type: str) -> str:
        ext = mimetypes.guess_extension(content_type or "")
        return (ext or "").lstrip(".")

    def _generate_unique_filename(self, content: bytes, extension: str = "") -> str:
        digest = hashlib.sha256(content).hexdigest()[:16]
        unique = uuid.uuid4().hex[:8]
        ext = f".{extension}" if extension else ""
        return f"{digest}-{unique}{ext}"

    def upload_media(
        self,
        base64_media: str,
        media_type: str,
    ) -> Tuple[str, str]:
        """Store a base64-encoded payload under a hash-derived filename.

        Returns ``(public_url, filename)``.
        """
        content = base64.b64decode(base64_media)
        ext = self._extension_from_content_type(media_type)
        filename = self._generate_unique_filename(content, ext)

        target = self.root / filename
        target.write_bytes(content)
        return self.get_media_url(filename), filename

    def get_media(self, filename: str) -> Optional[str]:
        """Return base64 content or None if missing."""
        target = self.root / filename
        if not target.exists():
            return None
        return base64.b64encode(target.read_bytes()).decode("ascii")

    def delete_media(self, filename: str) -> bool:
        target = self.root / filename
        try:
            target.unlink()
            return True
        except FileNotFoundError:
            return False
        except OSError as e:
            logger.warning(f"delete_media failed for {filename}: {e}")
            return False

    def get_media_url(self, filename: str) -> str:
        return f"{self.public_url_prefix}/{filename}"

    def is_allowed_bucket(self, bucket_name: str) -> bool:
        return bucket_name == self.bucket_name

    def get_signed_url(
        self,
        filename: str,
        expiration_seconds: int = 3600,
    ) -> str:
        """For local files there is no signing; expose the raw public URL."""
        return self.get_media_url(filename)
