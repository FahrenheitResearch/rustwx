from __future__ import annotations

import mimetypes
from pathlib import Path

import boto3
from botocore.client import BaseClient

from .config import Settings


def content_type_for(path: Path) -> str:
    guess, _ = mimetypes.guess_type(path.name)
    if guess:
        return guess
    if path.suffix.lower() == ".webp":
        return "image/webp"
    if path.suffix.lower() == ".png":
        return "image/png"
    if path.suffix.lower() == ".json":
        return "application/json"
    return "application/octet-stream"


class ArtifactStore:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._client: BaseClient | None = None

    def enabled(self) -> bool:
        return self.settings.r2_enabled()

    @property
    def client(self) -> BaseClient:
        if self._client is None:
            if not self.enabled():
                raise RuntimeError("R2 is not configured")
            self._client = boto3.client(
                "s3",
                endpoint_url=self.settings.r2_endpoint_url,
                aws_access_key_id=self.settings.r2_access_key_id,
                aws_secret_access_key=self.settings.r2_secret_access_key,
                region_name="auto",
            )
        return self._client

    def upload_file(self, local_path: Path, key: str, *, immutable: bool = True) -> str | None:
        if not self.enabled():
            return None
        cache_control = (
            "public, max-age=31536000, immutable"
            if immutable
            else "public, max-age=60, stale-while-revalidate=300"
        )
        self.client.upload_file(
            str(local_path),
            self.settings.r2_bucket,
            key,
            ExtraArgs={
                "ContentType": content_type_for(local_path),
                "CacheControl": cache_control,
            },
        )
        if self.settings.public_artifact_base_url:
            return f"{self.settings.public_artifact_base_url.rstrip('/')}/{key}"
        return None

    def upload_tree(self, root: Path, key_prefix: str) -> list[dict[str, str | None]]:
        uploads: list[dict[str, str | None]] = []
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            key = f"{key_prefix.rstrip('/')}/{rel}"
            url = self.upload_file(path, key, immutable=True)
            uploads.append(
                {
                    "path": str(path),
                    "key": key,
                    "url": url,
                    "format": path.suffix.lower().lstrip("."),
                    "size_bytes": path.stat().st_size,
                }
            )
        return uploads
