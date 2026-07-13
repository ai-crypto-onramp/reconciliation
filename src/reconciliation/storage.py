"""Object storage abstraction for archiving EOD reports.

Provides an in-memory fake (default, for tests/local dev) and a thin S3/GCS
adapter stub. Real cloud upload is implemented lazily via ``boto3`` so the
dependency is optional.
"""

from __future__ import annotations

import io
from typing import Any, Protocol


class ObjectStorage(Protocol):
    async def put(self, bucket: str, key: str, data: bytes, content_type: str = "text/csv") -> None: ...
    async def get(self, bucket: str, key: str) -> bytes: ...
    async def signed_url(self, bucket: str, key: str, expires: int = 3600) -> str: ...
    async def list_keys(self, bucket: str, prefix: str = "") -> list[str]: ...


class InMemoryObjectStorage:
    """In-memory object store used by tests and when no bucket is configured."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], bytes] = {}

    async def put(self, bucket: str, key: str, data: bytes, content_type: str = "text/csv") -> None:
        self._store[(bucket, key)] = data

    async def get(self, bucket: str, key: str) -> bytes:
        return self._store.get((bucket, key), b"")

    async def signed_url(self, bucket: str, key: str, expires: int = 3600) -> str:
        return f"memory://{bucket}/{key}?expires={expires}"

    async def list_keys(self, bucket: str, prefix: str = "") -> list[str]:
        return [key for (b, key) in self._store if b == bucket and key.startswith(prefix)]


class S3ObjectStorage:
    """S3-backed object store. ``boto3`` is imported lazily."""

    def __init__(self, region: str = "us-east-1") -> None:
        self.region = region
        self._client: Any = None

    def _ensure(self) -> None:
        if self._client is None:
            import boto3

            self._client = boto3.client("s3", region_name=self.region)

    async def put(self, bucket: str, key: str, data: bytes, content_type: str = "text/csv") -> None:
        self._ensure()
        assert self._client is not None
        self._client.put_object(Bucket=bucket, Key=key, Body=io.BytesIO(data), ContentType=content_type)

    async def get(self, bucket: str, key: str) -> bytes:
        self._ensure()
        assert self._client is not None
        resp = self._client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()

    async def signed_url(self, bucket: str, key: str, expires: int = 3600) -> str:
        self._ensure()
        assert self._client is not None
        return self._client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires
        )

    async def list_keys(self, bucket: str, prefix: str = "") -> list[str]:
        self._ensure()
        assert self._client is not None
        resp = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]


def build_storage(settings: object) -> ObjectStorage:
    """Factory: S3 when ``REPORTS_BUCKET`` is set and boto3 is available."""
    bucket = getattr(settings, "reports_bucket", "")
    if bucket:
        try:
            return S3ObjectStorage()
        except Exception:  # pragma: no cover - boto3 missing
            return InMemoryObjectStorage()
    return InMemoryObjectStorage()
