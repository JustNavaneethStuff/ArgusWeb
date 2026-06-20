from __future__ import annotations

import hashlib
import io
from datetime import timedelta

from minio import Minio

from argus_core.settings import Settings


def create_minio_client(settings: Settings) -> Minio:
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


class HtmlStorage:
    def __init__(self, client: Minio, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def ensure_bucket(self) -> None:
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    def storage_key(self, job_id: str, url_hash: str) -> str:
        return f"{job_id}/{url_hash}.html"

    def upload(self, job_id: str, url_hash: str, html: bytes) -> tuple[str, str, int]:
        self.ensure_bucket()
        key = self.storage_key(job_id, url_hash)
        checksum = hashlib.sha256(html).hexdigest()
        self._client.put_object(
            self._bucket,
            key,
            io.BytesIO(html),
            length=len(html),
            content_type="text/html; charset=utf-8",
        )
        return key, checksum, len(html)

    def download(self, storage_key: str) -> bytes:
        response = self._client.get_object(self._bucket, storage_key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def presigned_url(self, storage_key: str, expires: timedelta = timedelta(hours=1)) -> str:
        return self._client.presigned_get_object(self._bucket, storage_key, expires=expires)
