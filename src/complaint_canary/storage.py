"""Landing zone: immutable files keyed by source and snapshot date."""

from __future__ import annotations

import re
import shutil
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

_DATE_KEY = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/")


class Landing(ABC):
    @abstractmethod
    def put(self, local_path: Path, key: str) -> str:
        """Store a file under ``key`` and return its URI."""

    @abstractmethod
    def put_text(self, text: str, key: str) -> str: ...

    @abstractmethod
    def uri(self, key: str) -> str: ...

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]: ...

    def list_dates(self, prefix: str) -> list[date]:
        found = {m.group(1) for key in self.list_keys(prefix) if (m := _DATE_KEY.search(key))}
        return sorted(date.fromisoformat(d) for d in found)


class LocalLanding(Landing):
    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()

    def put(self, local_path: Path, key: str) -> str:
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target)
        return str(target)

    def put_text(self, text: str, key: str) -> str:
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        return str(target)

    def uri(self, key: str) -> str:
        return str(self.root / key)

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file())


class GcsLanding(Landing):
    def __init__(self, uri: str) -> None:
        bucket, _, prefix = uri.removeprefix("gs://").partition("/")
        self.bucket_name = bucket
        self.prefix = prefix.strip("/")
        self._client = None

    @property
    def _bucket(self):
        if self._client is None:
            from google.cloud import storage

            self._client = storage.Client()
        return self._client.bucket(self.bucket_name)

    def _name(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put(self, local_path: Path, key: str) -> str:
        self._bucket.blob(self._name(key)).upload_from_filename(str(local_path))
        return self.uri(key)

    def put_text(self, text: str, key: str) -> str:
        self._bucket.blob(self._name(key)).upload_from_string(text, content_type="text/markdown")
        return self.uri(key)

    def uri(self, key: str) -> str:
        return f"gs://{self.bucket_name}/{self._name(key)}"

    def list_keys(self, prefix: str) -> list[str]:
        strip = len(self.prefix) + 1 if self.prefix else 0
        blobs = self._bucket.client.list_blobs(self.bucket_name, prefix=self._name(prefix))
        return sorted(blob.name[strip:] for blob in blobs)


def get_landing(landing_uri: str) -> Landing:
    if landing_uri.startswith("gs://"):
        return GcsLanding(landing_uri)
    return LocalLanding(Path(landing_uri))
