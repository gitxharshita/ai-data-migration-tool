"""Target adapters: object stores that hold the migrated Parquet/CSV files.

All adapters expose the same tiny key/value interface, so the executor and
the reconciliation engine never care where the bytes live. LocalTarget
mirrors S3 key semantics on the filesystem and is used for tests/evaluation.
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


class TargetConnector(ABC):
    @abstractmethod
    def write_bytes(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def read_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def list(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def delete_prefix(self, prefix: str) -> None: ...


class LocalTarget(TargetConnector):
    def __init__(self, root: str):
        self.root = Path(root)

    def write_bytes(self, key, data):
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def read_bytes(self, key):
        return (self.root / key).read_bytes()

    def list(self, prefix):
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file())

    def delete_prefix(self, prefix):
        for key in self.list(prefix):
            (self.root / key).unlink()


class S3Target(TargetConnector):
    """Amazon S3 or any S3-compatible store such as MinIO (pass `endpoint` or
    set AWS_ENDPOINT_URL). Credentials come from the standard AWS chain:
    environment variables, ~/.aws/credentials, or an IAM role."""

    def __init__(self, bucket: str, prefix: str = "", endpoint: str | None = None):
        import boto3
        self.s3 = boto3.client("s3", endpoint_url=endpoint or os.environ.get("AWS_ENDPOINT_URL"),
                               region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        self.bucket, self.prefix = bucket, prefix.strip("/")

    def _k(self, key):
        return f"{self.prefix}/{key}" if self.prefix else key

    def write_bytes(self, key, data):
        self.s3.put_object(Bucket=self.bucket, Key=self._k(key), Body=data)

    def read_bytes(self, key):
        return self.s3.get_object(Bucket=self.bucket, Key=self._k(key))["Body"].read()

    def list(self, prefix):
        keys, strip = [], len(self.prefix) + 1 if self.prefix else 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self._k(prefix)):
            keys += [o["Key"][strip:] for o in page.get("Contents", [])]
        return sorted(keys)

    def delete_prefix(self, prefix):
        for key in self.list(prefix):
            self.s3.delete_object(Bucket=self.bucket, Key=self._k(key))


class GCSTarget(TargetConnector):
    def __init__(self, bucket: str, prefix: str = ""):
        from google.cloud import storage
        self.bucket = storage.Client().bucket(bucket)
        self.prefix = prefix.strip("/")

    def _k(self, key):
        return f"{self.prefix}/{key}" if self.prefix else key

    def write_bytes(self, key, data):
        self.bucket.blob(self._k(key)).upload_from_string(data)

    def read_bytes(self, key):
        return self.bucket.blob(self._k(key)).download_as_bytes()

    def list(self, prefix):
        strip = len(self.prefix) + 1 if self.prefix else 0
        return sorted(b.name[strip:] for b in self.bucket.list_blobs(prefix=self._k(prefix)))

    def delete_prefix(self, prefix):
        for b in self.bucket.list_blobs(prefix=self._k(prefix)):
            b.delete()


class AzureBlobTarget(TargetConnector):
    """Azure Data Lake Storage Gen2 or Azure Blob Storage.

    ADLS Gen2 accounts also serve the Blob API, so one client covers both.
    Authentication, first match wins: AZURE_STORAGE_CONNECTION_STRING,
    AZURE_STORAGE_KEY (account key), AZURE_STORAGE_SAS_TOKEN, otherwise
    DefaultAzureCredential (az login, managed identity) from azure-identity.
    """

    def __init__(self, account: str | None, container: str, prefix: str = ""):
        from azure.storage.blob import BlobServiceClient
        conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if conn:
            svc = BlobServiceClient.from_connection_string(conn)
        else:
            account = account or os.environ.get("AZURE_STORAGE_ACCOUNT")
            if not account:
                raise ValueError("Azure storage account name missing: use abfss://container@ACCOUNT.dfs.core.windows.net/...")
            cred = os.environ.get("AZURE_STORAGE_KEY") or os.environ.get("AZURE_STORAGE_SAS_TOKEN")
            if cred is None:
                from azure.identity import DefaultAzureCredential
                cred = DefaultAzureCredential()
            svc = BlobServiceClient(f"https://{account}.blob.core.windows.net", credential=cred)
        self.container = svc.get_container_client(container)
        self.prefix = prefix.strip("/")
        self._checked = False

    def _k(self, key):
        return f"{self.prefix}/{key}" if self.prefix else key

    def _ensure_container(self):
        if not self._checked:
            if not self.container.exists():
                self.container.create_container()
            self._checked = True

    def write_bytes(self, key, data):
        self._ensure_container()
        self.container.upload_blob(self._k(key), data, overwrite=True)

    def read_bytes(self, key):
        return self.container.download_blob(self._k(key)).readall()

    def list(self, prefix):
        if not self.container.exists():
            return []
        strip = len(self.prefix) + 1 if self.prefix else 0
        return sorted(b.name[strip:] for b in self.container.list_blobs(name_starts_with=self._k(prefix)))

    def delete_prefix(self, prefix):
        for key in self.list(prefix):
            self.container.delete_blob(self._k(key))


def open_target(uri: str) -> TargetConnector:
    """file:///dir | /plain/dir | s3://bucket/prefix[?endpoint=http://localhost:9000]
    | gs://bucket/prefix | abfss://container@account.dfs.core.windows.net/prefix
    | az://container/prefix (account from AZURE_STORAGE_ACCOUNT)"""
    u = urlparse(uri)
    if u.scheme == "s3":
        endpoint = parse_qs(u.query).get("endpoint", [None])[0]
        return S3Target(u.netloc, u.path, endpoint)
    if u.scheme in ("abfs", "abfss", "wasb", "wasbs"):
        container, _, host = u.netloc.partition("@")
        return AzureBlobTarget(host.split(".")[0] or None, container, u.path)
    if u.scheme == "az":
        return AzureBlobTarget(None, u.netloc, u.path)
    if u.scheme == "gs":
        return GCSTarget(u.netloc, u.path)
    if u.scheme == "file":
        path = unquote(u.path)
        if len(path) > 2 and path[0] == "/" and path[2] == ":":  # file:///C:/...
            path = path[1:]
        return LocalTarget(path)
    return LocalTarget(uri)
