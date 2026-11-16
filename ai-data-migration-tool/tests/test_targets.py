"""Target connectors. Cloud SDKs are replaced by in-memory fakes, so these
tests run offline and check our path, listing and delete logic."""
import os
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amt.connectors import AzureBlobTarget, open_target


class _Blob:
    def __init__(self, name):
        self.name = name


class _FakeContainer:
    def __init__(self):
        self.blobs, self.created = {}, False

    def exists(self):
        return self.created

    def create_container(self):
        self.created = True

    def upload_blob(self, name, data, overwrite=False):
        if name in self.blobs and not overwrite:
            raise RuntimeError("exists")
        self.blobs[name] = data

    def download_blob(self, name):
        return types.SimpleNamespace(readall=lambda: self.blobs[name])

    def list_blobs(self, name_starts_with=""):
        return [_Blob(n) for n in self.blobs if n.startswith(name_starts_with)]

    def delete_blob(self, name):
        del self.blobs[name]


class _FakeService:
    containers = {}

    def __init__(self, url, credential=None):
        self.url, self.credential = url, credential

    @classmethod
    def from_connection_string(cls, conn):
        return cls("from-connection-string")

    def get_container_client(self, name):
        return self.containers.setdefault(name, _FakeContainer())


def fake_azure():
    blob = types.ModuleType("azure.storage.blob")
    blob.BlobServiceClient = _FakeService
    return {"azure": types.ModuleType("azure"), "azure.storage": types.ModuleType("azure.storage"),
            "azure.storage.blob": blob}


class AzureTargetTests(unittest.TestCase):
    def setUp(self):
        _FakeService.containers = {}

    def test_abfss_uri_round_trip(self):
        with mock.patch.dict(sys.modules, fake_azure()), \
                mock.patch.dict(os.environ, {"AZURE_STORAGE_KEY": "k"}, clear=False):
            os.environ.pop("AZURE_STORAGE_CONNECTION_STRING", None)
            t = open_target("abfss://lake@myaccount.dfs.core.windows.net/migrations/run1")
            self.assertIsInstance(t, AzureBlobTarget)
            self.assertEqual(t.list("orders"), [])               # container not created yet
            t.write_bytes("orders/placed_at=2024-01/part-00000.parquet", b"a")
            t.write_bytes("orders/placed_at=2024-02/part-00000.parquet", b"b")
            t.write_bytes("orders/placed_at=2024-02/part-00000.parquet", b"c")   # overwrite
            self.assertEqual(t.list("orders"), ["orders/placed_at=2024-01/part-00000.parquet",
                                                "orders/placed_at=2024-02/part-00000.parquet"])
            self.assertEqual(t.read_bytes("orders/placed_at=2024-02/part-00000.parquet"), b"c")
            stored = _FakeService.containers["lake"].blobs
            self.assertIn("migrations/run1/orders/placed_at=2024-01/part-00000.parquet", stored)
            t.delete_prefix("orders")
            self.assertEqual(t.list("orders"), [])

    def test_connection_string_takes_priority(self):
        with mock.patch.dict(sys.modules, fake_azure()), \
                mock.patch.dict(os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "UseDevelopmentStorage=true"}):
            t = open_target("az://lake/x")
            t.write_bytes("k", b"v")
            self.assertEqual(t.read_bytes("k"), b"v")

    def test_missing_account_is_explained(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("AZURE_STORAGE")}
        with mock.patch.dict(sys.modules, fake_azure()), mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ValueError):
                open_target("az://lake/x")


class LocalTargetTests(unittest.TestCase):
    def test_file_uri_and_plain_path(self):
        import tempfile
        d = tempfile.mkdtemp()
        for t in (open_target(d), open_target("file:///" + d.replace("\\", "/"))):
            t.write_bytes("a/b.txt", b"x")
            self.assertEqual(t.list("a"), ["a/b.txt"])
            t.delete_prefix("a")


if __name__ == "__main__":
    unittest.main()
