"""Connection strings for SQL Server and Azure SQL Database (no server needed)."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amt.connectors.sources import mssql_connection_string as cs, parse_mssql_uri  # noqa: E402

D = "ODBC Driver 18 for SQL Server"


class AzureSqlTest(unittest.TestCase):
    def test_local_windows_login_unchanged(self):
        s = cs("localhost\\SQLEXPRESS", "amt_corpus", driver=D)
        self.assertIn("Trusted_Connection=yes", s)
        self.assertIn("TrustServerCertificate=yes", s)
        self.assertNotIn("Encrypt=yes", s)

    def test_azure_sql_login_is_encrypted_and_verified(self):
        cfg = parse_mssql_uri("mssql://sqladmin:p%40ss%3Bword@myserver.database.windows.net:1433/SalesDB"
                              "?schema=Sales&encrypt=yes")
        self.assertEqual((cfg["server"], cfg["database"], cfg["schema"], cfg["password"]),
                         ("myserver.database.windows.net,1433", "SalesDB", "Sales", "p@ss;word"))
        s = cs(cfg["server"], cfg["database"], cfg["user"], cfg["password"], D, cfg["authentication"], cfg["encrypt"])
        self.assertIn("Encrypt=yes", s)
        self.assertIn("TrustServerCertificate=no", s)
        self.assertIn("UID=sqladmin", s)
        self.assertIn("PWD={p@ss;word}", s)          # special characters are brace-quoted
        self.assertNotIn("Trusted_Connection", s)

    def test_azure_encrypts_even_without_flag(self):
        self.assertIn("Encrypt=yes", cs("myserver.database.windows.net,1433", "db", "u", "p", D))

    def test_entra_interactive_sends_no_password(self):
        cfg = parse_mssql_uri("mssql://you%40company.com@myserver.database.windows.net:1433/db"
                              "?encrypt=yes&authentication=ActiveDirectoryInteractive")
        s = cs(cfg["server"], cfg["database"], cfg["user"], cfg["password"], D, cfg["authentication"], cfg["encrypt"])
        self.assertIn("Authentication=ActiveDirectoryInteractive", s)
        self.assertIn("UID=you@company.com", s)
        self.assertNotIn("PWD", s)

    def test_entra_password(self):
        s = cs("myserver.database.windows.net,1433", "db", "you@company.com", "secret", D, "ActiveDirectoryPassword")
        self.assertIn("Authentication=ActiveDirectoryPassword", s)
        self.assertIn("PWD=secret", s)

    def test_managed_identity_needs_no_user(self):
        s = cs("myserver.database.windows.net,1433", "db", None, None, D, "ActiveDirectoryMsi")
        self.assertIn("Authentication=ActiveDirectoryMsi", s)
        self.assertNotIn("UID", s)

    def test_azure_without_sign_in_is_rejected(self):
        with self.assertRaises(ValueError):
            cs("myserver.database.windows.net,1433", "db", driver=D)

    def test_unknown_auth_rejected(self):
        with self.assertRaises(ValueError):
            cs("myserver.database.windows.net", "db", "u", "p", D, "NotAMethod")


if __name__ == "__main__":
    unittest.main()
