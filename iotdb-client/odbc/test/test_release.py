#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Release gate regressions, including rehashed malformed archives."""

import hashlib
import tempfile
import unittest
import zipfile
from pathlib import Path

from verify_release import verify


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.commit = "a" * 40
        self.platform = "windows-x86_64"
        self.package = (
            Path(self.temp.name) / "iotdb-odbc-2.0.11-SNAPSHOT-windows-x86_64.zip"
        )
        self.files = {
            name: b"fixture"
            for name in (
                "README.md",
                "LICENSE",
                "NOTICE",
                "conf/odbc.ini",
                "conf/odbcinst.ini",
                "licenses/curl.txt",
                "licenses/json.txt",
                "licenses/sdk/NOTICE",
                "bin/apache_iotdb_odbc.dll",
                "bin/install_driver.exe",
                "install.bat",
                "uninstall.bat",
                "test/odbc_manager.exe",
            )
        }
        self.files["BUILD-INFO.txt"] = (
            "version=2.0.11-SNAPSHOT\ncommit="
            + self.commit
            + "\nplatform="
            + self.platform
            + "\ncompiler=MSVC 19\nthrift=0.24.0\nopenssl=3.5.8"
            "\ncurl=8.4.0\njson=3.11.3\nsession.linkage=static\nopenssl.linkage=static\ncurl.linkage=static\n"
        ).encode()

    def pack(self):
        with zipfile.ZipFile(self.package, "w") as archive:
            for name, value in self.files.items():
                archive.writestr(self.package.stem + "/" + name, value)
        digest = hashlib.sha512(self.package.read_bytes()).hexdigest()
        Path(str(self.package) + ".sha512").write_text(
            digest + "  " + self.package.name + "\n"
        )

    def check(self):
        return verify(self.package, self.commit, self.platform)

    def test_valid(self):
        self.pack()
        self.assertEqual(self.check()["commit"], self.commit)

    def test_valid_linux(self):
        self.platform = "linux-x86_64-glibc2.28"
        self.package = self.package.with_name(
            self.package.name.replace("windows-x86_64", self.platform)
        )
        for name in (
            "bin/apache_iotdb_odbc.dll",
            "bin/install_driver.exe",
            "install.bat",
            "uninstall.bat",
            "test/odbc_manager.exe",
        ):
            del self.files[name]
        for name in (
            "lib/libapache_iotdb_odbc.so",
            "test/odbc_manager",
            "licenses/gcc/GPL-3.0.txt",
            "licenses/gcc/GCC-Runtime-Exception.txt",
        ):
            self.files[name] = b"fixture"
        self.files["BUILD-INFO.txt"] = self.files["BUILD-INFO.txt"].replace(
            b"windows-x86_64", self.platform.encode()
        )
        self.pack()
        self.assertEqual(self.check()["platform"], self.platform)

    def test_tampered_archive(self):
        self.pack()
        with self.package.open("ab") as stream:
            stream.write(b"tampered")
        with self.assertRaisesRegex(ValueError, "SHA-512"):
            self.check()

    def test_wrong_commit(self):
        self.pack()
        self.commit = "b" * 40
        with self.assertRaisesRegex(ValueError, "provenance"):
            self.check()

    def test_path_traversal(self):
        self.files["../outside.txt"] = b"escape"
        self.pack()
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            self.check()

    def test_missing_license(self):
        del self.files["LICENSE"]
        self.pack()
        with self.assertRaisesRegex(ValueError, "Missing release"):
            self.check()

    def test_extra_runtime(self):
        self.files["bin/libssl.dll"] = b"unbundled runtime"
        self.pack()
        with self.assertRaisesRegex(ValueError, "self-contained"):
            self.check()

    def test_duplicate_windows_name(self):
        self.files["readme.md"] = b"ambiguous"
        self.pack()
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.check()

    def test_empty_driver(self):
        self.files["bin/apache_iotdb_odbc.dll"] = b""
        self.pack()
        with self.assertRaisesRegex(ValueError, "Empty"):
            self.check()

    def test_wrong_checksum_filename(self):
        self.pack()
        checksum = Path(str(self.package) + ".sha512")
        checksum.write_text(
            checksum.read_text().replace(self.package.name, "other.zip")
        )
        with self.assertRaisesRegex(ValueError, "name exactly"):
            self.check()


if __name__ == "__main__":
    unittest.main()
