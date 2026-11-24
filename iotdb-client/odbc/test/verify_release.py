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
"""Verify an ODBC release before extraction, publication or runtime testing."""

import argparse
import hashlib
import re
import stat
import zipfile
from pathlib import Path, PurePosixPath


def verify(package, commit, platform):
    package = Path(package)
    checksum = Path(str(package) + ".sha512").read_text(encoding="ascii").strip()
    match = re.fullmatch(r"([0-9a-fA-F]{128})  (.+)", checksum)
    if not match or match[2] != package.name:
        raise ValueError("Checksum must name exactly this package")
    digest = hashlib.sha512()
    with package.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != match[1].lower():
        raise ValueError("SHA-512 mismatch")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Expected a full source commit SHA")
    root = package.stem
    required = {
        "BUILD-INFO.txt",
        "README.md",
        "LICENSE",
        "NOTICE",
        "conf/odbc.ini",
        "conf/odbcinst.ini",
        "licenses/curl.txt",
        "licenses/json.txt",
    }
    if platform == "windows-x86_64":
        required.update(
            {
                "bin/apache_iotdb_odbc.dll",
                "bin/install_driver.exe",
                "install.bat",
                "uninstall.bat",
                "test/odbc_manager.exe",
            }
        )
        binary = "bin/apache_iotdb_odbc.dll"
    elif platform == "linux-x86_64-glibc2.28":
        required.update(
            {
                "lib/libapache_iotdb_odbc.so",
                "test/odbc_manager",
                "licenses/gcc/GPL-3.0.txt",
                "licenses/gcc/GCC-Runtime-Exception.txt",
            }
        )
        binary = "lib/libapache_iotdb_odbc.so"
    else:
        raise ValueError("Unsupported release platform")
    with zipfile.ZipFile(package) as archive:
        files = {}
        seen = set()
        for entry in archive.infolist():
            path = PurePosixPath(entry.filename)
            if (
                "\\" in entry.filename
                or ":" in entry.filename
                or path.is_absolute()
                or ".." in path.parts
                or not path.parts
                or path.parts[0] != root
                or stat.S_ISLNK(entry.external_attr >> 16)
            ):
                raise ValueError(f"Unsafe package entry: {entry.filename}")
            name = str(path).casefold()
            if name in seen:
                raise ValueError(f"Duplicate package entry: {entry.filename}")
            seen.add(name)
            if not entry.is_dir():
                files[str(PurePosixPath(*path.parts[1:]))] = entry
        missing = required - files.keys()
        if missing:
            raise ValueError(f"Missing release files: {sorted(missing)}")
        if any(files[name].file_size == 0 for name in required):
            raise ValueError("Empty required release file")
        if not any(name.startswith("licenses/sdk/") for name in files):
            raise ValueError("Missing SDK third-party notices")
        libraries = {
            name
            for name in files
            if re.search(r"\.(dll|so(?:\..*)?|dylib)$", name, re.I)
        }
        if libraries != {binary}:
            raise ValueError(
                f"Expected one self-contained driver library: {sorted(libraries)}"
            )
        if any(
            PurePosixPath(name).suffix.lower() in {".pdb", ".obj", ".lib", ".a", ".log"}
            for name in files
        ):
            raise ValueError("Build intermediates or logs leaked into release")
        info = dict(
            line.split("=", 1)
            for line in archive.read(files["BUILD-INFO.txt"])
            .decode("utf-8")
            .splitlines()
            if "=" in line
        )
        if info.get("commit") != commit or info.get("platform") != platform:
            raise ValueError(
                "Package provenance does not match requested commit/platform"
            )
        version = info.get("version", "")
        if not version or root != f"iotdb-odbc-{version}-{platform}":
            raise ValueError(
                "Package filename does not match BUILD-INFO version/platform"
            )
        for dependency in ("thrift", "openssl", "curl", "json", "compiler"):
            if not info.get(dependency):
                raise ValueError(f"Missing build metadata: {dependency}")
        for dependency in ("session", "openssl", "curl"):
            if info.get(dependency + ".linkage") != "static":
                raise ValueError(f"Expected static {dependency} linkage")
        bad = archive.testzip()
        if bad:
            raise ValueError(f"Corrupt ZIP entry: {bad}")
    return info


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package", type=Path)
    parser.add_argument("--commit", required=True)
    parser.add_argument(
        "--platform",
        required=True,
        choices=["windows-x86_64", "linux-x86_64-glibc2.28"],
    )
    args = parser.parse_args()
    info = verify(args.package, args.commit, args.platform)
    print(f"Verified {args.package.name}: commit={info['commit']}")


if __name__ == "__main__":
    main()
