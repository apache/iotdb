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

"""Configure an IoTDB distribution for C++ TLS integration tests."""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

STORE_PASSWORD = "thrift"


def replace_property(text: str, key: str, value: str) -> str:
    pattern = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
    replacement = f"{key}={value}"
    if pattern.search(text):
        return pattern.sub(replacement, text, count=1)
    return text.rstrip() + "\n" + replacement + "\n"


def configure(dist_root: Path, fixtures_root: Path, mode: str) -> None:
    properties = dist_root / "conf" / "iotdb-system.properties"
    if mode == "plain":
        text = properties.read_text(encoding="utf-8")
        for key, value in {
            "enable_thrift_ssl": "false",
            "thrift_ssl_client_auth": "false",
            "key_store_path": "",
            "key_store_pwd": "",
            "trust_store_path": "",
            "trust_store_pwd": "",
            "ssl_protocol": "TLS",
        }.items():
            text = replace_property(text, key, value)
        properties.write_text(text, encoding="utf-8", newline="\n")
        return

    mutual_tls = mode == "mtls"
    ssl_dir = dist_root / "conf" / "cpp-ssl-it"
    ssl_dir.mkdir(parents=True, exist_ok=True)

    server_store = ssl_dir / "tls-server.p12"
    shutil.copy2(fixtures_root / "tls" / "tls-server.p12", server_store)

    trust_store = ""
    if mutual_tls:
        trust_store_path = ssl_dir / "tls-server-trust.p12"
        trust_store_path.unlink(missing_ok=True)
        subprocess.run(
            [
                "keytool",
                "-importcert",
                "-noprompt",
                "-alias",
                "cpp-ssl-it-ca",
                "-file",
                str(fixtures_root / "tls" / "ca.crt"),
                "-keystore",
                str(trust_store_path),
                "-storetype",
                "PKCS12",
                "-storepass",
                STORE_PASSWORD,
            ],
            check=True,
        )
        trust_store = trust_store_path.as_posix()

    text = properties.read_text(encoding="utf-8")
    settings = {
        "enable_thrift_ssl": "true",
        "thrift_ssl_client_auth": str(mutual_tls).lower(),
        "key_store_path": server_store.as_posix(),
        "key_store_pwd": STORE_PASSWORD,
        "trust_store_path": trust_store,
        "trust_store_pwd": STORE_PASSWORD if mutual_tls else "",
        "ssl_protocol": "TLS",
    }
    for key, value in settings.items():
        text = replace_property(text, key, value)
    properties.write_text(text, encoding="utf-8", newline="\n")


def main() -> int:
    if len(sys.argv) != 4 or sys.argv[3] not in ("plain", "tls", "mtls"):
        print(
            "usage: configure_iotdb_ssl_it.py <dist-root> <fixtures-root> <plain|tls|mtls>",
            file=sys.stderr,
        )
        return 2
    configure(Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve(), sys.argv[3])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
