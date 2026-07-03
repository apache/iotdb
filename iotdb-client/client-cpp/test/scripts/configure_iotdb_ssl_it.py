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

"""Patch IoTDB distribution RPC SSL settings for C++ integration tests."""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

STORE_PASSWORD = "thrift"
SERVER_PKCS12 = "tls-server.p12"


def replace_property(text: str, key: str, value: str) -> str:
    pattern = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
    replacement = f"{key}={value}"
    if pattern.search(text):
        return pattern.sub(replacement, text, count=1)
    return text.rstrip() + "\n" + replacement + "\n"


def stop_iotdb(dist_root: Path) -> None:
    if sys.platform == "win32":
        stop_script = dist_root / "sbin" / "windows" / "stop-standalone.bat"
    else:
        stop_script = dist_root / "sbin" / "stop-standalone.sh"
    if not stop_script.is_file():
        print(f"stop script not found, skip stop: {stop_script}", file=sys.stderr)
        return
    print(f"Stopping IoTDB via {stop_script}")
    subprocess.run([str(stop_script)], cwd=str(dist_root), check=False, shell=True)
    time.sleep(15)


def configure_plain(dist_root: Path) -> int:
    props_path = dist_root / "conf" / "iotdb-system.properties"
    if not props_path.is_file():
        print(f"iotdb-system.properties not found: {props_path}", file=sys.stderr)
        return 1

    text = props_path.read_text(encoding="utf-8")
    text = replace_property(text, "enable_thrift_ssl", "false")
    text = replace_property(text, "thrift_ssl_client_auth", "false")
    text = replace_property(text, "key_store_path", "")
    text = replace_property(text, "key_store_pwd", "")
    text = replace_property(text, "trust_store_path", "")
    text = replace_property(text, "trust_store_pwd", "")
    text = replace_property(text, "ssl_protocol", "TLS")
    props_path.write_text(text, encoding="utf-8", newline="\n")
    print(f"Configured plain RPC in {props_path}")
    return 0


def configure_tls(dist_root: Path, fixtures_root: Path) -> int:
    props_path = dist_root / "conf" / "iotdb-system.properties"
    if not props_path.is_file():
        print(f"iotdb-system.properties not found: {props_path}", file=sys.stderr)
        return 1

    ssl_dir = dist_root / "conf" / "cpp-ssl-it"
    ssl_dir.mkdir(parents=True, exist_ok=True)
    source = fixtures_root / "tls" / SERVER_PKCS12
    if not source.is_file():
        print(f"fixture missing: {source}", file=sys.stderr)
        return 1
    shutil.copy2(source, ssl_dir / SERVER_PKCS12)

    key_store = (ssl_dir / SERVER_PKCS12).as_posix()

    text = props_path.read_text(encoding="utf-8")
    text = replace_property(text, "enable_thrift_ssl", "true")
    text = replace_property(text, "thrift_ssl_client_auth", "false")
    text = replace_property(text, "key_store_path", key_store)
    text = replace_property(text, "key_store_pwd", STORE_PASSWORD)
    text = replace_property(text, "trust_store_path", "")
    text = replace_property(text, "trust_store_pwd", "")
    text = replace_property(text, "ssl_protocol", "TLS")
    props_path.write_text(text, encoding="utf-8", newline="\n")
    print(f"Configured TLS IT server properties in {props_path}")
    return 0


def main() -> int:
    if len(sys.argv) < 3:
        print(
            "usage: configure_iotdb_ssl_it.py <iotdb-dist-root> <fixtures-root> [enable|disable]",
            file=sys.stderr,
        )
        return 2

    dist_root = Path(sys.argv[1]).resolve()
    fixtures_root = Path(sys.argv[2]).resolve()
    mode = sys.argv[3].lower() if len(sys.argv) >= 4 else "enable"

    if mode in ("disable", "plain", "off"):
        stop_iotdb(dist_root)
        return configure_plain(dist_root)

    if mode in ("enable", "tls", "on"):
        stop_iotdb(dist_root)
        return configure_tls(dist_root, fixtures_root)

    print(f"unknown mode: {mode}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
