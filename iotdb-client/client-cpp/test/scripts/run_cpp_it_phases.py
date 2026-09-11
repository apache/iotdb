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

"""Run C++ client tests against plain TLS and mutual TLS IoTDB servers."""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path


def run(
    command: list[str],
    cwd: Path,
    env: dict[str, str] | None = None,
    shell: bool = False,
) -> None:
    print(f"+ {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=cwd, env=env, shell=shell, check=True)


def server_script(dist_root: Path, action: str) -> Path:
    if sys.platform == "win32":
        return dist_root / "sbin" / "windows" / f"{action}-standalone.bat"
    return dist_root / "sbin" / f"{action}-standalone.sh"


def stop_server(dist_root: Path, env: dict[str, str]) -> None:
    subprocess.run(
        [str(server_script(dist_root, "stop"))],
        cwd=dist_root,
        env=env,
        shell=sys.platform == "win32",
        check=False,
    )
    time.sleep(10)


def wait_for_rpc_port(timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    consecutive_successes = 0
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", 6667), timeout=1):
                consecutive_successes += 1
                if consecutive_successes == 3:
                    return
        except OSError:
            consecutive_successes = 0
        time.sleep(1)
    raise TimeoutError(f"IoTDB RPC port did not become ready within {timeout_seconds} seconds")


def start_server(dist_root: Path, wait_seconds: int, env: dict[str, str]) -> None:
    run(
        [str(server_script(dist_root, "start"))],
        dist_root,
        env=env,
        shell=sys.platform == "win32",
    )
    wait_for_rpc_port(wait_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build_dir", type=Path)
    parser.add_argument("dist_root", type=Path)
    parser.add_argument("fixtures_root", type=Path)
    parser.add_argument("--config", default="Release")
    parser.add_argument("--wait-seconds", type=int, default=120)
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=("plain", "tls", "mtls"),
        default=("plain", "tls", "mtls"),
    )
    args = parser.parse_args()

    build_dir = args.build_dir.resolve()
    dist_root = args.dist_root.resolve()
    fixtures_root = args.fixtures_root.resolve()
    configure_script = Path(__file__).with_name("configure_iotdb_ssl_it.py")
    ctest = ["ctest", "--output-on-failure", "-C", args.config]
    server_env = os.environ.copy()
    if sys.platform == "win32":
        # Node scripts pause after the Java process exits so an interactive console
        # stays open. In CI those paused cmd.exe children keep the job's standard
        # handles open after the tests finish, preventing Maven from returning.
        server_env["IOTDB_NO_PAUSE"] = "1"

    for mode in args.modes:
        stop_server(dist_root, server_env)
        run(
            [sys.executable, str(configure_script), str(dist_root), str(fixtures_root), mode],
            configure_script.parent,
        )
        try:
            start_server(dist_root, args.wait_seconds, server_env)
            env = server_env.copy()
            if mode == "mtls":
                env["IOTDB_CPP_SSL_MUTUAL_AUTH"] = "1"
            label = {"plain": "plain", "tls": "tls-only", "mtls": "mutual-auth"}[mode]
            run(ctest + ["-L", label], build_dir, env)
        finally:
            stop_server(dist_root, server_env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
