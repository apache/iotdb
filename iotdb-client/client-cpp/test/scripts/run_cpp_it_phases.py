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

"""Run C++ client integration tests in two IoTDB modes: plain then TLS."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


def run(cmd: list[str], cwd: Path) -> None:
    print(f"+ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=str(cwd), check=True)


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


def start_iotdb(dist_root: Path, start_script: Path, wait_s: int) -> None:
    if not start_script.is_file():
        raise FileNotFoundError(f"start script not found: {start_script}")
    print(f"Starting IoTDB via {start_script}")
    subprocess.Popen(
        [str(start_script)],
        cwd=str(dist_root),
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"Waiting {wait_s}s for IoTDB to become ready")
    time.sleep(wait_s)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build_dir", help="CMake build directory containing CTestTestfile.cmake")
    parser.add_argument("dist_root", help="IoTDB distribution root")
    parser.add_argument("fixtures_root", help="C++ test fixtures root")
    parser.add_argument("scripts_root", help="Directory containing configure_iotdb_ssl_it.py")
    parser.add_argument("start_script", help="Relative path to start-standalone script under dist sbin/")
    parser.add_argument("--config", default="Release", help="CTest build configuration (MSVC)")
    parser.add_argument("--wait-seconds", type=int, default=45, help="Seconds to wait after IoTDB start")
    args = parser.parse_args()

    build_dir = Path(args.build_dir).resolve()
    dist_root = Path(args.dist_root).resolve()
    fixtures_root = Path(args.fixtures_root).resolve()
    scripts_root = Path(args.scripts_root).resolve()
    start_script = dist_root / "sbin" / args.start_script

    ctest_base = ["ctest", "-j", "1", "--output-on-failure"]
    if args.config:
        ctest_base.extend(["-C", args.config])

    print("=== Phase 1: plain IoTDB (session IT + examples) ===")
    run(ctest_base + ["-L", "plain"], build_dir)

    print("=== Phase 2: restart IoTDB with TLS (rpc SSL/NTLS IT) ===")
    stop_iotdb(dist_root)
    configure = scripts_root / "configure_iotdb_ssl_it.py"
    run(
        [sys.executable, str(configure), str(dist_root), str(fixtures_root), "enable"],
        cwd=scripts_root,
    )
    start_iotdb(dist_root, start_script, args.wait_seconds)
    run(ctest_base + ["-L", "ssl"], build_dir)
    print("=== Phase 2b: NTLS (no IoTDB; openssl s_server) ===")
    run(ctest_base + ["-L", "ntls"], build_dir)

    print("All C++ integration test phases passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
