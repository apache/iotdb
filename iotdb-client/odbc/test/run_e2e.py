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
"""Run ODBC plain/TLS/mTLS checks against a disposable copy of a server distribution."""

import argparse
import importlib.util
import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manager", type=Path)
    parser.add_argument("driver", type=Path)
    parser.add_argument("distribution", type=Path)
    parser.add_argument("--password-env", default="IOTDB_TEST_PASSWORD")
    parser.add_argument(
        "--windows-consumers",
        action="store_true",
        help="Also run .NET ODBC and MSDASQL/ADO on Windows",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=["plain", "tls", "mtls"],
        default=["plain", "tls", "mtls"],
    )
    args = parser.parse_args()
    if os.name != "nt" and not (shutil.which("lsof") or shutil.which("netstat")):
        parser.error("Server stop scripts require lsof or netstat")
    manager, driver = args.manager.resolve(), args.driver.resolve()
    cpp = Path(__file__).resolve().parents[2] / "client-cpp" / "test"
    spec = importlib.util.spec_from_file_location(
        "cpp_phases", cpp / "scripts" / "run_cpp_it_phases.py"
    )
    phases = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(phases)
    fixtures = cpp / "fixtures"
    password = os.environ.get(args.password_env)
    if password is None:
        parser.error(f"Set {args.password_env} to the test server password")
    for port in [6667, 10710, 10720, 10730, 10740, 10750, 10760, 18080]:
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                parser.error(
                    f"Port {port} is already in use; use an isolated test environment"
                )
    env = os.environ.copy()
    env["IOTDB_NO_PAUSE"] = "1"
    processes = []
    launch_logs = []

    def start_server(dist):
        if os.name != "nt":
            phases.start_server(dist, 120, env)
            return
        for node in ("confignode", "datanode"):
            log = (dist / f"{node}-launcher.log").open("a", encoding="utf-8")
            launch_logs.append(log)
            processes.append(
                subprocess.Popen(
                    [
                        "cmd",
                        "/d",
                        "/c",
                        str(dist / "sbin" / "windows" / f"start-{node}.bat"),
                        "-f",
                    ],
                    cwd=dist,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
            )
        phases.wait_for_rpc_port(120)

    def stop_server(dist):
        if os.name != "nt":
            phases.stop_server(dist, env)
            return
        # Terminate only launchers and Java children created by this runner.
        # Port-based stop scripts can target an unrelated process or PID 0.
        for process in reversed(processes):
            if process.poll() is None:
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
            process.wait(timeout=30)
        processes.clear()
        for log in launch_logs:
            log.close()
        launch_logs.clear()
        time.sleep(2)

    def check(
        options,
        sql="SHOW VERSION",
        failure=False,
        rows=None,
        value=None,
        parameter_ints=None,
    ):
        run_env = env.copy()
        run_env["IOTDB_ODBC_CONNECTION"] = (
            "SERVER=127.0.0.1;PORT=6667;UID=root;PWD={"
            + password.replace("}", "}}")
            + "};"
            "LOGLEVEL=0;BATCHSIZE=2;" + options
        )
        run_env["IOTDB_ODBC_QUERY"] = sql
        run_env.pop("IOTDB_ODBC_EXPECT_FAILURE", None)
        run_env.pop("IOTDB_ODBC_EXPECT_ROWS", None)
        run_env.pop("IOTDB_ODBC_EXPECT_VALUE", None)
        run_env.pop("IOTDB_ODBC_PARAMETER_INTS", None)
        if rows is not None:
            run_env["IOTDB_ODBC_EXPECT_ROWS"] = str(rows)
        if value is not None:
            run_env["IOTDB_ODBC_EXPECT_VALUE"] = value
        if parameter_ints is not None:
            run_env["IOTDB_ODBC_PARAMETER_INTS"] = ",".join(
                str(value) for value in parameter_ints
            )
        if failure:
            run_env["IOTDB_ODBC_EXPECT_FAILURE"] = "1"
        subprocess.run([str(manager), str(driver)], env=run_env, check=True, timeout=90)

    with tempfile.TemporaryDirectory(prefix="iotdb-odbc-e2e-") as temporary:
        dist = Path(temporary) / "server"
        shutil.copytree(
            args.distribution, dist, ignore=shutil.ignore_patterns("data", "logs")
        )
        if os.name == "nt":
            # Older distribution launchers pause after exit. Only adjust this
            # disposable test copy so unattended tests can clean up all files.
            for script in (dist / "sbin" / "windows").glob("*.bat"):
                text = script.read_text(encoding="utf-8")
                text = re.sub(
                    r"(?im)^\s*@?pause\s*$",
                    "rem pause disabled in ODBC test copy",
                    text,
                )
                text = re.sub(r"(?im)^(\s*)start (?=cmd\b)", r"\1start /b ", text)
                script.write_text(text, encoding="utf-8")
        properties = dist / "conf" / "iotdb-system.properties"
        with properties.open("a", encoding="utf-8") as config:
            config.write("\nenable_rest_service=true\nrest_service_port=18080\n")
        wrong_store = Path(temporary) / "wrong-identity.p12"
        wrong_cert = Path(temporary) / "wrong-identity.crt"
        if any(mode != "plain" for mode in args.modes):
            subprocess.run(
                [
                    "keytool",
                    "-genkeypair",
                    "-alias",
                    "server",
                    "-keyalg",
                    "RSA",
                    "-keystore",
                    str(wrong_store),
                    "-storetype",
                    "PKCS12",
                    "-storepass",
                    "thrift",
                    "-keypass",
                    "thrift",
                    "-dname",
                    "CN=wrong.invalid",
                    "-ext",
                    "SAN=dns:wrong.invalid",
                    "-validity",
                    "3650",
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                [
                    "keytool",
                    "-exportcert",
                    "-rfc",
                    "-alias",
                    "server",
                    "-keystore",
                    str(wrong_store),
                    "-storepass",
                    "thrift",
                    "-file",
                    str(wrong_cert),
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        for mode in args.modes:
            phases.run(
                [
                    os.sys.executable,
                    str(cpp / "scripts" / "configure_iotdb_ssl_it.py"),
                    str(dist),
                    str(fixtures),
                    mode,
                ],
                dist,
            )
            tls = ""
            if mode != "plain":
                tls = f"SSL=1;SSLCA={{{fixtures / 'tls' / 'ca.crt'}}};"
            if mode == "mtls":
                tls += f"SSLCERT={{{fixtures / 'tls' / 'client.crt'}}};SSLKEY={{{fixtures / 'tls' / 'client.key'}}};"
            try:
                start_server(dist)
                check(tls + "ISTABLEMODEL=0;")
                check(tls + "ISTABLEMODEL=1;DATABASE=information_schema;")
                check(tls + "ISTABLEMODEL=0;", "CREATE DATABASE root.odbc_migration")
                check(
                    tls + "ISTABLEMODEL=0;",
                    "INSERT INTO root.odbc_migration.d(time,v) VALUES(1,1),(2,2),(3,3)",
                )
                check(
                    tls + "ISTABLEMODEL=0;",
                    "INSERT INTO root.odbc_migration.d(time,v) VALUES(?,42)",
                    parameter_ints=[4, 5],
                )
                check(
                    tls + "ISTABLEMODEL=0;",
                    "SELECT v FROM root.odbc_migration.d",
                    rows=5,
                )
                check(tls + "ISTABLEMODEL=0;", "DELETE DATABASE root.odbc_migration")
                table = tls + "ISTABLEMODEL=1;DATABASE=information_schema;"
                check(table, "CREATE DATABASE odbc_migration")
                check(
                    table,
                    "CREATE TABLE odbc_migration.samples (device STRING TAG, val INT32 FIELD, txt STRING FIELD, day DATE FIELD)",
                )
                check(
                    table,
                    "INSERT INTO odbc_migration.samples(time,device,val,txt,day) VALUES (1,'d',1,'中文','2024-02-29'),(2,'d',2,null,'2024-03-01'),(3,'d',3,'last','2024-03-02')",
                )
                check(
                    table,
                    "SELECT val,txt,day FROM odbc_migration.samples ORDER BY time",
                    rows=3,
                    value="中文",
                )
                if mode == "plain":
                    check("RESTFUL=1;PORT=18080;ISTABLEMODEL=0;")
                    check(
                        "RESTFUL=1;PORT=18080;ISTABLEMODEL=1;DATABASE=odbc_migration;",
                        "SELECT val,txt,day FROM samples ORDER BY time",
                        rows=3,
                        value="中文",
                    )
                if args.windows_consumers:
                    if os.name != "nt":
                        parser.error("--windows-consumers requires Windows")
                    consumer_env = env.copy()
                    consumer_env["IOTDB_ODBC_CONNECTION"] = (
                        "SERVER=127.0.0.1;PORT=6667;UID=root;PWD={"
                        + password.replace("}", "}}")
                        + "};"
                        "ISTABLEMODEL=1;DATABASE=information_schema;BATCHSIZE=2;" + tls
                    )
                    subprocess.run(
                        [
                            "pwsh",
                            "-NoProfile",
                            "-File",
                            str(Path(__file__).with_name("windows-consumers.ps1")),
                            str(driver),
                        ],
                        env=consumer_env,
                        check=True,
                        timeout=120,
                    )
                check(table, "DROP DATABASE odbc_migration")
                if mode != "plain":
                    check("ISTABLEMODEL=0;SSL=1;SSLCA=missing-ca.pem;", failure=True)
                    check(f"ISTABLEMODEL=0;SSL=1;SSLCA={{{wrong_cert}}};", failure=True)
                    check("ISTABLEMODEL=0;SSL=0;", failure=True)
                if mode == "mtls":
                    check(
                        f"ISTABLEMODEL=0;SSL=1;SSLCA={{{fixtures / 'tls' / 'ca.crt'}}};",
                        failure=True,
                    )
                    check(tls + f"SSLCERT={{{wrong_cert}}};", failure=True)
            finally:
                stop_server(dist)
            print(f"ODBC {mode} checks passed", flush=True)

        if wrong_store.exists():
            # Trust the actual self-signed server certificate, but connect using
            # localhost, which is absent from its SAN. This isolates identity
            # verification from CA validation and connection-refused failures.
            phases.run(
                [
                    os.sys.executable,
                    str(cpp / "scripts" / "configure_iotdb_ssl_it.py"),
                    str(dist),
                    str(fixtures),
                    "tls",
                ],
                dist,
            )
            text = properties.read_text(encoding="utf-8")
            text = re.sub(
                r"^key_store_path=.*$",
                "key_store_path=" + wrong_store.as_posix(),
                text,
                flags=re.MULTILINE,
            )
            properties.write_text(text, encoding="utf-8")
            try:
                start_server(dist)
                check(f"ISTABLEMODEL=0;SSL=1;SSLCA={{{wrong_cert}}};", failure=True)
            finally:
                stop_server(dist)
            print("ODBC server identity rejection passed", flush=True)


if __name__ == "__main__":
    main()
