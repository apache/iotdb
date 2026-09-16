#!/usr/bin/env python3
#
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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Prepare isolated databases and verify every row before model evaluation."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import itertools
import json
import os
from pathlib import Path
import secrets

from fixture import (
    TABLE_NAMES,
    ROLE_COLUMNS,
    fixture_sql,
    fixture_hash,
    canonical_hash,
    expected_schema,
)
from tool_adapter import run_cli, table_rows


def checked(config, sql, directory):
    response, _ = run_cli(config, "sql", sql, directory, timeout=120)
    if response["error_kind"]:
        raise RuntimeError("IoTDB preparation failed; inspect " + str(directory))
    return (Path(directory) / "stdout.txt").read_text()


def verify(config, role_map, directory, n=1000):
    directory = Path(directory)
    result = {}
    for role, table in role_map.items():
        path = config["database"] + "." + table
        schema_rows = table_rows(
            checked(config, "DESC " + path, directory / (role + "_schema"))
        )
        actual = [
            {
                "name": r["ColumnName"],
                "type": r["DataType"],
                "category": "TIME" if r["ColumnName"] == "time" else r["Category"],
            }
            for r in schema_rows
        ]
        if actual != expected_schema(role):
            raise RuntimeError("fixture schema mismatch: " + path)
        rows = table_rows(
            checked(
                config,
                "SELECT * FROM " + path + " ORDER BY time, device",
                directory / (role + "_rows"),
            )
        )
        digest = canonical_hash(rows, role)
        if len(rows) != 4 * n or digest != fixture_hash(n, role):
            raise RuntimeError("fixture full content mismatch: " + path)
        result[role] = {"table": table, "rows": len(rows), "sha256": digest}
    (directory / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def prepare(cli_bin, port, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    prefix = "llm_benchmark_sol_" + datetime.now(timezone.utc).strftime("%m%d%H%M%S")
    admin = {
        "cli_bin": cli_bin,
        "host": "127.0.0.1",
        "port": port,
        "username": "root",
        "password": os.environ.get("IOTDB_ADMIN_PASSWORD", "root"),
    }
    entries = []
    for index, names in enumerate(itertools.permutations(TABLE_NAMES)):
        database = prefix + "_" + str(index)
        role_map = dict(zip(ROLE_COLUMNS, names))
        dest = directory / database
        batch = []
        batch_chars = 0
        batch_index = 0
        for sql in fixture_sql(database, 1000, role_map):
            if batch_chars + len(sql) > 55000:
                checked(admin, ";".join(batch), dest / ("insert_" + str(batch_index)))
                batch = []
                batch_chars = 0
                batch_index += 1
            batch.append(sql)
            batch_chars += len(sql) + 1
        if batch:
            checked(admin, ";".join(batch), dest / ("insert_" + str(batch_index)))
        user = "llm_sol_" + prefix.removeprefix("llm_benchmark_sol_") + "_" + str(index)
        password = secrets.token_urlsafe(24)
        checked(
            admin,
            "CREATE USER "
            + user
            + " '"
            + password
            + "';GRANT SELECT ON DATABASE "
            + database
            + " TO USER "
            + user,
            dest / "reader",
        )
        config = {**admin, "database": database, "username": user, "password": password}
        hashes = verify(config, role_map, dest / "verify")
        # Read-only account must reject a write; use an existing table and unchanged value.
        denial, _ = run_cli(
            config,
            "sql",
            "DELETE FROM " + database + "." + role_map["target"] + " WHERE time < 0",
            dest / "write_denied",
        )
        if not denial["error_kind"]:
            raise RuntimeError("read-only account write was not denied")
        entries.append({"connection": config, "role_map": role_map, "hashes": hashes})
        secret_file = directory / "connections.private.json"
        secret_file.write_text(json.dumps(entries, indent=2) + "\n")
        secret_file.chmod(0o600)
        print(
            "Prepared and verified",
            database,
            "12000 rows; read-only account verified",
            flush=True,
        )
    return entries


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    prepare(args.cli, args.port, args.output)
