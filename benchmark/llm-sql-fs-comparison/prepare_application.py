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

"""Create and verify isolated databases for the application pilot."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import secrets

from application_fixture import (
    SCENARIOS,
    SCHEMA,
    fixture_hash,
    fixture_sql,
    scenario_rows,
)
from tool_adapter import run_cli, table_rows


def _execute(config, statements, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    response, _ = run_cli(config, "sql", ";".join(statements), directory, timeout=120)
    if response["error_kind"]:
        raise RuntimeError(f"IoTDB preparation failed: {directory}")


def _normalise(value, kind):
    """Normalise CLI strings and fixture values without applying tolerance."""
    if value is None or (
        isinstance(value, str) and value.strip().lower() in {"null", "none"}
    ):
        return None
    if kind in {"DOUBLE", "FLOAT", "INT32", "INT64", "TIMESTAMP", "DATE"}:
        try:
            number = Decimal(str(value).strip())
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"numeric value is invalid: {value!r}") from exc
        if not number.is_finite():
            raise ValueError(f"numeric value is not finite: {value!r}")
        return format(number.normalize(), "f")
    if kind == "BOOLEAN":
        text = str(value).strip().lower()
        if text not in {"true", "false"}:
            raise ValueError(f"boolean value is invalid: {value!r}")
        return text == "true"
    return str(value).strip()


def _table_digest(table, rows):
    columns = ["time"] + [name for name, _, _ in SCHEMA[table]]
    kinds = {"time": "TIMESTAMP", **{name: kind for name, kind, _ in SCHEMA[table]}}
    digest = hashlib.sha256()
    canonical = []
    for row in rows:
        if set(row) != set(columns):
            raise ValueError(f"{table}: returned columns do not match fixture schema")
        normalized = {name: _normalise(row[name], kinds[name]) for name in columns}
        canonical.append(normalized)
    canonical.sort(
        key=lambda row: tuple(
            "" if row[name] is None else str(row[name]) for name in columns
        )
    )
    for row in canonical:
        digest.update(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()
        )
        digest.update(b"\n")
    return digest.hexdigest(), len(canonical), canonical


def _verify_snapshot(config, scenario, directory):
    """Verify inventory, schema, complete rows and temporal bounds over CLI."""
    directory = Path(directory)
    expected = scenario_rows(scenario)
    database = config["database"]
    inventory_response, _ = run_cli(
        config,
        "sql",
        f"SHOW TABLES FROM {database}",
        directory / "inventory",
        timeout=120,
    )
    if inventory_response["error_kind"]:
        raise RuntimeError(f"{scenario}: SHOW TABLES failed")
    inventory = table_rows(inventory_response["stdout"])
    actual_tables = sorted(row.get("TableName", "").strip() for row in inventory)
    if actual_tables != sorted(expected):
        raise RuntimeError(f"{scenario}: table inventory mismatch: {actual_tables!r}")

    verification = {"scenario": scenario, "database": database, "tables": {}}
    for table, fixture_rows in expected.items():
        schema_response, _ = run_cli(
            config,
            "sql",
            f"DESC {database}.{table}",
            directory / f"{table}_schema",
            timeout=120,
        )
        if schema_response["error_kind"]:
            raise RuntimeError(f"{scenario}.{table}: DESC failed")
        schema_rows = table_rows(schema_response["stdout"])
        actual_schema = [
            {
                "name": row.get("ColumnName", "").strip(),
                "type": row.get("DataType", "").strip().upper(),
                "category": row.get("Category", "").strip().upper(),
            }
            for row in schema_rows
        ]
        expected_schema = [{"name": "time", "type": "TIMESTAMP", "category": "TIME"}]
        expected_schema.extend(
            {"name": name, "type": kind, "category": category}
            for name, kind, category in SCHEMA[table]
        )
        if actual_schema != expected_schema:
            raise RuntimeError(
                f"{scenario}.{table}: schema mismatch: {actual_schema!r}"
            )

        order_columns = ["time"] + [
            name
            for name, _, _ in SCHEMA[table]
            if name.endswith("_id") or name in {"pump_id", "box_id", "floor_id"}
        ]
        query = f"SELECT * FROM {database}.{table} ORDER BY {','.join(order_columns)}"
        rows_response, _ = run_cli(
            config, "sql", query, directory / f"{table}_rows", timeout=120
        )
        if rows_response["error_kind"]:
            raise RuntimeError(f"{scenario}.{table}: full read failed")
        actual_rows = table_rows(rows_response["stdout"])
        actual_digest, row_count, canonical = _table_digest(table, actual_rows)
        expected_rows = [
            {
                "time": row.get("time", 0),
                **{name: row[name] for name, _, _ in SCHEMA[table]},
            }
            for row in fixture_rows
        ]
        expected_digest, expected_count, expected_canonical = _table_digest(
            table, expected_rows
        )
        if row_count != expected_count or actual_digest != expected_digest:
            raise RuntimeError(
                f"{scenario}.{table}: content mismatch ({row_count}/{expected_count}, {actual_digest}/{expected_digest})"
            )
        actual_times = [
            int(row["time"]) for row in canonical if row["time"] is not None
        ]
        expected_times = [
            int(row["time"]) for row in expected_canonical if row["time"] is not None
        ]
        if (min(actual_times), max(actual_times)) != (
            min(expected_times),
            max(expected_times),
        ):
            raise RuntimeError(f"{scenario}.{table}: time range mismatch")
        verification["tables"][table] = {
            "rows": row_count,
            "sha256": actual_digest,
            "min_time": min(actual_times),
            "max_time": max(actual_times),
            "schema": actual_schema,
        }
    (directory / "verification.json").write_text(
        json.dumps(verification, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return verification


def prepare(cli_bin: str, port: int, output: str) -> list[dict]:
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    admin = {
        "cli_bin": cli_bin,
        "host": "127.0.0.1",
        "port": port,
        "username": "root",
        "password": os.environ.get("IOTDB_ADMIN_PASSWORD", "root"),
    }
    prefix = "llm_app_benchmark_" + datetime.now(timezone.utc).strftime("%m%d%H%M%S")
    entries = []
    for index, scenario in enumerate(SCENARIOS):
        database = prefix + "_" + scenario.lower()
        dest = root / scenario
        statements = fixture_sql(database, scenario)
        _execute(admin, statements, dest / "create")
        username = (
            "llm_app_"
            + prefix.removeprefix("llm_app_benchmark_")
            + "_"
            + scenario.lower()
        )
        password = secrets.token_urlsafe(24)
        _execute(
            admin,
            [
                f"CREATE USER {username} '{password}'",
                f"GRANT SELECT ON DATABASE {database} TO USER {username}",
            ],
            dest / "grant",
        )
        config = {
            **admin,
            "database": database,
            "username": username,
            "password": password,
        }
        expected = scenario_rows(scenario)
        verification = _verify_snapshot(config, scenario, dest / "verify")
        # The read-only account must reject a write before the trial begins.
        denied, _ = run_cli(
            config,
            "sql",
            f"DELETE FROM {database}.{next(iter(expected))} WHERE time < 0",
            dest / "write_denied",
            timeout=120,
        )
        if not denied["error_kind"]:
            raise RuntimeError(f"{scenario}: read-only account accepted a write")
        entries.append(
            {
                "scenario": scenario,
                "connection": config,
                "tables": sorted(expected),
                "fixture_hash": fixture_hash(scenario),
                "rows": {table: len(rows) for table, rows in expected.items()},
                "verification": verification,
            }
        )
        print(
            f"Prepared {scenario}: {database} ({sum(len(rows) for rows in expected.values())} rows)",
            flush=True,
        )
    private = root / "connections.private.json"
    private.write_text(
        json.dumps(entries, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    private.chmod(0o600)
    return entries


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    prepare(args.cli, args.port, args.output)
