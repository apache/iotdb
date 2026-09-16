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

"""Compare equivalent operations through IoTDB CLI SQL and filesystem modes.

The program deliberately starts one CLI process per operation. This measures
the complete non-interactive CLI path (JVM startup, login, parsing, execution,
and rendering) and makes SQL and filesystem commands directly comparable.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class Config:
    cli: str
    host: str
    port: str
    username: str
    password: str
    dialect: str
    database: str
    table: str
    write_table: str
    devices: int
    points: int
    batch_size: int
    query_limit: int
    warmup: int
    repeat: int
    timeout: int
    output_dir: Path
    include_write: bool


@dataclass(frozen=True)
class Case:
    name: str
    sql: str
    filesystem: str
    input_text: Optional[str] = None


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def integer(name: str, default: int) -> int:
    value = int(env(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def load_config(args: argparse.Namespace) -> Config:
    cli = args.cli or env("CLI_BIN", "")
    if not cli:
        raise ValueError("CLI_BIN or --cli is required")
    output = args.output or env("OUTPUT_DIR", "results")
    config = Config(
        cli=cli,
        host=args.host or env("HOST", "127.0.0.1"),
        port=args.port or env("PORT", "6667"),
        username=args.username or env("USERNAME", "root"),
        password=args.password or env("PASSWORD", "root"),
        dialect=args.dialect or env("SQL_DIALECT", "table"),
        database=args.database or env("DATABASE", "cli_benchmark"),
        table=args.table or env("TABLE", "telemetry"),
        write_table=args.write_table or env("WRITE_TABLE", "telemetry_write"),
        devices=integer("DEVICES", 4),
        points=integer("POINTS_PER_DEVICE", 1000),
        batch_size=integer("BATCH_SIZE", 200),
        query_limit=integer("QUERY_LIMIT", 100),
        warmup=integer("WARMUP", 2),
        repeat=integer("REPEAT", 5),
        timeout=integer("TIMEOUT_SECONDS", 120),
        output_dir=Path(output),
        include_write=args.include_write
        or env("INCLUDE_WRITE", "false").lower() == "true",
    )
    if config.dialect.lower() != "table":
        raise ValueError(
            "This benchmark uses the table SQL dialect; set SQL_DIALECT=table"
        )
    if config.devices == 0 or config.points == 0:
        raise ValueError("DEVICES and POINTS_PER_DEVICE must be greater than zero")
    if config.batch_size == 0 or config.query_limit == 0:
        raise ValueError("BATCH_SIZE and QUERY_LIMIT must be greater than zero")
    if config.repeat == 0:
        raise ValueError("REPEAT must be greater than zero")
    if config.timeout == 0:
        raise ValueError("TIMEOUT_SECONDS must be greater than zero")
    return config


def identifier(value: str) -> str:
    """Quote a table identifier after validating the benchmark config."""
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
    if not value or any(ch not in allowed for ch in value):
        raise ValueError(f"Invalid identifier: {value!r}")
    return value


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def cli_args(config: Config, mode: str, statement: str) -> list[str]:
    args = [
        config.cli,
        "-h",
        config.host,
        "-p",
        config.port,
        "-u",
        config.username,
        "-pw",
        config.password,
        "-sql_dialect",
        config.dialect,
    ]
    if mode == "filesystem":
        args += ["--access_mode", "filesystem"]
        if statement.lstrip().lower().startswith("tee "):
            args += ["--fs_write_mode", "enabled"]
    args += ["-e", statement]
    return args


def run_cli(
    config: Config,
    mode: str,
    statement: str,
    input_text: Optional[str] = None,
) -> tuple[float, int, int, int]:
    """Run one non-interactive CLI command and return time/status/byte counts."""
    started = time.perf_counter_ns()
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        completed = subprocess.run(
            cli_args(config, mode, statement),
            input=None if input_text is None else input_text.encode("utf-8"),
            stdout=stdout_file,
            stderr=stderr_file,
            timeout=config.timeout,
            check=False,
        )
        stdout_bytes = os.fstat(stdout_file.fileno()).st_size
        stderr_bytes = os.fstat(stderr_file.fileno()).st_size
        if completed.returncode != 0:
            stderr_file.seek(max(0, stderr_bytes - 1000))
            stderr = stderr_file.read().decode("utf-8", errors="replace").strip()
            stdout_file.seek(max(0, stdout_bytes - 1000))
            stdout = stdout_file.read().decode("utf-8", errors="replace").strip()
        else:
            stderr = stdout = ""
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    if completed.returncode != 0:
        detail = stderr or stdout or "no diagnostic output"
        raise RuntimeError(
            f"{mode} command failed with exit {completed.returncode}: {detail[-1000:]}\n"
            f"command: {statement}"
        )
    return elapsed_ms, completed.returncode, stdout_bytes, stderr_bytes


def schema_sql(config: Config, table: str) -> str:
    database = identifier(config.database)
    table = identifier(table)
    return (
        f"CREATE TABLE IF NOT EXISTS {database}.{table} ("
        "device STRING TAG, temperature DOUBLE FIELD, "
        "humidity DOUBLE FIELD, status BOOLEAN FIELD)"
    )


def rows(config: Config, table: str, start_time: int = 0) -> Iterable[str]:
    table_name = f"{identifier(config.database)}.{identifier(table)}"
    for device in range(config.devices):
        values = []
        for point in range(config.points):
            timestamp = start_time + point
            temperature = 20.0 + device + point * 0.01
            humidity = 40.0 + (point % 100) * 0.1
            status = "true" if point % 2 == 0 else "false"
            values.append(
                f"({timestamp}, {sql_literal(f'device_{device}')}, "
                f"{temperature:.4f}, {humidity:.4f}, {status})"
            )
            if len(values) == config.batch_size:
                yield (
                    f"INSERT INTO {table_name}(time, device, temperature, humidity, status) "
                    f"VALUES {', '.join(values)}"
                )
                values = []
        if values:
            yield (
                f"INSERT INTO {table_name}(time, device, temperature, humidity, status) "
                f"VALUES {', '.join(values)}"
            )


def csv_payload(config: Config, start_time: int) -> str:
    lines = ["time,device,temperature,humidity,status"]
    for point in range(config.batch_size):
        timestamp = start_time + point
        lines.append(
            f"{timestamp},device_0,{20.0 + point * 0.01:.4f},"
            f"{40.0 + (point % 100) * 0.1:.4f},{'true' if point % 2 == 0 else 'false'}"
        )
    return "\n".join(lines) + "\n"


def prepare(config: Config, reset: bool) -> None:
    database = identifier(config.database)
    commands = []
    if reset:
        commands.append(f"DROP DATABASE IF EXISTS {database}")
    commands += [
        f"CREATE DATABASE IF NOT EXISTS {database}",
        schema_sql(config, config.table),
    ]
    if config.include_write:
        commands.append(schema_sql(config, config.write_table))
    for command in commands:
        run_cli(config, "sql", command)
    for command in rows(config, config.table):
        run_cli(config, "sql", command)
    expected = config.devices * config.points
    elapsed, _, _, _ = run_cli(
        config,
        "sql",
        f"SELECT COUNT(*) FROM {database}.{identifier(config.table)}",
    )
    print(
        f"Prepared {expected} rows in {database}.{config.table}; "
        f"validation query completed in {elapsed:.2f} ms"
    )


def cases(config: Config) -> list[Case]:
    database = identifier(config.database)
    table = identifier(config.table)
    path = f"/{database}/{table}.csv"
    end = config.points - 1
    middle = max(0, config.points // 2)
    range_end = min(end, middle + config.query_limit - 1)
    select_columns = "time, device, temperature, humidity, status"
    return [
        Case(
            "point_lookup",
            f"SELECT {select_columns} FROM {database}.{table} WHERE time = {middle}",
            f"cat -f csv --start {middle} --end {middle} {path}",
        ),
        Case(
            "range_scan",
            f"SELECT {select_columns} FROM {database}.{table} "
            f"WHERE time >= {middle} AND time <= {range_end} ORDER BY time "
            f"LIMIT {config.query_limit}",
            f"cat -n {config.query_limit} -f csv --start {middle} --end {range_end} {path}",
        ),
        Case(
            "head",
            f"SELECT {select_columns} FROM {database}.{table} ORDER BY time "
            f"LIMIT {config.query_limit}",
            f"head -n {config.query_limit} -f csv {path}",
        ),
        Case(
            "full_scan",
            f"SELECT {select_columns} FROM {database}.{table} ORDER BY time",
            f"cat -f csv {path}",
        ),
        Case(
            "count",
            f"SELECT COUNT(*) FROM {database}.{table}",
            f"count -f csv {path}",
        ),
        Case(
            "aggregate",
            f"SELECT COUNT(temperature), AVG(temperature), MIN(temperature), "
            f"MAX(temperature) FROM {database}.{table}",
            f"stats -m temperature -f csv {path}",
        ),
        Case(
            "schema",
            f"DESC {database}.{table} DETAILS",
            f"schema -f csv {path}",
        ),
        Case(
            "metadata",
            f"SHOW TABLES DETAILS FROM {database}",
            f"meta -f csv {path}",
        ),
        Case(
            "list_tables",
            f"SHOW TABLES FROM {database}",
            f"ls -f csv /{database}",
        ),
    ]


def write_case(config: Config, iteration: int) -> Case:
    table = identifier(config.write_table)
    path = f"/{identifier(config.database)}/{table}.csv"
    start = 1_000_000 + iteration * config.batch_size
    payload = csv_payload(config, start)
    values = []
    for point in range(config.batch_size):
        timestamp = start + point
        values.append(
            f"({timestamp}, {sql_literal('device_0')}, "
            f"{20.0 + point * 0.01:.4f}, "
            f"{40.0 + (point % 100) * 0.1:.4f}, {'true' if point % 2 == 0 else 'false'})"
        )
    sql = (
        f"INSERT INTO {identifier(config.database)}.{table} "
        f"(time, device, temperature, humidity, status) VALUES {', '.join(values)}"
    )
    return Case("write_batch", sql, f"tee -a {path}", payload)


def result_row(
    run_id: str,
    case: str,
    mode: str,
    iteration: int,
    elapsed_ms: float,
    status: int,
    stdout_bytes: int,
    stderr_bytes: int,
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "case": case,
        "mode": mode,
        "iteration": iteration,
        "elapsed_ms": f"{elapsed_ms:.3f}",
        "return_code": status,
        "stdout_bytes": stdout_bytes,
        "stderr_bytes": stderr_bytes,
    }


def execute(config: Config, mode: str, run_id: str) -> list[dict[str, object]]:
    if mode not in {"sql", "filesystem", "compare"}:
        raise ValueError(f"Unsupported mode: {mode}")
    selected = cases(config)
    modes = ["sql", "filesystem"] if mode == "compare" else [mode]
    results: list[dict[str, object]] = []
    for case in selected:
        for selected_mode in modes:
            for _ in range(config.warmup):
                run_cli(
                    config, selected_mode, getattr(case, selected_mode), case.input_text
                )
            for iteration in range(config.repeat):
                timed = run_cli(
                    config, selected_mode, getattr(case, selected_mode), case.input_text
                )
                results.append(
                    result_row(run_id, case.name, selected_mode, iteration, *timed)
                )
    if config.include_write:
        for iteration in range(config.repeat):
            case = write_case(config, iteration)
            for selected_mode in modes:
                for _ in range(config.warmup):
                    run_cli(
                        config,
                        selected_mode,
                        getattr(case, selected_mode),
                        case.input_text,
                    )
                timed = run_cli(
                    config, selected_mode, getattr(case, selected_mode), case.input_text
                )
                results.append(
                    result_row(
                        run_id,
                        case.name,
                        selected_mode,
                        iteration,
                        *timed,
                    )
                )
    return results


def write_results(
    config: Config, run_id: str, results: list[dict[str, object]]
) -> Path:
    run_dir = config.output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    result_file = run_dir / "results.csv"
    with result_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)

    grouped: dict[tuple[str, str], list[float]] = {}
    for row in results:
        key = (str(row["case"]), str(row["mode"]))
        grouped.setdefault(key, []).append(float(row["elapsed_ms"]))
    summary_file = run_dir / "summary.csv"
    with summary_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "case",
                "mode",
                "repeat",
                "mean_ms",
                "median_ms",
                "p95_ms",
                "p99_ms",
                "ops_per_second",
            ]
        )
        for (case, mode), values in sorted(grouped.items()):
            values = sorted(values)
            p95_index = min(len(values) - 1, max(0, math.ceil(len(values) * 0.95) - 1))
            p99_index = min(len(values) - 1, max(0, math.ceil(len(values) * 0.99) - 1))
            mean = statistics.fmean(values)
            writer.writerow(
                [
                    case,
                    mode,
                    len(values),
                    f"{mean:.3f}",
                    f"{statistics.median(values):.3f}",
                    f"{values[p95_index]:.3f}",
                    f"{values[p99_index]:.3f}",
                    f"{1000 / mean:.3f}" if mean > 0 else "inf",
                ]
            )

    comparison_file = run_dir / "comparison.csv"
    means = {key: statistics.fmean(values) for key, values in grouped.items()}
    case_names = sorted({case for case, _ in grouped})
    with comparison_file.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "case",
                "sql_mean_ms",
                "filesystem_mean_ms",
                "sql_over_filesystem_speedup",
                "faster_mode",
            ]
        )
        for case in case_names:
            sql_mean = means.get((case, "sql"))
            filesystem_mean = means.get((case, "filesystem"))
            if sql_mean is None or filesystem_mean is None:
                writer.writerow([case, sql_mean or "", filesystem_mean or "", "", ""])
                continue
            speedup = (
                sql_mean / filesystem_mean if filesystem_mean > 0 else float("inf")
            )
            faster = "sql" if sql_mean < filesystem_mean else "filesystem"
            writer.writerow(
                [
                    case,
                    f"{sql_mean:.3f}",
                    f"{filesystem_mean:.3f}",
                    f"{speedup:.3f}",
                    faster,
                ]
            )

    metadata = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "platform": platform.platform(),
        "python": sys.version,
        "cli": config.cli,
        "endpoint": f"{config.host}:{config.port}",
        "sql_dialect": config.dialect,
        "database": config.database,
        "table": config.table,
        "devices": config.devices,
        "points_per_device": config.points,
        "batch_size": config.batch_size,
        "query_limit": config.query_limit,
        "warmup": config.warmup,
        "repeat": config.repeat,
        "include_write": config.include_write,
    }
    (run_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    return run_dir


def print_summary(run_dir: Path, results: list[dict[str, object]]) -> None:
    print(f"Results: {run_dir}")
    grouped: dict[tuple[str, str], list[float]] = {}
    for row in results:
        grouped.setdefault((str(row["case"]), str(row["mode"])), []).append(
            float(row["elapsed_ms"])
        )
    print("case,mode,median_ms,mean_ms,ops_per_second")
    for (case, mode), values in sorted(grouped.items()):
        mean = statistics.fmean(values)
        print(
            f"{case},{mode},{statistics.median(values):.3f},{mean:.3f},"
            f"{1000 / mean:.3f}"
        )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--mode", choices=["compare", "sql", "filesystem"], default="compare"
    )
    result.add_argument("--cli")
    result.add_argument("--host")
    result.add_argument("--port")
    result.add_argument("--username")
    result.add_argument("--password")
    result.add_argument("--dialect")
    result.add_argument("--database")
    result.add_argument("--table")
    result.add_argument("--write-table")
    result.add_argument("--output")
    result.add_argument("--include-write", action="store_true")
    result.add_argument("--prepare", action="store_true", help="prepare data and exit")
    result.add_argument(
        "--reset", action="store_true", help="drop the benchmark database first"
    )
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        config = load_config(args)
        if args.prepare:
            prepare(config, args.reset)
            return 0
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        results = execute(config, args.mode, run_id)
        run_dir = write_results(config, run_id, results)
        print_summary(run_dir, results)
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"benchmark failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
