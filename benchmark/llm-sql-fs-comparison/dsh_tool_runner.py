# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Fail-closed process boundary for the DSH SQL and filesystem tools."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
from pathlib import Path
import re
import sys

from tool_adapter import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_TAIL_LIMIT,
    MAX_PAGE_LIMIT,
    build_typed_filesystem_command,
    filesystem_help,
    run_process,
    validate_command,
)


ARMS = {"sql", "filesystem"}
DATABASE_NAME = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\Z")
MAX_REQUEST_BYTES = 32768
MAX_PAGE_RENDER_BYTES = 40000
MAX_PAGE_SOURCE_BYTES = 8 * 1024 * 1024
PAGE_COMMANDS = {"cat", "head", "tail"}
INTEGER_CELL = re.compile(r"[+-]?[0-9]+\Z")
NUMBER_CELL = re.compile(
    r"[+-]?(?:(?:[0-9]+(?:\.[0-9]*)?)|(?:\.[0-9]+))(?:[eE][+-]?[0-9]+)?\Z"
)


def _artifact_directory(root, call_id):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o700)
    digest = hashlib.sha256(call_id.encode("utf-8")).hexdigest()[:16]
    directory = root / ("call-" + digest)
    directory.mkdir(exist_ok=False)
    directory.chmod(0o700)
    return directory


def _read_csv_rows(output_dir):
    raw = (output_dir / "stdout.txt").read_bytes()
    if len(raw) > MAX_PAGE_SOURCE_BYTES:
        raise ValueError("filesystem page source exceeded 8 MiB")
    parsed = list(csv.reader(io.StringIO(raw.decode("utf-8"), newline="")))
    if not parsed or not parsed[0] or any(not column for column in parsed[0]):
        raise ValueError("filesystem page did not contain a CSV header")
    columns, *rows = parsed
    if len(set(columns)) != len(columns):
        raise ValueError("filesystem page contains duplicate CSV columns")
    if any(len(row) != len(columns) for row in rows):
        raise ValueError("filesystem page contains an inconsistent CSV row")
    return columns, rows


def _page_bytes(page):
    return len(
        json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )


def _compact_cell(column, value, measurement):
    if value == "\\N":
        return None
    if column.lower() == "time" and INTEGER_CELL.fullmatch(value):
        return int(value)
    if column == measurement and NUMBER_CELL.fullmatch(value):
        number = float(value)
        if math.isfinite(number):
            return int(value) if INTEGER_CELL.fullmatch(value) else number
    return value


def _structured_page(parameters, output_dir, compact=False):
    columns, fetched_rows = _read_csv_rows(output_dir)
    if compact:
        measurement = parameters.get("measurement")
        fetched_rows = [
            [
                _compact_cell(column, value, measurement)
                for column, value in zip(columns, row)
            ]
            for row in fetched_rows
        ]
    command = parameters["command"]
    default_limit = DEFAULT_TAIL_LIMIT if command == "tail" else DEFAULT_PAGE_LIMIT
    requested_limit = parameters.get("limit", default_limit)
    offset = parameters.get("offset", 0)
    rows = fetched_rows[:requested_limit]

    def make_page(selected):
        has_more = command in {"cat", "head"} and len(fetched_rows) > len(selected)
        return {
            "columns": columns,
            "rows": selected,
            "offset": offset,
            "limit": requested_limit,
            "returned_rows": len(selected),
            "has_more": has_more,
            "next_offset": offset + len(selected) if has_more else None,
            "null_value": "\\N",
        }

    page = make_page(rows)
    if command == "tail" and _page_bytes(page) > MAX_PAGE_RENDER_BYTES:
        raise ValueError(
            "tail page exceeds the model output budget; use a smaller limit"
        )
    while rows and _page_bytes(page) > MAX_PAGE_RENDER_BYTES:
        rows.pop()
        page = make_page(rows)
    if fetched_rows and not rows:
        raise ValueError("one filesystem row exceeds the model page-output budget")
    if _page_bytes(page) > MAX_PAGE_RENDER_BYTES:
        raise ValueError("filesystem page metadata exceeds the model output budget")
    return page


def execute_request(request):
    common_keys = {
        "arm",
        "database",
        "executable",
        "outputRoot",
        "callId",
        "timeoutSeconds",
    }
    if not isinstance(request, dict):
        raise ValueError("runner request must be an object")
    arm = request.get("arm")
    expected_keys = common_keys | (
        {"command"}
        if arm == "sql"
        else {
            "filesystemPath",
            "parameters",
            "structuredPages",
            "compactPages",
            "filteredStats",
        }
    )
    if set(request) != expected_keys:
        raise ValueError("runner request has an invalid shape")
    database = request["database"]
    executable = request["executable"]
    output_root = request["outputRoot"]
    call_id = request["callId"]
    timeout = request["timeoutSeconds"]
    if arm not in ARMS:
        raise ValueError("arm must be sql or filesystem")
    if not isinstance(database, str) or not DATABASE_NAME.fullmatch(database):
        raise ValueError("database must be an unquoted IoTDB identifier")
    if not isinstance(executable, str) or not Path(executable).is_absolute():
        raise ValueError("executable must be an absolute path")
    if not isinstance(output_root, str) or not Path(output_root).is_absolute():
        raise ValueError("outputRoot must be an absolute path")
    if not isinstance(call_id, str) or not call_id or len(call_id) > 512:
        raise ValueError("callId must be a nonempty bounded string")
    if type(timeout) not in {int, float} or not 0 < timeout <= 300:
        raise ValueError("timeoutSeconds must be in (0, 300]")
    if arm == "sql":
        command = request["command"]
        validation = validate_command(command, arm, database)
    else:
        parameters = request["parameters"]
        fixed_path = request["filesystemPath"]
        structured_pages = request["structuredPages"]
        compact_pages = request["compactPages"]
        filtered_stats = request["filteredStats"]
        if type(structured_pages) is not bool:
            raise ValueError("structuredPages must be a boolean")
        if type(compact_pages) is not bool:
            raise ValueError("compactPages must be a boolean")
        if type(filtered_stats) is not bool:
            raise ValueError("filteredStats must be a boolean")
        if compact_pages and not structured_pages:
            raise ValueError("compactPages requires structuredPages")
        command = build_typed_filesystem_command(
            parameters, fixed_path, database, filtered_stats=filtered_stats
        )
        if structured_pages and parameters["command"] in {"cat", "head"}:
            requested_limit = parameters.get("limit", DEFAULT_PAGE_LIMIT)
            fetch_parameters = dict(parameters, limit=requested_limit + 1)
            command = build_typed_filesystem_command(
                fetch_parameters,
                fixed_path,
                database,
                max_limit=MAX_PAGE_LIMIT + 1,
                filtered_stats=filtered_stats,
            )
        validation = {
            "kind": parameters["command"],
            "tables": [fixed_path.rsplit("/", 1)[-1].removesuffix(".csv")],
            "fixedPath": fixed_path,
            "parameters": parameters,
        }
    output_dir = _artifact_directory(output_root, call_id)
    if command is None:
        help_text = filesystem_help(
            fixed_path, structured_pages, compact_pages, filtered_stats
        )
        stdout_path = output_dir / "stdout.txt"
        stderr_path = output_dir / "stderr.txt"
        stdout_path.write_text(help_text, encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        stdout_path.chmod(0o600)
        stderr_path.chmod(0o600)
        result = {
            "stdout": help_text,
            "stderr": "",
            "exit_code": 0,
            "error_kind": None,
            "timed_out": False,
            "truncated": False,
            "stdout_bytes": len(help_text.encode("utf-8")),
            "stderr_bytes": 0,
        }
        timing = {"cli_process_ms": 0.0}
    else:
        result, timing = run_process(
            [executable, "-disableISO8601", "-e", command],
            output_dir,
            timeout=timeout,
            extra_environment=("IOTDB_USERNAME", "IOTDB_PASSWORD"),
        )
    page = None
    if (
        arm == "filesystem"
        and structured_pages
        and parameters["command"] in PAGE_COMMANDS
        and result["error_kind"] is None
    ):
        page = _structured_page(parameters, output_dir, compact_pages)
    result_kind = "page" if page is not None else "text"
    model_stdout = "" if page is not None else result["stdout"]
    model_truncated = False if page is not None else result["truncated"]
    metadata = {
        "arm": arm,
        "database": database,
        "validation": validation,
        "structuredPages": structured_pages if arm == "filesystem" else None,
        "compactPages": compact_pages if arm == "filesystem" else None,
        "filteredStats": filtered_stats if arm == "filesystem" else None,
        "cliStarted": command is not None,
        "exitCode": result["exit_code"],
        "errorKind": result["error_kind"],
        "timedOut": result["timed_out"],
        "truncated": result["truncated"],
        "stdoutBytes": result["stdout_bytes"],
        "stderrBytes": result["stderr_bytes"],
        "cliProcessMs": timing["cli_process_ms"],
        "resultKind": result_kind,
        "page": (
            {
                key: value
                for key, value in page.items()
                if key not in {"columns", "rows", "null_value"}
            }
            if page is not None
            else None
        ),
        "modelOutputBytes": _page_bytes(page) if page is not None else None,
    }
    metadata_path = output_dir / "metadata.json"
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    metadata_path.chmod(0o600)
    return {
        "ok": result["error_kind"] is None,
        "resultKind": result_kind,
        "stdout": model_stdout,
        "stderr": result["stderr"],
        "page": page,
        "exitCode": result["exit_code"],
        "errorKind": result["error_kind"],
        "timedOut": result["timed_out"],
        "truncated": model_truncated,
        "stdoutBytes": result["stdout_bytes"],
        "stderrBytes": result["stderr_bytes"],
        "cliProcessMs": timing["cli_process_ms"],
        "artifactDirectory": str(output_dir),
    }


def main():
    raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
    if len(raw) > MAX_REQUEST_BYTES:
        response = {
            "ok": False,
            "errorKind": "invalid_request",
            "message": "request too large",
        }
    else:
        try:
            response = execute_request(json.loads(raw.decode("utf-8")))
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            response = {
                "ok": False,
                "errorKind": "validation_error",
                "message": str(exc),
            }
        # Preserve one structured trial record on infrastructure bugs.
        except Exception as exc:
            response = {
                "ok": False,
                "errorKind": "infrastructure_error",
                "message": f"{type(exc).__name__}: {exc}",
            }
    sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
