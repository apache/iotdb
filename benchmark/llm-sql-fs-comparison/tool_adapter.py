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

"""Restricted command adapter and timed, isolated IoTDB CLI execution."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import time

FS_COMMANDS = {
    "ls",
    "schema",
    "meta",
    "cat",
    "head",
    "tail",
    "count",
    "stats",
    "find",
    "tree",
    "stat",
    "file",
    "help",
}
TYPED_FS_COMMANDS = {
    "help",
    "ls",
    "schema",
    "meta",
    "cat",
    "head",
    "tail",
    "count",
    "stats",
    "stat",
    "file",
}
TYPED_FS_FILTER_COMMANDS = {"cat", "head"}
TYPED_FS_LIMIT_COMMANDS = {"cat", "head", "tail"}
TYPED_FS_MEASUREMENT_COMMANDS = {
    "schema",
    "count",
    "stats",
    *TYPED_FS_FILTER_COMMANDS,
}
TYPED_FS_TAG_OPERATORS = {"eq", "neq", "regexp", "is-null", "not-null"}
TYPED_FS_AGGREGATES = {"count", "min", "max", "sum", "avg", "median"}
FS_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\Z")
DEFAULT_PAGE_LIMIT = 200
DEFAULT_TAIL_LIMIT = 10
MAX_PAGE_LIMIT = 500
FUNCTIONS = {
    "COUNT",
    "MIN",
    "MAX",
    "SUM",
    "AVG",
    "ROUND",
    "ABS",
    "COALESCE",
    "NULLIF",
    "CAST",
    "PERCENTILE",
    "LOWER",
    "UPPER",
}
SQL_TOKEN = re.compile(
    r"'(?:(?:'')|[^'])*'|\"(?:(?:\"\")|[^\"])*\"|[A-Za-z_][A-Za-z_0-9]*|(?:[0-9]+(?:\.[0-9]+)?)|<=|>=|<>|!=|[().,*+/<>=%-]",
    re.S,
)


def validate_filesystem_path(path, database):
    """Validate a trusted, trial-fixed virtual filesystem path."""
    if not isinstance(path, str) or not path:
        raise ValueError("filesystemPath must be a nonempty string")
    components = path.split("/")
    if (
        any(ord(char) < 32 or char == "\\" for char in path)
        or any(component in {".", ".."} for component in components[1:])
        or not (path == "/" + database or path.startswith("/" + database + "/"))
    ):
        raise ValueError("filesystemPath must stay inside the task database")
    return path


def filesystem_help(
    path, structured_pages=True, compact_pages=False, filtered_stats=False
):
    """Return the complete model-facing contract for the typed FS tool."""
    if compact_pages and not structured_pages:
        raise ValueError("compact pages require structured pages")
    output_contract = (
        """cat/head/tail return columns, rows, offset, limit, returned_rows, has_more and next_offset.
For cat/head, continue with offset=next_offset and the same filters until next_offset is null.
Each page is kept below the model-output budget; cat/head returned_rows may therefore be smaller than limit.
An oversized tail page is rejected with guidance to use a smaller limit because tail cannot resume by offset.
"""
        + (
            "In compact pages, time and the selected numeric measurement use JSON numbers, TAG values remain strings, and \\N uses JSON null."
            if compact_pages
            else "In standard pages, every CSV cell remains a JSON string."
        )
        if structured_pages
        else """cat/head/tail return raw CSV text. For cat/head pagination, supply a limit and increase offset by the number of returned data rows while keeping all filters unchanged.
This ablation mode does not bound model-facing CSV or provide next_offset; large results may be replaced by a DSH spill notice."""
    )
    stats_filter_scope = "cat/head/stats" if filtered_stats else "cat/head"
    stats_parameters = (
        "- aggregates (optional array): one or more of count, min, max, sum, avg, median; stats only\n"
        if filtered_stats
        else ""
    )
    stats_behavior = (
        "apply optional time/TAG filters before computing the requested aggregates; use one stats call instead of reading rows when the requested calculation is available"
        if filtered_stats
        else "compute whole-object statistics or statistics for one measurement; time and tag filters are not supported"
    )
    return f"""IoTDB controlled filesystem interface

Fixed object path: {path}
Output format: csv

Parameters:
- command (required enum): help, ls, schema, meta, cat, head, tail, count, stats, stat, file
- startMs (optional integer): inclusive UTC epoch-millisecond lower bound; {stats_filter_scope} only; omitted means no lower bound
- endMs (optional integer): inclusive UTC epoch-millisecond upper bound; {stats_filter_scope} only; omitted means no upper bound
- measurement (optional string): one FIELD measurement identifier; schema/count/stats/cat/head only; omitted means all applicable columns
- limit (optional integer): requested page size, 1..500; cat/head/tail only; defaults to 200 for cat/head and 10 for tail
- offset (optional integer): rows to skip, >=0; cat/head only; omitted means zero
- tagName (optional string): TAG column identifier; {stats_filter_scope} only and requires tagOperator
- tagOperator (optional enum): eq, neq, regexp, is-null, not-null; {stats_filter_scope} only and requires tagName
- tagValue (optional string): required for eq/neq/regexp and forbidden for is-null/not-null
{stats_parameters}

Command behavior:
- help: return this complete contract without starting the IoTDB CLI; no optional parameters
- ls: list the fixed path; no optional parameters
- schema: describe measurements at the fixed path; optional measurement
- meta: return metadata for the fixed path; no optional parameters
- cat: return one structured page of matched rows; supports filters and paging
- head: return one structured page from the start of matched rows; supports the same filters and paging parameters as cat
- tail: read the last 10 CSV lines by default; only limit is supported
- count: count the whole fixed object or one measurement; time and tag filters are not supported
- stats: {stats_behavior}
- stat: return filesystem metadata for the fixed path; no optional parameters
- file: identify the fixed path object type; no optional parameters

Time bounds are integers, not ISO timestamp strings. If both are supplied, startMs must be <= endMs.
{output_contract}
All calls are read-only and the path cannot be supplied or changed by the caller.
"""


def build_typed_filesystem_command(
    parameters, path, database, max_limit=MAX_PAGE_LIMIT, filtered_stats=False
):
    """Validate typed FS arguments and render one deterministic CLI command."""
    if not isinstance(parameters, dict):
        raise ValueError("filesystem parameters must be an object")
    allowed_keys = {
        "command",
        "startMs",
        "endMs",
        "measurement",
        "limit",
        "offset",
        "tagName",
        "tagOperator",
        "tagValue",
    }
    if filtered_stats:
        allowed_keys.add("aggregates")
    unknown = set(parameters) - allowed_keys
    if unknown:
        raise ValueError(
            "unknown filesystem parameter(s): " + ", ".join(sorted(unknown))
        )
    command = parameters.get("command")
    if command not in TYPED_FS_COMMANDS:
        raise ValueError("filesystem command is not in the allowed enum")
    path = validate_filesystem_path(path, database)
    optional = {key for key in parameters if key != "command"}
    if command == "help":
        if optional:
            raise ValueError("help does not accept optional parameters")
        return None

    integer_parameters = {
        "startMs": None,
        "endMs": None,
        "limit": (1, max_limit),
        "offset": (0, None),
    }
    for name, bounds in integer_parameters.items():
        if name not in parameters:
            continue
        value = parameters[name]
        if type(value) is not int:
            raise ValueError(f"{name} must be an integer")
        if bounds is not None:
            lower, upper = bounds
            if value < lower or (upper is not None and value > upper):
                requirement = (
                    f">={lower}" if upper is None else f"in [{lower}, {upper}]"
                )
                raise ValueError(f"{name} must be {requirement}")
    if (
        "startMs" in parameters
        and "endMs" in parameters
        and parameters["startMs"] > parameters["endMs"]
    ):
        raise ValueError("startMs must be less than or equal to endMs")

    for name in ("measurement", "tagName"):
        if name in parameters and (
            not isinstance(parameters[name], str)
            or not FS_IDENTIFIER.fullmatch(parameters[name])
        ):
            raise ValueError(f"{name} must be an unquoted identifier")
    if (
        "tagOperator" in parameters
        and parameters["tagOperator"] not in TYPED_FS_TAG_OPERATORS
    ):
        raise ValueError("tagOperator is not in the allowed enum")
    if "tagValue" in parameters and not isinstance(parameters["tagValue"], str):
        raise ValueError("tagValue must be a string")
    if "aggregates" in parameters:
        aggregates = parameters["aggregates"]
        if (
            not isinstance(aggregates, list)
            or not aggregates
            or any(item not in TYPED_FS_AGGREGATES for item in aggregates)
            or len(set(aggregates)) != len(aggregates)
        ):
            raise ValueError(
                "aggregates must be a nonempty unique array of allowed aggregate names"
            )

    filter_commands = TYPED_FS_FILTER_COMMANDS | ({"stats"} if filtered_stats else set())
    applicability = {
        "startMs": filter_commands,
        "endMs": filter_commands,
        "measurement": TYPED_FS_MEASUREMENT_COMMANDS,
        "limit": TYPED_FS_LIMIT_COMMANDS,
        "offset": TYPED_FS_FILTER_COMMANDS,
        "tagName": filter_commands,
        "tagOperator": filter_commands,
        "tagValue": filter_commands,
        "aggregates": {"stats"},
    }
    unsupported = {
        name for name in optional if command not in applicability.get(name, set())
    }
    if unsupported:
        raise ValueError(
            f"{command} does not accept: " + ", ".join(sorted(unsupported))
        )

    tag_keys = optional & {"tagName", "tagOperator", "tagValue"}
    if tag_keys:
        if not {"tagName", "tagOperator"}.issubset(optional):
            raise ValueError("tagName and tagOperator must be supplied together")
        needs_value = parameters["tagOperator"] in {"eq", "neq", "regexp"}
        if needs_value and "tagValue" not in optional:
            raise ValueError("tagValue is required for eq, neq and regexp")
        if not needs_value and "tagValue" in optional:
            raise ValueError("tagValue is forbidden for is-null and not-null")

    tokens = [command]
    if command in {"ls", "schema", "meta", "cat", "head", "count", "stats"}:
        tokens.extend(["--format", "csv"])
    if command == "tail":
        tokens.extend(["--format", "csv"])
    if "measurement" in parameters:
        tokens.extend(["--measurements", parameters["measurement"]])
    if "startMs" in parameters:
        tokens.extend(["--start", str(parameters["startMs"])])
    if "endMs" in parameters:
        tokens.extend(["--end", str(parameters["endMs"])])
    if "limit" in parameters:
        tokens.extend(
            ["-n" if command == "tail" else "--limit", str(parameters["limit"])]
        )
    if "offset" in parameters:
        tokens.extend(["--offset", str(parameters["offset"])])
    if "tagName" in parameters:
        tokens.extend(
            ["--tag-filter", parameters["tagName"], parameters["tagOperator"]]
        )
        if "tagValue" in parameters:
            tokens.append(parameters["tagValue"])
    if "aggregates" in parameters:
        tokens.extend(["--aggregates", ",".join(parameters["aggregates"])])
    tokens.append(path)
    rendered = shlex.join(tokens)
    validate_command(rendered, "filesystem", database)
    return rendered


def validate_command(command, arm, database):
    """Conservative SQL token grammar plus database read-only authorization.

    No SQL text is evaluated by an OS shell. SQL functions are restricted to
    builtins; FROM/JOIN references must be qualified, including subqueries.
    Comma-style table joins are intentionally unsupported in this pilot.
    """
    if not isinstance(command, str) or not command.strip() or len(command) > 16000:
        raise ValueError(
            "command must be a nonempty string of at most 16000 characters"
        )
    if arm == "filesystem":
        lexer = shlex.shlex(command, posix=True, punctuation_chars="|;&<>")
        lexer.whitespace_split = True
        lexer.commenters = ""
        tokens = list(lexer)
        if not tokens or tokens[0] not in FS_COMMANDS:
            raise ValueError("filesystem command not allowed")
        if any(t and all(c in "|;&<>" for c in t) for t in tokens):
            raise ValueError(
                "compound commands, pipelines and redirection are disabled"
            )
        if tokens[0] == "help":
            if len(tokens) != 2 or tokens[1] not in FS_COMMANDS:
                raise ValueError("use help <allowed command>")
            return {"kind": "help", "tables": []}
        name = tokens[0]
        value_options = {
            "ls": {"-f", "--format"},
            "schema": {"-f", "--format", "-m", "--measurements"},
            "meta": {"-f", "--format"},
            "count": {"-f", "--format", "-m", "--measurements"},
            "stats": {
                "-f",
                "--format",
                "-m",
                "--measurements",
                "--start",
                "--end",
                "--tag-match",
                "--tag-filter",
                "--aggregates",
            },
            "cat": {
                "-f",
                "--format",
                "-m",
                "--measurements",
                "-n",
                "--limit",
                "--offset",
                "--start",
                "--end",
                "--tag-match",
                "--tag-filter",
            },
            "head": {
                "-f",
                "--format",
                "-m",
                "--measurements",
                "-n",
                "--limit",
                "--offset",
                "--start",
                "--end",
                "--tag-match",
                "--tag-filter",
            },
            "tail": {
                "--format",
                "-m",
                "--measurements",
                "-n",
                "--limit",
                "-c",
                "--offset",
                "--start",
                "--end",
                "--tag-match",
                "--tag-filter",
            },
            "find": {"-name", "-type", "-maxdepth"},
            "tree": {"-L"},
            "stat": set(),
            "file": set(),
        }[name]
        # Parse operands instead of counting strings starting with '/': CAT,
        # HEAD and TAIL accept several paths, and option values can be paths.
        # Restrict the table experiment to its documented field/tag options;
        # tree-model -d/-t object selectors are not supported here.
        paths = []
        index = 1
        options_ended = False
        while index < len(tokens):
            token = tokens[index]
            index += 1
            if token == "--" and not options_ended:
                options_ended = True
                continue
            if options_ended or not token.startswith("-") or token == "-":
                paths.append(token)
                continue
            if name == "tail" and (
                token.startswith("--follow")
                or re.fullmatch(r"-[A-Za-z]*f[A-Za-z]*", token)
            ):
                raise ValueError(
                    "tail follow is disabled; use --format for output format"
                )
            if name == "ls" and re.fullmatch(r"-[alR]+", token):
                continue
            if name in {"head", "tail"} and re.fullmatch(r"-[0-9]+", token):
                continue
            flag = token
            attached = None
            if token.startswith("--") and "=" in token:
                flag, attached = token.split("=", 1)
            elif (
                not token.startswith("--")
                and len(token) > 2
                and token[:2] in value_options
            ):
                flag, attached = token[:2], token[2:]
            if flag not in value_options:
                raise ValueError("filesystem option not allowed for this command")
            if attached is None:
                if index >= len(tokens) or tokens[index] == "--":
                    raise ValueError("filesystem option is missing its value")
                index += 1
            if flag == "--tag-filter":
                if index >= len(tokens):
                    raise ValueError("tag filter requires a separate operator")
                operator = tokens[index]
                index += 1
                if operator not in {"eq", "neq", "regexp", "is-null", "not-null"}:
                    raise ValueError("tag filter operator is invalid")
                if operator in {"eq", "neq", "regexp"}:
                    if index >= len(tokens) or tokens[index] == "--":
                        raise ValueError("tag filter operator requires a value")
                    index += 1
        if len(paths) != 1:
            raise ValueError("use exactly one absolute object path")
        path = paths[0]
        components = path.split("/")
        if (
            any(ord(char) < 32 or char == "\\" for char in path)
            or any(component in {".", ".."} for component in components[1:])
            or not (path == "/" + database or path.startswith("/" + database + "/"))
        ):
            raise ValueError("path must stay inside the task database")
        return {
            "kind": tokens[0],
            "tables": [
                path.rsplit("/", 1)[-1].removesuffix(".csv").removesuffix(".meta")
            ],
        }
    text = command.strip().removesuffix(";").strip()
    parts = []
    pos = 0
    for match in SQL_TOKEN.finditer(text):
        if text[pos : match.start()].strip():
            raise ValueError("SQL contains unsupported syntax or compound statements")
        parts.append(match.group())
        pos = match.end()
    if text[pos:].strip() or not parts:
        raise ValueError("SQL contains unsupported syntax")
    upper = [t.upper() if not t.startswith("'") else "<LITERAL>" for t in parts]
    names = [t[1:-1].replace('""', '"') if t.startswith('"') else t for t in parts]
    if upper[0] == "SHOW":
        if (
            upper[:3] == ["SHOW", "TABLES", "FROM"]
            and len(parts) == 4
            and names[3] == database
        ):
            return {"kind": "inventory", "tables": []}
        if (
            upper[:4] == ["SHOW", "TABLES", "DETAILS", "FROM"]
            and len(parts) == 5
            and names[4] == database
        ):
            return {"kind": "inventory", "tables": []}
        raise ValueError("only SHOW TABLES [DETAILS] FROM task_database is allowed")
    if upper[0] in {"DESC", "DESCRIBE"}:
        if (
            len(parts) not in (4, 5)
            or names[1] != database
            or parts[2] != "."
            or (len(parts) == 5 and upper[4] != "DETAILS")
        ):
            raise ValueError("use DESC task_database.table [DETAILS]")
        return {"kind": "schema", "tables": [names[3]]}
    if upper[0] != "SELECT":
        raise ValueError("only SELECT, SHOW TABLES and DESCRIBE are allowed")
    forbidden = {
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "GRANT",
        "REVOKE",
        "INTO",
        "LOAD",
        "EXPORT",
        "IMPORT",
        "CALL",
    }
    if forbidden.intersection(upper) or "--" in text or "/*" in text:
        raise ValueError(
            "write, external, comment or administrative syntax is disabled"
        )
    tables = []
    depth = 0
    from_depths = set()
    for i, token in enumerate(upper):
        if token == "(":
            depth += 1
        if token == ")":
            from_depths.discard(depth)
            depth -= 1
            if depth < 0:
                raise ValueError("unbalanced parentheses")
        function_name = names[i].upper()
        if (
            i + 1 < len(parts)
            and parts[i + 1] == "("
            and (parts[i].startswith('"') or re.fullmatch(r"[A-Z_][A-Z_0-9]*", token))
        ):
            if (i > 0 and parts[i - 1] == ".") or function_name not in FUNCTIONS | {
                "IN",
                "NOT",
                "EXISTS",
                "FROM",
                "JOIN",
                "AS",
                "WHERE",
                "AND",
                "OR",
                "SELECT",
                "WHEN",
                "THEN",
                "ELSE",
                "ON",
                "OVER",
                "BY",
            }:
                raise ValueError("only documented built-in SQL functions are allowed")
        if token in {
            "WHERE",
            "GROUP",
            "ORDER",
            "HAVING",
            "LIMIT",
            "OFFSET",
            "UNION",
            "ON",
        }:
            from_depths.discard(depth)
        if token in {"FROM", "JOIN"}:
            from_depths.add(depth)
            if i + 1 < len(parts) and parts[i + 1] == "(":
                continue
            if (
                i + 3 >= len(parts)
                or names[i + 1] != database
                or parts[i + 2] != "."
                or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", names[i + 3])
            ):
                raise ValueError("every FROM/JOIN object must use task_database.table")
            tables.append(names[i + 3])
        if token == "," and depth in from_depths:
            raise ValueError(
                "comma-style table joins are not supported; use qualified JOIN"
            )
    if depth:
        raise ValueError("unbalanced parentheses")
    return {"kind": "select", "tables": tables}


def cli_args(config, arm, command):
    args = [
        config["cli_bin"],
        "-h",
        config["host"],
        "-p",
        str(config["port"]),
        "-u",
        config["username"],
        "-pw",
        config["password"],
        "-sql_dialect",
        "table",
        "-disableISO8601",
    ]
    if arm == "filesystem":
        args.extend(["--access_mode", "filesystem"])
    return args + ["-e", command]


def run_process(args, out_dir, timeout=30, extra_environment=()):
    """Run one CLI argv under an allowlisted environment and retain raw output."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.chmod(0o700)
    started = time.monotonic_ns()
    allowed_environment = {
        "PATH",
        "JAVA_HOME",
        "HOME",
        "LANG",
        "LC_ALL",
        "TZ",
        "TMPDIR",
        *extra_environment,
    }
    env = {k: os.environ[k] for k in allowed_environment if k in os.environ}
    timed_out = False
    with (out_dir / "stdout.txt").open("wb") as stdout, (out_dir / "stderr.txt").open(
        "wb"
    ) as stderr:
        (out_dir / "stdout.txt").chmod(0o600)
        (out_dir / "stderr.txt").chmod(0o600)
        process = subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            env=env,
            start_new_session=True,
        )
        try:
            process.wait(timeout=max(0.01, timeout))
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
    ended = time.monotonic_ns()
    raw_stdout = (out_dir / "stdout.txt").read_bytes()
    raw_stderr = (out_dir / "stderr.txt").read_bytes()
    error_text = (raw_stdout + raw_stderr).decode("utf-8", "replace")
    diagnostic_text = "\n".join(
        line
        for line in error_text.splitlines()
        if line.strip() != "Msg: The statement is executed successfully."
    )
    has_error = bool(
        re.search(
            r"(?im)^(?:Error(?:\b|:)|Msg:|Exception|.*SQLException:|.*StatementExecutionException:)",
            diagnostic_text,
        )
    )
    result = {
        "stdout": raw_stdout[:65536].decode("utf-8", "ignore"),
        "stderr": raw_stderr[:8192].decode("utf-8", "ignore"),
        "exit_code": process.returncode,
        "error_kind": (
            "tool_timeout"
            if timed_out
            else "cli_error" if process.returncode != 0 or has_error else None
        ),
        "timed_out": timed_out,
        "truncated": len(raw_stdout) > 65536 or len(raw_stderr) > 8192,
        "stdout_bytes": len(raw_stdout),
        "stderr_bytes": len(raw_stderr),
    }
    return result, {
        "process_start_ns": started,
        "process_end_ns": ended,
        "cli_process_ms": (ended - started) / 1e6,
    }


def run_cli(config, arm, command, out_dir, timeout=30):
    return run_process(cli_args(config, arm, command), out_dir, timeout)


def table_rows(output):
    rows = [
        [v.strip() for v in line.strip().strip("|").split("|")]
        for line in output.splitlines()
        if line.startswith("|")
    ]
    if not rows:
        raise ValueError("CLI result has no table header")
    header, *data = rows
    data = [row for row in data if row != header]
    if any(len(row) != len(header) for row in data):
        raise ValueError("inconsistent result columns")
    totals = re.findall(r"Total line number = (\d+)", output)
    if not totals or int(totals[-1]) != len(data):
        raise ValueError("CLI result incomplete")
    return [dict(zip(header, row)) for row in data]
