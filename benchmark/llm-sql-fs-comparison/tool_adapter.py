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
    "LOWER",
    "UPPER",
}
SQL_TOKEN = re.compile(
    r"'(?:(?:'')|[^'])*'|\"(?:(?:\"\")|[^\"])*\"|[A-Za-z_][A-Za-z_0-9]*|(?:[0-9]+(?:\.[0-9]+)?)|<=|>=|<>|!=|[().,*+/<>=%-]",
    re.S,
)


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
            "stats": {"-f", "--format", "-m", "--measurements"},
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


def run_cli(config, arm, command, out_dir, timeout=30):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    env = {
        k: os.environ[k]
        for k in ("PATH", "JAVA_HOME", "LANG", "LC_ALL", "TZ", "TMPDIR")
        if k in os.environ
    }
    timed_out = False
    with (out_dir / "stdout.txt").open("wb") as stdout, (out_dir / "stderr.txt").open(
        "wb"
    ) as stderr:
        process = subprocess.Popen(
            cli_args(config, arm, command),
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
        "error_kind": "tool_timeout"
        if timed_out
        else "cli_error"
        if process.returncode != 0 or has_error
        else None,
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
