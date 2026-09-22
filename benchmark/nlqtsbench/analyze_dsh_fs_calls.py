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

"""Audit IoTDB FS calls from canonical DSH sessions without copying tool output."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import shlex


def result_call_id(data):
    message = data.get("message")
    source = message.get("source") if isinstance(message, dict) else None
    return source.get("callId") if isinstance(source, dict) else None


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from strings(item)


def result_error(data):
    if isinstance(data.get("error"), dict):
        return True
    message = data.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    return isinstance(content, list) and any(
        isinstance(block, dict) and block.get("isError") is True for block in content
    )


def option(tokens, names):
    for index, token in enumerate(tokens):
        for name in names:
            if token == name and index + 1 < len(tokens):
                return tokens[index + 1]
            if token.startswith(name + "="):
                return token.split("=", 1)[1]
            if name.startswith("-") and not name.startswith("--"):
                if token.startswith(name) and token != name:
                    return token[len(name) :]
    return None


def artifact_for(trial_dir, call_id):
    digest = hashlib.sha256(call_id.encode("utf-8")).hexdigest()[:16]
    directory = trial_dir / "tool-output" / ("call-" + digest)
    metadata = directory / "metadata.json"
    if not metadata.exists():
        return None
    result = json.loads(metadata.read_text(encoding="utf-8"))
    for name in ("stdout.txt", "stderr.txt"):
        path = directory / name
        if path.exists():
            text = path.read_text(encoding="utf-8", errors="replace").strip()
            if text:
                result[name.removesuffix(".txt")] = text[:500]
    return result


def error_reason(data, artifact):
    if artifact:
        kind = artifact.get("errorKind") or "cli_success"
        detail = artifact.get("stderr") or artifact.get("stdout") or ""
        if kind != "cli_success":
            return kind + (": " + detail.splitlines()[0] if detail else "")
    candidates = [text for text in strings(data) if text.startswith("Error:")]
    if candidates:
        return candidates[0].splitlines()[0]
    return "tool_error" if result_error(data) else None


def command_record(call, result, trial_dir):
    arguments = call.get("arguments")
    try:
        payload = json.loads(arguments) if isinstance(arguments, str) else arguments
    except json.JSONDecodeError:
        payload = None
    command = payload.get("command") if isinstance(payload, dict) else None
    typed = (
        isinstance(payload, dict)
        and isinstance(command, str)
        and not any(character.isspace() for character in command)
    )
    try:
        tokens = (
            [command]
            if typed
            else shlex.split(command) if isinstance(command, str) else []
        )
    except ValueError:
        tokens = []
    call_id = call.get("callId")
    artifact = artifact_for(trial_dir, call_id) if isinstance(call_id, str) else None
    is_error = result is None or result_error(result)
    record = {
        "sequence": call.get("sequence"),
        "call_id": call_id,
        "command": (
            json.dumps(payload, sort_keys=True, separators=(",", ":"))
            if typed
            else command
        ),
        "verb": tokens[0] if tokens else "<invalid>",
        "is_error": is_error,
        "error_reason": (
            "missing_result" if result is None else error_reason(result, artifact)
        ),
        "cli_started": (
            artifact.get("cliStarted", True) if artifact is not None else False
        ),
        "cli_process_ms": artifact.get("cliProcessMs") if artifact else None,
        "output_truncated": artifact.get("truncated") if artifact else None,
        "stdout_bytes": artifact.get("stdoutBytes") if artifact else None,
        "result_kind": artifact.get("resultKind") if artifact else None,
        "compact_page": artifact.get("compactPages") if artifact else None,
        "returned_rows": (
            artifact.get("page", {}).get("returned_rows")
            if artifact and isinstance(artifact.get("page"), dict)
            else None
        ),
        "next_offset": (
            artifact.get("page", {}).get("next_offset")
            if artifact and isinstance(artifact.get("page"), dict)
            else None
        ),
        "model_output_bytes": artifact.get("modelOutputBytes") if artifact else None,
    }
    if tokens and tokens[0] == "cat":
        if typed:
            record.update(
                limit=payload.get("limit"),
                offset=payload.get("offset"),
                start=payload.get("startMs"),
                end=payload.get("endMs"),
                format="csv",
            )
        else:
            record.update(
                limit=option(tokens, ("-n", "--limit")),
                offset=option(tokens, ("--offset",)),
                start=option(tokens, ("--start",)),
                end=option(tokens, ("--end",)),
                format=option(tokens, ("-f", "--format")),
            )
    return record


def audit_session(session):
    trial_dir = session.parents[3]
    task_dir = session.parents[4]
    events = [json.loads(line) for line in session.read_text().splitlines()[1:]]
    pending = {}
    calls = []
    for event in events:
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        if event.get("type") == "tool/call":
            call = dict(data, sequence=event.get("seq"))
            pending[data.get("callId")] = call
        elif event.get("type") == "tool/result":
            call_id = result_call_id(data)
            call = pending.pop(call_id, None)
            if call is not None:
                calls.append(command_record(call, data, trial_dir))
    calls.extend(command_record(call, None, trial_dir) for call in pending.values())
    commands = Counter(call["verb"] for call in calls)
    errors = Counter(call["error_reason"] for call in calls if call["is_error"])
    cat_calls = [call for call in calls if call["verb"] == "cat"]
    offsets = [
        int(call["offset"])
        for call in cat_calls
        if (
            type(call.get("offset")) is int
            or (isinstance(call.get("offset"), str) and call["offset"].isdigit())
        )
    ]
    limits = Counter(
        str(call["limit"])
        for call in cat_calls
        if type(call.get("limit")) is int or isinstance(call.get("limit"), str)
    )
    return {
        "ordinal": int(task_dir.name.split("-", 1)[0]),
        "task_id": task_dir.name.split("-", 1)[1],
        "calls": len(calls),
        "errors": sum(call["is_error"] for call in calls),
        "cli_started": sum(call["cli_started"] for call in calls),
        "commands": dict(commands),
        "error_reasons": dict(errors),
        "unique_commands": len({call["command"] for call in calls}),
        "duplicate_calls": len(calls) - len({call["command"] for call in calls}),
        "cat_calls": len(cat_calls),
        "cat_truncated": sum(
            call.get("output_truncated") is True for call in cat_calls
        ),
        "cat_limits": dict(limits),
        "cat_offset_calls": len(offsets),
        "cat_max_offset": max(offsets, default=None),
        "calls_detail": calls,
    }


def summarize(tasks):
    calls = [call for task in tasks for call in task["calls_detail"]]
    commands = Counter(call["verb"] for call in calls)
    command_errors = Counter(call["verb"] for call in calls if call["is_error"])
    reasons = Counter(call["error_reason"] for call in calls if call["is_error"])
    return {
        "tasks": len(tasks),
        "calls": len(calls),
        "errors": sum(call["is_error"] for call in calls),
        "cli_started": sum(call["cli_started"] for call in calls),
        "validation_or_plugin_rejections": sum(
            call["is_error"] and not call["cli_started"] for call in calls
        ),
        "cli_errors": sum(call["is_error"] and call["cli_started"] for call in calls),
        "truncated_outputs": sum(
            call.get("output_truncated") is True for call in calls
        ),
        "structured_pages": sum(call.get("result_kind") == "page" for call in calls),
        "compact_pages": sum(
            call.get("result_kind") == "page" and call.get("compact_page") is True
            for call in calls
        ),
        "returned_rows": sum(call.get("returned_rows") or 0 for call in calls),
        "pages_with_next_offset": sum(
            call.get("next_offset") is not None for call in calls
        ),
        "max_model_output_bytes": max(
            (
                call["model_output_bytes"]
                for call in calls
                if type(call.get("model_output_bytes")) is int
            ),
            default=None,
        ),
        "commands": {
            name: {"calls": count, "errors": command_errors[name]}
            for name, count in commands.most_common()
        },
        "error_reasons": dict(reasons.most_common()),
    }


def report(summary, tasks):
    lines = [
        "# DSH filesystem call audit",
        "",
        "This report contains command and error metadata only; tool output bodies are excluded.",
        "",
        f"- Calls: {summary['calls']}",
        f"- Errors: {summary['errors']}",
        f"- Rejected before CLI: {summary['validation_or_plugin_rejections']}",
        f"- CLI errors: {summary['cli_errors']}",
        f"- Truncated outputs: {summary['truncated_outputs']}",
        f"- Structured pages: {summary['structured_pages']}",
        f"- Compact pages: {summary['compact_pages']}",
        f"- Rows returned in pages: {summary['returned_rows']}",
        f"- Pages with next_offset: {summary['pages_with_next_offset']}",
        f"- Maximum model page bytes: {summary['max_model_output_bytes']}",
        "",
        "| Command | Calls | Errors |",
        "| --- | ---: | ---: |",
    ]
    for name, row in summary["commands"].items():
        lines.append(f"| {name} | {row['calls']} | {row['errors']} |")
    lines.extend(
        [
            "",
            "| # | Calls | Errors | Cat | Truncated | Offset calls | Max offset | Duplicates |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for task in tasks:
        lines.append(
            f"| {task['ordinal']} | {task['calls']} | {task['errors']} | "
            f"{task['cat_calls']} | {task['cat_truncated']} | "
            f"{task['cat_offset_calls']} | {task['cat_max_offset']} | "
            f"{task['duplicate_calls']} |"
        )
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    sessions = sorted(
        args.run_directory.glob("trials/*/filesystem/dsh-session/*/*/session.v3.jsonl")
    )
    if not sessions:
        parser.error("no filesystem DSH sessions found")
    tasks = sorted(
        (audit_session(path) for path in sessions), key=lambda row: row["ordinal"]
    )
    summary = summarize(tasks)
    result = {"summary": summary, "tasks": tasks}
    if args.output:
        args.output.mkdir(parents=True, exist_ok=False)
        args.output.chmod(0o700)
        json_path = args.output / "fs-call-audit.json"
        json_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        json_path.chmod(0o600)
        report_path = args.output / "FS_CALL_AUDIT.md"
        report_path.write_text(report(summary, tasks), encoding="utf-8")
        report_path.chmod(0o600)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
