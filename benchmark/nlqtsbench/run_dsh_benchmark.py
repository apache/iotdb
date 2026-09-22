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

"""Run paired controlled DSH trials over a fixed NLQTSBench source-index range."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import re
import statistics
import sys


COMPARE_DIR = Path(__file__).resolve().parents[1] / "llm-sql-fs-comparison"
sys.path.insert(0, str(COMPARE_DIR))

from dsh_client import DshClient, controlled_baseline  # noqa: E402


NUMBER = re.compile(r"[+-]?(?:0|[1-9][0-9]*)\.[0-9]{3}\Z")
MILLI = Decimal("0.001")


def private_write(path, value):
    path = Path(path)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    path.chmod(0o600)


def load_inputs(sonar_root, equivalence_path, start, end):
    tasks = json.loads((Path(sonar_root) / "nlqtsbench" / "tasks.json").read_text())
    evidence = json.loads(Path(equivalence_path).read_text())
    records = {row["source_index"]: row for row in evidence["records"]}
    selected = []
    for source_index in range(start - 1, end):
        task = tasks[source_index]
        record = records.get(source_index)
        if record is None or record["task_id"] != task["id"]:
            raise ValueError(
                f"missing or mismatched evidence for source index {source_index}"
            )
        if not all(
            record[key]
            for key in ("storage_equal", "sql_oracle_equal", "fs_composed_oracle_equal")
        ):
            raise ValueError(
                f"task {task['id']} did not pass deterministic verification"
            )
        if task["level"] != 1:
            raise ValueError(f"task {task['id']} is outside the L1 experiment")
        selected.append((source_index, task, record))
    return selected


def render_prompt(task, record, arm, fs_output_mode="page"):
    database = record["database"]
    common = f"""Solve this original NLQTSBench question:
{task['question']}

The assigned database is {database}. It contains table raw_data with columns:
- time: TIMESTAMP (UTC)
- channel_id: STRING TAG
- value: DOUBLE FIELD
The original channel identifier is stored in channel_id. A named year or month
means that complete UTC calendar period. A date range includes the complete final
UTC day. Use only observations returned by your assigned IoTDB tool. Do not infer
the answer from the task identifier. Return exactly one numeric value with three
digits after the decimal point and no explanation, Markdown, or JSON.
"""
    if arm == "sql":
        return (
            common
            + f"""
Use exactly one iotdb_sql tool call containing one read-only SELECT statement.
Qualify the table as {database}.raw_data. Compute the requested result in IoTDB;
do not retrieve raw rows for arithmetic outside the query. IoTDB TABLE SQL compares
TIMESTAMP columns to integer UTC epoch milliseconds; string timestamp comparisons
are invalid. Available exact aggregates include MIN, MAX, AVG, and
PERCENTILE(value, 0.5) for median. ROUND accepts a numeric result and scale.
"""
        )
    if fs_output_mode == "raw":
        output_instructions = """This is the typed-interface/raw-output ablation. Data reads return raw CSV. For cat/head, use an explicit limit and paginate by increasing offset by the number of returned data rows while keeping all filters unchanged. DSH may spill oversized raw results."""
    elif fs_output_mode == "compact":
        output_instructions = """Data reads return compact structured pages: time and numeric measurement cells use JSON numbers, TAG cells remain strings, and null uses JSON null. Continue with offset=next_offset and unchanged filters until next_offset is null."""
    elif fs_output_mode == "filtered-stats":
        output_instructions = """Use stats with measurement=value, the requested UTC time bounds, channel_id TAG filter, and aggregates whenever it can compute the answer. It applies filters before aggregation and returns count, min, max, sum, avg, or median. Request min and max together for a range. Do not read or paginate raw rows when stats can answer the question."""
    else:
        output_instructions = """Data reads return structured pages whose CSV cells are JSON strings; continue with offset=next_offset and unchanged filters until next_offset is null."""
    stats_limit = (
        ""
        if fs_output_mode == "filtered-stats"
        else " stats and count operate on the whole object and do not accept time bounds."
    )
    return (
        common
        + f"""
Use only the typed iotdb_fs tool. Its object is fixed to
/{database}/raw_data.csv; do not supply a path. You may make multiple FS calls to
decompose the task. Use integer UTC epoch milliseconds for startMs and endMs. The
measurement is value and the channel TAG is channel_id. Call help once if you need
the complete parameter and command contract. {output_instructions}{stats_limit} Do
not use SQL or shell command syntax.
"""
    )


def score(final_text, expected):
    text = final_text.strip()
    expected_decimal = Decimal(str(expected)).quantize(MILLI, rounding=ROUND_HALF_UP)
    if not NUMBER.fullmatch(text):
        return {
            "answer_status": "invalid_format",
            "parsed_answer": None,
            "expected_answer": format(expected_decimal, ".3f"),
            "correct": False,
        }
    parsed = Decimal(text)
    return {
        "answer_status": "correct" if parsed == expected_decimal else "wrong_answer",
        "parsed_answer": text,
        "expected_answer": format(expected_decimal, ".3f"),
        "correct": parsed == expected_decimal,
    }


def trial_row(ordinal, source_index, task, arm, result, scored, trial_dir):
    usage = result.get("usage") if isinstance(result.get("usage"), dict) else {}
    return {
        "ordinal": ordinal,
        "source_index": source_index,
        "task_id": task["id"],
        "family": task["subtask"],
        "arm": arm,
        "database": f"nlqts_v1_{task['id'].rsplit('_', 1)[-1]}",
        "trial_status": result.get("status"),
        **scored,
        "task_wall_ms": result.get("task_wall_ms"),
        "model_api_ms": result.get("model_api_ms"),
        "tool_wall_ms": result.get("tool_wall_ms"),
        "cli_process_ms": result.get("cli_process_ms"),
        "tool_attempts": result.get("tool_attempts"),
        "tool_errors": result.get("tool_errors"),
        "model_requests": result.get("model_requests"),
        "retry_count": result.get("retry_count"),
        "input_tokens": usage.get("inputTokens"),
        "cache_read_tokens": usage.get("cacheReadTokens"),
        "cache_write_tokens": usage.get("cacheWriteTokens"),
        "output_tokens": usage.get("outputTokens"),
        "reasoning_tokens": usage.get("reasoningTokens"),
        "total_tokens": usage.get("totalTokensDerived"),
        "tool_policy_ok": result.get("tool_policy_ok"),
        "final_text": result.get("final_text"),
        "trial_directory": str(trial_dir),
    }


def median(rows, key):
    values = [row[key] for row in rows if type(row.get(key)) in {int, float}]
    return statistics.median(values) if values else None


def summarize(rows):
    arms = {}
    for arm in ("sql", "filesystem"):
        group = [row for row in rows if row["arm"] == arm]
        arms[arm] = {
            "trials": len(group),
            "completed": sum(row["trial_status"] == "completed" for row in group),
            "correct": sum(row["correct"] for row in group),
            "invalid_format": sum(
                row["answer_status"] == "invalid_format" for row in group
            ),
            "tool_attempts": sum(row.get("tool_attempts") or 0 for row in group),
            "tool_errors": sum(row.get("tool_errors") or 0 for row in group),
            "median_task_wall_ms": median(group, "task_wall_ms"),
            "median_model_api_ms": median(group, "model_api_ms"),
            "median_cli_process_ms": median(group, "cli_process_ms"),
            "median_total_tokens": median(group, "total_tokens"),
        }
    by_task = {}
    for row in rows:
        by_task.setdefault(row["task_id"], {})[row["arm"]] = row
    return {
        "trials": len(rows),
        "paired_tasks": len(by_task),
        "both_correct": sum(
            pair.get("sql", {}).get("correct", False)
            and pair.get("filesystem", {}).get("correct", False)
            for pair in by_task.values()
        ),
        "arms": arms,
    }


def write_report(output, rows, summary):
    lines = [
        "# NLQTSBench DSH controlled-tool pilot",
        "",
        "This is a first-pass observation run over source questions 1–10, not a "
        "publication-ready repeated benchmark.",
        "",
        "| Arm | Correct | Completed | Tool calls | Tool errors | Median wall ms | Median tokens |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for arm in ("sql", "filesystem"):
        item = summary["arms"][arm]
        lines.append(
            f"| {arm} | {item['correct']}/{item['trials']} | "
            f"{item['completed']}/{item['trials']} | {item['tool_attempts']} | "
            f"{item['tool_errors']} | {item['median_task_wall_ms']} | "
            f"{item['median_total_tokens']} |"
        )
    lines.extend(
        [
            "",
            "| # | Task | SQL | FS | Expected | SQL wall ms | FS wall ms | SQL/FS calls |",
            "| ---: | --- | --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    by_ordinal = {}
    for row in rows:
        by_ordinal.setdefault(row["ordinal"], {})[row["arm"]] = row
    for ordinal in sorted(by_ordinal):
        pair = by_ordinal[ordinal]
        sql, fs = pair.get("sql", {}), pair.get("filesystem", {})
        task_id = (sql or fs)["task_id"]
        expected = (sql or fs)["expected_answer"]
        lines.append(
            f"| {ordinal} | {task_id} | {sql.get('answer_status', 'missing')} | "
            f"{fs.get('answer_status', 'missing')} | {expected} | "
            f"{sql.get('task_wall_ms', '')} | {fs.get('task_wall_ms', '')} | "
            f"{sql.get('tool_attempts', '')}/{fs.get('tool_attempts', '')} |"
        )
    report = output / "REPORT.md"
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    report.chmod(0o600)


def persist(output, rows):
    summary = summarize(rows)
    private_write(output / "trials.json", rows)
    private_write(output / "summary.json", summary)
    write_report(output, rows, summary)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sonar-root", type=Path, required=True)
    parser.add_argument(
        "--equivalence",
        type=Path,
        default=Path(__file__).with_name("equivalence-summary.json"),
    )
    parser.add_argument("--dsh", required=True)
    parser.add_argument("--patch", action="append", default=[])
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=10)
    parser.add_argument(
        "--arms",
        nargs="+",
        choices=("sql", "filesystem"),
        default=["sql", "filesystem"],
    )
    parser.add_argument(
        "--fs-output-mode",
        choices=("raw", "page", "compact", "filtered-stats"),
        default="page",
        help="filesystem output ablation; compact uses typed JSON page cells",
    )
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args(argv)
    if args.start < 1 or args.end < args.start:
        parser.error("require 1 <= start <= end")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    output.chmod(0o700)
    selected = load_inputs(args.sonar_root, args.equivalence, args.start, args.end)
    public_manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_range": [args.start, args.end],
        "fs_output_mode": args.fs_output_mode,
        "tasks": [
            {
                "ordinal": source_index + 1,
                "source_index": source_index,
                "task_id": task["id"],
                "question": task["question"],
                "database": record["database"],
            }
            for source_index, task, record in selected
        ],
        "schedule": [
            {
                "ordinal": source_index + 1,
                "task_id": task["id"],
                "arms": [
                    arm
                    for arm in (
                        ["sql", "filesystem"]
                        if source_index % 2 == 0
                        else ["filesystem", "sql"]
                    )
                    if arm in args.arms
                ],
            }
            for source_index, task, _ in selected
        ],
    }
    private_write(output / "run.json", public_manifest)
    rows = []
    for source_index, task, record in selected:
        ordinal = source_index + 1
        arms = tuple(
            arm
            for arm in (
                ("sql", "filesystem")
                if source_index % 2 == 0
                else ("filesystem", "sql")
            )
            if arm in args.arms
        )
        for arm in arms:
            trial_dir = output / "trials" / f"{ordinal:03d}-{task['id']}" / arm
            baseline, environment = controlled_baseline(
                arm,
                record["database"],
                trial_dir,
                args.dsh,
                filesystem_path=f"/{record['database']}/raw_data.csv",
                structured_pages=args.fs_output_mode != "raw",
                compact_pages=args.fs_output_mode in {"compact", "filtered-stats"},
                filtered_stats=args.fs_output_mode == "filtered-stats",
            )
            expected_tool = "iotdb_sql" if arm == "sql" else "iotdb_fs"
            client = DshClient(
                args.dsh,
                "headless",
                [*args.patch, baseline],
                args.workspace,
                expected_tool=expected_tool,
                max_tool_calls=1 if arm == "sql" else None,
            )
            try:
                result = client.run_task(
                    render_prompt(task, record, arm, args.fs_output_mode),
                    trial_dir,
                    timeout=args.timeout,
                    environment=environment,
                )
                scored = score(result.get("final_text", ""), record["oracle_public"])
                if result.get("status") != "completed" and scored["correct"]:
                    scored.update(
                        answer_status=result.get("status", "agent_error"), correct=False
                    )
            except Exception as exc:
                result = {"status": "runner_error", "final_text": "", "error": str(exc)}
                scored = score("", record["oracle_public"])
            row = trial_row(ordinal, source_index, task, arm, result, scored, trial_dir)
            rows.append(row)
            persist(output, rows)
            print(
                f"{ordinal:02d} {arm:10s} {row['trial_status']:20s} "
                f"{row['answer_status']:14s} calls={row.get('tool_attempts')}",
                flush=True,
            )
    summary = persist(output, rows)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
