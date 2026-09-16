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

"""Run the executable application scenarios through the same Codex adapter."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import csv
import hashlib
import json
from pathlib import Path
import random
import re
import tempfile
import time

from application_fixture import expected_application_answer, fixture_hash
from codex_client import CodexClient
from fixture import answer_json_schema, compare_answer
from tool_adapter import run_cli, validate_command

ROOT = Path(__file__).resolve().parent


def _load_tasks():
    return json.loads((ROOT / "application_tasks.json").read_text(encoding="utf-8"))[
        "tasks"
    ]


def _render(task, database):
    public = task["public"]
    rendered = {
        "database": database,
        "time_unit": "milliseconds",
        "table_names": "返回不含数据库前缀或 .csv/.meta 后缀的逻辑表名。",
        "goal": public["goal"],
        "role": public["role"],
        "trigger": public["trigger"],
        "answer_json_schema": answer_json_schema(task),
    }
    # Keep public acceptance rules visible to the model.  Private fixture rows
    # and expected answers never live in application_tasks.json's public map.
    if "rules" in public:
        rendered["rules"] = public["rules"]
    if "time_window" in public:
        rendered["time_window"] = public["time_window"]
    return json.dumps(rendered, ensure_ascii=False, indent=2)


def _strict_json(text):
    return json.loads(
        text, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value))
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connections", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--tasks", nargs="*")
    parser.add_argument("--seed", type=int, default=20260914)
    parser.add_argument("--stream-retries", type=int, default=2)
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    connections = json.loads(Path(args.connections).read_text(encoding="utf-8"))
    by_scenario = {item["scenario"]: item for item in connections}
    tasks = [
        task for task in _load_tasks() if not args.tasks or task["id"] in args.tasks
    ]
    required_scenarios = {task["scenario"] for task in tasks}
    missing_scenarios = sorted(required_scenarios - set(by_scenario))
    if missing_scenarios:
        raise SystemExit(
            "connections file has no scenario(s): " + ", ".join(missing_scenarios)
        )
    stale_scenarios = sorted(
        scenario
        for scenario in required_scenarios
        if by_scenario[scenario].get("fixture_hash") != fixture_hash(scenario)
    )
    if stale_scenarios:
        raise SystemExit(
            "fixture hash mismatch for "
            + ", ".join(stale_scenarios)
            + "; prepare a fresh application snapshot before running"
        )
    if args.repeats <= 0:
        raise SystemExit("--repeats must be positive")
    if args.stream_retries < 0:
        raise SystemExit("--stream-retries must be non-negative")
    selected_tasks = {task["id"] for task in tasks}
    if args.tasks and selected_tasks != set(args.tasks):
        missing = sorted(set(args.tasks) - selected_tasks)
        raise SystemExit("unknown application task(s): " + ", ".join(missing))
    rng = random.Random(args.seed)
    pairs = []
    for repeat in range(args.repeats):
        for task in tasks:
            arms = ["sql", "filesystem"]
            rng.shuffle(arms)
            pairs.append(
                {
                    "pair_id": f"APP_{task['id']}_{repeat+1:02d}",
                    "task_id": task["id"],
                    "repeat": repeat + 1,
                    "arms": arms,
                }
            )
    rng.shuffle(pairs)
    plan = []
    for pair in pairs:
        for order, arm in enumerate(pair["arms"], 1):
            plan.append(
                {
                    **{key: pair[key] for key in ("pair_id", "task_id", "repeat")},
                    "arm": arm,
                    "order": order,
                }
            )
    metadata = {
        "status": "running",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "low",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "planned_trials": len(plan),
        "seed": args.seed,
        "scenario_fixture_hashes": {
            scenario: fixture_hash(scenario) for scenario in by_scenario
        },
        "connection_strategy": "CLI process per tool call",
        "purpose": "application_scenarios",
        "stream_retry_limit": args.stream_retries,
    }
    (output / "run.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    with (output / "schedule.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=["index", "pair_id", "task_id", "repeat", "order", "arm"]
        )
        writer.writeheader()
        for index, item in enumerate(plan, 1):
            writer.writerow(
                {"index": index, **{key: item[key] for key in writer.fieldnames[1:]}}
            )
    common = (ROOT / "prompts/common.md").read_text(encoding="utf-8")
    application = (ROOT / "prompts/application.md").read_text(encoding="utf-8")
    rows = []
    with tempfile.TemporaryDirectory(prefix="codex-iotdb-app-empty-") as workspace:
        with CodexClient(
            model="gpt-5.6-sol", effort="low", workspace=workspace
        ) as client:
            metadata.update(
                model_provider=client.provider, codex_config_overrides=client.config
            )
            (output / "run.json").write_text(
                json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
            )
            for index, item in enumerate(plan, 1):
                task = next(t for t in tasks if t["id"] == item["task_id"])
                connection = by_scenario[task["scenario"]]["connection"]
                database = connection["database"]
                trial_id = item["pair_id"] + "_" + item["arm"]
                directory = output / "trials" / trial_id
                directory.mkdir(parents=True)
                calls = []

                def callback(command, remaining):
                    call_dir = directory / "tools" / f"{len(calls)+1:02d}"
                    call_dir.mkdir(parents=True)
                    call = {"command": command}
                    started = time.monotonic_ns()
                    try:
                        call["validation"] = validate_command(
                            command, item["arm"], database
                        )
                        response, timing = run_cli(
                            connection,
                            item["arm"],
                            command,
                            call_dir,
                            min(30, remaining),
                        )
                        call.update(timing)
                    except ValueError as exc:
                        response = {
                            "stdout": "",
                            "stderr": str(exc),
                            "exit_code": 1,
                            "error_kind": "validation_error",
                            "truncated": False,
                            "timed_out": False,
                        }
                    call["response"] = response
                    call["wall_ms"] = (time.monotonic_ns() - started) / 1e6
                    calls.append(call)
                    (call_dir / "call.json").write_text(
                        json.dumps(call, ensure_ascii=False, indent=2) + "\n"
                    )
                    return response

                mode = (
                    ROOT
                    / "prompts"
                    / ("sql.md" if item["arm"] == "sql" else "filesystem.md")
                ).read_text(encoding="utf-8")
                prompt = (
                    common
                    + "\n\n"
                    + mode
                    + "\n\n"
                    + application
                    + "\n\n当前应用任务公开信息：\n"
                    + _render(task, database)
                )
                result = client.run_task(
                    prompt,
                    callback,
                    directory,
                    timeout=180,
                    max_tool_attempts=12,
                    max_model_requests=13,
                    max_stream_retries=args.stream_retries,
                )
                result.update(
                    {
                        "trial_id": trial_id,
                        "pair_id": item["pair_id"],
                        "task_id": task["id"],
                        "scenario": task["scenario"],
                        "repeat": item["repeat"],
                        "order": item["order"],
                        "arm": item["arm"],
                        "database": database,
                        "tool_errors": sum(
                            bool(c["response"].get("error_kind")) for c in calls
                        ),
                        "cli_process_ms": sum(
                            c.get("cli_process_ms", 0) for c in calls
                        ),
                    }
                )
                if result["status"] == "completed":
                    try:
                        actual = _strict_json(result["final_text"])
                        correct, reason = compare_answer(
                            actual, expected_application_answer(task["id"]), task["id"]
                        )
                        result.update(
                            answer_correct=correct,
                            status="success" if correct else "wrong_answer",
                            evaluation_reason=reason,
                        )
                    except (ValueError, TypeError) as exc:
                        result.update(
                            answer_correct=False,
                            status="invalid_answer",
                            evaluation_reason=str(exc),
                        )
                else:
                    result.update(
                        answer_correct=False,
                        evaluation_reason=result.get("error", result["status"]),
                    )
                result["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
                (directory / "result.json").write_text(
                    json.dumps(result, ensure_ascii=False, indent=2) + "\n"
                )
                rows.append(result)
                with (output / "trials.jsonl").open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(result, ensure_ascii=False) + "\n")
                print(
                    f"{index}/{len(plan)} {trial_id} {result['status']} {result.get('task_wall_ms', 0)/1000:.2f}s",
                    flush=True,
                )
    metadata.update(
        status="completed",
        completed_trials=len(rows),
        completed_at_utc=datetime.now(timezone.utc).isoformat(),
    )
    (output / "run.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    )
    _write_trial_csv(output, rows)
    _write_report(output, metadata, rows)


def _write_trial_csv(output, rows):
    fields = [
        "trial_id",
        "pair_id",
        "task_id",
        "scenario",
        "repeat",
        "order",
        "arm",
        "status",
        "answer_correct",
        "evaluation_reason",
        "task_wall_ms",
        "tool_wall_ms",
        "non_tool_wall_ms",
        "cli_process_ms",
        "tool_attempts",
        "tool_errors",
        "model_responses",
        "stream_retry_count",
        "stream_attempts",
        "stream_retry_limit",
    ]
    with (output / "trials.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_report(output, metadata, rows):
    by_task = {}
    for row in rows:
        by_task.setdefault(row["task_id"], []).append(row)
    lines = [
        "# IoTDB 大模型真实应用场景测试报告",
        "",
        f"模型：{metadata.get('model')}；provider：{metadata.get('model_provider')}；场景任务：{len(by_task)}；实际任务数：{len(rows)}；随机种子：{metadata.get('seed')}；流断开重试上限：{metadata.get('stream_retry_limit', 2)}。",
        "",
        "本报告比较同一模型通过 SQL 与 filesystem 接口完成真实业务问题的端到端表现。fs 内部仍通过 SQL 后端，fs 命令解析、元数据读取、客户端统计和模型纠错均属于被测链路。模型服务在 response.completed 前断开时，客户端会建立新的 ephemeral thread 重试，重试时间计入同一任务预算。",
        "",
        "| 场景 | SQL 成功率 | fs 成功率 | SQL 完成中位秒 | fs 完成中位秒 | SQL 成功中位秒 | fs 成功中位秒 | 成功配对 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for task_id in sorted(by_task):
        group = by_task[task_id]
        sql = [r for r in group if r["arm"] == "sql"]
        fs = [r for r in group if r["arm"] == "filesystem"]
        pairs = [(a, b) for a in sql for b in fs if a["pair_id"] == b["pair_id"]]
        paired = [(a, b) for a, b in pairs if a["status"] == b["status"] == "success"]
        median = (
            lambda values: f"{sorted(values)[len(values) // 2] / 1000:.2f}"
            if values
            else "—"
        )
        completed = lambda arm: [
            r["task_wall_ms"]
            for r in arm
            if r.get("status") in {"success", "wrong_answer", "invalid_answer"}
        ]
        success = lambda arm: [
            r["task_wall_ms"] for r in arm if r.get("status") == "success"
        ]
        lines.append(
            f"| {task_id} | {sum(r['status']=='success' for r in sql)}/{len(sql)} | {sum(r['status']=='success' for r in fs)}/{len(fs)} | {median(completed(sql))} | {median(completed(fs))} | {median(success(sql))} | {median(success(fs))} | {len(paired)} |"
        )
    error_counts = Counter(
        row.get("status") for row in rows if row.get("status") != "success"
    )
    lines.extend(["", "## 非成功状态", "", "| 状态 | 次数 |", "| --- | ---: |"])
    for status, count in sorted(error_counts.items()):
        lines.append(f"| {status} | {count} |")
    lines.extend(
        [
            "",
            "## 业务验收",
            "",
            "A1 冷却泵：排除 STOPPED 状态的高温记录，合并连续异常并报告缺测、重复上报。A2 冷链：按 UTC 毫秒判断 2–8°C 合规区间，区分高温、低温和离线并去重。A3 楼宇：按小时比较功率异常，读取同楼层同小时基线，识别累计电能回退并输出重建小时的 `energy_kwh`。答案由独立 fixture oracle 判定，模型看不到 oracle。",
            "",
            "## 文件",
            "",
            "原始任务记录见 `trials.jsonl`，每次调用见 `trials/<trial_id>/tools/`，运行配置见 `run.json`。",
        ]
    )
    (output / "REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    _write_comparison_csv(output, by_task)


def _write_comparison_csv(output, by_task):
    fields = [
        "task_id",
        "scenario",
        "sql_trials",
        "fs_trials",
        "sql_success",
        "fs_success",
        "sql_completed_median_ms",
        "fs_completed_median_ms",
        "sql_success_median_ms",
        "fs_success_median_ms",
        "paired_success",
    ]
    with (output / "comparison.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for task_id in sorted(by_task):
            group = by_task[task_id]
            sql = [r for r in group if r["arm"] == "sql"]
            fs = [r for r in group if r["arm"] == "filesystem"]
            med = (
                lambda arm, statuses: sorted(
                    [r["task_wall_ms"] for r in arm if r.get("status") in statuses]
                )[len([r for r in arm if r.get("status") in statuses]) // 2]
                if any(r.get("status") in statuses for r in arm)
                else None
            )
            paired = sum(
                1
                for a in sql
                for b in fs
                if a["pair_id"] == b["pair_id"]
                and a["status"] == b["status"] == "success"
            )
            writer.writerow(
                {
                    "task_id": task_id,
                    "scenario": group[0]["scenario"],
                    "sql_trials": len(sql),
                    "fs_trials": len(fs),
                    "sql_success": sum(r["status"] == "success" for r in sql),
                    "fs_success": sum(r["status"] == "success" for r in fs),
                    "sql_completed_median_ms": med(
                        sql, {"success", "wrong_answer", "invalid_answer"}
                    ),
                    "fs_completed_median_ms": med(
                        fs, {"success", "wrong_answer", "invalid_answer"}
                    ),
                    "sql_success_median_ms": med(sql, {"success"}),
                    "fs_success_median_ms": med(fs, {"success"}),
                    "paired_success": paired,
                }
            )


if __name__ == "__main__":
    main()
