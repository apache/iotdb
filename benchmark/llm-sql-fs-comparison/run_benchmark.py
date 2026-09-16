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

"""Run paired, genuine local-Codex tasks against SQL and filesystem tools."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import random
import re
import subprocess
import tempfile
import time

from codex_client import CodexClient
from fixture import load_tasks, expected_answer, compare_answer, render_task
from prepare import verify, checked
from tool_adapter import run_cli, validate_command

ROOT = Path(__file__).resolve().parent


def save(path, value):
    Path(path).write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def strict_json(text):
    def pairs(values):
        obj = {}
        for key, value in values:
            if key in obj:
                raise ValueError("duplicate JSON key: " + key)
            obj[key] = value
        return obj

    def constant(value):
        raise ValueError("nonfinite JSON number: " + value)

    return json.loads(text, object_pairs_hook=pairs, parse_constant=constant)


def schedule(tasks, repeats, seed):
    rng = random.Random(seed)
    orders = {}
    for task in tasks:
        first = ["sql"] * ((repeats + 1) // 2) + ["filesystem"] * (repeats // 2)
        rng.shuffle(first)
        orders[task["id"]] = first
    entries = []
    for repeat in range(repeats):
        round_tasks = list(tasks)
        rng.shuffle(round_tasks)
        for task in round_tasks:
            first = orders[task["id"]][repeat]
            entries.append(
                {
                    "pair_id": f"S_{task['id']}_{repeat+1:02d}",
                    "task_id": task["id"],
                    "repeat": repeat + 1,
                    "fixture_index": rng.randrange(6),
                    "arms": [first, "filesystem" if first == "sql" else "sql"],
                }
            )
    return entries


def evidence(task_id, arm, target, calls):
    good = [
        c
        for c in calls
        if not c["response"].get("error_kind")
        and c["response"].get("exit_code") == 0
        and not c["response"].get("truncated")
        and c.get("validation", {}).get("kind") != "help"
    ]
    if not good:
        return "insufficient_evidence", "No complete successful database observation"
    if task_id == "D01":
        if any(
            c["validation"]["kind"] in {"inventory", "ls", "find", "tree"} for c in good
        ):
            return "success", "Complete inventory observed and final names match oracle"
        return "evidence_pending", "Inventory coverage requires review"
    relevant = [c for c in good if target in c.get("validation", {}).get("tables", [])]
    if not relevant:
        return "insufficient_evidence", "No successful observation of target table"
    if task_id == "D02":
        if any(c["validation"]["kind"] == "schema" for c in relevant):
            return (
                "success",
                "Target schema observed and all final columns match oracle",
            )
    elif task_id in {"K01", "K02", "K03", "K04", "D03"}:
        data = [
            c
            for c in relevant
            if c["validation"]["kind"] in {"select", "cat", "head", "tail"}
        ]
        if data:
            return (
                "evidence_pending",
                "Correct final records; review whether observed rows cover requested records",
            )
    else:
        data = [
            c
            for c in relevant
            if c["validation"]["kind"] in {"count", "stats"}
            or (
                c["validation"]["kind"] == "select"
                and re.search(r"(?i)\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", c["command"])
            )
        ]
        if data:
            return (
                "evidence_pending",
                "Correct final statistics; review aggregate scope and complete group coverage",
            )
    return "evidence_pending", "Correct answer; evidence sufficiency requires review"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connections", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--tasks", nargs="*")
    parser.add_argument("--seed", type=int, default=20260913)
    parser.add_argument(
        "--schedule-file",
        help="Replay an existing schedule exactly instead of generating a new one",
    )
    parser.add_argument(
        "--replacement-source",
        help="Original trials.jsonl whose matching trial IDs are being replaced",
    )
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    connections = json.loads(Path(args.connections).read_text())
    if len(connections) != 6:
        raise ValueError("all six role permutations must be prepared")
    tasks = load_tasks()["tasks"]
    if args.tasks:
        tasks = [t for t in tasks if t["id"] in args.tasks]
    if args.schedule_file:
        plan = strict_json(Path(args.schedule_file).read_text())
        if not isinstance(plan, list) or not all(
            isinstance(item, dict) for item in plan
        ):
            raise ValueError("schedule file must contain a JSON list of pair objects")
        allowed = {task["id"] for task in tasks}
        plan = [item for item in plan if item.get("task_id") in allowed]
        if not plan:
            raise ValueError("schedule file has no selected tasks")
    else:
        plan = schedule(tasks, args.repeats, args.seed)
    replacement_ids = set()
    if args.replacement_source:
        for line in Path(args.replacement_source).read_text().splitlines():
            if not line.strip():
                continue
            try:
                item = strict_json(line)
                trial_id = item.get("trial_id") if isinstance(item, dict) else None
            except ValueError:
                trial_id = line.strip()
            if trial_id:
                replacement_ids.add(trial_id)
    save(output / "schedule.json", plan)
    with (output / "schedule.csv").open("w", newline="") as file:
        writer = csv.DictWriter(
            file, fieldnames=["pair_id", "task_id", "repeat", "fixture_index", "arms"]
        )
        writer.writeheader()
        writer.writerows(plan)
    source_hashes = {
        str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in [
            *ROOT.glob("*.py"),
            ROOT / "tasks.json",
            ROOT / "restricted-model-catalog.json",
            *ROOT.glob("prompts/*.md"),
        ]
    }
    metadata = {
        "status": "running",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "low",
        "codex_version": subprocess.check_output(
            ["codex", "--version"], text=True
        ).strip(),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_hashes": source_hashes,
        "pairs": len(plan),
        "planned_trials": 2 * len(plan),
        "seed": args.seed,
        "points_per_device": 1000,
        "rows_per_table": 4000,
        "connection_strategy": "CLI process per tool call",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "codex_timing_limit": "API request start unavailable; model_api_ms null; report non_tool_wall_ms",
        "fixture_strategy": "six immutable role permutations selected per pair; per-pair full hash verification; equal warmup before each arm",
        "fixtures": [
            {
                "database": c["connection"]["database"],
                "role_map": c["role_map"],
                "hashes": c["hashes"],
            }
            for c in connections
        ],
        "iotdb_endpoint": "127.0.0.1:" + str(connections[0]["connection"]["port"]),
    }
    save(output / "run.json", metadata)
    verification = Path("/tmp/codex-iotdb-runtime-verification.json")
    if verification.exists():
        save(output / "runtime-verification.json", json.loads(verification.read_text()))
    metadata["design_deviations"] = [
        "Local Codex app-server through configured provider, not a directly instrumented model API; API request timing/count unavailable.",
        "Non-tool wall time includes model/network/queue/Codex overhead; model_api_ms remains null.",
        "60 seconds is stream idle timeout; only task 180 seconds and CLI 30 seconds are hard wall limits.",
        "One tool per model response is prompted, not enforceable by this Codex interface; database callbacks execute serially and at most 12 are executed.",
        "13 model responses are limited at completed-response boundaries; a following request may already be in flight.",
        "4096 output-token cap, sampling seed and disabling provider prefix cache are unavailable; actual usage/cache counts are recorded.",
        "Codex internal exec can perform arithmetic and forward iotdb_command; catalog has no shell/file/network/other database tools.",
        "Six immutable database role permutations replace per-pair rebuilding; each pair selects one permutation and verifies complete data hashes.",
        "SQL uses a conservative token grammar and per-database SELECT-only account; comma joins, comments and undocumented functions are rejected.",
    ]
    save(output / "run.json", metadata)
    all_results = []
    task_map = {t["id"]: t for t in tasks}
    common = re.sub(
        r"^<!--.*?-->\s*", "", (ROOT / "prompts/common.md").read_text(), flags=re.S
    )
    with tempfile.TemporaryDirectory(prefix="codex-iotdb-empty-") as workspace:
        with CodexClient(
            model="gpt-5.6-sol", effort="low", workspace=workspace
        ) as client:
            metadata["codex_config_overrides"] = client.config
            metadata["model_provider"] = client.provider
            save(output / "run.json", metadata)
            for pair in plan:
                task = task_map[pair["task_id"]]
                entry = connections[pair["fixture_index"]]
                config = entry["connection"]
                target = entry["role_map"]["target"]
                verify(
                    config, entry["role_map"], output / "preflight" / pair["pair_id"]
                )
                for order, arm in enumerate(pair["arms"], 1):
                    trial_id = pair["pair_id"] + "_" + arm
                    directory = output / "trials" / trial_id
                    directory.mkdir(parents=True)
                    # Same complete table/schema warmup before both arms, never shown to model.
                    warm = []
                    for table in sorted(entry["role_map"].values()):
                        q = config["database"] + "." + table
                        warm.extend(
                            [
                                "DESC " + q,
                                "SELECT * FROM " + q + " ORDER BY time, device",
                                "SELECT COUNT(*),MIN(time),MAX(time) FROM " + q,
                            ]
                        )
                    checked(config, ";".join(warm), directory / "warmup")
                    mode = re.sub(
                        r"^<!--.*?-->\s*",
                        "",
                        (ROOT / "prompts" / (arm + ".md")).read_text(),
                        flags=re.S,
                    )
                    prompt = (
                        common
                        + "\n\n"
                        + mode
                        + "\n\n当前任务公开信息：\n"
                        + render_task(task, config["database"], target)
                    )
                    calls = []

                    def tool_callback(command, remaining):
                        call_dir = directory / "tools" / f"{len(calls)+1:02d}"
                        call_dir.mkdir(parents=True)
                        start = time.monotonic_ns()
                        call = {"command": command, "tool_start_ns": start}
                        try:
                            call["validation"] = validate_command(
                                command, arm, config["database"]
                            )
                            call["validation_end_ns"] = time.monotonic_ns()
                            response, timing = run_cli(
                                config, arm, command, call_dir, min(30, remaining)
                            )
                            call.update(timing)
                        except ValueError as exc:
                            response = {
                                "stdout": "",
                                "stderr": str(exc),
                                "exit_code": 1,
                                "error_kind": "validation_error",
                                "timed_out": False,
                                "truncated": False,
                                "stdout_bytes": 0,
                                "stderr_bytes": 0,
                            }
                        call["response"] = response
                        call["observation_ready_ns"] = time.monotonic_ns()
                        calls.append(call)
                        save(call_dir / "call.json", call)
                        return response

                    result = client.run_task(
                        prompt,
                        tool_callback,
                        directory,
                        timeout=180,
                        max_tool_attempts=12,
                        max_model_requests=13,
                    )
                    result.update(
                        {
                            "trial_id": trial_id,
                            "pair_id": pair["pair_id"],
                            "task_id": task["id"],
                            "scale": "S",
                            "repeat": pair["repeat"],
                            "arm": arm,
                            "order": order,
                            "information": task["information"],
                            "database": config["database"],
                            "target_table": target,
                            "cli_process_ms": sum(
                                c.get("cli_process_ms", 0) for c in calls
                            ),
                            "tool_errors": sum(
                                bool(c["response"].get("error_kind")) for c in calls
                            ),
                            "stdout_bytes": sum(
                                c["response"].get("stdout_bytes", 0) for c in calls
                            ),
                        }
                    )
                    if trial_id in replacement_ids:
                        result["replacement_of"] = trial_id
                    result["execution_status"] = result["status"]
                    if result["status"] == "completed":
                        try:
                            actual = strict_json(result["final_text"])
                            correct, reason = compare_answer(
                                actual,
                                expected_answer(task["id"], 1000, target),
                                task["id"],
                            )
                            result["answer_correct"] = correct
                            if correct:
                                (
                                    result["status"],
                                    result["evaluation_reason"],
                                ) = evidence(task["id"], arm, target, calls)
                            else:
                                result.update(
                                    status="wrong_answer", evaluation_reason=reason
                                )
                        except (ValueError, TypeError) as exc:
                            result.update(
                                status="invalid_answer",
                                answer_correct=False,
                                evaluation_reason=str(exc),
                            )
                    else:
                        result["status"] = {
                            "timeout": "task_timeout",
                            "infrastructure_error": "infra_error",
                        }.get(result["status"], result["status"])
                        result["answer_correct"] = False
                    result["evaluation_finished_utc"] = datetime.now(
                        timezone.utc
                    ).isoformat()
                    save(directory / "result.json", result)
                    all_results.append(result)
                    with (output / "trials.jsonl").open("a") as file:
                        file.write(json.dumps(result, ensure_ascii=False) + "\n")
                    print(
                        f"{len(all_results)}/{2*len(plan)} {trial_id} {result['status']} {result['task_wall_ms']/1000:.2f}s tools={result['tool_attempts']}",
                        flush=True,
                    )
                    if result["status"] in {
                        "protocol_invalid",
                        "tool_policy_violation",
                    }:
                        metadata.update(
                            status="protocol_invalid_stopped",
                            completed_trials=len(all_results),
                            invalid_trial=trial_id,
                            invalid_reason=result.get(
                                "error", "Unexpected tool capability"
                            ),
                        )
                        save(output / "run.json", metadata)
                        raise RuntimeError(
                            "Tool protocol violation; entire run excluded from comparison"
                        )
    metadata.update(
        status="completed",
        completed_at_utc=datetime.now(timezone.utc).isoformat(),
        completed_trials=len(all_results),
    )
    save(output / "run.json", metadata)


if __name__ == "__main__":
    main()
