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

"""Offline execution equivalence and trace statistics; never feeds gold to agents."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import sys

from tool_runner import execute


def read(path):
    return json.loads(Path(path).read_text())


def final_object(text):
    fenced = re.findall(r"```(?:json)?\s*([\s\S]*?)```", text)
    if len(fenced) == 1:
        text = fenced[0]
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip())
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else {}
    except ValueError:
        return {}


def offline_query(config, output, label, sql):
    config = {**config, "output": str(output)}
    request = {"kind": "sql", "arguments": {"sql": sql}, "callId": label}
    folder = output / ("call-" + hashlib.sha256(label.encode()).hexdigest()[:20])
    if (folder / "result.json").exists():
        if read(folder / "request.json") != request:
            raise ValueError("Refusing to reuse validation output for different SQL")
        result = read(folder / "result.json")
    else:
        result = execute(config, request)
    return result, (folder / "stdout.txt").read_text() if (folder / "stdout.txt").exists() else ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.root / "datasets/tqtsbench/src"))
    from tsdb_benchmark.results import MatchPolicy, compare_outputs, normalize_output

    rows = []
    for task in read(args.output / "selection.json"):
        trial = args.output / "trials" / task["id"]
        if not (trial / "dsh_result.json").exists():
            continue
        result = read(trial / "dsh_result.json")
        end_reason = result.get("turn_end_reason") or {}
        calls = []
        for request_path in sorted((trial / "tool-output").glob("*/request.json"), key=lambda p: p.stat().st_mtime):
            request = read(request_path)
            result_path = request_path.parent / "result.json"
            response = read(result_path) if result_path.exists() else {}
            content = json.loads(response.get("content", "{}"))
            stdout = content.get("stdout", "")
            calls.append({**request, "elapsed_ms": response.get("elapsedMs"),
                          "is_error": response.get("isError", True),
                          "truncated": content.get("truncated", False),
                          "empty": "Empty set." in stdout or content.get("matched") == 0,
                          "response_chars": len(response.get("content", "")),
                          "output_ref": str(request_path.parent.relative_to(args.output))})
        counts = Counter(c["kind"] for c in calls)
        sql_calls = [c for c in calls if c["kind"] == "sql"]
        query_counts = Counter(re.sub(r"\s+", " ", c["arguments"]["sql"].strip().rstrip(";")).casefold() for c in sql_calls)
        answer = final_object(result.get("final_text", ""))
        config = read(trial / "tool-config.json")
        validation = args.output / "validation" / task["id"]
        gold, gold_stdout = offline_query(config, validation, "gold", task["gold_query"])
        policy = MatchPolicy.from_dict(task.get("match_policy"))
        normalized_gold = normalize_output(gold_stdout, policy.profile)
        status, detail = "no_final_sql", "Agent did not provide an executable final SQL"
        if gold["isError"]:
            status, detail = "gold_execution_error", json.loads(gold["content"])
        elif not normalized_gold.structured:
            status, detail = "gold_unparsed", "Reference result could not be parsed as a table"
        elif not normalized_gold.rows:
            status, detail = "gold_empty", "Reference returned no rows"
        elif isinstance(answer.get("final_sql"), str) and answer["final_sql"].strip():
            candidate, candidate_stdout = offline_query(config, validation, "final", answer["final_sql"])
            if candidate["isError"]:
                status, detail = "final_execution_error", json.loads(candidate["content"])
            else:
                matched, detail = compare_outputs(gold_stdout, candidate_stdout, policy)
                status = "execution_match" if matched else "execution_mismatch"
        rows.append({"id": task["id"], "package": task["package"], "difficulty": task["difficulty"],
                     "question": task["question"], "status": result["status"],
                     "turn_end_reason": end_reason,
                     "completion_class": "generation_budget_exhausted" if end_reason.get("kind") == "max-tokens" else result["status"],
                     "validation_status": status, "validation_detail": detail,
                     "gold_rows": len(normalized_gold.rows), "final": answer,
                     "wall_seconds": result["task_wall_ms"] / 1000,
                     "model_seconds": result.get("model_api_ms", 0) / 1000,
                     "tool_interval_sum_seconds": result.get("tool_wall_ms", 0) / 1000,
                     "tool_attempts": result.get("tool_attempts", 0),
                     "executed_tools": len(calls), "budget_rejections": result.get("tool_attempts", 0) - len(calls),
                     "context_calls": counts["context"], "sql_calls": counts["sql"],
                     "schema_sql_calls": sum(bool(re.match(r"\s*(SHOW|COUNT)\b", c["arguments"]["sql"], re.I)) for c in sql_calls),
                     "tool_errors": sum(c["is_error"] for c in calls),
                     "empty_sql_results": sum(c["empty"] for c in sql_calls),
                     "exact_repeated_sql": sum(max(0, n-1) for n in query_counts.values()),
                     "truncated_sql_results": sum(c["truncated"] for c in sql_calls),
                     "response_chars": sum(c["response_chars"] for c in calls),
                     "model_requests": result.get("model_requests"),
                     "transport_retries": result.get("retry_count", 0),
                     "usage": result.get("usage", {}), "cache_hit_ratio": result.get("cache_hit_ratio"),
                     "policy_audit": read(trial / "policy-audit.json"), "calls": calls})
    summary = {
        "scope": "Exploratory SQL-agent traces with on-demand public context; not an official benchmark score or SQL/FS comparison",
        "offline_evaluator_environment": {k: v for k, v in os.environ.items() if k.startswith("TSDB_")},
        "expected_trials": len(read(args.output / "selection.json")), "recorded_trials": len(rows),
        "completed_trials": sum(r["status"] == "completed" for r in rows),
        "completion_counts": dict(Counter(r["completion_class"] for r in rows)),
        "validation_counts": dict(Counter(r["validation_status"] for r in rows)),
        "total_tokens": sum(r["usage"].get("totalTokens", 0) for r in rows),
        "total_tool_attempts": sum(r["tool_attempts"] for r in rows),
        "total_executed_tools": sum(r["executed_tools"] for r in rows),
        "median_wall_seconds": statistics.median(r["wall_seconds"] for r in rows) if rows else None,
        "median_total_tokens": statistics.median(r["usage"].get("totalTokens", 0) for r in rows) if rows else None,
        "rows": rows,
    }
    (args.output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps({k: v for k, v in summary.items() if k != "rows"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
