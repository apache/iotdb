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

"""Compare saved same-question FS pilots; no database or model invocation."""
import argparse
import json
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    args = parser.parse_args()
    read = lambda p: json.loads(p.read_text())
    before, after = read(args.before / "summary.json"), read(args.after / "summary.json")
    old = {r["id"]: r for r in before["rows"]}
    rows = []
    for new in after["rows"]:
        prior = old[new["id"]]
        row = {"id": new["id"]}
        for key in ["wall_seconds", "model_seconds", "tool_interval_sum_seconds", "tool_attempts", "fs_calls", "context_calls", "tool_errors", "heap_oom_calls", "peak_cli_rss_kib", "completion_class", "transport_retries", "model_requests"]:
            row[key] = {"before": prior[key], "after": new[key]}
        row["wall_reduction_percent"] = 100 * (1 - new["wall_seconds"] / prior["wall_seconds"])
        row["usage"] = {"before": prior["usage"], "after": new["usage"]}
        windows = [c for c in new["calls"] if c["kind"] == "fs" and c["arguments"].get("windows")]
        row["window_calls"] = len(windows)
        row["windows_requested"] = sum(len(c["arguments"]["windows"]) for c in windows)
        row["window_errors"] = sum(c["is_error"] for c in windows)
        row["pre_dispatch_rejections"] = len(new["pre_dispatch_rejections"])
        rows.append(row)
    def totals(source):
        selected = [r for r in source["rows"] if r["id"] in {x["id"] for x in rows}]
        result = {key: sum(r[key] for r in selected) for key in ["wall_seconds", "model_seconds", "tool_interval_sum_seconds", "tool_attempts", "fs_calls", "context_calls", "tool_errors", "heap_oom_calls", "transport_retries", "model_requests"]}
        for key in ["totalTokens", "inputTokens", "cachedInputTokens", "outputTokens", "reasoningTokens"]:
            result[key] = sum(r["usage"].get(key, 0) for r in selected)
        result["median_wall_seconds"] = statistics.median(r["wall_seconds"] for r in selected) if selected else None
        result["median_total_tokens"] = statistics.median(r["usage"].get("totalTokens", 0) for r in selected) if selected else None
        return result
    result = {"scope": "Same-question, same-budget historical FS comparison, one trial each; model randomness, cache and host variation not controlled. CLI and tool contract changed together.", "paired_questions": len(rows), "expected_questions": len(old), "before": totals(before), "after": totals(after), "rows": rows}
    (args.after / "comparison.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != "rows"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
