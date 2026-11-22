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

"""Attribute model input tokens to tool calls, from per-request usage in saved traces.

Each model request k reports its prompt size P_k = inputTokens + cacheReadTokens.
Within a turn the next prompt is the previous prompt plus the assistant message
(all output tokens, reasoning included: DeepSeek resends reasoning content inside a
tool-calling turn) plus the tool results of that step, so

    tool tokens of step k = P_{k+1} - P_k - outputTokens_k.

A step's tool tokens are split over its calls in proportion to returned characters
(evenly if none returned content). A call's carried tokens are its tokens times
the number of later model requests, i.e. what it adds to end-to-end input. Calls
rejected by the tool budget have no saved output; their share stays with the other
calls of the step. Negative step deltas (e.g. context compaction) are reported and
attributed as zero.

Classes come from analyze_bottlenecks.analyze_trial, unchanged. Reported shares:
carried tokens of B1-B4 union over total input tokens, and over total tokens
(input plus output).
"""
import argparse
from collections import defaultdict
import json
from pathlib import Path

from analyze_bottlenecks import analyze_trial, trial_dirs

UNION = {"B1", "B2", "B3", "B4"}


def attribute(trial, large=8000):
    calls, _ = analyze_trial(trial, large)
    events = [json.loads(line) for line in (trial / "events-summary.jsonl").open()]
    messages = [e for e in events if e["type"] == "assistant/message"]
    step_of = {e["callId"]: e["step"] for e in events if e["type"] == "tool/call"}
    prompt = [m["usage"].get("inputTokens", 0) + m["usage"].get("cacheReadTokens", 0) for m in messages]
    output = [m["usage"].get("outputTokens", 0) for m in messages]
    by_step = defaultdict(list)
    for c in calls:
        by_step[step_of.get(c["call_id"])].append(c)
    negative = 0
    for i, m in enumerate(messages[:-1]):
        delta = prompt[i + 1] - prompt[i] - output[i]
        if delta < 0:
            negative += 1
            delta = 0
        members = by_step.get(m["step"], [])
        chars = sum(c["chars"] for c in members)
        for c in members:
            c["tokens"] = delta * (c["chars"] / chars if chars else 1 / len(members))
    for c in calls:
        c.setdefault("tokens", 0.0)
        c["carried_tokens"] = c["tokens"] * c["later_messages"]
    totals = dict(input_tokens=sum(prompt), output_tokens=sum(output), requests=len(messages), negative_steps=negative)
    return calls, totals


def summarize(calls, totals):
    union = [c for c in calls if set(c["classes"]) & UNION]
    tool = sum(c["tokens"] for c in calls)
    return dict(calls=len(calls), union_calls=len(union),
                input_tokens=totals["input_tokens"], output_tokens=totals["output_tokens"],
                tool_result_tokens=round(tool), union_tool_result_tokens=round(sum(c["tokens"] for c in union)),
                carried_tokens=round(sum(c["carried_tokens"] for c in calls)),
                union_carried_tokens=round(sum(c["carried_tokens"] for c in union)),
                negative_steps=totals["negative_steps"])


def shares(s):
    return dict(calls=s["union_calls"] / max(1, s["calls"]),
                input_tokens=s["union_carried_tokens"] / max(1, s["input_tokens"]),
                total_tokens=s["union_carried_tokens"] / max(1, s["input_tokens"] + s["output_tokens"]),
                tool_result_tokens=s["union_tool_result_tokens"] / max(1, s["tool_result_tokens"]))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--arm", type=Path, action="append", required=True)
    p.add_argument("--selection", type=Path, help="optional selection.json to group by class and package")
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    meta = {q["id"]: q for q in json.loads(args.selection.read_text())["selected"]} if args.selection else {}
    report = {}
    for arm in args.arm:
        trials, groups = [], defaultdict(lambda: ([], dict(input_tokens=0, output_tokens=0, requests=0, negative_steps=0)))
        for label, trial in trial_dirs(arm):
            if not (trial / "events-summary.jsonl").exists():
                continue
            calls, totals = attribute(trial)
            qid = label.split("/")[0]
            keys = ["all"] + (["class:" + meta[qid]["class"], "package:" + meta[qid]["package"]] if qid in meta else [])
            for key in keys:
                group_calls, group_totals = groups[key]
                group_calls.extend(calls)
                for k in group_totals:
                    group_totals[k] += totals[k]
            s = summarize(calls, totals)
            trials.append(dict(trial=label, **s, shares=shares(s)))
        summary = {}
        for key, (group_calls, group_totals) in groups.items():
            s = summarize(group_calls, group_totals)
            summary[key] = dict(**s, shares=shares(s))
        report[arm.name] = dict(summary=summary, trials=trials)
    (args.output / "token-attribution.json").write_text(json.dumps(dict(rules=__doc__, arms=report), ensure_ascii=False, indent=1) + "\n")
    lines = ["| arm | group | calls | union calls % | input tokens | union carried % of input | % of total | union % of tool-result tokens |", "|---|---|" + "---:|" * 6]
    for arm, data in report.items():
        for key, s in sorted(data["summary"].items(), key=lambda kv: (kv[0] != "all", kv[0])):
            sh = s["shares"]
            lines.append("| %s | %s | %d | %.1f%% | %d | %.1f%% | %.1f%% | %.1f%% |" % (
                arm, key, s["calls"], 100 * sh["calls"], s["input_tokens"], 100 * sh["input_tokens"], 100 * sh["total_tokens"], 100 * sh["tool_result_tokens"]))
    (args.output / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
