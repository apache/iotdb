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

"""Offline access-shape bottleneck classifier over saved TQTS agent traces.

Reads only saved trials (events-summary.jsonl and tool-output/*/{request,result}.json);
issues no database queries and no model requests. Classes follow the FSView design
(B1-B6) plus X1 for adapter misuse, which is not an access-shape bottleneck:

  B1 sibling repetition: >=2 calls whose arguments are identical except the object
     (FS path, or every root.* path in a SQL statement); every call after the
     first in such a group counts.
  B2 path/object miss: an SQL or FS call that returns an empty result or a path
     error (an empty dataset_context search is a search miss, not counted here).
  B3 broad structure: a structure call (ls/find/tree/schema/meta/stat/file/help,
     SQL SHOW/COUNT) returning more than --large chars or truncated, or the third
     and later call of a run of consecutive structure calls.
  B4 large context: a dataset_context call returning more than --large chars.
  B5 final synthesis (proxy only): reasoning and output tokens of model messages
     after the last tool result. It does not identify what was hand-computed.
  B6 convention recheck: a dataset_context call after the first compute call.
  X1 adapter misuse: an error caused by unsupported or malformed tool parameters.

Cost proxies per class: calls, returned characters, tool seconds and carried
characters (returned characters times the number of later model messages, an
upper-bound proxy for repeated prompt input). Classes may overlap; the union is
reported separately. Heuristic rules; validate with a human-labelled sample.
"""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import random
import re

STRUCT_FS = {"help", "ls", "ll", "find", "tree", "schema", "meta", "stat", "file"}
PROBE_FS = {"head", "tail", "cat", "count", "grep", "cut", "wc", "paste", "join"}
AGG = re.compile(r"\b(avg|count|sum|min_value|max_value|first_value|last_value|max_by|min_by|m4|time_difference|stddev|variance|mode)\s*\(|group\s+by", re.I)
ROOT_PATH = re.compile(r"root(?:\.[A-Za-z0-9_*`]+)+")
PATH_ERROR = re.compile(r"absolute virtual Tree path|does not exist|not exist|no such|not found|Path is outside|unknown path|PathNotExist", re.I)
MISUSE_ERROR = re.compile(r"Unsupported parameters|must be|requires|Unknown command|Invalid|exclusive|exceeds output budget", re.I)
CLASSES = ("B1", "B2", "B3", "B4", "B5", "B6", "X1")


def category(request):
    kind, args = request.get("kind"), request.get("arguments", {})
    if kind == "context":
        return "context"
    if kind == "sql":
        sql = args.get("sql", "").strip()
        if re.match(r"(show|count)\s", sql, re.I):
            return "struct"
        return "compute" if AGG.search(sql) else "probe"
    command = args.get("command", "")
    if command in STRUCT_FS:
        return "struct"
    if command in PROBE_FS:
        return "probe"
    return "compute"


def sibling_signature(request):
    kind, args = request.get("kind"), dict(request.get("arguments", {}))
    if kind == "sql":
        sql = args.get("sql", "")
        objects = tuple(ROOT_PATH.findall(sql))
        return (kind, ROOT_PATH.sub("<PATH>", sql)), objects
    if kind == "fs":
        objects = tuple([args.pop("path")] if "path" in args else args.pop("paths", []))
        return (kind, json.dumps(args, sort_keys=True)), objects
    return None, ()


def outcome(result):
    """Returns (empty, error_text, returned_chars, truncated)."""
    content = result.get("content", "")
    try:
        payload = json.loads(content)
    except ValueError:
        payload = {}
    error = ""
    if result.get("isError") or payload.get("is_error"):
        error = str(payload.get("error") or payload.get("stderr") or "")
    stdout = payload.get("stdout")
    empty = (isinstance(payload.get("rows"), list) and not payload["rows"]) or (
        isinstance(stdout, str) and "Empty set." in stdout) or (
        isinstance(payload.get("entries"), list) and not payload["entries"])
    return empty, error, len(content), bool(payload.get("truncated"))


def analyze_trial(trial, large):
    requests = {}
    for path in (trial / "tool-output").glob("*/request.json"):
        request = json.loads(path.read_text())
        result_path = path.with_name("result.json")
        result = json.loads(result_path.read_text()) if result_path.exists() else {}
        requests[request.get("callId")] = (request, result, path.parent.name)
    events = [json.loads(line) for line in (trial / "events-summary.jsonl").open()]
    messages = [e for e in events if e["type"] == "assistant/message"]
    results = {e["callId"]: e for e in events if e["type"] == "tool/result"}
    calls = []
    for e in events:
        if e["type"] != "tool/call" or e["callId"] not in requests:
            continue
        request, result, ref = requests[e["callId"]]
        empty, error, chars, truncated = outcome(result)
        end = results.get(e["callId"], e)["time"]
        calls.append(dict(seq=e["seq"], call_id=e["callId"], ref=ref, kind=request.get("kind"),
                          category=category(request), arguments=request.get("arguments", {}),
                          empty=empty, error=error, chars=chars, truncated=truncated,
                          tool_seconds=(end - e["time"]) / 1000,
                          later_messages=sum(1 for m in messages if m["seq"] > e["seq"]), classes=[]))
    first_compute = next((c["seq"] for c in calls if c["category"] == "compute"), None)
    groups = defaultdict(list)
    for c in calls:
        signature, objects = sibling_signature({"kind": c["kind"], "arguments": c["arguments"]})
        if signature is not None and objects:
            groups[signature].append((c, objects))
    for members in groups.values():
        if len({objects for _, objects in members}) >= 2:
            for c, _ in members[1:]:
                c["classes"].append("B1")
    run = 0
    for c in calls:
        if c["error"] and MISUSE_ERROR.search(c["error"]) and not PATH_ERROR.search(c["error"]):
            c["classes"].append("X1")
        elif c["kind"] != "context" and (c["empty"] or (c["error"] and PATH_ERROR.search(c["error"]))):
            c["classes"].append("B2")
        run = run + 1 if c["category"] == "struct" else 0
        if c["category"] == "struct" and (c["chars"] > large or c["truncated"] or run >= 3):
            c["classes"].append("B3")
        if c["category"] == "context" and c["chars"] > large:
            c["classes"].append("B4")
        if c["category"] == "context" and first_compute is not None and c["seq"] > first_compute:
            c["classes"].append("B6")
    last_result = max((results[c["call_id"]]["seq"] for c in calls if c["call_id"] in results), default=-1)
    synthesis = [m for m in messages if m["seq"] > last_result]
    b5 = dict(messages=len(synthesis),
              reasoning_tokens=sum(m.get("usage", {}).get("reasoningTokens", 0) for m in synthesis),
              output_tokens=sum(m.get("usage", {}).get("outputTokens", 0) for m in synthesis))
    return calls, b5


def cost(calls):
    return dict(calls=len(calls), chars=sum(c["chars"] for c in calls),
                tool_seconds=round(sum(c["tool_seconds"] for c in calls), 2),
                carried_chars=sum(c["chars"] * c["later_messages"] for c in calls))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--arm", type=Path, action="append", required=True, help="directory containing trials/<question>/")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--large", type=int, default=8000, help="returned-character threshold for B3/B4")
    p.add_argument("--sample", type=int, default=50, help="calls in the human-labelling sample")
    p.add_argument("--seed", type=int, default=20261120)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    report, every = {}, []
    for arm in args.arm:
        name = arm.parent.name + "/" + arm.name if arm.name in {"base", "both", "grid-base", "grid-reduce", "composition", "temporal", "extreme", "grid-only", "reduce-only"} else arm.name
        trials, all_calls = [], []
        for trial in sorted((arm / "trials").iterdir()):
            if not (trial / "events-summary.jsonl").exists():
                continue
            calls, b5 = analyze_trial(trial, args.large)
            for c in calls:
                c["arm"], c["question_id"] = name, trial.name
            all_calls += calls
            trials.append(dict(question_id=trial.name, total=cost(calls), b5=b5,
                               classes={k: cost([c for c in calls if k in c["classes"]]) for k in CLASSES if k != "B5"},
                               access_shape_union=cost([c for c in calls if set(c["classes"]) & {"B1", "B2", "B3", "B4"}])))
        total = cost(all_calls)
        summary = dict(total=total,
                       classes={k: cost([c for c in all_calls if k in c["classes"]]) for k in CLASSES if k != "B5"},
                       access_shape_union=cost([c for c in all_calls if set(c["classes"]) & {"B1", "B2", "B3", "B4"}]),
                       b5=dict(reasoning_tokens=sum(t["b5"]["reasoning_tokens"] for t in trials),
                               output_tokens=sum(t["b5"]["output_tokens"] for t in trials)),
                       categories=dict(Counter(c["category"] for c in all_calls)))
        report[name] = dict(summary=summary, trials=trials)
        every += all_calls
    rng = random.Random(args.seed)
    labelled = [c for c in every if c["classes"]]
    unlabelled = [c for c in every if not c["classes"]]
    sample = rng.sample(labelled, min(len(labelled), args.sample * 3 // 5)) + rng.sample(unlabelled, min(len(unlabelled), args.sample - min(len(labelled), args.sample * 3 // 5)))
    rng.shuffle(sample)
    with (args.output / "labelling-sample.tsv").open("w") as out:
        out.write("arm\tquestion_id\tcall_ref\tcategory\targuments\tresult_chars\tempty\terror\tauto_classes\thuman_classes\tnote\n")
        for c in sample:
            out.write("\t".join([c["arm"], c["question_id"], c["ref"], c["category"], json.dumps(c["arguments"], ensure_ascii=False)[:400],
                                 str(c["chars"]), str(c["empty"]), c["error"][:160].replace("\t", " ").replace("\n", " "), ",".join(c["classes"]), "", ""]) + "\n")
    (args.output / "bottlenecks.json").write_text(json.dumps(dict(
        rules=__doc__, large_threshold=args.large, seed=args.seed, arms=report,
        calls=[{k: v for k, v in c.items() if k != "arguments"} | {"arguments": c["arguments"]} for c in every]), ensure_ascii=False, indent=1) + "\n")
    lines = ["| arm | calls | B1 | B2 | B3 | B4 | B6 | X1 | B1–B4 union calls | union chars share | union carried share | B5 reasoning tokens |", "|---|" + "---:|" * 11]
    for name, data in report.items():
        s = data["summary"]; t = s["total"]; u = s["access_shape_union"]
        lines.append("| %s | %d | %s | %d | %.0f%% | %.0f%% | %d |" % (
            name, t["calls"], " | ".join(str(s["classes"][k]["calls"]) for k in ("B1", "B2", "B3", "B4", "B6", "X1")),
            u["calls"], 100 * u["chars"] / max(1, t["chars"]), 100 * u["carried_chars"] / max(1, t["carried_chars"]), s["b5"]["reasoning_tokens"]))
    (args.output / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
