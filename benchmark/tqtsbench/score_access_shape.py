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

"""Score access-shape arms on the frozen phase-1 selection (final-table v1 and v2).

Gold: every selected gold_query is executed once, read-only, with the per-package
credentials through tool_runner.run_sql; stdout is kept under <output>/gold/<id>/.
Row counts are checked against the selection's gold_check (stdout hashes are
reported but not required to match, because the CLI prints elapsed time).

Policies: v1 is the original TQTS match_policy; v2 removes normalization.pivot and
normalization.reshape (rescore_final_table_v2.v2_policy). Gold and comparator are
unmodified. Only the designated final_table is scored; no prose or tool-output
recovery.

Validity gates, reported per policy version:
  * gold self-match and reversed gold rows must match for each question; a question
    failing them is excluded from that version's score and listed;
  * every accepted submission must be rejected after perturbing all numeric cells,
    after duplicating its first row, and after dropping its last row (if several).
"""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import sys

from analyze_bottlenecks import trial_dirs
from analyze_pilot import final_object
from final_table_submission import serialize_final_table
from rescore_final_table_v2 import perturbed, v2_policy
from tool_runner import run_sql


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_policy(raw):
    return raw if isinstance(raw, dict) else ast.literal_eval(raw)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot", type=Path, required=True, help="TQTS dataset dir with code-snapshot.json, src/ and benchmark/packages/")
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--credentials-dir", type=Path, required=True)
    p.add_argument("--cli", required=True)
    p.add_argument("--arm", type=Path, action="append", required=True, help="directory with trials/<id>/r<k>/dsh_result.json")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--gold-seconds", type=int, default=180)
    args = p.parse_args()

    snapshot = json.loads((args.snapshot / "code-snapshot.json").read_text())
    for entry in snapshot["files"]:
        assert sha(args.snapshot / entry["path"]) == entry["sha256"], entry["path"]
    os.environ["TSDB_RESOURCE_GUARD"] = "0"
    sys.path.insert(0, str(args.snapshot / "src"))
    from tsdb_benchmark.results import MatchPolicy, compare_outputs, normalize_output

    selection = json.loads(args.selection.read_text())
    checks = (selection.get("gold_check") or {}).get("checks", {})
    selected = selection["selected"]
    qa = {}
    for package in sorted({q["package"] for q in selected}):
        for item in json.loads((args.snapshot / "benchmark/packages" / package / "qa.json").read_text())["qa"]:
            qa[item["id"]] = item

    args.output.mkdir(parents=True, exist_ok=False)
    gold, gold_report = {}, []
    for q in selected:
        qid, item = q["id"], qa[q["id"]]
        folder = args.output / "gold" / qid
        folder.mkdir(parents=True)
        config = {"package": q["package"], "credentials": str(args.credentials_dir / (q["package"] + ".json")),
                  "cli": args.cli, "sql_output_chars": 0}
        try:
            result = run_sql(config, item["gold_query"], folder, seconds=args.gold_seconds)
        except ValueError as e:
            result = {"is_error": True, "error": str(e)}
        stdout = (folder / "stdout.txt").read_text() if (folder / "stdout.txt").exists() else ""
        rows = len(normalize_output(stdout, "relational").rows) if stdout and not result.get("is_error") else None
        expected = checks.get(qid, {}).get("rows")
        gold_report.append(dict(question_id=qid, is_error=bool(result.get("is_error")), error=result.get("error"), rows=rows, selection_rows=expected,
                                rows_agree=rows == expected, stdout_sha256=hashlib.sha256(stdout.encode()).hexdigest(),
                                selection_stdout_sha256=checks.get(qid, {}).get("stdout_sha256")))
        if not result.get("is_error"):
            gold[qid] = stdout

    def policy(qid, version):
        raw = load_policy(qa[qid]["match_policy"])
        return MatchPolicy.from_dict(v2_policy(raw) if version == "v2" else raw)

    def compare(qid, candidate, version):
        return compare_outputs(gold[qid], candidate, policy(qid, version))

    # Gate 1: gold self-match and reversed rows, per question and version.
    gate1, valid = [], {"v1": set(), "v2": set()}
    for qid in gold:
        table = normalize_output(gold[qid], "relational")
        rows = [dict(zip(table.columns, row)) for row in table.rows]
        for version in ("v1", "v2"):
            self_ok, self_detail = compare(qid, gold[qid], version)
            rev_ok, rev_detail = compare(qid, json.dumps(rows[::-1]), version)
            gate1.append(dict(question_id=qid, version=version, self_match=self_ok, reversed_match=rev_ok,
                              detail=None if self_ok and rev_ok else [self_detail, rev_detail]))
            if self_ok and rev_ok:
                valid[version].add(qid)

    scores, gate2 = [], []
    for arm in args.arm:
        for label, trial in trial_dirs(arm):
            qid = label.split("/")[0]
            row = dict(arm=arm.name, trial=label, question_id=qid)
            if not (trial / "dsh_result.json").exists():
                row.update(submission="missing", v1=None, v2=None)
                scores.append(row)
                continue
            final = final_object(json.loads((trial / "dsh_result.json").read_text()).get("final_text", ""))
            try:
                candidate = serialize_final_table(final)
            except ValueError as e:
                row.update(submission="invalid", error=str(e), v1=False, v2=False)
                scores.append(row)
                continue
            row["submission"] = "valid"
            for version in ("v1", "v2"):
                if qid not in valid[version]:
                    row[version], row[version + "_detail"] = None, "question excluded by gate 1"
                    continue
                matched, detail = compare(qid, candidate, version)
                row[version], row[version + "_detail"] = matched, detail
                if matched:
                    submitted = json.loads(candidate)
                    mutations = {"perturb_all_numeric_by_1000": perturbed(submitted), "duplicate_first_row": submitted + submitted[:1]}
                    if len(submitted) > 1:
                        mutations["drop_last_row"] = submitted[:-1]
                    for name, mutated in mutations.items():
                        hit, mdetail = compare(qid, json.dumps(mutated), version)
                        gate2.append(dict(arm=arm.name, trial=label, version=version, mutation=name, matched=hit, detail=mdetail))
            scores.append(row)

    def tally(arm, version):
        rows = [s for s in scores if s["arm"] == arm]
        return dict(trials=len(rows), scored=sum(1 for s in rows if s[version] is not None),
                    matched=sum(1 for s in rows if s[version]),
                    invalid_or_null=sum(1 for s in rows if s["submission"] == "invalid"),
                    missing=sum(1 for s in rows if s["submission"] == "missing"))

    report = dict(
        script_sha256=sha(Path(__file__)), selection_sha256=sha(args.selection),
        rule="v1: original match_policy; v2: minus normalization.pivot and normalization.reshape; unmodified gold and comparator",
        gold=gold_report,
        gates=dict(gold_rows_agree=all(g["rows_agree"] for g in gold_report),
                   excluded_by_gate1={v: sorted(set(gold) - valid[v]) for v in valid},
                   mutations_rejected=all(not m["matched"] for m in gate2)),
        summary={a.name: {v: tally(a.name, v) for v in ("v1", "v2")} for a in args.arm},
        scores=scores, gold_checks=gate1, mutation_checks=gate2,
        limits="Designated final tables only; per-trial results are descriptive.")
    (args.output / "scores.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("gates", "summary")}, ensure_ascii=False, indent=2))
    if not report["gates"]["mutations_rejected"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
