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

"""Offline rescoring of designated final tables under final-table-v2.

final-table-v1 passes the serialized designated table and the original match
policy to the unmodified TQTS comparator. For questions whose policy enables
pivot identity binding, the binding resolves field identities through the
gold column names (for example ``observed_at`` or ``max_by(Time, ...)``).
Those names are evaluator-only, so a designated table can only pass by
reproducing them, independent of its values.

final-table-v2 keeps the gold output, the comparator, row/column modes,
NULL rules and numeric tolerances, and removes only ``normalization.pivot``
and ``normalization.reshape`` from the policy. The saved gold outputs of the
frozen questions are already wide tables, so the comparison is the ordinary
relational one: every focused gold column must map injectively to a
submitted column, extra columns are tolerated, and the multiset of row
tuples must match. Both versions are reported; v1 scores are not replaced.

Validity gates, all required before any v2 score is reported:
  * every audit comparator check (gold self-match, reversed rows and the
    known negative transformations) keeps its expected outcome under v2;
  * every submission accepted by v2 is rejected after perturbing all
    numeric cells, after dropping a row (if it has several), and after
    duplicating a row.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import sys

from analyze_pilot import final_object
from final_table_submission import serialize_final_table


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def v2_policy(policy):
    policy = copy.deepcopy(policy)
    normalization = policy.get("normalization", {})
    normalization.pop("pivot", None)
    normalization.pop("reshape", None)
    return policy


def perturbed(rows):
    changed = copy.deepcopy(rows)
    for row in changed:
        for key, value in row.items():
            if isinstance(value, bool) or value is None:
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            row[key] = str(number + 1000.0) if isinstance(value, str) else number + 1000.0
    return changed


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot", type=Path, required=True, help="TQTS dataset dir with code-snapshot.json and src/")
    p.add_argument("--contracts", type=Path, required=True)
    p.add_argument("--audit-checks", type=Path, required=True)
    p.add_argument("--gold-root", type=Path, required=True, help="SQL pilot validation directory")
    p.add_argument("--arm", type=Path, action="append", required=True, help="arm directory with trials/<id>/dsh_result.json")
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()

    snapshot = json.loads((args.snapshot / "code-snapshot.json").read_text())
    for entry in snapshot["files"]:
        assert sha(args.snapshot / entry["path"]) == entry["sha256"], entry["path"]
    os.environ["TSDB_RESOURCE_GUARD"] = "0"
    sys.path.insert(0, str(args.snapshot / "src"))
    from tsdb_benchmark.results import MatchPolicy, compare_outputs, normalize_output

    contracts = {q["id"]: q for q in json.loads(args.contracts.read_text())["questions"]}
    gold = {}
    for qid, q in contracts.items():
        path = args.gold_root / qid / "call-24d7f03d8dc3c3666969" / "stdout.txt"
        assert sha(path) == q["gold_stdout_sha256"], qid
        gold[qid] = path.read_text()

    def compare(qid, candidate, version):
        policy = contracts[qid]["match_policy"]
        if version == "v2":
            policy = v2_policy(policy)
        return compare_outputs(gold[qid], candidate, MatchPolicy.from_dict(policy))

    # Gate 1: audit checks keep their expected outcome under v2.
    gate1 = []
    for check in json.loads(args.audit_checks.read_text())["checks"]:
        qid = check["question_id"]
        table = normalize_output(gold[qid], "relational")
        rows = [dict(zip(table.columns, row)) for row in table.rows]
        case = check["case"]
        if case == "gold_self_match":
            candidate = gold[qid]
        elif case == "reversed_rows":
            candidate = json.dumps(rows[::-1])
        elif case == "denominator_24_instead_of_23":
            changed = copy.deepcopy(rows)
            for row in changed:
                row["mean_gap_change_per_cycle"] = str(float(row["mean_gap_change_per_cycle"]) * 23 / 24)
            candidate = json.dumps(changed)
        elif case == "omit_excluded_time":
            candidate = json.dumps([{k: v for k, v in r.items() if k != "time"} for r in rows])
        elif case == "omit_required_device":
            candidate = json.dumps([{k: v for k, v in r.items() if k != "device"} for r in rows])
        elif case == "duplicate_selected_row":
            candidate = json.dumps(rows + rows[:1])
        elif case == "new_york_midnight_instead_of_utc":
            changed = copy.deepcopy(rows)
            changed[0]["time"] = "1560571200000"
            candidate = json.dumps(changed)
        elif case == "omit_required_window_start":
            candidate = json.dumps([{k: v for k, v in r.items() if k != "time"} for r in rows])
        else:
            raise AssertionError("unknown audit case " + case)
        matched, detail = compare(qid, candidate, "v2")
        gate1.append(dict(question_id=qid, case=case, expected_match=check["expected_match"], v2_matched=matched, detail=detail))

    # Score every submission under v1 and v2; gate 2 mutates v2 acceptances.
    scores, gate2 = [], []
    for arm in args.arm:
        for trial in sorted((arm / "trials").iterdir()):
            qid = trial.name
            result = json.loads((trial / "dsh_result.json").read_text())
            # Same parser as the v1 analysis (analyze_fs_pilot.py).
            final = final_object(result.get("final_text", ""))
            row = dict(arm=arm.name, question_id=qid, final_sha256=sha(trial / "dsh_result.json"))
            try:
                candidate = serialize_final_table(final)
            except ValueError as e:
                row.update(submission="invalid", error=str(e), v1=False, v2=False)
                scores.append(row)
                continue
            for version in ("v1", "v2"):
                matched, detail = compare(qid, candidate, version)
                row[version], row[version + "_detail"] = matched, detail
            row["submission"] = "valid"
            scores.append(row)
            if row["v2"]:
                submitted = json.loads(candidate)
                mutations = {"perturb_all_numeric_by_1000": perturbed(submitted), "duplicate_first_row": submitted + submitted[:1]}
                if len(submitted) > 1:
                    mutations["drop_last_row"] = submitted[:-1]
                for name, mutated in mutations.items():
                    matched, detail = compare(qid, json.dumps(mutated), "v2")
                    gate2.append(dict(arm=arm.name, question_id=qid, mutation=name, v2_matched=matched, detail=detail))

    gate1_ok = all(c["v2_matched"] == c["expected_match"] for c in gate1)
    gate2_ok = all(not m["v2_matched"] for m in gate2)
    args.output.mkdir(parents=True, exist_ok=False)
    report = dict(
        version="final-table-v2",
        rule="v1 policy minus normalization.pivot and normalization.reshape; unmodified gold and comparator",
        script_sha256=sha(Path(__file__)),
        contracts_sha256=sha(args.contracts),
        gates=dict(audit_checks_preserved=gate1_ok, mutations_rejected=gate2_ok),
        summary={a.name: dict(submissions=sum(1 for s in scores if s["arm"] == a.name),
                              v1=sum(1 for s in scores if s["arm"] == a.name and s["v1"]),
                              v2=sum(1 for s in scores if s["arm"] == a.name and s["v2"])) for a in args.arm},
        scores=scores, audit_checks=gate1, mutation_checks=gate2,
        limits="Scores only designated final tables; no prose or tool-output recovery. v2 is a protocol variant, reported beside v1.")
    (args.output / "rescoring.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("gates", "summary")}, ensure_ascii=False, indent=2))
    if not (gate1_ok and gate2_ok):
        sys.exit(1)


if __name__ == "__main__":
    main()
