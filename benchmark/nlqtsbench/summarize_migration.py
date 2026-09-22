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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Summarize completed deterministic verification; keep gold in private reports."""
import argparse
from collections import Counter
import csv
from datetime import datetime
import hashlib
import json
from pathlib import Path

from equivalence import answer_equal


def summarize(root, sonar_root):
    tasks = [
        (i, t)
        for i, t in enumerate(
            json.loads((sonar_root / "nlqtsbench/tasks.json").read_text())
        )
        if t["level"] == 1
    ]
    records = []
    hashes = {
        name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
        for name in ["migrate.py", "equivalence.py"]
    }
    for i, task in tasks:
        row = json.loads((root / "tasks" / task["id"] / "result.json").read_text())
        if (
            row["status"] not in {"verified", "answer_mismatch"}
            or row["source_index"] != i
            or row["code_sha256"] != hashes
        ):
            raise ValueError(f"Incomplete or stale verification: {task['id']}")
        a = row["answers"]
        data = row["data"]
        storage_equal = (
            data["source"] == data["sql"] == data["fs"] == row["tsfile"]["data"]
        )
        assert storage_equal, task["id"]
        reason = "none"
        if not a["public_gold_equal"]:
            reason = (
                "private_time_boundary"
                if a["private_gold_equal"]
                else "gold_semantics_review"
            )
        gold_days = None
        requested_days = None
        if task["subtask"] == "Sliding Window":
            start, end = map(datetime.fromisoformat, task["ground_truth"])
            gold_days = (end - start).total_seconds() / 86400
            requested_days = int(task["meta"]["args"]["window"][:-1])
            oracle_start, oracle_end = map(datetime.fromisoformat, a["oracle_public"])
            expected_days = (oracle_end - oracle_start).total_seconds() / 86400
            if abs(gold_days - expected_days) > 1e-10:
                reason = "incomplete_gold_window"
        records.append(
            {
                "task_id": task["id"],
                "source_index": i,
                "family": task["subtask"],
                "database": row["database"],
                "source_rows": data["source"]["rows"],
                "source_csv_sha256": row["source_sha256"],
                "canonical_sha256": data["source"]["sha256"],
                "storage_equal": storage_equal,
                "sql_oracle_equal": a["sql_oracle_equal"],
                "private_sql_oracle_equal": a["private_sql_oracle_equal"],
                "fs_composed_oracle_equal": a["fs_oracle_equal"],
                "sql_fs_equal": answer_equal(
                    task, a["sql_public"], a["fs_composed_public"]
                ),
                "public_gold_equal": a["public_gold_equal"],
                "private_gold_equal": a["private_gold_equal"],
                "private_time_boundary": a["public_bounds_ms"]
                != a["private_bounds_ms"],
                "gold_issue": reason,
                "gold_interval_days": gold_days,
                "requested_window_days": requested_days,
                "oracle_public": a["oracle_public"],
                "sql_public": a["sql_public"],
                "fs_composed_public": a["fs_composed_public"],
                "original_gold": a["gold"],
                "official_public_score": a["public_official_score"],
                "official_sql_score": a["sql_official_score"],
                "official_fs_score": a["fs_official_score"],
                "tsfile": row["tsfile"]["path"],
                "tsfile_sha256": row["tsfile"]["sha256"],
                "tsfile_bytes": row["tsfile"]["bytes"],
            }
        )
    summary = {
        "tasks": len(records),
        "rows": sum(r["source_rows"] for r in records),
        "storage_verified": sum(r["storage_equal"] for r in records),
        "sql_oracle_equal": sum(r["sql_oracle_equal"] for r in records),
        "private_sql_oracle_equal": sum(r["private_sql_oracle_equal"] for r in records),
        "fs_composed_oracle_equal": sum(r["fs_composed_oracle_equal"] for r in records),
        "sql_fs_equal": sum(r["sql_fs_equal"] for r in records),
        "public_gold_equal": sum(r["public_gold_equal"] for r in records),
        "private_gold_equal": sum(r["private_gold_equal"] for r in records),
        "private_time_boundary": sum(r["private_time_boundary"] for r in records),
        "gold_issues": dict(Counter(r["gold_issue"] for r in records)),
        "tsfile_bytes": sum(r["tsfile_bytes"] for r in records),
        "code_sha256": hashes,
        "model_used": False,
        "fs_profile": "cat_plus_external_generic_composition_not_native_fs",
        "experiment_ready": False,
    }
    (root / "equivalence-summary.json").write_text(
        json.dumps({"summary": summary, "records": records}, indent=2) + "\n"
    )
    with (root / "equivalence.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(
            {
                k: json.dumps(v) if isinstance(v, (list, dict)) else v
                for k, v in r.items()
            }
            for r in records
        )
    lines = [
        "# Deterministic migration verification",
        "",
        f"Verified tasks: {len(records)}; rows: {summary['rows']:,}.",
        "",
        "No model was used. FS results include external generic deterministic computation.",
        "",
        "| Family | Tasks | Storage | SQL/oracle | FS/oracle | Public/gold | Private/gold |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for family in dict.fromkeys(r["family"] for r in records):
        group = [r for r in records if r["family"] == family]
        counts = [len(group)] + [
            sum(r[key] for r in group)
            for key in [
                "storage_equal",
                "sql_oracle_equal",
                "fs_composed_oracle_equal",
                "public_gold_equal",
                "private_gold_equal",
            ]
        ]
        lines.append("| " + family + " | " + " | ".join(map(str, counts)) + " |")
    lines += [
        "",
        "## Original gold discrepancies",
        "",
        "Original questions and gold were not modified. Private bounds are diagnostic only.",
        "",
        "| Task | Reason | Requested window days | Gold interval days | Public oracle | Original gold |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]
    for r in records:
        if r["gold_issue"] != "none":
            lines.append(
                f"| {r['task_id']} | {r['gold_issue']} | {r['requested_window_days'] or ''} | {r['gold_interval_days'] if r['gold_interval_days'] is not None else ''} | {json.dumps(r['oracle_public'])} | {json.dumps(r['original_gold'])} |"
            )
    lines += [
        "",
        "## Interface discrepancies",
        "",
        "| Task | SQL | FS composition | Oracle |",
        "| --- | --- | --- | --- |",
    ]
    for r in records:
        if not r["sql_oracle_equal"] or not r["fs_composed_oracle_equal"]:
            lines.append(
                f"| {r['task_id']} | {json.dumps(r['sql_public'])} | {json.dumps(r['fs_composed_public'])} | {json.dumps(r['oracle_public'])} |"
            )
    if all(r["sql_oracle_equal"] and r["fs_composed_oracle_equal"] for r in records):
        lines += ["", "None under the declared public answer contract."]
    license_header = (
        Path(__file__).with_name("README.md").read_text().split("-->", 1)[0] + "-->\n\n"
    )
    (root / "EQUIVALENCE.md").write_text(license_header + "\n".join(lines) + "\n")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sonar-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(summarize(args.output, args.sonar_root), indent=2))
