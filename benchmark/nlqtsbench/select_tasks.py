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

"""Read-only NLQTSBench preflight; never claims database execution verification."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

POLICY_VERSION = 1
SUBTASKS = {
    "Global Aggregation": "rel_acc",
    "Temporal Localization": "hit",
    "Interval Discovery": "iou",
    "Sliding Window": "iou",
}
BASE_ADAPTATIONS = [
    "TABLE_MODEL_LONG_SCHEMA",
    "TASK_ISOLATION",
    "TIMESTAMP_TO_TIME",
    "FREEZE_TIMEZONE_AND_PRECISION",
    "PRESERVE_NULL_AND_ROW_IDENTITY",
    "ORIGINAL_INDEX_EVALUATOR_MAPPING",
    "NO_GOLD_OR_PRIVATE_ARGS_IN_PROMPT",
]
EVIDENCE_PATHS = [
    "benchmark/llm-sql-fs-comparison/tool_adapter.py",
    "benchmark/llm-sql-fs-comparison/prompts/filesystem.md",
    "iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/FsRowReader.java",
    "iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/FsStatistics.java",
    "iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/TableFilesystemSchemaProvider.java",
    "iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/udf/builtin/relational/TableBuiltinAggregationFunction.java",
    "iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/udf/builtin/relational/TableBuiltinWindowFunction.java",
    "iotdb-core/relational-grammar/src/main/antlr4/org/apache/iotdb/db/relational/grammar/sql/RelationalSql.g4",
    "integration-test/src/test/java/org/apache/iotdb/relational/it/db/it/IoTDBWindowFunctionIT.java",
]
SONAR_EVIDENCE = [
    "nlqtsbench/tasks.json",
    "sonar_ts/storage.py",
    "sonar_ts/evaluator.py",
    "sonar_ts/skills/library/sliding-window-scan.md",
]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_revision(root):
    result = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def timestamp(value):
    """Use an explicit UTC convention; never the host's local timezone."""
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?",
        value,
    ):
        raise ValueError("timestamp requires ISO seconds and at most microseconds")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def public_period(task):
    """Return only an unambiguous calendar period explicitly present in the NLQ.

    Private meta.args dates are used to locate, never to silently enrich, the NLQ.
    Explicit ranges need boundary review and therefore return None.
    """
    value = str(task.get("meta", {}).get("args", {}).get("time", ""))
    if not re.fullmatch(r"\d{4}(?:-\d{2})?", value):
        return None
    if not re.search(r"(?<![\d-])" + re.escape(value) + r"(?![\d-])", task["question"]):
        return None
    year = int(value[:4])
    month = int(value[5:]) if len(value) == 7 else 1
    try:
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        end = (
            datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            if len(value) == 4 or month == 12
            else datetime(year, month + 1, 1, tzinfo=timezone.utc)
        )
    except ValueError:
        return None
    return start, end


def audit_csv(task, data_root):
    """Inspect the full source CSV, without modifying it or using an importer."""
    relative = Path(task["ts_data_path"])
    root = data_root.resolve()
    path = (root / relative).resolve()
    result = {"status": "missing", "issues": [], "rows": 0, "csv_sha256": None}
    if relative.is_absolute() or not path.is_relative_to(root):
        return {**result, "status": "reject", "issues": ["CSV_OUTSIDE_DATA_ROOT"]}
    if not path.is_file():
        return {**result, "issues": ["CSV_MISSING"]}
    counts = Counter()
    seen = set()
    steps = set()
    previous = None
    low = high = None
    try:
        result["csv_sha256"] = sha256(path)
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream)
            header = next(reader, [])
            if (
                any(not name.strip() for name in header)
                or len(set(header)) != len(header)
                or "timestamp" not in header
                or len(header) < 2
            ):
                return {**result, "status": "reject", "issues": ["INVALID_CSV_HEADER"]}
            if str(task.get("channel")) not in header:
                return {
                    **result,
                    "status": "reject",
                    "issues": ["TASK_CHANNEL_MISSING"],
                }
            ts_col = header.index("timestamp")
            result["channels"] = [name for name in header if name != "timestamp"]
            for row in reader:
                result["rows"] += 1
                if len(row) != len(header):
                    counts["MALFORMED_ROW"] += 1
                    continue
                try:
                    point = timestamp(row[ts_col])
                except ValueError:
                    counts["TIMESTAMP_PARSE_OR_PRECISION_REVIEW"] += 1
                    continue
                # Original SQLite loader truncates to seconds before its PK check.
                if point.microsecond:
                    counts["SOURCE_SECOND_TRUNCATION_REVIEW"] += 1
                if point.utcoffset().total_seconds() != 0:
                    counts["TIMEZONE_OFFSET_REVIEW"] += 1
                source_key = row[ts_col][:19].replace("T", " ")
                if source_key in seen:
                    counts["DUPLICATE_SOURCE_SECOND"] += 1
                seen.add(source_key)
                if previous is not None:
                    delta = (point - previous).total_seconds()
                    if delta <= 0:
                        counts["NOT_STRICTLY_INCREASING"] += 1
                    else:
                        steps.add(delta)
                previous = point
                low = point if low is None else min(low, point)
                high = point if high is None else max(high, point)
                for index, value in enumerate(row):
                    if index == ts_col:
                        continue
                    if value.strip().lower() in {"", "nan", "null", "na", "n/a"}:
                        counts["NULL_VALUE_REVIEW"] += 1
                        continue
                    try:
                        if not math.isfinite(float(value)):
                            counts["NONFINITE_VALUE_REVIEW"] += 1
                    except ValueError:
                        counts["SOURCE_NUMERIC_COERCION_REVIEW"] += 1
        if len(steps) > 1:
            counts["IRREGULAR_SAMPLING_REVIEW"] = 1
        if not result["rows"]:
            counts["EMPTY_CSV"] = 1
        period = public_period(task)
        result["all_rows_in_public_period"] = bool(
            period and low is not None and period[0] <= low and high < period[1]
        )
        result["min_time"] = low.isoformat() if low else None
        result["max_time"] = high.isoformat() if high else None
        result["sampling_step_seconds"] = next(iter(steps)) if len(steps) == 1 else None
        result["issue_counts"] = dict(sorted(counts.items()))
        result["issues"] = sorted(counts)
        rejected = {"DUPLICATE_SOURCE_SECOND", "MALFORMED_ROW", "EMPTY_CSV"}
        result["status"] = (
            "reject"
            if rejected.intersection(counts)
            else "review" if counts else "pass_static"
        )
        return result
    except (OSError, UnicodeError, csv.Error, ValueError) as exc:
        return {
            **result,
            "status": "review",
            "issues": ["CSV_READ_ERROR"],
            "error": str(exc),
        }


def assess(task, index, data):
    subtask = task["subtask"]
    row = {
        "task_id": task["id"],
        "source_index": index,
        "source_csv": task["ts_data_path"],
        "subtask": subtask,
        "scope": (
            "candidate" if task["level"] == 1 and subtask in SUBTASKS else "excluded"
        ),
        "data": data,
        "adaptations": list(BASE_ADAPTATIONS),
        "semantic_reviews": [],
        "required_operations": [],
        "sql": {"engine": "not_assessed", "existing_adapter": "not_assessed"},
        "fs": {"native": "not_assessed", "composed_proposal": "not_assessed"},
        "execution": "not_verified",
        "experiment_ready": False,
    }
    if row["scope"] == "excluded":
        row["decision"] = "excluded_scope"
        row["reason"] = (
            "Pattern/semantic/report workloads are outside this single-query pilot."
        )
        return row
    args = task.get("meta", {}).get("args", {})
    reviews = row["semantic_reviews"]
    if task.get("eval_metric") != SUBTASKS[subtask]:
        reviews.append("UNEXPECTED_EVALUATION_METRIC")
    if task.get("ground_truth") is None:
        reviews.append("GROUND_TRUTH_MISSING")
    raw_time = str(args.get("time", ""))
    if "Timestamp(" in raw_time:
        reviews.append("PRIVATE_TIME_RANGE_BOUNDARIES")
    elif public_period(task) is None:
        reviews.append("PUBLIC_TIME_PERIOD_UNRESOLVED")
    row["sql"] = {
        "engine": "supported_in_source",
        "existing_adapter": "supported_surface",
    }
    row["fs"] = {
        "native": "requires_composition",
        "composed_proposal": "not_implemented",
    }
    row["required_operations"] = ["time_filter", "channel_selection"]
    row["adaptations"].append("TIME_RANGE_BOUNDARY_REWRITE")
    if subtask == "Global Aggregation":
        agg = args.get("agg")
        row["required_operations"].append(str(agg))
        if agg == "median":
            row["sql"] = {
                "engine": "rewrite_and_probe",
                "existing_adapter": "requires_extension",
            }
            reviews.append("EXACT_MEDIAN_EVEN_AND_NULL_SEMANTICS")
            row["adaptations"].append("PERCENTILE_OR_RANKED_MEDIAN_REWRITE")
        elif agg not in {"average", "minimum", "maximum", "range"}:
            reviews.append("UNKNOWN_AGGREGATE")
            row["sql"]["engine"] = "not_assessed"
        elif data.get("all_rows_in_public_period") and data["status"] == "pass_static":
            # stats is per channel TAG; AVG/range need only scalar arithmetic.
            row["fs"]["native"] = "supported_with_scalar_arithmetic"
        else:
            row["fs"]["native"] = "conditional_on_whole_file_scope"
        reviews.append("ROUNDING_AND_OFFICIAL_RELATIVE_SCORE")
    elif subtask == "Temporal Localization":
        row["required_operations"] += [
            "value_filter_or_extremum",
            "ordered_timestamp_selection",
        ]
        reviews.append("TIE_BREAK_AND_THRESHOLD_CROSSING")
        row["adaptations"].append("FS_FIELD_PREDICATE_AND_ARG_EXTREMUM")
    elif subtask == "Interval Discovery":
        row["required_operations"] += [
            "threshold",
            "lag",
            "run_segmentation",
            "duration_argmax",
        ]
        row["sql"] = {
            "engine": "rewrite_and_probe",
            "existing_adapter": "requires_extension",
        }
        row["adaptations"] += [
            "LAG_RUNNING_SUM_SUBQUERY_REWRITE",
            "FS_RUN_SEGMENTATION",
        ]
        reviews += ["GAP_AND_NULL_CONTINUITY", "DURATION_AND_TIE_BREAK"]
    elif subtask == "Sliding Window":
        row["required_operations"] += ["rolling_aggregate", "window_arg_extremum"]
        row["sql"] = {
            "engine": "rewrite_and_probe",
            "existing_adapter": "query_dependent_probe",
        }
        row["adaptations"] += ["ROWS_VS_TIME_WINDOW_REWRITE", "FS_ROLLING_COMPOSITION"]
        reviews += ["NATIVE_CADENCE_WINDOW_LENGTH", "COMPLETE_WINDOW_AND_TIE_BREAK"]
        if args.get("metric") == "highest variance":
            row["sql"]["existing_adapter"] = "requires_extension"
            reviews.append("VARIANCE_DDOF_ONE")
        elif args.get("metric") not in {
            "largest range",
            "lowest average",
            "highest average",
        }:
            reviews.append("UNKNOWN_ROLLING_METRIC")
            row["sql"]["engine"] = "not_assessed"
    if row["fs"]["native"] != "supported_with_scalar_arithmetic":
        row["adaptations"].append("FS_COMPOSITION_AND_EQUAL_SQL_COMPUTE_BUDGET")
    row["decision"] = (
        "blocked_data"
        if data["status"] in {"missing", "reject"}
        else (
            "needs_data_review"
            if data["status"] == "review"
            else "needs_semantic_and_execution_review"
        )
    )
    return row


def scan(sonar_root, data_root, iotdb_root):
    tasks_path = sonar_root / "nlqtsbench/tasks.json"
    tasks = json.loads(tasks_path.read_text(encoding="utf-8"))
    seen = set()
    rows = []
    public = []
    for index, task in enumerate(tasks):
        for key in ("id", "level", "subtask", "question", "ts_data_path"):
            if key not in task:
                raise ValueError(f"task at index {index}: missing {key}")
        if not isinstance(task["id"], str) or task["id"] in seen:
            raise ValueError(f"duplicate or invalid task ID at index {index}")
        seen.add(task["id"])
        data = audit_csv(task, data_root)
        row = assess(task, index, data)
        rows.append(row)
        if row["scope"] == "candidate":
            # Deliberately omit meta.args, answer, ground_truth, perfect predictions,
            # feature tables, and Sonar's task-specific solution skills.
            public.append(
                {
                    "task_id": task["id"],
                    "source_index": index,
                    "question": task["question"],
                }
            )
    candidates = [row for row in rows if row["scope"] == "candidate"]
    return {
        "policy_version": POLICY_VERSION,
        "selector_sha256": sha256(Path(__file__)),
        "stage": "static_preflight_only",
        "model": "table",
        "source_revision": git_revision(sonar_root),
        "iotdb_revision": git_revision(iotdb_root),
        "evidence_hashes": {
            label: {
                name: sha256(root / name) if (root / name).is_file() else None
                for name in names
            }
            for label, root, names in [
                ("sonar", sonar_root, SONAR_EVIDENCE),
                ("iotdb", iotdb_root, EVIDENCE_PATHS),
            ]
        },
        "summary": {
            "total": len(rows),
            "candidate_count": len(candidates),
            "excluded_count": len(rows) - len(candidates),
            "experiment_ready_count": 0,
            "candidate_data": dict(
                Counter(row["data"]["status"] for row in candidates)
            ),
            "candidate_subtasks": dict(Counter(row["subtask"] for row in candidates)),
            "candidate_sql_adapter": dict(
                Counter(row["sql"]["existing_adapter"] for row in candidates)
            ),
            "candidate_fs_native": dict(
                Counter(row["fs"]["native"] for row in candidates)
            ),
            "semantic_reviews": dict(
                Counter(
                    reason for row in candidates for reason in row["semantic_reviews"]
                )
            ),
        },
        "candidate_ids": [row["task_id"] for row in candidates],
        "records": rows,
    }, public


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sonar-root", type=Path, required=True)
    parser.add_argument(
        "--data-root",
        type=Path,
        help="Directory containing ts_data/; defaults to SONAR/nlqtsbench",
    )
    parser.add_argument(
        "--iotdb-root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New output directory; never overwrite a run",
    )
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output directory already exists; choose a new run directory")
    result, public = scan(
        args.sonar_root,
        args.data_root or args.sonar_root / "nlqtsbench",
        args.iotdb_root,
    )
    args.output.mkdir(parents=True)
    for name, content in [
        ("selection.json", result),
        ("public-candidates.json", public),
    ]:
        (args.output / name).write_text(
            json.dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    with (args.output / "selection.csv").open(
        "w", newline="", encoding="utf-8"
    ) as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "task_id",
                "source_index",
                "subtask",
                "scope",
                "data",
                "sql_engine",
                "sql_adapter",
                "fs_native",
                "decision",
                "semantic_reviews",
                "adaptations",
            ]
        )
        for row in result["records"]:
            writer.writerow(
                [
                    row["task_id"],
                    row["source_index"],
                    row["subtask"],
                    row["scope"],
                    row["data"]["status"],
                    row["sql"]["engine"],
                    row["sql"]["existing_adapter"],
                    row["fs"]["native"],
                    row["decision"],
                    ";".join(row["semantic_reviews"]),
                    ";".join(row["adaptations"]),
                ]
            )
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    print(
        "Static candidates only. Import, both interfaces, and oracle equivalence remain unverified."
    )


if __name__ == "__main__":
    main()
