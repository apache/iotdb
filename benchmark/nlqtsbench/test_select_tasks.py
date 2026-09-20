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

"""Exercise import hazards, capability gates, and benchmark identity isolation."""

import copy
import json
import tempfile
import unittest
from pathlib import Path

from select_tasks import assess, audit_csv, public_period, scan


def task():
    return {
        "id": "public-id",
        "level": 1,
        "subtask": "Global Aggregation",
        "question": "What is the minimum of channel 1 in 2020?",
        "ts_data_path": "ts_data/sample.csv",
        "channel": "1",
        "eval_metric": "rel_acc",
        "ground_truth": 1.0,
        "answer": "SECRET_ANSWER",
        "meta": {"args": {"time": "2020", "agg": "minimum"}},
    }


class SelectorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / "nlqtsbench"
        (self.data / "ts_data").mkdir(parents=True)
        self.sample = self.data / "ts_data/sample.csv"

    def write_csv(self, text):
        self.sample.write_text(text, encoding="utf-8")

    def test_missing_data_is_not_importable_or_verified(self):
        result = assess(task(), 17, audit_csv(task(), self.data))
        self.assertEqual(result["data"]["status"], "missing")
        self.assertEqual(result["decision"], "blocked_data")
        self.assertEqual(result["execution"], "not_verified")

    def test_valid_csv_scope_enables_only_static_whole_file_stats(self):
        self.write_csv("timestamp,1\n2020-01-01 00:00:00,1\n2020-01-01 00:15:00,2\n")
        data = audit_csv(task(), self.data)
        self.assertEqual(data["status"], "pass_static")
        self.assertEqual(data["sampling_step_seconds"], 900)
        result = assess(task(), 0, data)
        self.assertEqual(result["fs"]["native"], "supported_with_scalar_arithmetic")
        self.assertEqual(result["execution"], "not_verified")

    def test_outside_window_cannot_use_whole_table_stats(self):
        self.write_csv("timestamp,1\n2019-12-31 23:45:00,1\n2020-01-01 00:00:00,2\n")
        result = assess(task(), 0, audit_csv(task(), self.data))
        self.assertEqual(result["fs"]["native"], "conditional_on_whole_file_scope")

    def test_duplicate_timestamp_rejected_before_iotdb_overwrite(self):
        self.write_csv("timestamp,1\n2020-01-01 00:00:00,1\n2020-01-01 00:00:00,2\n")
        self.assertEqual(audit_csv(task(), self.data)["status"], "reject")

    def test_fractional_source_truncation_collision(self):
        self.write_csv(
            "timestamp,1\n2020-01-01 00:00:00.001,1\n2020-01-01 00:00:00.002,2\n"
        )
        data = audit_csv(task(), self.data)
        self.assertEqual(data["status"], "reject")
        self.assertIn("SOURCE_SECOND_TRUNCATION_REVIEW", data["issues"])

    def test_dirty_values_and_irregular_sampling_need_review(self):
        self.write_csv(
            "timestamp,1\n2020-01-01 00:00:00,NaN\n2020-01-01 00:01:00,inf\n2020-01-01 00:03:00,bad\n"
        )
        data = audit_csv(task(), self.data)
        self.assertEqual(data["status"], "review")
        self.assertTrue(
            {
                "NULL_VALUE_REVIEW",
                "NONFINITE_VALUE_REVIEW",
                "SOURCE_NUMERIC_COERCION_REVIEW",
                "IRREGULAR_SAMPLING_REVIEW",
            }.issubset(data["issues"])
        )

    def test_bad_schema_and_rows_are_not_accepted(self):
        for text, code in [
            ("timestamp,1,1\n", "INVALID_CSV_HEADER"),
            ("timestamp,2\n", "TASK_CHANNEL_MISSING"),
            ("timestamp,1\n", "EMPTY_CSV"),
            ("timestamp,1\n2020-01-01 00:00:00,1,2\n", "MALFORMED_ROW"),
        ]:
            with self.subTest(code=code):
                self.write_csv(text)
                data = audit_csv(task(), self.data)
                self.assertEqual(data["status"], "reject")
                self.assertIn(code, data["issues"])

    def test_path_traversal_and_symlink_escape_rejected(self):
        outside = self.root / "secret.csv"
        outside.write_text("secret", encoding="utf-8")
        t = task()
        for path in ["../secret.csv", str(outside)]:
            t["ts_data_path"] = path
            self.assertEqual(
                audit_csv(t, self.data)["issues"], ["CSV_OUTSIDE_DATA_ROOT"]
            )
        self.sample.symlink_to(outside)
        self.assertEqual(
            audit_csv(task(), self.data)["issues"], ["CSV_OUTSIDE_DATA_ROOT"]
        )

    def test_private_time_bounds_do_not_become_public_predicates(self):
        t = task()
        t["meta"]["args"][
            "time"
        ] = "(Timestamp('2020-01-01 17:15:00'), Timestamp('2020-01-02 17:15:00'))"
        self.assertIsNone(public_period(t))
        self.assertIn(
            "PRIVATE_TIME_RANGE_BOUNDARIES",
            assess(t, 0, {"status": "missing"})["semantic_reviews"],
        )

    def test_calendar_period_must_appear_in_question(self):
        t = task()
        t["meta"]["args"]["time"] = "2019"
        self.assertIsNone(public_period(t))

    def test_median_does_not_claim_approximate_percentile_equivalence(self):
        t = task()
        t["meta"]["args"]["agg"] = "median"
        result = assess(
            t, 0, {"status": "pass_static", "all_rows_in_public_period": True}
        )
        self.assertEqual(result["sql"]["engine"], "rewrite_and_probe")
        self.assertEqual(result["sql"]["existing_adapter"], "requires_extension")
        self.assertNotEqual(result["fs"]["native"], "supported_with_scalar_arithmetic")

    def test_window_capability_and_adapter_are_distinct(self):
        t = task()
        t.update(subtask="Sliding Window", eval_metric="iou")
        t["meta"]["args"].update(metric="highest variance", window="7D")
        result = assess(t, 0, {"status": "missing"})
        self.assertEqual(result["sql"]["engine"], "rewrite_and_probe")
        self.assertEqual(result["sql"]["existing_adapter"], "requires_extension")
        self.assertEqual(result["fs"]["composed_proposal"], "not_implemented")
        self.assertIn("VARIANCE_DDOF_ONE", result["semantic_reviews"])

    def test_source_indices_survive_exclusion_and_no_oracle_leak(self):
        excluded = copy.deepcopy(task())
        excluded.update(id="excluded", level=4, subtask="Insight Synthesis")
        (self.data / "tasks.json").write_text(json.dumps([excluded, task()]))
        report, public = scan(self.root, self.data, self.root)
        self.assertEqual(public[0]["source_index"], 1)
        self.assertEqual(report["candidate_ids"], ["public-id"])
        self.assertEqual(set(public[0]), {"task_id", "source_index", "question"})
        self.assertNotIn("SECRET_ANSWER", json.dumps(public))

    def test_duplicate_ids_fail_instead_of_overwriting(self):
        (self.data / "tasks.json").write_text(json.dumps([task(), task()]))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            scan(self.root, self.data, self.root)


if __name__ == "__main__":
    unittest.main()
