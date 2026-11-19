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

"""Tests for the controlled NLQTSBench DSH pilot runner."""

from decimal import Decimal
import unittest

from run_dsh_benchmark import interval_iou, patches_for_arm, render_prompt, score


class DshBenchmarkRunnerTest(unittest.TestCase):
    def setUp(self):
        self.task = {
            "id": "L1_T1_Global_Aggregation_00007",
            "subtask": "Global Aggregation",
            "question": "What is the median value of channel 147 in 2020?",
            "meta": {"args": {"answer_sentinel": "DO_NOT_EXPOSE"}},
        }
        self.record = {
            "database": "nlqts_v1_00007",
            "oracle_public": 0.15,
        }

    def test_prompts_contain_public_schema_without_oracle_or_hidden_args(self):
        sql = render_prompt(self.task, self.record, "sql")
        filesystem = render_prompt(self.task, self.record, "filesystem")
        self.assertIn(self.task["question"], sql)
        self.assertIn("exactly one iotdb_sql", sql)
        self.assertIn("/nlqts_v1_00007/raw_data.csv", filesystem)
        self.assertIn("startMs", filesystem)
        self.assertIn("do not supply a path", filesystem)
        self.assertNotIn("--start", filesystem)
        raw = render_prompt(self.task, self.record, "filesystem", "raw")
        self.assertIn("typed-interface/raw-output ablation", raw)
        self.assertNotIn("offset=next_offset", raw)
        compact = render_prompt(self.task, self.record, "filesystem", "compact")
        self.assertIn("compact structured pages", compact)
        self.assertIn("JSON numbers", compact)
        self.assertIn("offset=next_offset", compact)
        filtered_stats = render_prompt(
            self.task, self.record, "filesystem", "filtered-stats"
        )
        self.assertIn("Use stats with measurement=value", filtered_stats)
        self.assertIn("Do not read or paginate raw rows", filtered_stats)
        self.assertNotIn("0.15", sql + filesystem)
        self.assertNotIn("DO_NOT_EXPOSE", sql + filesystem)

    def test_score_requires_exact_three_decimal_standalone_answer(self):
        correct = score("0.150", 0.15)
        rounded = score("0.425", 0.42499999999999993)
        wrong = score("0.151", 0.15)
        invalid = score("The answer is 0.150", 0.15)
        self.assertTrue(correct["correct"])
        self.assertTrue(rounded["correct"])
        self.assertEqual(Decimal(correct["parsed_answer"]), Decimal("0.150"))
        self.assertEqual(wrong["answer_status"], "wrong_answer")
        self.assertEqual(invalid["answer_status"], "invalid_format")

    def test_score_interval_uses_strict_format_and_official_iou(self):
        expected = ["2020-01-01 00:00:00", "2020-01-01 02:00:00"]
        correct = score("[2020-01-01 00:00:00, 2020-01-01 02:00:00]", expected)
        partial = score("[2020-01-01 01:00:00, 2020-01-01 03:00:00]", expected)
        invalid = score('["2020-01-01 00:00:00", "2020-01-01 02:00:00"]', expected)
        self.assertTrue(correct["correct"])
        self.assertEqual(correct["official_score"], 1.0)
        self.assertEqual(partial["answer_status"], "wrong_answer")
        self.assertAlmostEqual(partial["official_score"], 1 / 3)
        self.assertEqual(invalid["answer_status"], "invalid_format")

    def test_interval_iou_matches_zero_duration_rule(self):
        interval = ["2020-01-01 00:00:00", "2020-01-01 00:00:00"]
        self.assertEqual(interval_iou(interval, interval), 1.0)

    def test_sql_dialect_patch_is_last_and_sql_only(self):
        self.assertEqual(
            patches_for_arm(["agent.yml"], "baseline.yml", "sql", "dialect.yml"),
            ["agent.yml", "baseline.yml", "dialect.yml"],
        )
        self.assertEqual(
            patches_for_arm(["agent.yml"], "baseline.yml", "filesystem", "dialect.yml"),
            ["agent.yml", "baseline.yml"],
        )

    def test_interval_prompts_keep_schema_but_not_oracle(self):
        task = {
            "id": "L1_T3_Interval_Discovery_00340",
            "subtask": "Interval Discovery",
            "question": "Find the longest period where channel 71 remained above 0.01.",
        }
        record = {
            "database": "nlqts_v1_00340",
            "oracle_public": ["2020-09-02 07:45:00", "2020-09-10 17:00:00"],
        }
        sql = render_prompt(task, record, "sql", "filtered-stats")
        filesystem = render_prompt(task, record, "filesystem", "filtered-stats")
        self.assertIn("aggregate window functions", sql)
        self.assertIn("direct TIMESTAMP subtraction is invalid", sql)
        self.assertIn("exactly one UTC interval", sql)
        self.assertIn("stats command cannot compute contiguous intervals", filesystem)
        self.assertIn("offset=next_offset", filesystem)
        self.assertNotIn("2020-09-02", sql + filesystem)


if __name__ == "__main__":
    unittest.main()
