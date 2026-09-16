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

import unittest

from application_fixture import (
    SCENARIOS,
    _contiguous_groups,
    expected_application_answer,
    fixture_hash,
    fixture_sql,
    scenario_rows,
)


class ApplicationFixtureTest(unittest.TestCase):
    def test_scenarios_are_deterministic_and_nonempty(self):
        for scenario in SCENARIOS:
            rows = scenario_rows(scenario)
            self.assertTrue(rows)
            self.assertEqual(fixture_hash(scenario), fixture_hash(scenario))
            self.assertEqual(
                fixture_sql("llm_app_benchmark_test", scenario)[0],
                "CREATE DATABASE llm_app_benchmark_test",
            )

    def test_a1_excludes_stopped_pump_and_counts_quality_events(self):
        answer = expected_application_answer("A1-TRIAGE")
        self.assertEqual(
            [item["pump_id"] for item in answer["priority_pumps"]],
            ["pump_02", "pump_02"],
        )
        self.assertEqual(
            [(item["start"], item["end"]) for item in answer["priority_pumps"]],
            [
                (1_700_001_200_000, 1_700_001_200_000),
                (1_700_000_600_000, 1_700_000_600_000),
            ],
        )
        self.assertEqual(answer["data_quality"], {"missing": 1, "duplicates": 1})

    def test_intervals_split_after_a_gap_but_keep_exact_boundary(self):
        rows = [{"time": time} for time in (0, 300_000, 900_001, 1_200_001)]
        groups = _contiguous_groups(rows, 300_000)
        self.assertEqual(
            [[row["time"] for row in group] for group in groups],
            [[0, 300_000], [900_001, 1_200_001]],
        )

    def test_a2_deduplicates_and_reports_three_review_boxes(self):
        answer = expected_application_answer("A2-COMPLIANCE")
        self.assertEqual(answer["compliance"], "NON_COMPLIANT")
        self.assertEqual(
            [item["box_id"] for item in answer["review_list"]],
            ["box-1", "box-2", "box-3"],
        )
        self.assertEqual(len(answer["excursions"]), 3)
        self.assertEqual(len(answer["offline_periods"]), 1)

    def test_a3_marks_meter_rollback(self):
        answer = expected_application_answer("A3-ENERGY")
        self.assertEqual(answer["floor_ranking"][0]["floor_id"], "floor_04")
        self.assertEqual(
            [item["floor_id"] for item in answer["floor_ranking"]],
            ["floor_04", "floor_01", "floor_02"],
        )
        self.assertEqual(answer["floor_ranking"][0]["actual_power_kw"], 92.0)
        self.assertEqual(answer["floor_ranking"][0]["baseline_power_kw"], 59.0)
        self.assertEqual(answer["floor_ranking"][0]["reason"], "peak_power")
        self.assertEqual(
            answer["reconstructed_intervals"],
            [{"floor_id": "floor_03", "time": 1_700_246_800_000, "energy_kwh": 59.0}],
        )

    def test_a3_materializes_same_floor_hour_baselines(self):
        rows = scenario_rows("A3")
        self.assertIn("floor_baselines", rows)
        self.assertEqual(len(rows["floor_baselines"]), 4 * 24)
        baseline = next(
            row
            for row in rows["floor_baselines"]
            if row["floor_id"] == "floor_04"
            and row["time"] == 1_700_200_000_000 + 14 * 3_600_000
        )
        self.assertEqual(baseline["baseline_power_kw"], 59.0)
        sql = fixture_sql("llm_app_benchmark_test", "A3")
        self.assertTrue(
            any(
                "CREATE TABLE llm_app_benchmark_test.floor_baselines" in statement
                for statement in sql
            )
        )


if __name__ == "__main__":
    unittest.main()
