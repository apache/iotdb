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

import unittest
import numpy as np
import pandas as pd

from equivalence import bounds, epoch, fingerprint, rolling_composition, solve, sql_for
from migrate import cli_rows


def task(family, **args):
    return {
        "subtask": family,
        "channel": "12",
        "question": "in 2020",
        "meta": {"args": {"time": "2020", **args}},
    }


def frame(values):
    return pd.DataFrame(
        {
            "time": np.arange(len(values)) * 86400000,
            "channel_id": "12",
            "value": np.asarray(values, dtype=float),
        }
    )


class SemanticsTest(unittest.TestCase):
    def test_public_dates_do_not_leak_private_hours(self):
        t = task(
            "Global Aggregation",
            time="(Timestamp('2020-02-28 17:15:00'), Timestamp('2020-03-01 17:15:00'))",
        )
        t["question"] = "in 2020-02-28 to 2020-03-01"
        self.assertEqual(bounds(t), (epoch("2020-02-28"), epoch("2020-03-02")))
        self.assertEqual(
            bounds(t, True),
            (epoch("2020-02-28 17:15:00"), epoch("2020-03-01 17:15:00") + 1),
        )

    def test_december_and_year_half_open(self):
        t = task("Global Aggregation", time="2020-12")
        t["question"] = "in 2020-12"
        self.assertEqual(bounds(t), (epoch("2020-12-01"), epoch("2021-01-01")))
        self.assertEqual(
            bounds(task("Global Aggregation")),
            (epoch("2020-01-01"), epoch("2021-01-01")),
        )

    def test_median_even_ignores_null(self):
        t = task("Global Aggregation", agg="median")
        for composition in [False, True]:
            self.assertEqual(solve(t, frame([1, 2, np.nan, 8, 10]), composition), 5)

    def test_complete_windows_sample_variance_and_null(self):
        values = np.array([1, 3, 2, np.nan, 8, 7, 12, 9], float)
        for metric in ["highest average", "highest variance", "largest range"]:
            actual = rolling_composition(values, 3, metric)
            rolling = pd.Series(values).rolling(3)
            expected = (
                rolling.mean()
                if "average" in metric
                else (
                    rolling.var(ddof=1)
                    if "variance" in metric
                    else rolling.max() - rolling.min()
                )
            )
            np.testing.assert_allclose(
                actual, expected, equal_nan=True, rtol=1e-12, atol=1e-12
            )

    def test_interval_null_break_and_earliest_tie(self):
        t = task("Interval Discovery", threshold="2")
        self.assertEqual(
            solve(t, frame([3, 4, np.nan, 7, 8, 1])),
            ["1970-01-01 00:00:00", "1970-01-02 00:00:00"],
        )

    def test_threshold_is_point_predicate_contract(self):
        t = task(
            "Temporal Localization", action="first rise above 2", threshold_high="2"
        )
        self.assertEqual(solve(t, frame([3, 1, 4])), "1970-01-01 00:00:00")

    def test_fingerprint_detects_float_and_time_changes(self):
        original = frame([1, 2, 3])
        changed = original.copy()
        changed.loc[1, "value"] = np.nextafter(2.0, 3.0)
        self.assertNotEqual(
            fingerprint(original)["sha256"], fingerprint(changed)["sha256"]
        )
        changed = original.copy()
        changed.loc[1, "time"] += 1
        self.assertNotEqual(
            fingerprint(original)["sha256"], fingerprint(changed)["sha256"]
        )

    def test_sql_single_statement_and_no_gold(self):
        t = task("Sliding Window", window="3D", metric="highest variance")
        sql = sql_for(
            t, "nlqts_v1_00001", (0, 864000000), frame([1, 2, 3, 4]).time.to_numpy()
        )
        self.assertIn("var_samp(value)", sql)
        self.assertIn("WHERE n=3", sql)
        self.assertNotIn(";", sql)

    def test_cli_table_errors_are_not_empty_answers(self):
        self.assertEqual(
            cli_rows("+------+\n|answer|\n+------+\n| 5.0  |\n+------+"),
            [{"answer": "5.0"}],
        )
        with self.assertRaises(ValueError):
            cli_rows("Msg: failed")

    def test_named_channels_are_tag_values(self):
        t = task("Global Aggregation", agg="average")
        for channel in ["147", "MUFL", "m_06"]:
            t["channel"] = channel
            sql = sql_for(t, "nlqts_v1_00001", (0, 1000), np.array([0, 500]))
            self.assertIn(f"channel_id='{channel}'", sql)
        t["channel"] = "x' OR 1=1"
        with self.assertRaises(ValueError):
            sql_for(t, "nlqts_v1_00001", (0, 1000), np.array([0, 500]))


if __name__ == "__main__":
    unittest.main()
