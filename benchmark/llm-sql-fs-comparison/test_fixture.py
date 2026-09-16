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

"""Verify fixture acceptance, public isolation, and final-answer rejection."""

import copy
import json
import unittest
from decimal import Decimal

from fixture import (
    ROLE_COLUMNS,
    TABLE_NAMES,
    answer_json_schema,
    canonical_hash,
    canonical_rows,
    compare_answer,
    expected_answer,
    expected_schema,
    fixture_hash,
    fixture_sql,
    load_tasks,
    render_task,
)


class FixtureTest(unittest.TestCase):
    def test_oracles_match_independent_full_data_reductions(self):
        rows = list(canonical_rows(1000))
        by_device = {
            device: [row for row in rows if row["device"] == f"device_{device}"]
            for device in range(4)
        }
        temperatures = expected_answer("K06", 1000, "sample_29")["devices"]
        statuses = expected_answer("K07", 1000, "sample_29")["devices"]
        for device in range(4):
            values = [row["temperature"] for row in by_device[device]]
            self.assertEqual(temperatures[device]["count"], len(values))
            self.assertEqual(temperatures[device]["min"], float(min(values)))
            self.assertEqual(temperatures[device]["max"], float(max(values)))
            self.assertEqual(temperatures[device]["sum"], float(sum(values)))
            self.assertEqual(
                temperatures[device]["mean"], float(sum(values) / len(values))
            )
            self.assertEqual(
                statuses[device]["true_count"],
                sum(row["status"] for row in by_device[device]),
            )
        humidity = [row["humidity"] for row in rows]
        self.assertEqual(
            expected_answer("K08", 1000, "sample_29"),
            {
                "count": len(humidity),
                "min": float(min(humidity)),
                "max": float(max(humidity)),
                "sum": float(sum(humidity)),
                "mean": float(sum(humidity) / len(humidity)),
            },
        )
        self.assertEqual(
            expected_answer("K01", 1000, "sample_29")["rows"][0],
            {
                "time": 731,
                "device": "device_2",
                "temperature": 29.31,
                "humidity": 43.1,
                "status": False,
            },
        )
        self.assertEqual(
            [row["time"] for row in expected_answer("K04", 1000, "sample_29")["rows"]],
            [997, 998, 999],
        )

    def test_hash_accepts_jdbc_spelling_and_rejects_corruption(self):
        for role in ROLE_COLUMNS:
            rows = list(canonical_rows(5, role))
            jdbc_rows = [
                {
                    name: str(value).lower() if name != "device" else value
                    for name, value in row.items()
                }
                for row in rows
            ]
            self.assertEqual(fixture_hash(5, role), canonical_hash(jdbc_rows, role))
            changed = copy.deepcopy(rows)
            changed[0]["status"] = False
            self.assertNotEqual(fixture_hash(5, role), canonical_hash(changed, role))
            self.assertNotEqual(fixture_hash(5, role), canonical_hash(rows[:-1], role))
            with self.assertRaises(ValueError):
                canonical_hash(list(reversed(rows)), role)
            with self.assertRaises(ValueError):
                canonical_hash([rows[0], rows[0]], role)
        corrupt = list(canonical_rows(1))
        corrupt[0]["temperature"] = Decimal("NaN")
        with self.assertRaises(ValueError):
            canonical_hash(corrupt)

    def test_sql_isolated_create_and_complete_role_projections(self):
        role_map = dict(zip(ROLE_COLUMNS, TABLE_NAMES))
        statements = list(fixture_sql("llm_benchmark_test", 3, role_map, batch_size=5))
        self.assertEqual(statements[0], "CREATE DATABASE llm_benchmark_test")
        self.assertEqual(len(statements), 1 + 3 * (1 + 3))
        self.assertFalse(
            any(
                "DROP" in statement or "IF NOT EXISTS" in statement
                for statement in statements
            )
        )
        for role, table in role_map.items():
            table_statements = [
                statement
                for statement in statements
                if f"llm_benchmark_test.{table} " in statement
            ]
            forbidden = set(ROLE_COLUMNS["target"]) - set(ROLE_COLUMNS[role])
            for column in forbidden:
                self.assertTrue(
                    all(column not in statement for statement in table_statements)
                )
            inserts = [
                statement
                for statement in table_statements
                if statement.startswith("INSERT")
            ]
            self.assertEqual(
                sum(statement.split(" VALUES ")[1].count("(") for statement in inserts),
                12,
            )
        for database in (
            "production",
            "llm_benchmark; DROP DATABASE x",
            "llm_benchmark.x",
        ):
            with self.assertRaises(ValueError):
                list(fixture_sql(database, 3, role_map))
        with self.assertRaises(ValueError):
            list(
                fixture_sql(
                    "llm_benchmark_test",
                    3,
                    {role: "sample_17" for role in ROLE_COLUMNS},
                )
            )

    def test_strict_answer_rejects_types_order_fields_and_nonfinite(self):
        expected = expected_answer("K01", 1000, "sample_17")
        self.assertTrue(compare_answer(copy.deepcopy(expected), expected, "K01")[0])
        for field, value in (
            ("time", "731"),
            ("time", 731.0),
            ("status", 0),
            ("temperature", True),
            ("humidity", float("nan")),
            ("humidity", float("inf")),
            ("humidity", 10**1000),
        ):
            changed = copy.deepcopy(expected)
            changed["rows"][0][field] = value
            self.assertFalse(
                compare_answer(changed, expected, "K01")[0], (field, value)
            )
        for changed in (
            {"rows": []},
            {"rows": expected["rows"], "extra": 1},
            {"rows": [{}]},
        ):
            self.assertFalse(compare_answer(changed, expected, "K01")[0])
        ordered = expected_answer("K02", 1000, "sample_17")
        reversed_answer = {"rows": list(reversed(ordered["rows"]))}
        self.assertFalse(compare_answer(reversed_answer, ordered, "K02")[0])
        changed = copy.deepcopy(expected)
        changed["rows"][0]["temperature"] += 0.0000005
        self.assertTrue(compare_answer(changed, expected, "K01")[0])
        changed["rows"][0]["temperature"] += 0.000002
        self.assertFalse(compare_answer(changed, expected, "K01")[0])

    def test_schema_order_only_exception_and_no_input_mutation(self):
        expected = expected_answer("D02", 1000, "sample_43")
        actual = {"table": "sample_43", "columns": list(reversed(expected["columns"]))}
        before = copy.deepcopy(actual)
        self.assertTrue(compare_answer(actual, expected, "D02")[0])
        self.assertEqual(actual, before)
        actual["columns"][0] = actual["columns"][1]
        self.assertFalse(compare_answer(actual, expected, "D02")[0])

    def test_public_renderer_cannot_leak_private_or_discovery_target(self):
        manifest = load_tasks()
        self.assertEqual(len(manifest["tasks"]), 12)
        for original in manifest["tasks"]:
            task = copy.deepcopy(original)
            task["private"] = {"oracle": "DO_NOT_SEND_PRIVATE_SENTINEL"}
            task["public"]["unexpected"] = "DO_NOT_SEND_EXTRA_SENTINEL"
            rendered = render_task(task, "llm_benchmark_public", "secret_target_name")
            self.assertNotIn("DO_NOT_SEND", rendered)
            public = json.loads(rendered)
            if task["information"] == "discovery":
                self.assertNotIn("secret_target_name", rendered)
                self.assertNotIn("columns", public)
            else:
                self.assertEqual(public["target_table"], "secret_target_name")
                self.assertEqual(public["columns"], expected_schema())
            schema = answer_json_schema(task)
            self.assertFalse(schema["additionalProperties"])
            self.assertEqual(set(schema["required"]), set(schema["properties"]))
            for n in (1000, 10000, 100000):
                expected = expected_answer(task["id"], n, "sample_43")
                self.assertEqual(set(schema["properties"]), set(expected))
                self.assertTrue(
                    compare_answer(
                        json.loads(json.dumps(expected)), expected, task["id"]
                    )[0]
                )


if __name__ == "__main__":
    unittest.main()
