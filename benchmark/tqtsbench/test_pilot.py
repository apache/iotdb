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

"""Focused boundary and evidence parsing checks; no model or database required."""
import unittest
import json
from pathlib import Path
import subprocess
import tempfile
from unittest.mock import patch

from analyze_pilot import final_object
from tool_runner import context_page, execute, validate_sql


class PilotBoundaryTest(unittest.TestCase):
    def test_sql_timeout_does_not_publish_private_argv(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            credentials = root / "credentials.json"
            credentials.write_text(json.dumps({"username": "reader", "password": "private-test-value"}))
            config = {"output": str(root / "out"), "package": "iotdb_ndbc", "credentials": str(credentials), "cli": "/fake/cli"}
            failure = subprocess.TimeoutExpired(["/fake/cli", "-pw", "private-test-value"], 45)
            with patch("tool_runner.subprocess.run", side_effect=failure):
                result = execute(config, {"kind": "sql", "arguments": {"sql": "SHOW VERSION"}, "callId": "timeout-test"})
            self.assertTrue(result["isError"])
            self.assertNotIn("private-test-value", json.dumps(result))
            self.assertIn("45 second", result["content"])

    def test_rejects_writes_multiple_statements_and_other_databases(self):
        for sql in (
            "SELECT s INTO root.iotdb_ndbc.copy FROM root.iotdb_ndbc.d",
            "SELECT s FROM root.iotdb_ndbc.d; SHOW VERSION",
            "SELECT s FROM root.iotdb_ndbc.d, root.iotdb_cmapss.d",
            "SHOW TIMESERIES root.**",
            "SELECT s FROM root.iotdb_ndbc.d /* hidden syntax */",
        ):
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                validate_sql(sql, "iotdb_ndbc")

    def test_valid_window_and_literal_are_not_rejected(self):
        sql = "SELECT avg(s) FROM root.iotdb_ndbc.d WHERE label = 'CREATE; root.other' GROUP BY ([0,60000),1s);"
        self.assertEqual(validate_sql(sql, "iotdb_ndbc"), sql[:-1])

    def test_discovery_can_recover_record_and_page_without_skipping(self):
        context = {"fields": [{"name": "sensor_02", "meaning": "LPC temperature", "unit": "R"}]}
        hit = context_page(context, {"query": "temperature"})
        parent = hit["entries"][0]["path"].rsplit("/", 1)[0]
        page = context_page(context, {"pointer": parent, "limit": 2})
        last = context_page(context, {"pointer": parent, "limit": 2, "offset": page["next_offset"]})
        self.assertEqual([x["value"] for x in page["entries"] + last["entries"]], ["sensor_02", "LPC temperature", "R"])
        self.assertIsNone(last["next_offset"])

    def test_final_json_with_prose_is_preserved_without_guessing(self):
        self.assertEqual(final_object('Verified.\n```json\n{"final_sql":"SELECT s FROM root.d"}\n```')["final_sql"], "SELECT s FROM root.d")
        self.assertEqual(final_object("No supported answer."), {})
        self.assertEqual(final_object('```json\n{}\n```\n```json\n{}\n```'), {})


if __name__ == "__main__":
    unittest.main()
