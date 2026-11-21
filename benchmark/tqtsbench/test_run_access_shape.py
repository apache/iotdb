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

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import fs_tool_runner
import tool_runner
from run_access_shape import ARMS, prompt


class Proc:
    def __init__(self, stdout):
        self.stdout, self.stderr, self.returncode = stdout, "", 0


class AccessShapeArmsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        (root / "creds.json").write_text(json.dumps({"username": "u", "password": "p"}))
        self.config = {"package": "iotdb_ndbc", "credentials": str(root / "creds.json"), "cli": "cli",
                       "output": str(root / "out"), "sql_seconds": 180, "sql_output_chars": 40000}

    def tearDown(self):
        self.tmp.cleanup()

    def call(self, runner, config, kind, arguments, call_id="c1"):
        return json.loads(runner.execute(config, {"kind": kind, "arguments": arguments, "callId": call_id})["content"])

    def test_single_statement_uses_configured_limits(self):
        with mock.patch.object(tool_runner.subprocess, "run", return_value=Proc("x" * 30000)) as run:
            payload = self.call(tool_runner, self.config, "sql", {"sql": "SELECT wspd FROM root.iotdb_ndbc.station_42001"})
        self.assertEqual(run.call_args.kwargs["timeout"], 180)
        self.assertFalse(payload["truncated"])
        with mock.patch.object(tool_runner.subprocess, "run", return_value=Proc("x" * 30000)):
            payload = self.call(tool_runner, {k: v for k, v in self.config.items() if k != "sql_output_chars"}, "sql",
                                {"sql": "SELECT wspd FROM root.iotdb_ndbc.station_42001"}, "c2")
        self.assertTrue(payload["truncated"])

    def test_multi_statement_validates_all_and_shares_output(self):
        config = dict(self.config, multi_statement=True)
        good = ["SELECT wspd FROM root.iotdb_ndbc.station_42001", "SHOW DEVICES root.iotdb_ndbc.**"]
        with mock.patch.object(tool_runner.subprocess, "run", return_value=Proc("y" * 25000)) as run:
            payload = self.call(tool_runner, config, "sql", {"statements": good})
        self.assertEqual(run.call_count, 2)
        self.assertEqual([r["statement_index"] for r in payload["results"]], [0, 1])
        self.assertTrue(all(r["truncated"] for r in payload["results"]))
        for i, bad in enumerate([{"statements": []}, {"statements": good * 5}, {"sql": good[0]},
                                 {"statements": [good[0], "DELETE FROM root.iotdb_ndbc.station_42001"]},
                                 {"statements": [good[0], "SELECT x FROM root.iotdb_cmapss.a"]}]):
            with mock.patch.object(tool_runner.subprocess, "run") as run:
                payload = self.call(tool_runner, config, "sql", bad, f"bad{i}")
            self.assertTrue(payload["is_error"])
            run.assert_not_called()

    def test_hybrid_routes_sql_and_fs_runner_keeps_sql_out_otherwise(self):
        config = dict(self.config, hybrid=True, features=[])
        with mock.patch.object(tool_runner.subprocess, "run", return_value=Proc("ok")) as run:
            payload = self.call(fs_tool_runner, config, "sql", {"sql": "SHOW DEVICES root.iotdb_ndbc.**"})
        self.assertFalse(payload["is_error"]); run.assert_called_once()
        with mock.patch.object(tool_runner.subprocess, "run") as run:
            payload = self.call(fs_tool_runner, dict(self.config, features=[]), "sql", {"sql": "SHOW DEVICES root.iotdb_ndbc.**"}, "c2")
        self.assertTrue(payload["is_error"]); run.assert_not_called()

    def test_help_and_prompts_differ_only_in_tools(self):
        hybrid = fs_tool_runner.help_text("/root/iotdb_ndbc", [], True)
        self.assertIn("iotdb_sql", hybrid["scope_note"])
        self.assertNotIn("iotdb_sql", fs_tool_runner.help_text("/root/iotdb_ndbc", [])["scope_note"])
        self.assertIn("directory path covers every device", hybrid["commands"]["stats"])
        task = {"question": "Q?", "package": "iotdb_ndbc"}
        texts = {arm: prompt(task, arm) for arm in ARMS}
        self.assertNotIn("iotdb_fs", texts["S"]); self.assertNotIn("iotdb_fs", texts["S+"])
        self.assertIn("up to 8 independent statements", texts["S+"])
        self.assertIn("iotdb_fs", texts["H"]); self.assertIn("/root/iotdb_ndbc", texts["H"])
        for text in texts.values():
            self.assertIn("Question: Q?", text); self.assertIn("final_table", text)
            self.assertIn("120 tool calls and 900 seconds", text)


if __name__ == "__main__":
    unittest.main()
