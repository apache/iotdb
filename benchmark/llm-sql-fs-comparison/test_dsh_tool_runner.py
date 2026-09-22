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

"""Security-boundary tests for the controlled DSH IoTDB tool runner."""

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from dsh_client import controlled_baseline
from dsh_tool_runner import (
    MAX_PAGE_RENDER_BYTES,
    _page_bytes,
    _structured_page,
    execute_request,
)


class ControlledToolRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="iotdb_dsh_tool_test_")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cli = self.root / "fake-cli"
        self.cli.write_text(
            f"#!{sys.executable}\n"
            "import json,os,sys\n"
            "print(json.dumps({'args':sys.argv[1:],'environment':sorted(os.environ)}))\n",
            encoding="utf-8",
        )
        self.cli.chmod(0o755)

    def request(self, arm, command, call_id="call-1"):
        request = {
            "arm": arm,
            "database": "task_database",
            "executable": str(self.cli),
            "outputRoot": str(self.root / "artifacts"),
            "callId": call_id,
            "timeoutSeconds": 2,
        }
        if arm == "sql":
            request["command"] = command
        else:
            request.update(
                filesystemPath="/task_database/telemetry.csv",
                structuredPages=True,
                compactPages=False,
                filteredStats=False,
                parameters=command,
            )
        return request

    def test_sql_command_is_one_argument_and_model_secrets_are_not_inherited(self):
        command = "SELECT '$(not_a_shell)' FROM task_database.telemetry"
        with patch.dict(
            os.environ,
            {
                "EXP_LLM_API_KEY": "model-secret",
                "BENCHMARK_MODEL_API_KEY": "other-secret",
                "IOTDB_PASSWORD": "database-secret",
            },
        ):
            result = execute_request(self.request("sql", command))
        observed = json.loads(result["stdout"])
        self.assertEqual(observed["args"], ["-disableISO8601", "-e", command])
        self.assertNotIn("EXP_LLM_API_KEY", observed["environment"])
        self.assertNotIn("BENCHMARK_MODEL_API_KEY", observed["environment"])
        self.assertIn("IOTDB_PASSWORD", observed["environment"])
        self.assertTrue(result["ok"])
        artifact = Path(result["artifactDirectory"])
        self.assertEqual(artifact.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(artifact.stat().st_mode & 0o777, 0o700)
        self.assertEqual((artifact / "stdout.txt").stat().st_mode & 0o777, 0o600)

    def test_sql_rejects_writes_and_other_databases_before_process_launch(self):
        commands = [
            "DROP DATABASE task_database",
            "SELECT * FROM other_database.telemetry",
            "SELECT * FROM task_database.telemetry; SELECT 1",
        ]
        for index, command in enumerate(commands):
            with self.subTest(command=command):
                with self.assertRaises(ValueError):
                    execute_request(self.request("sql", command, f"denied-{index}"))
        self.assertFalse((self.root / "artifacts").exists())

    def test_filesystem_builds_typed_scoped_read_without_a_model_path(self):
        arguments_path = self.root / "fs-arguments.json"
        self.cli.write_text(
            f"#!{sys.executable}\n"
            "import json,pathlib,sys\n"
            f"pathlib.Path({str(arguments_path)!r}).write_text(json.dumps(sys.argv[1:]))\n"
            "print('time,device,temperature')\n"
            "print('1000,device_2,20.0')\n"
            "print('1100,device_2,21.0')\n"
            "print('1200,device_2,22.0')\n",
            encoding="utf-8",
        )
        allowed = {
            "command": "cat",
            "startMs": 1000,
            "endMs": 1999,
            "measurement": "temperature",
            "limit": 2,
            "offset": 200,
            "tagName": "device",
            "tagOperator": "eq",
            "tagValue": "device_2",
        }
        result = execute_request(self.request("filesystem", allowed, "fs-allowed"))
        self.assertTrue(result["ok"])
        observed = json.loads(arguments_path.read_text(encoding="utf-8"))
        self.assertEqual(
            observed,
            [
                "-disableISO8601",
                "-e",
                "cat --format csv --measurements temperature --start 1000 --end "
                "1999 --limit 3 --offset 200 --tag-filter device eq device_2 "
                "/task_database/telemetry.csv",
            ],
        )
        self.assertEqual(result["resultKind"], "page")
        self.assertEqual(result["stdout"], "")
        self.assertEqual(
            result["page"],
            {
                "columns": ["time", "device", "temperature"],
                "rows": [
                    ["1000", "device_2", "20.0"],
                    ["1100", "device_2", "21.0"],
                ],
                "offset": 200,
                "limit": 2,
                "returned_rows": 2,
                "has_more": True,
                "next_offset": 202,
                "null_value": "\\N",
            },
        )
        artifact = Path(result["artifactDirectory"])
        self.assertIn(
            "1200,device_2,22.0",
            (artifact / "stdout.txt").read_text(encoding="utf-8"),
        )
        for index, parameters in enumerate(
            (
                {"command": "cat", "path": "/other_database/telemetry.csv"},
                {"command": "cat", "startMs": "1000"},
                {"command": "stats", "startMs": 1000},
            )
        ):
            with self.subTest(parameters=parameters):
                with self.assertRaises(ValueError):
                    execute_request(
                        self.request("filesystem", parameters, f"fs-denied-{index}")
                    )

    def test_compact_page_uses_native_numbers_but_preserves_tag_strings(self):
        self.cli.write_text(
            f"#!{sys.executable}\n"
            "print('time,device,temperature')\n"
            "print('1000,007,20.500')\n"
            "print(r'1100,007,\\N')\n",
            encoding="utf-8",
        )
        parameters = {
            "command": "cat",
            "measurement": "temperature",
            "limit": 2,
            "tagName": "device",
            "tagOperator": "eq",
            "tagValue": "007",
        }
        compact_request = self.request("filesystem", parameters, "compact-page")
        compact_request["compactPages"] = True
        compact = execute_request(compact_request)
        artifact = Path(compact["artifactDirectory"])
        standard_page = _structured_page(parameters, artifact)
        self.assertEqual(
            compact["page"]["rows"],
            [[1000, "007", 20.5], [1100, "007", None]],
        )
        self.assertEqual(
            standard_page["rows"],
            [["1000", "007", "20.500"], ["1100", "007", "\\N"]],
        )
        compact_metadata = json.loads(
            (artifact / "metadata.json").read_text()
        )
        self.assertLess(
            compact_metadata["modelOutputBytes"],
            _page_bytes(standard_page),
        )
        self.assertTrue(compact_metadata["compactPages"])

    def test_filesystem_help_is_complete_and_does_not_start_cli(self):
        self.cli.write_text(
            f"#!{sys.executable}\nraise SystemExit('CLI must not start for help')\n",
            encoding="utf-8",
        )
        result = execute_request(
            self.request("filesystem", {"command": "help"}, "fs-help")
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["resultKind"], "text")
        self.assertIsNone(result["page"])
        self.assertEqual(result["cliProcessMs"], 0.0)
        self.assertIn(
            "Fixed object path: /task_database/telemetry.csv", result["stdout"]
        )
        self.assertIn("startMs (optional integer)", result["stdout"])
        self.assertIn("tagOperator (optional enum)", result["stdout"])
        artifact = Path(result["artifactDirectory"])
        self.assertEqual(
            (artifact / "stdout.txt").read_text(encoding="utf-8"), result["stdout"]
        )
        metadata = json.loads((artifact / "metadata.json").read_text(encoding="utf-8"))
        self.assertFalse(metadata["cliStarted"])

    def test_typed_raw_ablation_keeps_csv_without_page_metadata(self):
        self.cli.write_text(
            f"#!{sys.executable}\nprint('time,device,value')\nprint('1000,device_2,20.0')\n",
            encoding="utf-8",
        )
        request = self.request("filesystem", {"command": "cat", "limit": 2}, "fs-raw")
        request["structuredPages"] = False
        result = execute_request(request)
        self.assertTrue(result["ok"])
        self.assertEqual(result["resultKind"], "text")
        self.assertIsNone(result["page"])
        self.assertEqual(result["stdout"], "time,device,value\n1000,device_2,20.0\n")

    def test_structured_page_shrinks_at_the_model_byte_budget(self):
        output = self.root / "wide-page"
        output.mkdir()
        rows = ["time,device,value"] + [
            f"{index},device_2,{'x' * 200}" for index in range(500)
        ]
        (output / "stdout.txt").write_text("\n".join(rows) + "\n", encoding="utf-8")
        page = _structured_page(
            {"command": "cat", "limit": 500, "offset": 1000}, output
        )
        self.assertLessEqual(_page_bytes(page), MAX_PAGE_RENDER_BYTES)
        self.assertLess(page["returned_rows"], 500)
        self.assertTrue(page["has_more"])
        self.assertEqual(page["next_offset"], 1000 + page["returned_rows"])

    def test_controlled_baseline_selects_only_the_requested_interface(self):
        patch_path, environment = controlled_baseline(
            "sql",
            "task_database",
            self.root / "trial",
            "/experiment/bin/dsh-exp",
        )
        self.assertEqual(patch_path.name, "dsh-baseline.patch.yml")
        self.assertEqual(environment["EXP_DSH_TOOL_MODE"], "sql")
        self.assertEqual(
            environment["EXP_DSH_TOOLS_MODULE"],
            "file:///experiment/runtime/agent/node_modules/%40deepseek-ai/"
            "dsh-tools/lib/index.js",
        )
        self.assertEqual(environment["EXP_TASK_DATABASE"], "task_database")
        self.assertEqual(
            environment["EXP_IOTDB_TOOL_EXECUTABLE"], "/experiment/bin/iotdb-sql"
        )
        _, fs_environment = controlled_baseline(
            "filesystem",
            "task_database",
            self.root / "fs-trial",
            "/experiment/bin/dsh-exp",
            filesystem_path="/task_database/telemetry.csv",
        )
        self.assertEqual(
            fs_environment["EXP_TASK_FS_PATH"], "/task_database/telemetry.csv"
        )
        self.assertEqual(fs_environment["EXP_FS_STRUCTURED_PAGES"], "true")
        self.assertEqual(fs_environment["EXP_FS_COMPACT_PAGES"], "false")
        self.assertEqual(fs_environment["EXP_FS_FILTERED_STATS"], "false")
        _, raw_environment = controlled_baseline(
            "filesystem",
            "task_database",
            self.root / "raw-fs-trial",
            "/experiment/bin/dsh-exp",
            filesystem_path="/task_database/telemetry.csv",
            structured_pages=False,
        )
        self.assertEqual(raw_environment["EXP_FS_STRUCTURED_PAGES"], "false")
        self.assertEqual(raw_environment["EXP_FS_COMPACT_PAGES"], "false")
        _, compact_environment = controlled_baseline(
            "filesystem",
            "task_database",
            self.root / "compact-fs-trial",
            "/experiment/bin/dsh-exp",
            filesystem_path="/task_database/telemetry.csv",
            compact_pages=True,
        )
        self.assertEqual(compact_environment["EXP_FS_STRUCTURED_PAGES"], "true")
        self.assertEqual(compact_environment["EXP_FS_COMPACT_PAGES"], "true")
        _, stats_environment = controlled_baseline(
            "filesystem",
            "task_database",
            self.root / "filtered-stats-trial",
            "/experiment/bin/dsh-exp",
            filesystem_path="/task_database/telemetry.csv",
            compact_pages=True,
            filtered_stats=True,
        )
        self.assertEqual(stats_environment["EXP_FS_FILTERED_STATS"], "true")
        with self.assertRaises(ValueError):
            controlled_baseline(
                "filesystem",
                "task_database; DROP DATABASE x",
                self.root / "trial-2",
                "/experiment/bin/dsh-exp",
            )
        with self.assertRaises(ValueError):
            controlled_baseline(
                "filesystem",
                "task_database",
                self.root / "trial-3",
                "/experiment/bin/dsh-exp",
                filesystem_path="/other_database/telemetry.csv",
            )


if __name__ == "__main__":
    unittest.main()
