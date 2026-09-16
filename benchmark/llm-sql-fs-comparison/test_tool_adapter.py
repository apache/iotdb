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

"""Exercise real subprocess boundaries and benchmark command restrictions."""

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from tool_adapter import cli_args, run_cli, table_rows, validate_command


DATABASE = "llm_benchmark_test"
TABLE = DATABASE + ".sample_17"
PATH = "/" + DATABASE + "/sample_17.csv"


class CommandValidationTest(unittest.TestCase):
    def test_sql_supports_all_task_families(self):
        commands = [
            f"SELECT * FROM {TABLE} WHERE device='device_2' AND time=731;",
            f"SELECT time,device,temperature,humidity FROM {TABLE} WHERE device='device_1' AND time>=501 AND time<=507 ORDER BY time",
            f"SELECT time,device,temperature FROM {TABLE} WHERE time=777 AND device IN ('device_0','device_3') ORDER BY device",
            f"SELECT * FROM {TABLE} WHERE device='device_2' ORDER BY time DESC LIMIT 3",
            f"SELECT COUNT(*),COUNT(DISTINCT device),MIN(time),MAX(time),COUNT(temperature),COUNT(*)-COUNT(temperature) FROM {TABLE}",
            f"SELECT device,COUNT(temperature),MIN(temperature),MAX(temperature),SUM(temperature),AVG(temperature) FROM {TABLE} GROUP BY device ORDER BY device",
            f"SELECT device,SUM(CASE WHEN status=true THEN 1 ELSE 0 END),SUM(CASE WHEN status=false THEN 1 ELSE 0 END),SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) FROM {TABLE} GROUP BY device ORDER BY device",
            f"SELECT COUNT(humidity),MIN(humidity),MAX(humidity),SUM(humidity),AVG(humidity) FROM {TABLE}",
            f"SHOW TABLES FROM {DATABASE}",
            f"SHOW TABLES DETAILS FROM {DATABASE}",
            f"DESC {TABLE}",
            f"DESCRIBE {TABLE} DETAILS",
            f'SELECT COUNT(*) FROM "{DATABASE}"."sample_17"',
            f"SELECT * FROM (SELECT * FROM {TABLE} WHERE time=731) AS records",
        ]
        for command in commands:
            with self.subTest(command=command):
                validate_command(command, "sql", DATABASE)

    def test_sql_rejects_external_objects_writes_and_functions(self):
        commands = [
            "SELECT * FROM other_database.sample_17",
            f"SELECT * FROM {TABLE} JOIN other_database.sample_17 ON 1=1",
            f"SELECT * FROM {TABLE} WHERE device IN (SELECT device FROM other_database.sample_17)",
            f"SELECT * FROM {TABLE}, other_database.sample_17",
            f"SELECT * FROM {TABLE}; DROP DATABASE {DATABASE}",
            f"INSERT INTO {TABLE} (time,device) VALUES (0,'x')",
            f"SELECT external_udf(temperature) FROM {TABLE}",
            f'SELECT "external_udf"(temperature) FROM {TABLE}',
            f"SELECT other_database.AVG(temperature) FROM {TABLE}",
            f"SELECT * INTO other_database.sample_17 FROM {TABLE}",
        ]
        for command in commands:
            with self.subTest(command=command):
                with self.assertRaises(ValueError):
                    validate_command(command, "sql", DATABASE)

    def test_filesystem_supports_documented_reads_and_option_aliases(self):
        commands = [
            f"cat -f csv --start 731 --end 731 --tag-filter device eq device_2 {PATH}",
            f"cat --format=ndjson --start=501 --end=507 --tag-filter device eq device_1 -m temperature -m humidity {PATH}",
            f"head -fcsv -n3 --tag-filter device regexp 'device_(0|3)' {PATH}",
            f"head -3 {PATH}",
            f"tail -n 3 --format csv --tag-filter device eq 'device_2' {PATH}",
            f"tail --format=csv --offset=1 --limit=3 {PATH}",
            f"cat {PATH} --tag-filter device eq device_0 --tag-filter device eq device_3 --tag-match any",
            f"cat {PATH} --tag-filter device is-null",
            f"cat {PATH} --tag-filter device not-null",
            f"count -f csv {PATH}",
            f"stats -m temperature -f csv {PATH}",
            f"schema -f csv {PATH}",
            f"meta -f csv {PATH}",
            f"ls -laR -f csv /{DATABASE}",
            f"ls -f csv /{DATABASE}/",
            f"find /{DATABASE} -name '*.csv' -type f -maxdepth 1",
            f"tree -L 1 /{DATABASE}",
            f"stat {PATH}",
            f"file {PATH}",
            "help stats",
        ]
        for command in commands:
            with self.subTest(command=command):
                validate_command(command, "filesystem", DATABASE)

    def test_filesystem_rejects_relative_extra_paths_and_hidden_scope(self):
        commands = [
            f"cat {PATH} ../other_database/sample_17.csv",
            f"cat {PATH} other_database/sample_17.csv",
            f"cat -- {PATH} ../other_database/sample_17.csv",
            f"cat -f {PATH} ../other_database/sample_17.csv",
            "cat /other_database/sample_17.csv",
            f"cat /{DATABASE}/../other_database/sample_17.csv",
            f"cat /{DATABASE}/..\\\n/other_database/sample_17.csv",
            f"cat {PATH} -t other_database.sample_17",
            f"cat {PATH} --table other_database.sample_17",
            f"cat {PATH} --device root.production.secret",
            f"tail -f {PATH}",
            f"tail --follow {PATH}",
            f"tail --follow=false {PATH}",
            f"cat {PATH} | sql SELECT * FROM other_database.sample_17",
            f"cat {PATH} > /tmp/export.csv",
            f"cat {PATH} < /tmp/input.csv",
            "help sql",
            "help",
            f"sql SELECT * FROM {TABLE}",
        ]
        for command in commands:
            with self.subTest(command=command):
                with self.assertRaises(ValueError):
                    validate_command(command, "filesystem", DATABASE)


class ToolProcessTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="iotdb_tool_adapter_test_")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.cli = self.directory / "fake_cli"
        self.config = {
            "cli_bin": str(self.cli),
            "host": "127.0.0.1",
            "port": 6667,
            "username": "test_reader",
            "password": "test_fixture_password",
        }

    def write_cli(self, body):
        self.cli.write_text(f"#!{sys.executable}\n" + body, encoding="utf-8")
        self.cli.chmod(0o755)

    def run_fake(self, arm="sql", command="SELECT 1", timeout=2):
        return run_cli(self.config, arm, command, self.directory / "output", timeout)

    def test_model_text_remains_one_argument_and_credentials_are_not_in_environment(
        self,
    ):
        self.write_cli(
            "import json,os,sys\nprint(json.dumps({'args':sys.argv[1:],'keys':list(os.environ)}))\n"
        )
        command = "SELECT '$(never_execute_this)'"
        with patch.dict(os.environ, {"BENCHMARK_MODEL_API_KEY": "private_sentinel"}):
            result, timing = self.run_fake("filesystem", command)
        received = json.loads(result["stdout"])
        self.assertEqual(
            received["args"], cli_args(self.config, "filesystem", command)[1:]
        )
        self.assertEqual(received["args"][-1], command)
        self.assertNotIn("BENCHMARK_MODEL_API_KEY", received["keys"])
        self.assertIsNone(result["error_kind"])
        self.assertGreaterEqual(timing["cli_process_ms"], 0)

    def test_truncation_retains_raw_output_and_utf8_boundaries(self):
        self.write_cli(
            "import sys\nsys.stdout.buffer.write(b'x'*65535+'你'.encode())\nsys.stderr.buffer.write(b'e'*9000)\n"
        )
        result, _ = self.run_fake()
        self.assertEqual(result["stdout_bytes"], 65538)
        self.assertEqual(result["stderr_bytes"], 9000)
        self.assertTrue(result["truncated"])
        self.assertLessEqual(len(result["stdout"].encode("utf-8")), 65536)
        self.assertEqual(len(result["stderr"].encode("utf-8")), 8192)
        self.assertEqual((self.directory / "output/stdout.txt").stat().st_size, 65538)
        self.assertEqual((self.directory / "output/stderr.txt").stat().st_size, 9000)

    def test_statement_success_is_not_misclassified_as_error(self):
        self.write_cli("print('Msg: The statement is executed successfully.')\n")
        result, _ = self.run_fake()
        self.assertIsNone(result["error_kind"])
        self.assertEqual(result["exit_code"], 0)

    def test_cli_error_is_detected_even_if_exit_code_is_zero(self):
        self.write_cli("print('Msg: java.sql.SQLException: invalid query')\n")
        result, _ = self.run_fake()
        self.assertEqual(result["error_kind"], "cli_error")

    def test_timeout_kills_the_cli_process_group(self):
        child_pid = self.directory / "child.pid"
        self.write_cli(
            "import pathlib,subprocess,sys,time\n"
            "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])\n"
            f"pathlib.Path({str(child_pid)!r}).write_text(str(child.pid))\n"
            "time.sleep(30)\n"
        )
        result, _ = self.run_fake(timeout=0.3)
        self.assertTrue(result["timed_out"])
        self.assertEqual(result["error_kind"], "tool_timeout")
        self.assertLess(result["exit_code"], 0)
        if child_pid.exists():
            pid = int(child_pid.read_text())
            process_status = Path(f"/proc/{pid}/status")
            if process_status.exists():
                self.assertRegex(process_status.read_text(), r"State:\s+Z")

    def test_table_parser_accepts_paginated_headers_but_rejects_missing_rows(self):
        output = (
            "+----+------+\n|time|device|\n+----+------+\n|0|device_0|\n"
            "+----+------+\nIt costs 0.001s\n"
            "+----+------+\n|time|device|\n+----+------+\n|1|device_0|\n"
            "+----+------+\nTotal line number = 2\n"
        )
        self.assertEqual(
            table_rows(output),
            [
                {"time": "0", "device": "device_0"},
                {"time": "1", "device": "device_0"},
            ],
        )
        with self.assertRaises(ValueError):
            table_rows(output.replace("|1|device_0|\n", ""))


if __name__ == "__main__":
    unittest.main()
