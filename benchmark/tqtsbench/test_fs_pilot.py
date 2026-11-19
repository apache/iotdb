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

import unittest

from fs_tool_runner import build_command, command_failed, help_text, resource_metrics


class FsBoundaryTest(unittest.TestCase):
    def test_rejects_local_paths_other_databases_and_sql_escape(self):
        for request in [
            {"command": "cat", "path": "/etc/passwd"},
            {"command": "cat", "path": "/root/iotdb_ndbc/../iotdb_cmapss"},
            {"command": "cat", "path": "/root/iotdb_cmapss"},
            {"command": "sql", "sql": "SELECT * FROM root.iotdb_ndbc"},
            {"command": "grep", "pattern": "x | sql SELECT", "path": "/root/iotdb_ndbc"},
            {"command": "cat", "command_text": "cat /etc/passwd"},
            {"command": "join", "paths": ["/root/iotdb_ndbc", "/etc/passwd"]},
        ]:
            with self.subTest(request=request), self.assertRaises(ValueError):
                build_command(request, "iotdb_ndbc")

    def test_pagination_fetches_extra_row_without_changing_bounds(self):
        command = build_command({"command": "cat", "path": "/root/iotdb_ndbc/station_42001", "measurements": ["wspd"], "start_ms": 0, "end_ms": 599999, "offset": 100, "limit": 500}, "iotdb_ndbc")
        self.assertIn("-n 501", command)
        self.assertIn("--offset 100", command)
        self.assertIn("--end 599999", command)
        self.assertIn("-m wspd", command)

    def test_unsupported_computation_is_not_silently_added(self):
        for request in [
            {"command": "stats", "aggregates": ["argmax"]},
            {"command": "stats", "window_ms": 10000},
            {"command": "count", "start_ms": 0},
            {"command": "tail", "offset": 10},
        ]:
            with self.assertRaises(ValueError):
                build_command(request, "iotdb_ndbc")
        self.assertIn("NOT argmin/argmax", help_text("/root/iotdb_ndbc")["commands"]["stats"])

    def test_windows_are_typed_and_cannot_escape_the_command(self):
        request = {"command": "stats", "windows": [{"start_ms": -10, "end_ms": 0}, {"start_ms": 0, "end_ms": 10}], "aggregates": ["first", "last"]}
        command = build_command(request, "iotdb_ndbc")
        self.assertIn("--windows -10:0,0:10", command)
        self.assertIn("--aggregates first,last", command)
        for bad in [[], [{"start_ms": 2, "end_ms": 1}], [{"start_ms": True, "end_ms": 1}], [{"start_ms": "0;sql", "end_ms": 1}], [{"start_ms": 0, "end_ms": 1, "sql": "SELECT"}]]:
            with self.assertRaises(ValueError):
                build_command({"command": "stats", "windows": bad}, "iotdb_ndbc")
        with self.assertRaises(ValueError):
            build_command({**request, "start_ms": 0}, "iotdb_ndbc")

    def test_feature_gates_and_temporal_contracts(self):
        scope = "/root/iotdb_ndbc/station_42001"
        requests = [
            {"command":"gaps","path":scope,"field":"wspd"},
            {"command":"sessions","path":scope,"field":"wspd","gap_ms":14400000},
            {"command":"extreme","path":scope,"field":"wspd","mode":"max","measurements":["wtmp"]},
        ]
        for request in requests:
            with self.assertRaises(ValueError):
                build_command(request,"iotdb_ndbc")
            command=build_command({**request,"limit":1,"offset":2},"iotdb_ndbc",["temporal","extreme"])
            self.assertIn("-n 2",command)
            self.assertIn("--offset 2",command)
        for request in [
            {**requests[0],"field":"wspd;sql"},
            {**requests[1],"gap_ms":True},
            {**requests[1],"gap_ms":0},
            {**requests[1],"gap_ms":2**63},
            {**requests[1],"windows":[{"start_ms":0,"end_ms":10}]},
            {**requests[2],"mode":"average"},
            {"command":"gaps","field":"wspd"},
        ]:
            with self.assertRaises(ValueError):
                build_command(request,"iotdb_ndbc",["temporal","extreme"])
        self.assertNotIn("extreme",help_text(scope,["temporal"])["commands"])
        self.assertNotIn("sessions",help_text(scope,["extreme"])["commands"])

    def test_grep_no_match_is_distinct_from_fatal_jvm_exit(self):
        self.assertFalse(command_failed("grep",1,False,""))
        self.assertTrue(command_failed("grep",1,False,"java.lang.OutOfMemoryError: Java heap space"))
        self.assertTrue(command_failed("grep",1,False,'Exception in thread "main" java.lang.RuntimeException'))
        self.assertTrue(command_failed("grep",1,True,""))
        self.assertTrue(command_failed("stats",1,False,""))

    def test_resource_metrics_preserve_units(self):
        self.assertEqual(resource_metrics("Maximum resident set size (kbytes): 1234\nUser time (seconds): 1.25\nSystem time (seconds): 0.10"), {"peak_rss_kib": 1234.0, "cpu_user_seconds": 1.25, "cpu_system_seconds": 0.1})


if __name__ == "__main__":
    unittest.main()
