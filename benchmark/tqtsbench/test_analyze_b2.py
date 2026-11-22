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

from pathlib import Path
import tempfile
import unittest

from analyze_b2 import classify, load_schema, measurements


class B2SplitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        path = Path(self.tmp.name) / "schema.sql"
        path.write_text("CREATE DATABASE root.p;\nCREATE ALIGNED TIMESERIES root.p.train.fd001.engine_1 (sensor_02 DOUBLE, cycle INT64);\n"
                        "CREATE ALIGNED TIMESERIES root.p.train.fd001.engine_2 (sensor_02 DOUBLE, cycle INT64);\n")
        self.series, self.devices = load_schema(path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_schema(self):
        self.assertIn("root.p.train.fd001.engine_2.cycle", self.series)
        self.assertEqual(len(self.devices), 2)

    def test_measurements_skip_functions_and_aliases(self):
        self.assertEqual(measurements("avg(sensor_02) AS a, max_value(cycle)"), {"sensor_02", "cycle"})
        self.assertEqual(measurements("*"), {"*"})
        self.assertEqual(measurements("metric_1.value - metric_2.value"), {"metric_1.value", "metric_2.value"})
        self.assertEqual(measurements("first_value(pres), last_value(pres)"), {"pres"})

    def test_verdicts(self):
        c = lambda sql: classify(sql, "", self.series, self.devices)
        self.assertEqual(c("SELECT sensor_02 FROM root.p.train.fd001.engine_9 WHERE time > 0"), "device_miss")
        self.assertEqual(c("SELECT sensor_02 FROM root.p.train.fd001.engine_1.sensor_02"), "type_confusion")
        self.assertEqual(c("SELECT sensor_99 FROM root.p.train.fd001.engine_1"), "measurement_miss")
        self.assertEqual(c("SELECT avg(sensor_02) FROM root.p.train.fd001.* WHERE time > 5"), "predicate_empty")
        self.assertEqual(c("SELECT avg(sensor_02) FROM root.p.train.fd001.engine_1 GROUP BY ([0, 10), 1s)"), "predicate_empty")
        self.assertEqual(c("SELECT avg(sensor_02) FROM root.p.train.fd001"), "device_miss")
        self.assertEqual(c("SELECT count(sensor_02) FROM root.p.train.fd001.engine_1.**"), "pattern_miss")
        self.assertEqual(c("SELECT count(*) FROM root.p.train.fd001.engine_1.cycle.**"), "type_confusion")
        self.assertEqual(c("SELECT count(sensor_02) FROM root.p.train.**"), "other")
        self.assertEqual(c("SHOW TIMESERIES root.p.test.**"), "device_miss")
        self.assertEqual(c("SHOW TIMESERIES root.p.train.**"), "other")
        self.assertEqual(c("SHOW DEVICES root.p"), "pattern_miss")
        self.assertEqual(c("SHOW TIMESERIES root.p.train.fd001.engine_1"), "pattern_miss")


if __name__ == "__main__":
    unittest.main()
