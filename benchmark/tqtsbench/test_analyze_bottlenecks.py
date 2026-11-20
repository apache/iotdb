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
import unittest

from analyze_bottlenecks import category, outcome, sibling_signature


class BottleneckRulesTest(unittest.TestCase):
    def test_categories(self):
        self.assertEqual(category({"kind": "context", "arguments": {"query": "x"}}), "context")
        self.assertEqual(category({"kind": "sql", "arguments": {"sql": "SHOW TIMESERIES root.a.**"}}), "struct")
        self.assertEqual(category({"kind": "sql", "arguments": {"sql": "SELECT avg(s) FROM root.a.b"}}), "compute")
        self.assertEqual(category({"kind": "sql", "arguments": {"sql": "SELECT s FROM root.a.b LIMIT 3"}}), "probe")
        self.assertEqual(category({"kind": "fs", "arguments": {"command": "tree"}}), "struct")
        self.assertEqual(category({"kind": "fs", "arguments": {"command": "stats"}}), "compute")

    def test_sibling_signature_ignores_only_objects(self):
        a, oa = sibling_signature({"kind": "sql", "arguments": {"sql": "SELECT avg(s) FROM root.a.d1 WHERE time>0"}})
        b, ob = sibling_signature({"kind": "sql", "arguments": {"sql": "SELECT avg(s) FROM root.a.d2 WHERE time>0"}})
        c, _ = sibling_signature({"kind": "sql", "arguments": {"sql": "SELECT avg(s) FROM root.a.d2 WHERE time>1"}})
        self.assertEqual(a, b); self.assertNotEqual(oa, ob); self.assertNotEqual(a, c)
        f1, p1 = sibling_signature({"kind": "fs", "arguments": {"command": "stats", "path": "/x/d1", "measurements": ["v"]}})
        f2, p2 = sibling_signature({"kind": "fs", "arguments": {"command": "stats", "path": "/x/d2", "measurements": ["v"]}})
        self.assertEqual(f1, f2); self.assertEqual(p1, ("/x/d1",))
        self.assertEqual(sibling_signature({"kind": "context", "arguments": {}}), (None, ()))

    def test_outcome_detects_empty_and_errors(self):
        empty_sql = {"content": json.dumps({"stdout": "+----+\n|Time|\n+----+\nEmpty set.\n", "is_error": False})}
        self.assertTrue(outcome(empty_sql)[0])
        self.assertTrue(outcome({"content": json.dumps({"rows": [], "is_error": False})})[0])
        err = outcome({"isError": True, "content": json.dumps({"is_error": True, "error": "Unsupported parameters for tail: measurements"})})
        self.assertFalse(err[0]); self.assertIn("Unsupported", err[1])
        self.assertTrue(outcome({"content": json.dumps({"stdout": "x", "truncated": True})})[3])


if __name__ == "__main__":
    unittest.main()
