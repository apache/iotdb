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

from rescore_final_table_v2 import perturbed, v2_policy


class RescoreV2Test(unittest.TestCase):
    def test_removes_only_pivot_and_reshape(self):
        policy = {"profile": "relational", "columns": {"mode": "gold_subset", "names": "ignore"},
                  "rows": {"mode": "unordered"}, "values": {"nulls": "strict"},
                  "normalization": {"pivot": True, "reshape": {"type": "tabular"}, "other": 1}}
        result = v2_policy(policy)
        self.assertEqual(result["normalization"], {"other": 1})
        self.assertEqual({k: v for k, v in result.items() if k != "normalization"},
                         {k: v for k, v in policy.items() if k != "normalization"})
        self.assertIn("pivot", policy["normalization"])

    def test_perturbs_numbers_and_keeps_text_and_null(self):
        rows = [{"a": "1.5", "b": 2, "c": "root.x", "d": None, "e": True}]
        out = perturbed(rows)[0]
        self.assertEqual(out["a"], "1001.5")
        self.assertEqual(out["b"], 1002.0)
        self.assertEqual((out["c"], out["d"], out["e"]), ("root.x", None, True))
        self.assertEqual(rows[0]["a"], "1.5")


if __name__ == "__main__":
    unittest.main()
