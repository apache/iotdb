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

from score_access_shape import load_policy
from rescore_final_table_v2 import v2_policy


class ScoreAccessShapeTest(unittest.TestCase):
    def test_policy_accepts_repr_string_and_dict(self):
        raw = "{'profile': 'relational', 'normalization': {'pivot': {'x': 1}, 'reshape': {'type': 'long_to_wide'}, 'trim': True}}"
        policy = load_policy(raw)
        self.assertEqual(policy, load_policy(policy))
        self.assertEqual(v2_policy(policy)["normalization"], {"trim": True})
        self.assertIn("pivot", policy["normalization"])


if __name__ == "__main__":
    unittest.main()
