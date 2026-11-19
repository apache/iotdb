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
from final_table_submission import serialize_final_table


class FinalTableTest(unittest.TestCase):
    def test_preserves_identity_null_decimals_and_duplicate_rows(self):
        value={"final_table":{"columns":["Time","Device","value"],"rows":[[0,"root.x","1.234"],[0,"root.x","1.234"],[1,"root.y",None]]},"answer":"ignored"}
        rows=json.loads(serialize_final_table(value))
        self.assertEqual(rows[0],rows[1]);self.assertIsNone(rows[2]["value"])
        self.assertEqual(rows[0]["value"],"1.234")
    def test_no_prose_or_tool_fallback(self):
        for value in [{"answer":"42"},{"final_table":None},{"final_table":{"columns":["a"],"rows":[[1,2]]}},{"final_table":{"columns":["A","a"],"rows":[[1,2]]}},{"final_table":{"columns":["a"],"rows":[[{"value":1}]]}}]:
            with self.assertRaises(ValueError):serialize_final_table(value)
