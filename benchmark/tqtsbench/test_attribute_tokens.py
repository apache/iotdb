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

from attribute_tokens import attribute


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class AttributeTokensTest(unittest.TestCase):
    def test_step_delta_split_by_chars_and_carried(self):
        with tempfile.TemporaryDirectory() as tmp:
            trial = Path(tmp)
            events = [
                {"seq": 1, "type": "assistant/message", "step": 1, "time": 0, "usage": {"inputTokens": 100, "cacheReadTokens": 0, "outputTokens": 10}},
                {"seq": 2, "type": "tool/call", "step": 1, "time": 0, "callId": "a"},
                {"seq": 3, "type": "tool/result", "step": 1, "time": 1, "callId": "a"},
                {"seq": 4, "type": "tool/call", "step": 1, "time": 1, "callId": "b"},
                {"seq": 5, "type": "tool/result", "step": 1, "time": 2, "callId": "b"},
                {"seq": 6, "type": "assistant/message", "step": 2, "time": 3, "usage": {"inputTokens": 50, "cacheReadTokens": 100, "outputTokens": 5}},
                {"seq": 7, "type": "assistant/message", "step": 3, "time": 4, "usage": {"inputTokens": 5, "cacheReadTokens": 155, "outputTokens": 5}},
            ]
            (trial / "events-summary.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n")
            write(trial / "tool-output/call-a/request.json", {"callId": "a", "kind": "sql", "arguments": {"sql": "SELECT s FROM root.p.d1 LIMIT 1"}})
            write(trial / "tool-output/call-a/result.json", {"content": json.dumps({"stdout": "x" * 30})})
            write(trial / "tool-output/call-b/request.json", {"callId": "b", "kind": "sql", "arguments": {"sql": "SELECT s FROM root.p.d2 LIMIT 1"}})
            write(trial / "tool-output/call-b/result.json", {"content": json.dumps({"stdout": "x" * 30})})
            calls, totals = attribute(trial)
            # step 1 delta: 150 - 100 - 10 = 40 tool tokens, equal chars -> 20 each, carried over 2 later requests.
            self.assertEqual(sorted(round(c["tokens"]) for c in calls), [20, 20])
            self.assertEqual(sorted(round(c["carried_tokens"]) for c in calls), [40, 40])
            self.assertEqual(totals["input_tokens"], 100 + 150 + 160)
            self.assertEqual(totals["negative_steps"], 0)
            self.assertEqual(sorted(c["classes"] for c in calls), [[], ["B1"]])


if __name__ == "__main__":
    unittest.main()
