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

"""Unit tests for the DSH canonical-session adapter."""

import json
from pathlib import Path
import tempfile
import unittest

from dsh_client import (
    DshClient,
    collect_session_metrics,
    enforce_tool_policy,
    read_session_log,
)


def event(sequence, timestamp, kind, **data):
    return {"seq": sequence, "time": timestamp, "type": kind, "data": data}


class DshMetricsTest(unittest.TestCase):
    def test_collects_timing_usage_tools_and_retries(self):
        header = {"type": "session", "version": 3, "id": "session-test"}
        events = [
            event(1, 1000, "turn/start", turn=1),
            event(
                2,
                1005,
                "request/header",
                header={"tools": [{"name": "iotdb_sql"}]},
            ),
            event(3, 1010, "request/context", provider="test", model="model-a"),
            event(4, 1020, "step/start", turn=1, step=1),
            event(
                5,
                1060,
                "assistant/message",
                turn=1,
                step=1,
                stream=[
                    {
                        "type": "text-chunks",
                        "time0": 1040,
                        "texts": ["", "hi"],
                        "dt": [5, 0],
                    }
                ],
                usage={
                    "inputTokens": 100,
                    "outputTokens": 10,
                    "cacheReadTokens": 50,
                    "cacheWriteTokens": 5,
                    "reasoningTokens": 3,
                    "totalTokens": 165,
                },
            ),
            event(
                6,
                1061,
                "tool/call",
                turn=1,
                step=1,
                callId="c1",
                name="iotdb_sql",
            ),
            event(
                7,
                1081,
                "tool/result",
                turn=1,
                step=1,
                message={
                    "source": {"kind": "tool", "callId": "c1"},
                    "content": [{"toolCallId": "c1", "isError": False}],
                },
                meta={"interface": "sql", "cliProcessMs": 17.5},
            ),
            event(8, 1082, "step/end", turn=1, step=1),
            event(9, 1090, "step/start", turn=1, step=2),
            event(
                10,
                1110,
                "assistant/attempt",
                turn=1,
                step=2,
                stream=[
                    {
                        "type": "chunk",
                        "time": 1100,
                        "chunk": {
                            "type": "usage",
                            "usage": {
                                "inputTokens": 20,
                                "outputTokens": 1,
                                "cacheReadTokens": 10,
                                "reasoningTokens": 1,
                                "totalTokens": 31,
                            },
                        },
                    }
                ],
            ),
            event(
                11,
                1111,
                "llm/retry",
                retryId="r1",
                turn=1,
                step=2,
                provider="test",
                mode="normal",
                policyKey="default",
                retry=1,
                maxRetries=2,
                delayMs=25,
                failure={"code": "RATE_LIMIT", "status": 429},
            ),
            event(
                12,
                1136,
                "llm/retry-started",
                retryId="r1",
                turn=1,
                step=2,
                retry=1,
            ),
            event(
                13,
                1180,
                "assistant/message",
                turn=1,
                step=2,
                stream=[
                    {
                        "type": "reasoning-chunks",
                        "time0": 1150,
                        "texts": ["thinking"],
                        "dt": [0],
                    }
                ],
                usage={
                    "inputTokens": 30,
                    "outputTokens": 4,
                    "reasoningTokens": 2,
                    "totalTokens": 34,
                },
            ),
            event(14, 1181, "step/end", turn=1, step=2),
            event(15, 1182, "turn/end", turn=1, reason={"kind": "completed"}),
        ]
        result = collect_session_metrics(header, events, task_wall_ms=250.0)
        self.assertTrue(result["completed"])
        self.assertEqual((result["turns"], result["steps"]), (1, 2))
        self.assertEqual((result["model_requests"], result["model_responses"]), (3, 2))
        self.assertEqual(result["assistant_attempts"], 1)
        self.assertEqual((result["retry_count"], result["retry_started_count"]), (1, 1))
        self.assertEqual(result["retry_delay_ms"], 25)
        self.assertEqual((result["tool_attempts"], result["tool_errors"]), (1, 0))
        self.assertEqual(result["exposed_tools"], ["iotdb_sql"])
        self.assertEqual(result["request_tool_sets"], [["iotdb_sql"]])
        self.assertEqual(result["tool_wall_ms"], 20)
        self.assertEqual(result["cli_process_ms"], 17.5)
        self.assertEqual(result["cli_process_samples"], 1)
        self.assertEqual(result["model_api_ms"], 130)
        self.assertEqual(result["ttft_ms"], 85)
        self.assertEqual(result["decode_ms"], 45)
        self.assertEqual(result["decode_tokens"], 14)
        self.assertEqual(result["usage"]["inputTokens"], 150)
        self.assertEqual(result["usage"]["cachedInputTokens"], 60)
        self.assertEqual(result["usage"]["cacheWriteTokens"], 5)
        self.assertEqual(result["usage"]["outputTokens"], 15)
        self.assertEqual(result["usage"]["reasoningOutputTokens"], 6)
        self.assertEqual(result["usage"]["totalTokens"], 230)
        self.assertEqual(
            (result["cache_hit_requests"], result["cache_miss_requests"]), (2, 1)
        )
        self.assertAlmostEqual(result["cache_hit_ratio"], 60 / 215)

    def test_tool_policy_requires_exact_inventory_and_call_names(self):
        valid = {
            "status": "completed",
            "request_tool_sets": [["iotdb_fs"], ["iotdb_fs"]],
            "tool_calls_by_name": {"iotdb_fs": 2},
        }
        enforce_tool_policy(valid, "iotdb_fs")
        self.assertTrue(valid["tool_policy_ok"])
        self.assertEqual(valid["status"], "completed")

        invalid = {
            "status": "completed",
            "request_tool_sets": [["iotdb_fs", "read"]],
            "tool_calls_by_name": {"read": 1},
        }
        enforce_tool_policy(invalid, "iotdb_fs")
        self.assertFalse(invalid["tool_policy_ok"])
        self.assertEqual(invalid["status"], "infrastructure_error")
        self.assertIn("unexpected tool calls", invalid["error"])

        too_many = {
            "status": "completed",
            "request_tool_sets": [["iotdb_sql"]],
            "tool_calls_by_name": {"iotdb_sql": 2},
            "tool_attempts": 2,
        }
        enforce_tool_policy(too_many, "iotdb_sql", max_tool_calls=1)
        self.assertFalse(too_many["tool_policy_ok"])
        self.assertIn("expected at most 1 tool calls", too_many["error"])

    def test_latest_usage_replaces_same_attempt_sample(self):
        header = {"type": "session", "version": 3, "id": "session-test"}
        events = [
            event(1, 1000, "step/start", turn=1, step=1),
            event(
                2,
                1020,
                "assistant/attempt",
                turn=1,
                step=1,
                stream=[
                    {
                        "type": "chunk",
                        "time": 1015,
                        "chunk": {
                            "type": "usage",
                            "usage": {"inputTokens": 10, "outputTokens": 1},
                        },
                    }
                ],
            ),
            event(
                3,
                1030,
                "assistant/message",
                turn=1,
                step=1,
                stream=[],
                usage={"inputTokens": 12, "outputTokens": 2, "totalTokens": 14},
            ),
            event(4, 1031, "step/end", turn=1, step=1),
        ]
        result = collect_session_metrics(header, events, task_wall_ms=50)
        self.assertEqual(result["usage_reported_requests"], 1)
        self.assertEqual(result["usage"]["inputTokens"], 12)
        self.assertEqual(result["usage"]["outputTokens"], 2)
        self.assertEqual(result["usage"]["totalTokens"], 14)
        self.assertEqual(result["usage"]["totalTokensDerived"], 14)

    def test_tool_errors_and_unmatched_pairs_are_visible(self):
        header = {"type": "session", "version": 3, "id": "session-test"}
        events = [
            event(1, 1000, "tool/call", callId="open", name="bash"),
            event(
                2,
                1010,
                "tool/result",
                message={
                    "source": {"callId": "unknown"},
                    "content": [{"isError": True}],
                },
                error={"name": "Error", "code": "FAILED"},
            ),
        ]
        result = collect_session_metrics(header, events, task_wall_ms=20)
        self.assertEqual((result["tool_attempts"], result["tool_errors"]), (1, 1))
        self.assertEqual(result["unmatched_tool_calls"], 1)
        self.assertEqual(result["unmatched_tool_results"], 1)

    def test_read_session_log_rejects_noncontiguous_sequence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session.v3.jsonl"
            rows = [
                {"type": "session", "version": 3, "id": "session-test"},
                event(1, 1000, "turn/start", turn=1),
                event(3, 1001, "turn/end", turn=1, reason={"kind": "completed"}),
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
            with self.assertRaisesRegex(ValueError, "not contiguous"):
                read_session_log(path)

    def test_launch_failure_is_a_persisted_infrastructure_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trial = root / "trial"
            client = DshClient(root / "missing-dsh", workspace=root / "workspace")
            result = client.run_task("test prompt", trial, timeout=1)
            self.assertEqual(result["status"], "infrastructure_error")
            self.assertIn("failed to start DSH", result["error"])
            self.assertEqual(
                result["session_log_error"], "expected one DSH session log, found 0"
            )
            self.assertTrue((trial / "dsh_result.json").exists())
            self.assertEqual(trial.stat().st_mode & 0o777, 0o700)
            self.assertEqual((trial / "prompt.txt").stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()
