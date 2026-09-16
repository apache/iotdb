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

"""Protocol and measurement boundary checks without model or database access."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from codex_client import CodexClient


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


class ScriptedClient(CodexClient):
    """Replace transport, preserving the production task loop and event filter."""

    def __init__(self, workspace, clock, scripts):
        self.workspace = Path(workspace)
        self.model = "gpt-5.6-sol"
        self.provider = "test_provider"
        self.effort = "low"
        self.event_callback = None
        self.clock = clock
        self.scripts = iter(scripts)
        self.sent = []
        self.rpcs = []
        self.thread_count = 0
        self.sequence = 0
        self.frames = []

    def _rpc(self, method, params, timeout=60):
        self.rpcs.append((method, params))
        if method == "thread/start":
            self.thread_count += 1
            self.thread_id = "test_thread_" + str(self.thread_count)
            self.frames = iter(next(self.scripts))
            return {
                "thread": {"id": self.thread_id},
                "model": self.model,
                "modelProvider": self.provider,
                "reasoningEffort": "low",
                "serviceTier": "priority",
            }
        return {}

    def _request(self, method, params):
        self.sequence += 1
        self.sent.append({"method": method, "params": params})
        return self.sequence

    def _send(self, value):
        self.sent.append(value)

    def _receive(self, deadline):
        if self.clock.now >= deadline:
            raise TimeoutError()
        frame = next(self.frames)
        if isinstance(frame, Exception):
            raise frame
        at, event = frame
        self.clock.now = max(self.clock.now, at)
        event = copy.deepcopy(event)
        if "params" in event:
            event["params"].setdefault("threadId", self.thread_id)
        return at, event


def event(at, method, **params):
    return at, {"method": method, "params": params}


def started(at=100.1):
    return event(at, "turn/started", turn={"id": "turn_1", "status": "inProgress"})


def completed(at=101.0, status="completed", error=None):
    return event(
        at, "turn/completed", turn={"id": "turn_1", "status": status, "error": error}
    )


def final(at=100.8, text='{"answer":42}'):
    return event(
        at,
        "item/completed",
        item={"type": "agentMessage", "phase": "final_answer", "text": text},
    )


def call(at=100.2, ident=0, tool="iotdb_command", arguments=None):
    return at, {
        "id": ident,
        "method": "item/tool/call",
        "params": {
            "tool": tool,
            "arguments": arguments
            if arguments is not None
            else {"command": "SELECT 42"},
        },
    }


class CodexBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="iotdb_codex_boundary_test_")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.clock = FakeClock()
        self.clock_patch = patch("codex_client.time.perf_counter", self.clock)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)
        self.calls = []

    def callback(self, command, remaining):
        self.calls.append((command, remaining))
        self.clock.now += 0.4
        return {"stdout": "42", "stderr": "", "exit_code": 0}

    def run_script(self, frames, **kwargs):
        self.client = ScriptedClient(self.root, self.clock, [frames])
        return self.client.run_task(
            "query task", self.callback, self.root / "trial", **kwargs
        )

    def test_final_receipt_ends_timing_and_delayed_queue_events_keep_receipt_time(self):
        result = self.run_script(
            [
                started(),
                call(),
                event(100.3, "rawResponse/completed", usage={"inputTokens": 20}),
                final(),
                event(100.9, "rawResponse/completed", usage={"inputTokens": 30}),
                event(
                    101.0,
                    "thread/tokenUsage/updated",
                    tokenUsage={"total": {"inputTokens": 50}},
                ),
                completed(102.0),
            ]
        )
        self.assertEqual(result["status"], "completed")
        self.assertAlmostEqual(result["task_wall_ms"], 800)
        self.assertAlmostEqual(result["tool_wall_ms"], 400)
        self.assertAlmostEqual(result["non_tool_wall_ms"], 400)
        self.assertEqual(result["model_responses"], 2)
        self.assertIsNone(result["model_api_ms"])
        self.assertIsNone(result["model_requests"])
        self.assertEqual(result["usage"]["inputTokens"], 50)
        events = [
            json.loads(line)
            for line in (self.root / "trial/events.jsonl").read_text().splitlines()
        ]
        response = next(e for e in events if e["method"] == "rawResponse/completed")
        self.assertAlmostEqual(response["elapsed_ms"], 300)
        self.assertAlmostEqual(self.calls[0][1], 179.8)

    def test_thirteenth_tool_attempt_is_recorded_but_not_executed(self):
        result = self.run_script(
            [started(), *[call(100.2 + i, ident=i) for i in range(13)]]
        )
        self.assertEqual(result["status"], "budget_exhausted")
        self.assertEqual(result["tool_attempts"], 13)
        self.assertEqual(len(self.calls), 12)
        self.assertIn("turn/interrupt", [method for method, _ in self.client.rpcs])

    def test_invalid_dynamic_arguments_do_not_reach_database_callback(self):
        result = self.run_script(
            [
                started(),
                call(arguments={"command": "SELECT 42", "extra": "not allowed"}),
                final(),
                completed(),
            ]
        )
        self.assertEqual(result["tool_attempts"], 1)
        self.assertEqual(self.calls, [])
        response = next(value for value in self.client.sent if value.get("id") == 0)
        self.assertFalse(response["result"]["success"])
        self.assertIn(
            "Invalid tool call", response["result"]["contentItems"][0]["text"]
        )

    def test_provider_failure_is_infrastructure_error_with_original_error(self):
        error = {"message": "upstream 502", "codexErrorInfo": "other"}
        result = self.run_script([started(), completed(100.3, "failed", error)])
        self.assertEqual(result["status"], "infrastructure_error")
        self.assertEqual(result["error"], error)
        self.assertEqual(result["tool_attempts"], 0)

    def test_stream_disconnect_retries_on_a_fresh_thread(self):
        error = {
            "message": "stream disconnected before completion: stream closed before response.completed",
            "codexErrorInfo": "other",
        }
        client = ScriptedClient(
            self.root,
            self.clock,
            [
                [started(), completed(100.3, "failed", error)],
                [started(100.4), final(100.6, '{"answer":7}'), completed(100.7)],
            ],
        )
        with patch("codex_client.time.sleep"):
            result = client.run_task(
                "retry task",
                self.callback,
                self.root / "retry-trial",
                max_stream_retries=1,
            )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["final_text"], '{"answer":7}')
        self.assertEqual(result["stream_retry_count"], 1)
        self.assertEqual(result["stream_attempts"], 2)
        self.assertEqual(len(result["stream_retry_history"]), 2)
        self.assertEqual(client.thread_count, 2)
        self.assertTrue(
            (
                self.root / "retry-trial" / "retries" / "attempt-02" / "events.jsonl"
            ).exists()
        )

    def test_stream_retry_can_be_disabled(self):
        error = {
            "message": "stream disconnected before completion: stream closed before response.completed"
        }
        result = self.run_script(
            [started(), completed(100.3, "failed", error)],
            max_stream_retries=0,
        )
        self.assertEqual(result["status"], "infrastructure_error")
        self.assertEqual(result["stream_retry_count"], 0)
        self.assertEqual(result["stream_attempts"], 1)

    def test_timeout_interrupts_turn_and_next_task_has_a_fresh_thread(self):
        client = ScriptedClient(
            self.root,
            self.clock,
            [
                [started(), TimeoutError()],
                [
                    event(
                        100.2,
                        "item/completed",
                        threadId="test_thread_1",
                        item={
                            "type": "agentMessage",
                            "phase": "final_answer",
                            "text": "old answer",
                        },
                    ),
                    started(100.3),
                    final(100.4, '{"answer":7}'),
                    completed(100.5),
                ],
            ],
        )
        first = client.run_task("first task", self.callback, self.root / "first")
        second = client.run_task("second task", self.callback, self.root / "second")
        self.assertEqual(first["status"], "timeout")
        self.assertEqual(second["status"], "completed")
        self.assertNotEqual(first["session_id"], second["session_id"])
        self.assertEqual(second["final_text"], '{"answer":7}')
        self.assertIn("turn/interrupt", [method for method, _ in client.rpcs])

    def test_response_boundary_cap_does_not_invent_request_count(self):
        result = self.run_script(
            [
                started(),
                event(100.2, "rawResponse/completed", usage={"inputTokens": 20}),
            ],
            max_model_requests=1,
        )
        self.assertEqual(result["status"], "budget_exhausted")
        self.assertEqual(result["model_responses"], 1)
        self.assertIsNone(result["model_requests"])
        self.assertEqual(
            result["model_request_limit_enforcement"],
            "completed_response_boundary_only",
        )


class RuntimeIsolationTest(unittest.TestCase):
    def test_parent_codex_session_context_and_unconfigured_secrets_are_not_inherited(
        self,
    ):
        client = object.__new__(CodexClient)
        with tempfile.TemporaryDirectory() as codex_home:
            Path(codex_home, "config.toml").write_text(
                'model_provider="test"\n[model_providers.test]\nenv_key="TEST_PROVIDER_KEY"\n'
            )
            from unittest.mock import patch
            import os

            with patch.dict(
                os.environ,
                {
                    "CODEX_HOME": codex_home,
                    "CODEX_REMOTE_PAYLOAD": "private_sentinel",
                    "CODEX_THREAD_ID": "parent_thread",
                    "CODEX_SESSION_ID": "parent_session",
                    "CODEX_PERMISSION_PROFILE": "parent_permissions",
                    "TEST_PROVIDER_KEY": "provider_sentinel",
                    "OTHER_SECRET": "private_sentinel",
                },
                clear=True,
            ):
                child = client._child_environment()
            self.assertEqual(set(child), {"CODEX_HOME", "TEST_PROVIDER_KEY"})
            self.assertEqual(child["TEST_PROVIDER_KEY"], "provider_sentinel")

    def test_unapproved_top_level_tool_stops_trial_without_logging_its_arguments(self):
        clock = FakeClock()
        frames = [
            started(),
            event(
                100.2,
                "rawResponseItem/completed",
                item={
                    "type": "function_call",
                    "name": "spawn_agent",
                    "arguments": '{"message":"private_sentinel"}',
                    "call_id": "bad_call",
                },
            ),
        ]
        with tempfile.TemporaryDirectory() as directory:
            client = ScriptedClient(directory, clock, [frames])
            with patch("codex_client.time.perf_counter", clock):
                result = client.run_task(
                    "task", lambda *_: self.fail("unexpected DB call"), directory
                )
            trace = Path(directory, "events.jsonl").read_text()
        self.assertEqual(result["status"], "protocol_invalid")
        self.assertIn("spawn_agent", result["error"])
        self.assertNotIn("private_sentinel", trace)
        self.assertIn("arguments_redacted", trace)
        self.assertIn("turn/interrupt", [method for method, _ in client.rpcs])

    def test_subagent_events_are_redacted_and_rejected_even_without_raw_tool_call(self):
        event = {
            "method": "item/started",
            "params": {
                "item": {"type": "subAgentActivity", "message": "private_sentinel"}
            },
        }
        self.assertIsNotNone(CodexClient._protocol_violation(event))
        public = CodexClient._public_event(event)
        self.assertEqual(public["method"], "protocol/violation")
        self.assertNotIn("private_sentinel", json.dumps(public))


class ProtocolPrivacyTest(unittest.TestCase):
    def test_private_reasoning_is_not_exposed(self):
        for method, params in [
            (
                "rawResponseItem/completed",
                {
                    "item": {
                        "type": "reasoning",
                        "encrypted_content": "private_sentinel",
                    }
                },
            ),
            (
                "item/completed",
                {"item": {"type": "reasoning", "summary": ["private_sentinel"]}},
            ),
            ("item/reasoning/summaryTextDelta", {"delta": "private_sentinel"}),
        ]:
            with self.subTest(method=method):
                self.assertIsNone(
                    CodexClient._public_event({"method": method, "params": params})
                )

    def test_tool_exchange_preserves_audit_fields_and_drops_other_fields(self):
        value = CodexClient._public_event(
            {
                "method": "rawResponseItem/completed",
                "params": {
                    "threadId": "test",
                    "turnId": "turn",
                    "item": {
                        "type": "custom_tool_call",
                        "name": "exec",
                        "input": "await tools.iotdb_command({command:'SELECT 42'})",
                        "call_id": "call_1",
                        "encrypted_content": "private_sentinel",
                        "unknown_private_field": "private_sentinel",
                    },
                },
            }
        )
        self.assertEqual(value["method"], "model/toolExchange")
        self.assertEqual(value["params"]["item"]["name"], "exec")
        self.assertNotIn("private_sentinel", json.dumps(value))

    def test_completed_response_keeps_usage_without_billing_or_attribution_metadata(
        self,
    ):
        value = CodexClient._public_event(
            {
                "method": "rawResponse/completed",
                "params": {
                    "threadId": "test",
                    "responseId": "response",
                    "usage": {"inputTokens": 42},
                    "usageMetadata": {"private": "private_sentinel"},
                },
            }
        )
        self.assertEqual(value["params"]["usage"]["inputTokens"], 42)
        self.assertNotIn("private_sentinel", json.dumps(value))

    def test_turn_completed_drops_embedded_reasoning_items(self):
        value = CodexClient._public_event(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "test",
                    "turn": {
                        "id": "turn",
                        "status": "completed",
                        "error": None,
                        "items": [
                            {"type": "reasoning", "summary": ["private_sentinel"]}
                        ],
                    },
                },
            }
        )
        self.assertNotIn("private_sentinel", json.dumps(value))


if __name__ == "__main__":
    unittest.main()
