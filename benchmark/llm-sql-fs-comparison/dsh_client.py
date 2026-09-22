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

"""DeepSeek Harness adapter with durable per-trial measurements.

Each invocation adds an isolated, uncompressed session-persistence root. This
provides an authoritative session log for token, cache, tool, retry, and model
timing metrics without guessing which global DSH session belongs to a trial.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

from tool_adapter import validate_filesystem_path


USAGE_FIELDS = (
    "inputTokens",
    "outputTokens",
    "cacheReadTokens",
    "cacheWriteTokens",
    "reasoningTokens",
    "totalTokens",
)
DATABASE_NAME = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\Z")


def _number(value):
    if type(value) in {int, float} and math.isfinite(value) and value >= 0:
        return value
    return None


def _usage(event):
    data = event.get("data", {})
    if event.get("type") == "assistant/message" and isinstance(data.get("usage"), dict):
        return data["usage"]
    for record in reversed(data.get("stream", [])):
        chunk = record.get("chunk", {}) if isinstance(record, dict) else {}
        if chunk.get("type") == "usage" and isinstance(chunk.get("usage"), dict):
            return chunk["usage"]
    return None


def _first_token_time(stream):
    """Mirror DSH assistantStreamFirstTokenTime for compact stream records."""
    if not isinstance(stream, list):
        return None
    for record in stream:
        if not isinstance(record, dict):
            continue
        if record.get("type") == "chunk":
            chunk = record.get("chunk", {})
            kind = chunk.get("type")
            if kind in {"text-delta", "reasoning-delta"} and chunk.get("text") != "":
                return _number(record.get("time"))
            if kind == "tool-call-delta" and (
                chunk.get("argumentsDelta") != "" or "name" in chunk
            ):
                return _number(record.get("time"))
            continue
        kind = record.get("type")
        fragments = record.get("args" if kind == "tool-call-chunks" else "texts")
        current = _number(record.get("time0"))
        if not isinstance(fragments, list) or current is None:
            continue
        if kind == "tool-call-chunks" and "name" in record:
            return current
        deltas = record.get("dt", [])
        for index, fragment in enumerate(fragments):
            if index > 0 and index - 1 < len(deltas):
                current += _number(deltas[index - 1]) or 0
            if fragment != "":
                return current
    return None


def read_session_log(path):
    """Read a complete DSH v3 JSONL log and validate its sequence."""
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ValueError("DSH session log is empty")
    try:
        header = json.loads(lines[0])
        events = [json.loads(line) for line in lines[1:]]
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"invalid JSON in DSH session log at line {exc.lineno}"
        ) from exc
    if header.get("type") != "session" or header.get("version") != 3:
        raise ValueError("expected a DSH v3 session log")
    if not all(isinstance(event, dict) for event in events):
        raise ValueError("DSH session event is not an object")
    sequences = [event.get("seq") for event in events]
    if sequences and sequences != list(range(sequences[0], sequences[0] + len(events))):
        raise ValueError("DSH session event sequence is not contiguous")
    return header, events


def _result_call_id(data):
    message = data.get("message")
    source = message.get("source") if isinstance(message, dict) else None
    return source.get("callId") if isinstance(source, dict) else None


def _result_is_error(data):
    if isinstance(data.get("error"), dict):
        return True
    message = data.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    return isinstance(content, list) and any(
        isinstance(block, dict) and block.get("isError") is True for block in content
    )


def _update_usage(totals, usage, multiplier=1):
    for key in USAGE_FIELDS:
        value = _number(usage.get(key))
        if value is not None:
            totals[key] += multiplier * value


def collect_session_metrics(header, events, task_wall_ms):
    """Fold canonical DSH events into benchmark-compatible trial metrics."""
    tools = {}
    tool_names = {}
    tool_ms = 0.0
    cli_process_ms = 0.0
    cli_process_samples = 0
    tool_errors = 0
    calls_by_name = Counter()
    errors_by_name = Counter()
    unmatched_calls = 0
    unmatched_results = 0
    turns = steps = 0
    model_request_starts = 0
    last_turn = None
    open_step = None
    llm_ms = ttft_ms = decode_ms = 0.0
    ttft_steps = decode_tokens = 0
    totals = Counter({key: 0 for key in USAGE_FIELDS})
    usage_slot = None
    usage_samples = cache_hits = cache_misses = 0
    missing_total_samples = 0
    retries = []
    retry_started = 0
    retry_delay_ms = 0.0
    model_responses = assistant_attempts = 0
    request_context = None
    request_tool_sets = []
    turn_reason = None
    turn_start = turn_end = None
    first_time = last_time = None

    for event in events:
        kind = event.get("type")
        stamp = _number(event.get("time"))
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        if stamp is not None:
            first_time = stamp if first_time is None else min(first_time, stamp)
            last_time = stamp if last_time is None else max(last_time, stamp)

        if kind == "turn/start" and stamp is not None:
            turn_start = stamp
        elif kind == "turn/end":
            turn_end = stamp
            turn_reason = data.get("reason")
            unmatched_calls += len(tools)
            tools.clear()
        elif kind == "request/context":
            request_context = {
                key: data[key]
                for key in ("provider", "model", "contextWindow", "systemPromptUpdate")
                if key in data
            }
        elif kind == "request/header":
            request_header = (
                data.get("header") if isinstance(data.get("header"), dict) else {}
            )
            definitions = request_header.get("tools", [])
            request_tool_sets.append(
                [
                    (
                        definition.get("name", "<invalid>")
                        if isinstance(definition, dict)
                        else "<invalid>"
                    )
                    for definition in definitions
                ]
                if isinstance(definitions, list)
                else ["<invalid>"]
            )
        elif kind == "step/start" and stamp is not None:
            model_request_starts += 1
            open_step = {
                "turn": data.get("turn"),
                "step": data.get("step"),
                "start": stamp,
                "first": None,
            }
        elif kind == "assistant/attempt":
            assistant_attempts += 1
            if (
                open_step
                and (open_step["turn"], open_step["step"])
                == (data.get("turn"), data.get("step"))
                and open_step["first"] is None
            ):
                open_step["first"] = _first_token_time(data.get("stream"))
        elif kind == "assistant/message":
            model_responses += 1
            if (
                open_step
                and stamp is not None
                and (open_step["turn"], open_step["step"])
                == (data.get("turn"), data.get("step"))
            ):
                first = open_step["first"]
                if first is None:
                    first = _first_token_time(data.get("stream"))
                llm_ms += max(0.0, stamp - open_step["start"])
                if first is not None:
                    ttft_ms += max(0.0, first - open_step["start"])
                    ttft_steps += 1
                    sample = data.get("usage")
                    output = (
                        _number(sample.get("outputTokens"))
                        if isinstance(sample, dict)
                        else None
                    )
                    if output is not None:
                        decode_ms += max(0.0, stamp - first)
                        decode_tokens += output
                open_step = None
        elif kind == "step/end":
            steps += 1
            if last_turn != data.get("turn"):
                turns += 1
                last_turn = data.get("turn")
            open_step = None
        elif kind == "tool/call" and stamp is not None:
            call_id = data.get("callId")
            name = (
                data.get("name") if isinstance(data.get("name"), str) else "<unknown>"
            )
            if isinstance(call_id, str):
                tools[call_id] = stamp
                tool_names[call_id] = name
            calls_by_name[name] += 1
        elif kind == "tool/result" and stamp is not None:
            call_id = _result_call_id(data)
            start = tools.pop(call_id, None)
            name = tool_names.pop(call_id, "<unknown>")
            if start is None:
                unmatched_results += 1
            else:
                tool_ms += max(0.0, stamp - start)
            if _result_is_error(data):
                tool_errors += 1
                errors_by_name[name] += 1
            meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
            cli_ms = _number(meta.get("cliProcessMs"))
            if cli_ms is not None:
                cli_process_ms += cli_ms
                cli_process_samples += 1
        elif kind == "llm/retry":
            failure = (
                data.get("failure") if isinstance(data.get("failure"), dict) else {}
            )
            delay = _number(data.get("delayMs")) or 0.0
            retry_delay_ms += delay
            retries.append(
                {
                    key: value
                    for key, value in {
                        "retry_id": data.get("retryId"),
                        "turn": data.get("turn"),
                        "step": data.get("step"),
                        "provider": data.get("provider"),
                        "mode": data.get("mode"),
                        "policy_key": data.get("policyKey"),
                        "retry": data.get("retry"),
                        "max_retries": data.get("maxRetries"),
                        "delay_ms": data.get("delayMs"),
                        "failure_code": failure.get("code"),
                        "failure_status": failure.get("status"),
                        "provider_retry_after_ms": failure.get("providerRetryAfterMs"),
                    }.items()
                    if value is not None
                }
            )
        elif kind == "llm/retry-started":
            retry_started += 1
            if usage_slot and usage_slot[:2] == (data.get("turn"), data.get("step")):
                usage_slot = None

        if kind in {"assistant/message", "assistant/attempt"}:
            sample = _usage(event)
            if not isinstance(sample, dict):
                continue
            normalized = {key: (_number(sample.get(key)) or 0) for key in USAGE_FIELDS}
            slot_key = (data.get("turn"), data.get("step"))
            previous = (
                usage_slot[2:] if usage_slot and usage_slot[:2] == slot_key else None
            )
            if previous:
                previous_usage, previous_has_total = previous
                _update_usage(totals, previous_usage, -1)
                cache_hits -= previous_usage["cacheReadTokens"] > 0
                cache_misses -= previous_usage["cacheReadTokens"] == 0
                missing_total_samples -= not previous_has_total
                usage_samples -= 1
            _update_usage(totals, normalized)
            cache_hits += normalized["cacheReadTokens"] > 0
            cache_misses += normalized["cacheReadTokens"] == 0
            usage_samples += 1
            has_total = _number(sample.get("totalTokens")) is not None
            missing_total_samples += not has_total
            usage_slot = (slot_key[0], slot_key[1], normalized, has_total)

    prompt_tokens = (
        totals["inputTokens"] + totals["cacheReadTokens"] + totals["cacheWriteTokens"]
    )
    usage = {
        "inputTokens": totals["inputTokens"],
        "cachedInputTokens": totals["cacheReadTokens"],
        "cacheReadTokens": totals["cacheReadTokens"],
        "cacheWriteTokens": totals["cacheWriteTokens"],
        "outputTokens": totals["outputTokens"],
        "reasoningOutputTokens": totals["reasoningTokens"],
        "reasoningTokens": totals["reasoningTokens"],
        "totalTokens": (
            totals["totalTokens"]
            if usage_samples > 0 and missing_total_samples == 0
            else None
        ),
        "totalTokensDerived": prompt_tokens + totals["outputTokens"],
    }
    reason_kind = turn_reason.get("kind") if isinstance(turn_reason, dict) else None
    return {
        "session_id": header.get("id"),
        "model_provider": (request_context or {}).get("provider"),
        "resolved_model": (request_context or {}).get("model"),
        "context_window": (request_context or {}).get("contextWindow"),
        "turn_end_reason": turn_reason,
        "turns": turns,
        "steps": steps,
        "task_wall_ms": task_wall_ms,
        "session_wall_ms": (
            max(0.0, last_time - first_time)
            if first_time is not None and last_time is not None
            else None
        ),
        "agent_turn_ms": (
            max(0.0, turn_end - turn_start)
            if turn_start is not None and turn_end is not None
            else None
        ),
        "tool_wall_ms": tool_ms,
        "cli_process_ms": cli_process_ms if cli_process_samples else None,
        "cli_process_samples": cli_process_samples,
        "non_tool_wall_ms": max(0.0, task_wall_ms - tool_ms),
        "model_api_ms": llm_ms,
        "llm_wall_ms": llm_ms,
        "ttft_ms": ttft_ms,
        "ttft_steps": ttft_steps,
        "mean_ttft_ms": ttft_ms / ttft_steps if ttft_steps else None,
        "decode_ms": decode_ms,
        "decode_tokens": decode_tokens,
        "tool_attempts": sum(calls_by_name.values()),
        "tool_errors": tool_errors,
        "tool_calls_by_name": dict(sorted(calls_by_name.items())),
        "tool_errors_by_name": dict(sorted(errors_by_name.items())),
        "exposed_tools": sorted(
            {name for names in request_tool_sets for name in names}
        ),
        "request_tool_sets": request_tool_sets,
        "unmatched_tool_calls": unmatched_calls + len(tools),
        "unmatched_tool_results": unmatched_results,
        "model_requests": model_request_starts + retry_started,
        "model_responses": model_responses,
        "assistant_attempts": assistant_attempts,
        "retry_count": len(retries),
        "retry_started_count": retry_started,
        "retry_delay_ms": retry_delay_ms,
        "retries": retries,
        "usage_reported_requests": usage_samples,
        "cache_hit_requests": cache_hits,
        "cache_miss_requests": cache_misses,
        "cache_hit_ratio": (
            totals["cacheReadTokens"] / prompt_tokens if prompt_tokens else None
        ),
        "usage": usage,
        "completed": reason_kind == "completed",
        "timing_note": (
            "task_wall_ms measures subprocess launch through exit; model_api_ms mirrors "
            "DSH sessionStats llmMs (step/start through assistant/message, including "
            "in-step retry delay); tool_wall_ms sums matched tool intervals"
        ),
    }


def enforce_tool_policy(result, expected_tool, max_tool_calls=None):
    """Fail a controlled trial when DSH exposed or invoked another tool."""
    expected = [expected_tool]
    request_tool_sets = result.get("request_tool_sets", [])
    called_tools = sorted(result.get("tool_calls_by_name", {}))
    violations = []
    if not request_tool_sets:
        violations.append("canonical session has no request/header tool inventory")
    elif any(tool_set != expected for tool_set in request_tool_sets):
        violations.append(
            f"expected every request to expose {expected}, got {request_tool_sets}"
        )
    unexpected_calls = [name for name in called_tools if name != expected_tool]
    if unexpected_calls:
        violations.append(f"unexpected tool calls: {unexpected_calls}")
    attempts = result.get("tool_attempts")
    if max_tool_calls is not None and (
        not isinstance(attempts, int) or attempts > max_tool_calls
    ):
        violations.append(
            f"expected at most {max_tool_calls} tool calls, got {attempts}"
        )
    result.update(
        tool_policy_expected=expected,
        tool_policy_ok=not violations,
        tool_policy_violations=violations,
    )
    if violations:
        result.update(status="infrastructure_error", error="; ".join(violations))


def _public_event(event):
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    public = {key: event.get(key) for key in ("seq", "time", "type")}
    for key in ("turn", "step", "callId", "name"):
        if key in data:
            public[key] = data[key]
    if event.get("type") in {"assistant/message", "assistant/attempt"}:
        if _usage(event) is not None:
            public["usage"] = _usage(event)
    elif event.get("type") == "tool/result":
        public.update(callId=_result_call_id(data), isError=_result_is_error(data))
        meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
        public["metrics"] = {
            key: meta[key]
            for key in (
                "interface",
                "cliProcessMs",
                "stdoutBytes",
                "stderrBytes",
                "truncated",
                "resultKind",
                "returnedRows",
                "nextOffset",
            )
            if key in meta
        }
    elif event.get("type") == "llm/retry":
        failure = data.get("failure") if isinstance(data.get("failure"), dict) else {}
        for key in ("retryId", "provider", "mode", "retry", "maxRetries", "delayMs"):
            if key in data:
                public[key] = data[key]
        public.update(
            failureCode=failure.get("code"), failureStatus=failure.get("status")
        )
    elif event.get("type") == "turn/end":
        public["reason"] = data.get("reason")
    return public


class DshClient:
    """Run one fresh DSH process per benchmark trial."""

    def __init__(
        self,
        dsh_command="dsh",
        profile="headless",
        patches=None,
        workspace=None,
        expected_tool=None,
        max_tool_calls=None,
    ):
        self.dsh_command = str(dsh_command)
        self.profile = profile
        self.patches = [str(Path(path).resolve()) for path in (patches or [])]
        self.workspace = Path(workspace or Path.cwd()).resolve()
        self.expected_tool = expected_tool
        self.max_tool_calls = max_tool_calls

    def run_task(self, prompt, task_dir, timeout=180, environment=None):
        if not isinstance(prompt, str) or not prompt:
            raise ValueError("prompt must be a non-empty string")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive finite number")
        task_dir = Path(task_dir).resolve()
        session_root = task_dir / "dsh-session"
        if session_root.exists() or (task_dir / "dsh_result.json").exists():
            raise FileExistsError("trial directory already contains DSH results")
        task_dir.mkdir(parents=True, exist_ok=True)
        task_dir.chmod(0o700)
        self.workspace.mkdir(parents=True, exist_ok=True)
        prompt_path = task_dir / "prompt.txt"
        prompt_path.write_text(prompt, encoding="utf-8")
        prompt_path.chmod(0o600)
        trial_patch = task_dir / "dsh-metrics.patch.yml"
        trial_patch.write_text(
            "- id: session-persistence-jsonl\n"
            "  config:\n"
            f"    root: {json.dumps(str(session_root))}\n"
            "    compression: none\n",
            encoding="utf-8",
        )
        trial_patch.chmod(0o600)
        argv = [self.dsh_command, "--profile", self.profile]
        for patch in self.patches:
            argv.extend(["--patch", patch])
        argv.extend(["--patch", str(trial_patch), prompt])
        metadata = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "command": [*argv[:-1], "<prompt from prompt.txt>"],
            "cwd": str(self.workspace),
            "timeout_seconds": timeout,
        }
        started = time.perf_counter()
        timed_out = False
        returncode = None
        launch_error = None
        with (task_dir / "stdout.txt").open("wb") as stdout, (
            task_dir / "stderr.txt"
        ).open("wb") as stderr:
            (task_dir / "stdout.txt").chmod(0o600)
            (task_dir / "stderr.txt").chmod(0o600)
            try:
                process = subprocess.Popen(
                    argv,
                    cwd=self.workspace,
                    env=environment,
                    stdout=stdout,
                    stderr=stderr,
                    start_new_session=True,
                )
            except OSError as exc:
                launch_error = f"failed to start DSH: {exc}"
            else:
                try:
                    returncode = process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        returncode = process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        returncode = process.wait()
        task_wall_ms = (time.perf_counter() - started) * 1000
        metadata.update(
            finished_at=datetime.now(timezone.utc).isoformat(),
            returncode=returncode,
            timed_out=timed_out,
            launch_error=launch_error,
            task_wall_ms=task_wall_ms,
        )
        process_path = task_dir / "process.json"
        process_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        process_path.chmod(0o600)

        result = {
            "status": "timeout" if timed_out else "infrastructure_error",
            "returncode": returncode,
            "final_text": (task_dir / "stdout.txt")
            .read_text(encoding="utf-8", errors="replace")
            .strip(),
            "task_wall_ms": task_wall_ms,
            "tool_wall_ms": 0.0,
            "cli_process_ms": None,
            "non_tool_wall_ms": task_wall_ms,
            "model_api_ms": None,
            "tool_attempts": 0,
            "tool_errors": 0,
            "model_responses": 0,
            "model_requests": 0,
            "retry_count": 0,
            "usage": {},
        }
        if timed_out:
            result["error"] = "DSH trial deadline exceeded"
        elif launch_error is not None:
            result["error"] = launch_error
        elif returncode != 0:
            result["error"] = f"DSH exited with status {returncode}"
        logs = (
            list(session_root.rglob("session.v3.jsonl"))
            if session_root.exists()
            else []
        )
        if len(logs) != 1:
            session_error = f"expected one DSH session log, found {len(logs)}"
            result["session_log_error"] = session_error
            if "error" not in result:
                result["error"] = session_error
        else:
            try:
                header, events = read_session_log(logs[0])
                result.update(collect_session_metrics(header, events, task_wall_ms))
                result["session_log"] = str(logs[0].relative_to(task_dir))
                if timed_out:
                    result["status"] = "timeout"
                elif returncode != 0:
                    result["status"] = "infrastructure_error"
                elif result["completed"]:
                    result["status"] = "completed"
                else:
                    reason = result.get("turn_end_reason")
                    kind = reason.get("kind") if isinstance(reason, dict) else None
                    result["status"] = {
                        "max-tokens": "max_tokens",
                        "aborted": "aborted",
                        "blocked": "blocked",
                    }.get(kind, "agent_error")
                if self.expected_tool is not None:
                    enforce_tool_policy(result, self.expected_tool, self.max_tool_calls)
                with (task_dir / "events-summary.jsonl").open(
                    "w", encoding="utf-8"
                ) as stream:
                    for event in events:
                        stream.write(
                            json.dumps(_public_event(event), ensure_ascii=False) + "\n"
                        )
                (task_dir / "events-summary.jsonl").chmod(0o600)
            except (OSError, ValueError) as exc:
                result["session_log_error"] = str(exc)
                if not timed_out:
                    result.update(status="infrastructure_error", error=str(exc))
        temporary = task_dir / ".dsh_result.json.tmp"
        temporary.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        temporary.chmod(0o600)
        temporary.replace(task_dir / "dsh_result.json")
        return result


def controlled_baseline(
    arm,
    database,
    task_dir,
    dsh_command,
    iotdb_cli=None,
    tool_python=None,
    filesystem_path=None,
    structured_pages=True,
    compact_pages=False,
    filtered_stats=False,
):
    """Return the final baseline patch and process environment for one trial."""
    if arm not in {"sql", "filesystem"}:
        raise ValueError("baseline arm must be sql or filesystem")
    if not isinstance(database, str) or not DATABASE_NAME.fullmatch(database):
        raise ValueError("database must be an unquoted IoTDB identifier")
    if arm == "filesystem":
        if type(structured_pages) is not bool:
            raise ValueError("structured_pages must be a boolean")
        if type(compact_pages) is not bool:
            raise ValueError("compact_pages must be a boolean")
        if type(filtered_stats) is not bool:
            raise ValueError("filtered_stats must be a boolean")
        if compact_pages and not structured_pages:
            raise ValueError("compact_pages requires structured_pages")
        filesystem_path = validate_filesystem_path(
            filesystem_path or f"/{database}", database
        )
    source_dir = Path(__file__).resolve().parent
    executable_name = "iotdb-sql" if arm == "sql" else "iotdb-fs"
    if iotdb_cli is None:
        dsh_path = Path(dsh_command)
        if dsh_path.is_absolute():
            iotdb_cli = dsh_path.parent / executable_name
        else:
            iotdb_cli = shutil.which(executable_name)
    if iotdb_cli is None or not Path(iotdb_cli).is_absolute():
        raise ValueError("controlled baseline requires an absolute IoTDB CLI path")
    dsh_path = Path(dsh_command)
    if not dsh_path.is_absolute():
        resolved_dsh = shutil.which(str(dsh_command))
        if resolved_dsh is None:
            raise ValueError("controlled baseline requires a resolvable DSH executable")
        dsh_path = Path(resolved_dsh)
    experiment_root = dsh_path.resolve().parent.parent
    tools_module = (
        experiment_root
        / "runtime"
        / "agent"
        / "node_modules"
        / "@deepseek-ai"
        / "dsh-tools"
        / "lib"
        / "index.js"
    )
    environment = os.environ.copy()
    environment.update(
        {
            "EXP_DSH_TOOL_MODE": arm,
            "EXP_DSH_TOOLS_MODULE": tools_module.as_uri(),
            "EXP_TASK_DATABASE": database,
            "EXP_DSH_TOOL_RUNNER": str(source_dir / "dsh_tool_runner.py"),
            "EXP_IOTDB_TOOL_EXECUTABLE": str(Path(iotdb_cli)),
            "EXP_DSH_TOOL_OUTPUT_ROOT": str(Path(task_dir).resolve() / "tool-output"),
            "EXP_DSH_TOOL_PYTHON": str(Path(tool_python or sys.executable).resolve()),
        }
    )
    if arm == "filesystem":
        environment["EXP_TASK_FS_PATH"] = filesystem_path
        environment["EXP_FS_STRUCTURED_PAGES"] = str(structured_pages).lower()
        environment["EXP_FS_COMPACT_PAGES"] = str(compact_pages).lower()
        environment["EXP_FS_FILTERED_STATS"] = str(filtered_stats).lower()
    return source_dir / "dsh-baseline.patch.yml", environment


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsh", default="dsh", help="DSH executable or wrapper")
    parser.add_argument("--profile", default="headless")
    parser.add_argument("--patch", action="append", default=[], help="base DSH patch")
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prompt-file", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument(
        "--baseline",
        choices=("sql", "filesystem"),
        help="expose exactly one controlled IoTDB interface tool",
    )
    parser.add_argument("--database", help="task database for the controlled tool")
    parser.add_argument(
        "--filesystem-path",
        help="fixed virtual object path for a filesystem baseline",
    )
    parser.add_argument(
        "--fs-output-mode",
        choices=("raw", "page", "compact", "filtered-stats"),
        default="page",
        help="filesystem result mode used for output-format ablations",
    )
    parser.add_argument("--iotdb-cli", type=Path, help="absolute SQL/FS CLI wrapper")
    parser.add_argument("--tool-python", type=Path, help="Python for the tool runner")
    args = parser.parse_args(argv)
    patches = list(args.patch)
    environment = None
    expected_tool = None
    max_tool_calls = None
    if args.baseline:
        if not args.database:
            parser.error("--database is required with --baseline")
        patch, environment = controlled_baseline(
            args.baseline,
            args.database,
            args.output,
            args.dsh,
            args.iotdb_cli,
            args.tool_python,
            args.filesystem_path,
            args.fs_output_mode != "raw",
            args.fs_output_mode in {"compact", "filtered-stats"},
            args.fs_output_mode == "filtered-stats",
        )
        patches.append(patch)
        expected_tool = "iotdb_sql" if args.baseline == "sql" else "iotdb_fs"
        max_tool_calls = 1 if args.baseline == "sql" else None
    elif args.database or args.filesystem_path or args.iotdb_cli or args.tool_python:
        parser.error(
            "--database/--filesystem-path/--iotdb-cli/--tool-python require --baseline"
        )
    if args.filesystem_path and args.baseline != "filesystem":
        parser.error("--filesystem-path requires --baseline filesystem")
    client = DshClient(
        args.dsh,
        args.profile,
        patches,
        args.workspace,
        expected_tool=expected_tool,
        max_tool_calls=max_tool_calls,
    )
    result = client.run_task(
        args.prompt_file.read_text(encoding="utf-8"),
        args.output,
        args.timeout,
        environment,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())
