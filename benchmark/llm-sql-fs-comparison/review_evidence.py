#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Conservative offline evidence review of model-visible IoTDB observations.

This recognizes direct field reads and explicit aggregate expressions only.
Unsupported or ambiguous query plans remain evidence_pending. No raw output
beyond the model-visible response prefix is used, and protocol validity is
independent of evidence sufficiency.
"""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal, InvalidOperation
import io
import json
from pathlib import Path
import re
import shlex

from fixture import compare_answer, expected_answer, expected_schema
from tool_adapter import table_rows


ROW_TASKS = {"K01", "K02", "K03", "K04", "D03"}
FIELDS = {"time", "device", "temperature", "humidity", "status"}


def strict_json(text):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("重复 JSON key")
            result[key] = value
        return result

    def constant(value):
        raise ValueError("非有限 JSON 数字：" + value)

    return json.loads(text, object_pairs_hook=pairs, parse_constant=constant)


def parse_output(stdout, arm):
    """Read only the response stdout actually visible to the model."""
    if not isinstance(stdout, str) or not stdout.strip():
        raise ValueError("无可解析的模型可见输出")
    if arm == "sql":
        return table_rows(stdout)
    lines = [line for line in stdout.splitlines() if line.strip()]
    if all(line.lstrip().startswith("{") for line in lines):
        rows = [strict_json(line) for line in lines]
        if not all(isinstance(row, dict) for row in rows):
            raise ValueError("NDJSON 行不是对象")
        return rows
    if any(line.startswith("|") for line in lines):
        cells = [
            [value.strip() for value in line.strip().strip("|").split("|")]
            for line in lines
            if line.startswith("|")
        ]
        header, *data = cells
        if len(set(header)) != len(header) or any(
            len(row) != len(header) for row in data
        ):
            raise ValueError("表格列数或表头不合法")
        return [dict(zip(header, row)) for row in data if row != header]
    values = list(csv.reader(io.StringIO(stdout), strict=True))
    if not values:
        raise ValueError("CSV 为空")
    header, *data = values
    data = [row for row in data if row]
    if len(set(header)) != len(header) or any(len(row) != len(header) for row in data):
        raise ValueError("CSV 列数或表头不合法")
    return [dict(zip(header, row)) for row in data]


def cast_value(value, expected):
    if type(expected) is bool:
        if type(value) is bool:
            return value
        if isinstance(value, str) and value.lower() in {"true", "false"}:
            return value.lower() == "true"
        raise ValueError("观察值不是布尔值")
    if type(expected) in {int, float}:
        if value is None or isinstance(value, bool):
            raise ValueError("观察值不是数值")
        try:
            number = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError("观察值不是数值") from exc
        if not number.is_finite():
            raise ValueError("观察值非有限")
        if type(expected) is int:
            if number != number.to_integral_value():
                raise ValueError("观察值不是整数")
            return int(number)
        return float(number)
    if type(value) is not type(expected):
        raise ValueError("观察值类型不匹配")
    return value


def projected(row, expected):
    return {key: cast_value(row[key], value) for key, value in expected.items()}


def matches(row, expected, task_id="evidence"):
    try:
        return compare_answer(projected(row, expected), expected, task_id)[0]
    except (KeyError, ValueError, TypeError, OverflowError):
        return False


def sql_plan(command, database, target):
    """Parse a deliberately small, single-table SELECT subset; reject others."""
    text = command.strip().rstrip(";").strip()
    table = re.escape(database) + r"\." + re.escape(target)
    match = re.fullmatch(r"(?is)SELECT\s+(.*?)\s+FROM\s+" + table + r"\s*(.*)", text)
    if not match:
        return None
    select, rest = match.groups()
    if re.search(
        r"(?i)\b(SELECT|JOIN|UNION|INTERSECT|EXCEPT|HAVING|INTO|WITH)\b", rest
    ):
        return None
    clauses = {}
    pattern = re.compile(r"(?i)\b(WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|OFFSET)\b")
    markers = list(pattern.finditer(rest))
    if not markers and rest:
        return None
    if markers and rest[: markers[0].start()].strip():
        return None
    for index, marker in enumerate(markers):
        key = re.sub(r"\s+", " ", marker.group().upper())
        if key in clauses:
            return None
        end = markers[index + 1].start() if index + 1 < len(markers) else len(rest)
        clauses[key] = rest[marker.end() : end].strip()
    # No quoted literals containing commas are allowed in this supported subset.
    expressions, current, depth = [], [], 0
    for char in select:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            expressions.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        if depth < 0:
            return None
    expressions.append("".join(current).strip())
    if depth:
        return None
    selected = {}
    for expression in expressions:
        alias = re.fullmatch(r"(?is)(.*?)\s+AS\s+([a-z_][a-z_0-9]*)", expression)
        if alias:
            body, name = alias.groups()
        elif re.fullmatch(r"[a-z_][a-z_0-9]*|\*", expression, re.I):
            body, name = expression, expression
        else:
            return None
        name = name.lower()
        if name in selected:
            return None
        selected[name] = re.sub(r"\s+", "", body).lower()
    return {"selected": selected, **clauses}


def fs_tokens(command):
    lexer = shlex.shlex(command, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    return list(lexer)


def simple_fs_call(call, kind):
    """Aggregate/schema commands must use their fixed whole-table scope."""
    if call.get("validation", {}).get("kind") != kind:
        return False
    tokens = fs_tokens(call["command"])
    forbidden = {
        "--start",
        "--end",
        "--offset",
        "--tag-filter",
        "--tag-match",
        "-d",
        "--device",
        "-t",
        "--table",
        "-n",
        "--limit",
    }
    return (
        tokens
        and tokens[0] == kind
        and not any(token.split("=", 1)[0] in forbidden for token in tokens[1:])
    )


def read_calls(directory, trial):
    observations = []
    for path in sorted(
        (directory / "trials" / trial["trial_id"] / "tools").glob("*/call.json")
    ):
        call = json.loads(path.read_text(encoding="utf-8"))
        response = call.get("response", {})
        if (
            response.get("exit_code") != 0
            or response.get("error_kind")
            or response.get("timed_out")
            or response.get("truncated")
            or trial["target_table"] not in call.get("validation", {}).get("tables", [])
        ):
            continue
        try:
            parsed = parse_output(response.get("stdout"), trial["arm"])
        except (ValueError, TypeError, KeyError, csv.Error):
            continue
        call["observed_rows"] = parsed
        call["evidence_file"] = str(path.relative_to(directory))
        call["sql_plan"] = (
            sql_plan(call["command"], trial["database"], trial["target_table"])
            if trial["arm"] == "sql"
            else None
        )
        observations.append(call)
    return observations


def direct_data(call, arm):
    if arm == "filesystem":
        return call.get("validation", {}).get("kind") in {"cat", "head", "tail"}
    plan = call["sql_plan"]
    return (
        plan is not None
        and (
            plan["selected"] == {"*": "*"}
            or all(
                key in FIELDS and body == key for key, body in plan["selected"].items()
            )
        )
        and "GROUP BY" not in plan
    )


def latest_scope(call, arm):
    if arm == "sql":
        plan = call["sql_plan"]
        return (
            plan is not None
            and re.fullmatch(r"(?i)device\s*=\s*'device_2'", plan.get("WHERE", ""))
            is not None
            and re.fullmatch(
                r"(?i)time\s+DESC(?:\s*,\s*device(?:\s+ASC)?)?",
                plan.get("ORDER BY", ""),
            )
            is not None
            and plan.get("LIMIT") == "3"
            and plan.get("OFFSET", "0") == "0"
        )
    tokens = fs_tokens(call["command"])
    if not tokens or tokens[0] != "tail":
        return False
    # Recognize the documented spelling only; aliases remain available for manual review.
    try:
        limit = tokens[tokens.index("-n") + 1]
        marker = tokens.index("--tag-filter")
    except (ValueError, IndexError):
        return False
    return (
        limit == "3"
        and tokens[marker + 1 : marker + 4] == ["device", "eq", "device_2"]
        and tokens.count("--tag-filter") == 1
        and "--format" in tokens
        and not any(
            token.split("=", 1)[0]
            in {"--start", "--end", "--offset", "--tag-match", "-f"}
            for token in tokens
        )
    )


def target_identity(calls, arm):
    wanted = expected_schema()
    for call in calls:
        rows = call["observed_rows"]
        if call.get("validation", {}).get("kind") == "schema":
            try:
                if arm == "sql":
                    columns = [
                        {
                            "name": row["ColumnName"],
                            "type": row["DataType"],
                            "category": "TIME"
                            if row["ColumnName"] == "time"
                            else row["Category"],
                        }
                        for row in rows
                    ]
                else:
                    columns = [
                        {
                            "name": row["column"],
                            "type": row["data_type"],
                            "category": row["category"],
                        }
                        for row in rows
                    ]
                if sorted(columns, key=lambda row: row["name"]) == sorted(
                    wanted, key=lambda row: row["name"]
                ):
                    return True
            except KeyError:
                pass
        if direct_data(call, arm) and any(FIELDS <= set(row) for row in rows):
            return True
    return False


def whole_scope(plan, field=None, grouped=False):
    if plan is None or any(key in plan for key in {"LIMIT", "OFFSET"}):
        return False
    where = re.sub(r"\s+", "", plan.get("WHERE", "")).lower()
    if where and where != (field or "") + "isnotnull":
        return False
    group = re.sub(r"\s+", "", plan.get("GROUP BY", "")).lower()
    return group == ("device" if grouped else "")


def select_contract(plan, expected_expressions):
    return all(
        plan["selected"].get(key) in expressions
        for key, expressions in expected_expressions.items()
    )


def sql_statistics(call, task_id, expected):
    plan = call["sql_plan"]
    rows = call["observed_rows"]
    if task_id in {"K05", "D04"}:
        if not whole_scope(plan):
            return False
        contract = {
            "row_count": {"count(*)"},
            "device_count": {"count(distinctdevice)"},
            "min_time": {"min(time)"},
            "max_time": {"max(time)"},
            "temperature_non_null": {"count(temperature)"},
            "temperature_null": {"count(*)-count(temperature)"},
        }
        return (
            select_contract(plan, contract)
            and len(rows) == 1
            and matches(rows[0], expected)
        )
    if task_id in {"K06", "K08"}:
        field = "temperature" if task_id == "K06" else "humidity"
        if not whole_scope(plan, field, grouped=task_id == "K06"):
            return False
        contract = {
            name: {f"{fn}({field})"}
            for name, fn in (
                ("count", "count"),
                ("min", "min"),
                ("max", "max"),
                ("sum", "sum"),
                ("mean", "avg"),
            )
        }
        if task_id == "K06":
            contract["device"] = {"device"}
        if not select_contract(plan, contract):
            return False
        wanted = expected["devices"] if task_id == "K06" else [expected]
        return len(rows) == len(wanted) and all(
            any(matches(row, want) for row in rows) for want in wanted
        )
    if task_id == "K07":
        if not whole_scope(plan, grouped=True):
            return False
        contract = {
            "device": {"device"},
            "true_count": {
                "sum(casewhenstatus=truethen1else0end)",
                "count(casewhenstatus=truethen1end)",
            },
            "false_count": {
                "sum(casewhenstatus=falsethen1else0end)",
                "count(casewhenstatus=falsethen1end)",
            },
            "null_count": {
                "sum(casewhenstatusisnullthen1else0end)",
                "count(*)-count(status)",
            },
        }
        return (
            select_contract(plan, contract)
            and len(rows) == len(expected["devices"])
            and all(
                any(matches(row, want) for row in rows) for want in expected["devices"]
            )
        )
    return False


def fs_statistics(call, task_id, expected, target):
    rows = [
        row
        for row in call["observed_rows"]
        if row.get("object") == target and row.get("model") == "table"
    ]
    try:
        if task_id in {"K05", "D04"} and simple_fs_call(call, "count"):
            selected = [row for row in rows if row.get("column") == "temperature"]
            if len(selected) != 1:
                return False
            observed = selected[0]
            mapped = {
                "row_count": observed["row_count"],
                "device_count": observed["entity_count"],
                "min_time": observed["min_time"],
                "max_time": observed["max_time"],
                "temperature_non_null": observed["non_null_count"],
                "temperature_null": observed["null_count"],
            }
            return observed.get("time_source") == "scan" and matches(mapped, expected)
        if not simple_fs_call(call, "stats"):
            return False
        field = {"K06": "temperature", "K07": "status", "K08": "humidity"}.get(task_id)
        rows = [
            row
            for row in rows
            if row.get("field") == field and row.get("stats_source") == "scan"
        ]
        devices = [row["tag.device"] for row in rows]
        if sorted(devices) != [f"device_{index}" for index in range(4)]:
            return False
        if task_id == "K06":
            derived = []
            for row in rows:
                count = cast_value(row["non_null_count"], 0)
                total = cast_value(row["sum"], 0.0)
                derived.append(
                    {
                        "device": row["tag.device"],
                        "count": count,
                        "min": cast_value(row["min"], 0.0),
                        "max": cast_value(row["max"], 0.0),
                        "sum": total,
                        "mean": total / count,
                    }
                )
            return compare_answer(
                {"devices": sorted(derived, key=lambda row: row["device"])},
                expected,
                task_id,
            )[0]
        if task_id == "K07":
            derived = []
            for row in rows:
                if row.get("data_type") != "BOOLEAN":
                    return False
                true_count = cast_value(row["sum"], 0)
                non_null = cast_value(row["non_null_count"], 0)
                derived.append(
                    {
                        "device": row["tag.device"],
                        "true_count": true_count,
                        "false_count": non_null - true_count,
                        "null_count": cast_value(row["null_count"], 0),
                    }
                )
            return compare_answer(
                {"devices": sorted(derived, key=lambda row: row["device"])},
                expected,
                task_id,
            )[0]
        if task_id == "K08":
            count = sum(cast_value(row["non_null_count"], 0) for row in rows)
            total = sum(cast_value(row["sum"], 0.0) for row in rows)
            derived = {
                "count": count,
                "min": min(cast_value(row["min"], 0.0) for row in rows),
                "max": max(cast_value(row["max"], 0.0) for row in rows),
                "sum": total,
                "mean": total / count,
            }
            return compare_answer(derived, expected, task_id)[0]
    except (KeyError, ValueError, TypeError, ZeroDivisionError, OverflowError):
        return False
    return False


def review_trial(directory, trial, n):
    task_id, arm = trial["task_id"], trial["arm"]
    expected = expected_answer(task_id, n, trial["target_table"])
    try:
        final = strict_json(trial.get("final_text", ""))
    except (ValueError, TypeError):
        return {"status": "evidence_pending", "reason": "审核时最终 JSON 无法独立解析"}
    if (
        trial.get("answer_correct") is not True
        or not compare_answer(final, expected, task_id)[0]
    ):
        return {"status": "evidence_pending", "reason": "审核时最终答案未通过独立完整 oracle，需核查评分"}
    calls = read_calls(directory, trial)
    if task_id in {"D03", "D04"} and not target_identity(calls, arm):
        return {"status": "evidence_pending", "reason": "需人工确认目标表完整 signature 的观察依据"}
    if task_id in ROW_TASKS:
        usable = [call for call in calls if direct_data(call, arm)]
        if task_id == "K04":
            usable = [call for call in usable if latest_scope(call, arm)]
        observed = [row for call in usable for row in call["observed_rows"]]
        if all(
            any(matches(row, want) for row in observed) for want in expected["rows"]
        ):
            return {
                "status": "success",
                "reason": "模型可见的完整成功观察含全部所需行及字段；"
                + ("最近三条的尾部/降序查询范围已核对；" if task_id == "K04" else "")
                + "依据："
                + ", ".join(call["evidence_file"] for call in usable),
            }
        return {"status": "evidence_pending", "reason": "未自动确认全部记录字段或最近三条查询范围；需人工核对原轨迹"}
    metrics = {key: value for key, value in expected.items() if key != "table"}
    for call in calls:
        verified = (
            sql_statistics(call, task_id, metrics)
            if arm == "sql"
            else fs_statistics(call, task_id, metrics, trial["target_table"])
        )
        if verified:
            return {
                "status": "success",
                "reason": "已核对真实全表范围、完整分组及聚合表达式/统计字段；"
                "最终指标均能由模型可见观察得到。依据：" + call["evidence_file"],
            }
    return {"status": "evidence_pending", "reason": "复杂查询、别名或合并多次观察尚未自动证明充分；保留人工审核"}


def review(result_dir):
    directory = Path(result_dir).resolve()
    metadata = json.loads((directory / "run.json").read_text(encoding="utf-8"))
    n = metadata.get("points_per_device")
    if type(n) is not int or n <= 0:
        raise ValueError("run.json 必须记录 points_per_device")
    raw = (directory / "trials.jsonl").read_text(encoding="utf-8")
    trials = [strict_json(line) for line in raw.splitlines() if line.strip()]
    output = directory / "evidence_review.json"
    existing = (
        strict_json(output.read_text(encoding="utf-8")) if output.exists() else {}
    )
    if not isinstance(existing, dict):
        raise ValueError("已有审核文件不是对象映射")
    pending_ids = {
        trial["trial_id"]
        for trial in trials
        if trial.get("status") == "evidence_pending"
    }
    if len({trial["trial_id"] for trial in trials}) != len(trials):
        raise ValueError("重复 trial_id，拒绝覆盖审核记录")
    if set(existing) - pending_ids:
        raise ValueError("已有审核文件含非 evidence_pending 或本轮不存在的 trial")
    if any(
        not isinstance(value, dict)
        or value.get("status")
        not in {"success", "insufficient_evidence", "evidence_pending"}
        or not isinstance(value.get("reason"), str)
        or not value["reason"].strip()
        for value in existing.values()
    ):
        raise ValueError("已有审核记录状态或理由不合法")
    decisions = dict(existing)
    for trial in trials:
        if trial.get("status") != "evidence_pending":
            continue
        if (
            trial["trial_id"] in existing
            and existing[trial["trial_id"]].get("status") != "evidence_pending"
        ):
            continue
        decisions[trial["trial_id"]] = review_trial(directory, trial, n)
    output.write_text(
        json.dumps(decisions, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return {
        "review_file": str(output),
        "success": sum(value["status"] == "success" for value in decisions.values()),
        "pending": sum(
            value["status"] == "evidence_pending" for value in decisions.values()
        ),
        "protocol_status": metadata.get("status"),
        "note": "证据核验不改变协议有效性；protocol_invalid 运行仍不可用于接口优劣结论",
    }


def main():
    parser = argparse.ArgumentParser(description="离线核对模型实际可见的数据库观察")
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()
    print(json.dumps(review(args.result_dir), ensure_ascii=False))


if __name__ == "__main__":
    main()
