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

"""Private fixture, independent answer oracle, and public task rendering.

Never provide this module, its fixture rows, or the whole tasks.json to a model.
Only render_task's explicit public allowlist is suitable for model input.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping


TABLE_NAMES = ("sample_17", "sample_29", "sample_43")
ROLE_COLUMNS = {
    "target": ("time", "device", "temperature", "humidity", "status"),
    "decoy_a": ("time", "device", "temperature", "status"),
    "decoy_b": ("time", "device", "humidity", "status"),
}
SCHEMA = (
    {"name": "time", "type": "TIMESTAMP", "category": "TIME"},
    {"name": "device", "type": "STRING", "category": "TAG"},
    {"name": "temperature", "type": "DOUBLE", "category": "FIELD"},
    {"name": "humidity", "type": "DOUBLE", "category": "FIELD"},
    {"name": "status", "type": "BOOLEAN", "category": "FIELD"},
)
ABS_TOL = 1e-6
REL_TOL = 1e-8


def load_tasks() -> dict[str, Any]:
    return json.loads(
        Path(__file__).with_name("tasks.json").read_text(encoding="utf-8")
    )


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError("数据库或表名必须是小写字母开头的字母、数字或下划线")
    return value


def _point_count(n: int) -> None:
    if type(n) is not int or n <= 0:
        raise ValueError("每设备时间点数必须是正整数")


def expected_schema(role: str = "target") -> list[dict[str, str]]:
    columns = ROLE_COLUMNS[role]
    return [dict(column) for column in SCHEMA if column["name"] in columns]


def _decimal_row(device: int, point: int) -> dict[str, Any]:
    return {
        "time": point,
        "device": f"device_{device}",
        "temperature": Decimal(20 + device) + Decimal(point) / 100,
        "humidity": Decimal(40) + Decimal(point % 100) / 10,
        "status": point % 2 == 0,
    }


def _answer_row(device: int, point: int) -> dict[str, Any]:
    return {
        key: float(value) if isinstance(value, Decimal) else value
        for key, value in _decimal_row(device, point).items()
    }


def canonical_rows(n: int, role: str = "target") -> Iterable[dict[str, Any]]:
    """Yield exact Decimal-valued rows in ORDER BY time, device order."""
    _point_count(n)
    columns = ROLE_COLUMNS[role]
    for point in range(n):
        for device in range(4):
            row = _decimal_row(device, point)
            yield {column: row[column] for column in columns}


def _canonical_value(column: str, value: Any) -> Any:
    """Normalize JDBC string values and fixture values without rounding data."""
    if column == "device":
        if not isinstance(value, str):
            raise ValueError("device 必须是字符串")
        return value
    if column == "status":
        if type(value) is bool:
            return value
        if isinstance(value, str) and value.lower() in {"true", "false"}:
            return value.lower() == "true"
        raise ValueError("status 必须是布尔值")
    if value is None or isinstance(value, bool):
        raise ValueError(f"{column} 必须是非空数值")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{column} 必须是数值") from exc
    if not number.is_finite():
        raise ValueError(f"{column} 不得是非有限数")
    if column == "time":
        if number != number.to_integral_value():
            raise ValueError("time 必须是毫秒整数")
        return int(number)
    return format(number.normalize(), "f")


def canonical_hash(rows: Iterable[Mapping[str, Any]], role: str = "target") -> str:
    """Hash every row; input must have exact columns and strict time/device order.

    No float tolerance is applied: Decimal normalization only removes spelling
    differences such as 20.00 versus 20. The hash includes field names per row.
    """
    columns = ROLE_COLUMNS[role]
    digest = hashlib.sha256()
    previous = None
    for row in rows:
        if set(row) != set(columns):
            raise ValueError("数据列与预期 schema 不符")
        normalized = {name: _canonical_value(name, row[name]) for name in columns}
        key = normalized["time"], normalized["device"]
        if previous is not None and key <= previous:
            raise ValueError("数据必须按 time/device 严格升序，且不能有重复行")
        previous = key
        digest.update(
            json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
        )
        digest.update(b"\n")
    return digest.hexdigest()


def fixture_hash(n: int, role: str = "target") -> str:
    return canonical_hash(canonical_rows(n, role), role)


def fixture_sql(
    database: str,
    n: int,
    role_map: Mapping[str, str],
    batch_size: int = 200,
) -> Iterable[str]:
    """Create a fresh isolated database and populate three exact role projections.

    Creation intentionally has no IF NOT EXISTS and never drops existing data.
    A name collision is an error for the caller to handle before trial timing.
    """
    _identifier(database)
    if not database.startswith("llm_benchmark"):
        raise ValueError("只能生成 llm_benchmark 前缀的独立试验库")
    _point_count(n)
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("写入批大小必须是正整数")
    if set(role_map) != set(ROLE_COLUMNS) or sorted(role_map.values()) != list(
        TABLE_NAMES
    ):
        raise ValueError("role_map 必须是三个固定表名到 target/decoy_a/decoy_b 的置换")
    yield f"CREATE DATABASE {database}"
    for role in ROLE_COLUMNS:
        table = _identifier(role_map[role])
        columns = ROLE_COLUMNS[role]
        definitions = ", ".join(
            f"{column['name']} {column['type']} {column['category']}"
            for column in expected_schema(role)
            if column["name"] != "time"
        )
        yield f"CREATE TABLE {database}.{table} ({definitions})"
        values = []
        prefix = f"INSERT INTO {database}.{table} ({', '.join(columns)}) VALUES "
        for row in canonical_rows(n, role):
            fields = []
            for name in columns:
                value = row[name]
                if name == "device":
                    fields.append(f"'{value}'")
                elif type(value) is bool:
                    fields.append("true" if value else "false")
                else:
                    fields.append(str(value))
            values.append("(" + ", ".join(fields) + ")")
            if len(values) == batch_size:
                yield prefix + ", ".join(values)
                values = []
        if values:
            yield prefix + ", ".join(values)


def expected_answer(task_id: str, n: int, target_table: str) -> dict[str, Any]:
    """Compute task answers independently, without executing manifest formulas."""
    _point_count(n)
    _identifier(target_table)
    if task_id in {"K01", "D03"}:
        if n <= 731:
            raise ValueError("该任务要求时间戳 731 存在")
        result = {"rows": [_answer_row(2, 731)]}
        return {"table": target_table, **result} if task_id == "D03" else result
    if task_id == "K02":
        if n <= 507:
            raise ValueError("该任务要求时间戳 507 存在")
        return {
            "rows": [
                {
                    key: value
                    for key, value in _answer_row(1, point).items()
                    if key != "status"
                }
                for point in range(501, 508)
            ]
        }
    if task_id == "K03":
        if n <= 777:
            raise ValueError("该任务要求时间戳 777 存在")
        return {
            "rows": [
                {
                    key: value
                    for key, value in _answer_row(device, 777).items()
                    if key in {"time", "device", "temperature"}
                }
                for device in (0, 3)
            ]
        }
    if task_id == "K04":
        if n < 3:
            raise ValueError("该任务至少需要三个时间点")
        return {"rows": [_answer_row(2, point) for point in range(n - 3, n)]}
    if task_id in {"K05", "D04"}:
        result = {
            "row_count": 4 * n,
            "device_count": 4,
            "min_time": 0,
            "max_time": n - 1,
            "temperature_non_null": 4 * n,
            "temperature_null": 0,
        }
        return {"table": target_table, **result} if task_id == "D04" else result
    if task_id == "K06":
        devices = []
        for device in range(4):
            total = Decimal(n * (20 + device)) + Decimal(n * (n - 1)) / 200
            devices.append(
                {
                    "device": f"device_{device}",
                    "count": n,
                    "min": float(20 + device),
                    "max": float(Decimal(20 + device) + Decimal(n - 1) / 100),
                    "sum": float(total),
                    "mean": float(total / n),
                }
            )
        return {"devices": devices}
    if task_id == "K07":
        return {
            "devices": [
                {
                    "device": f"device_{device}",
                    "true_count": (n + 1) // 2,
                    "false_count": n // 2,
                    "null_count": 0,
                }
                for device in range(4)
            ]
        }
    if task_id == "K08":
        if n % 100:
            raise ValueError("湿度任务当前要求 N 是 100 的倍数")
        return {
            "count": 4 * n,
            "min": 40.0,
            "max": 49.9,
            "sum": float(Decimal(4 * n) * Decimal("44.95")),
            "mean": 44.95,
        }
    if task_id == "D01":
        return {"tables": list(TABLE_NAMES)}
    if task_id == "D02":
        return {"table": target_table, "columns": expected_schema()}
    raise ValueError(f"未知任务：{task_id}")


def compare_answer(actual: Any, expected: Any, task_id: str) -> tuple[bool, str]:
    """Strict recursive final-answer scoring; only D02 column order is ignored.

    This function scores content. Evidence and time/tool budgets are separately
    checked by the trial runner; a content match alone is not trial success.
    """
    if task_id == "D02" and isinstance(actual, dict) and isinstance(expected, dict):
        actual, expected = dict(actual), dict(expected)
        for value in (actual, expected):
            columns = value.get("columns")
            if isinstance(columns, list) and all(
                isinstance(column, dict) and isinstance(column.get("name"), str)
                for column in columns
            ):
                value["columns"] = sorted(columns, key=lambda column: column["name"])

    def check(got: Any, want: Any, path: str) -> str:
        if isinstance(want, dict):
            if not isinstance(got, dict):
                return f"{path} 必须是 JSON 对象"
            if set(got) != set(want):
                return f"{path} 字段缺失或含额外字段"
            for key in want:
                failure = check(got[key], want[key], f"{path}.{key}")
                if failure:
                    return failure
        elif isinstance(want, list):
            if not isinstance(got, list) or len(got) != len(want):
                return f"{path} 数组长度不符"
            for index, (left, right) in enumerate(zip(got, want)):
                failure = check(left, right, f"{path}[{index}]")
                if failure:
                    return failure
        elif type(want) is bool:
            if type(got) is not bool or got != want:
                return f"{path} 布尔值不符"
        elif type(want) is int:
            if type(got) is not int or got != want:
                return f"{path} 整数值或类型不符"
        elif isinstance(want, float):
            if type(got) not in {int, float}:
                return f"{path} 必须是 JSON 数字"
            try:
                matches = math.isfinite(got) and abs(got - want) <= max(
                    ABS_TOL, REL_TOL * abs(want)
                )
            except OverflowError:
                matches = False
            if not matches:
                return f"{path} 数值超出容差或为非有限数"
        elif type(got) is not type(want) or got != want:
            return f"{path} 值或类型不符"
        return ""

    reason = check(actual, expected, "$")
    return not reason, reason or "答案符合完整 oracle"


def answer_json_schema(task: Mapping[str, Any]) -> dict[str, Any]:
    """Compile the public type-shape DSL, never using private answer values."""

    def convert(shape: Any) -> dict[str, Any]:
        if isinstance(shape, dict):
            return {
                "type": "object",
                "properties": {key: convert(value) for key, value in shape.items()},
                "required": list(shape),
                "additionalProperties": False,
            }
        if isinstance(shape, list) and len(shape) == 1:
            return {"type": "array", "items": convert(shape[0])}
        if isinstance(shape, str) and shape in {
            "string",
            "number",
            "integer",
            "boolean",
            "null",
        }:
            return {"type": shape}
        raise ValueError("答案结构包含不支持的类型")

    return convert(task["public"]["answer_shape"])


def render_task(task: Mapping[str, Any], database: str, target_table: str) -> str:
    """Render only public fields; discovery tasks never receive target_table."""
    _identifier(database)
    _identifier(target_table)
    if task["information"] not in {"known_schema", "discovery"}:
        raise ValueError("未知任务信息条件")
    public = {
        "database": database,
        "time_unit": "milliseconds",
        "table_names": "返回不含数据库前缀或 .csv/.meta 后缀的逻辑表名。",
        "schema_categories": "隐含 time 列表示为 time/TIMESTAMP/TIME；其他列使用 TAG/FIELD。",
        "goal": task["public"]["goal"],
        "answer_json_schema": answer_json_schema(task),
    }
    if task["information"] == "known_schema":
        public["target_table"] = target_table
        public["columns"] = expected_schema()
    return json.dumps(public, ensure_ascii=False, indent=2)
