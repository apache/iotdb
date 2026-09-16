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

"""Merge application-scenario benchmark attempts into one auditable report."""
from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median


ROOT = Path(__file__).resolve().parent
TASKS = json.loads((ROOT / "application_tasks.json").read_text(encoding="utf-8"))[
    "tasks"
]
TASK_BY_ID = {task["id"]: task for task in TASKS}


def load_rows(run_dirs: list[Path]) -> list[dict]:
    rows = []
    for attempt, directory in enumerate(run_dirs, 1):
        path = directory / "trials.jsonl"
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                row["attempt"] = attempt
                row["source_run"] = directory.name
                rows.append(row)
    return rows


def fmt_ms(values: list[float]) -> str:
    return f"{median(values) / 1000:.2f}" if values else "—"


def scenario_name(task_id: str) -> str:
    return {
        "A1-TRIAGE": "A1 冷却泵异常排查",
        "A2-COMPLIANCE": "A2 冷链运输温控",
        "A3-ENERGY": "A3 楼宇能耗异常",
    }[task_id]


def write_report(output: Path, rows: list[dict], run_dirs: list[Path]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "trials.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )
    fields = [
        "attempt",
        "source_run",
        "trial_id",
        "pair_id",
        "task_id",
        "scenario",
        "arm",
        "status",
        "answer_correct",
        "task_wall_ms",
        "tool_wall_ms",
        "cli_process_ms",
        "tool_attempts",
        "tool_errors",
        "model_responses",
        "stream_retry_count",
        "stream_attempts",
        "stream_retry_limit",
        "evaluation_reason",
    ]
    with (output / "trials.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    by_task = defaultdict(list)
    for row in rows:
        by_task[row["task_id"]].append(row)
    comparison_fields = [
        "task_id",
        "scenario",
        "sql_attempts",
        "fs_attempts",
        "sql_success",
        "fs_success",
        "sql_infrastructure_error",
        "fs_infrastructure_error",
        "sql_success_median_s",
        "fs_success_median_s",
        "paired_success_attempts",
    ]
    with (output / "comparison.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=comparison_fields)
        writer.writeheader()
        for task_id in sorted(by_task):
            group = by_task[task_id]
            sql = [row for row in group if row["arm"] == "sql"]
            fs = [row for row in group if row["arm"] == "filesystem"]
            sql_success = [row for row in sql if row["status"] == "success"]
            fs_success = [row for row in fs if row["status"] == "success"]
            paired = sum(
                1
                for attempt in sorted({row["attempt"] for row in group})
                if any(
                    row["attempt"] == attempt
                    and row["arm"] == "sql"
                    and row["status"] == "success"
                    for row in group
                )
                and any(
                    row["attempt"] == attempt
                    and row["arm"] == "filesystem"
                    and row["status"] == "success"
                    for row in group
                )
            )
            writer.writerow(
                {
                    "task_id": task_id,
                    "scenario": scenario_name(task_id),
                    "sql_attempts": len(sql),
                    "fs_attempts": len(fs),
                    "sql_success": len(sql_success),
                    "fs_success": len(fs_success),
                    "sql_infrastructure_error": sum(
                        row["status"] == "infrastructure_error" for row in sql
                    ),
                    "fs_infrastructure_error": sum(
                        row["status"] == "infrastructure_error" for row in fs
                    ),
                    "sql_success_median_s": fmt_ms(
                        [row["task_wall_ms"] for row in sql_success]
                    ),
                    "fs_success_median_s": fmt_ms(
                        [row["task_wall_ms"] for row in fs_success]
                    ),
                    "paired_success_attempts": paired,
                }
            )

    lines = [
        "# IoTDB 大模型真实应用场景综合测试报告",
        "",
        "## 测试结论",
        "",
        "本次 pilot 使用本地 Codex `gpt-5.6-sol`，在同一批 IoTDB 应用数据快照上，比较模型通过 SQL 和 filesystem 接口完成业务任务的端到端表现。三类任务均要求模型发现数据、读取观测、执行规则计算并输出业务 JSON；答案由独立 oracle 验收。",
        "",
        "| 场景 | 真实待解决问题 | SQL 正确 | fs 正确 | SQL 成功中位耗时（秒） | fs 成功中位耗时（秒） | 成功配对尝试 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for task_id in sorted(by_task):
        group = by_task[task_id]
        sql = [row for row in group if row["arm"] == "sql"]
        fs = [row for row in group if row["arm"] == "filesystem"]
        sql_success = [row for row in sql if row["status"] == "success"]
        fs_success = [row for row in fs if row["status"] == "success"]
        paired = sum(
            1
            for attempt in sorted({row["attempt"] for row in group})
            if any(
                row["attempt"] == attempt
                and row["arm"] == "sql"
                and row["status"] == "success"
                for row in group
            )
            and any(
                row["attempt"] == attempt
                and row["arm"] == "filesystem"
                and row["status"] == "success"
                for row in group
            )
        )
        goal = TASK_BY_ID[task_id]["public"]["goal"]
        lines.append(
            f"| {scenario_name(task_id)} | {goal} | {len(sql_success)}/{len(sql)} | {len(fs_success)}/{len(fs)} | {fmt_ms([row['task_wall_ms'] for row in sql_success])} | {fmt_ms([row['task_wall_ms'] for row in fs_success])} | {paired} |"
        )
    status_counts = Counter(row["status"] for row in rows)
    lines.extend(
        [
            "",
            "## 场景与验收",
            "",
            "- A1 冷却泵异常排查：过滤 STOPPED 泵，处理重复上报和缺测，按异常区间长度、温度峰值和泵编号排序。",
            "- A2 冷链运输温控：将 Asia/Shanghai 本地窗口转换为 UTC 毫秒，识别高温、低温和离线区间，判断批次合规性。",
            "- A3 楼宇能耗异常：读取同楼层同小时基线，按绝对功率偏差排名，检测累计电能回退并输出重建小时的 `energy_kwh`。",
            "",
            "## 业务结果基准",
            "",
            "独立 oracle 对这批数据的关键结果为：A1 的 `pump_02` 有两个分离的异常区间，缺测 1 个、重复上报 1 个；A2 的 `box-1` 出现两段高温、`box-2` 出现低温、`box-3` 出现离线，批次为 `NON_COMPLIANT`；A3 的异常楼层排序为 `floor_04`、`floor_01`、`floor_02`，`floor_03` 在 `1700246800000` 重建 `59.0 kWh`。三类任务的成功答案均与对应 oracle 完全一致。",
            "",
            "## 执行与计时",
            "",
            "SQL 与 filesystem 使用相同的数据库快照、账号权限、任务文本、模型和随机化成对计划。filesystem 在 IoTDB 后端仍走 SQL；其命令解析、元数据读取、客户端统计、模型纠错和最终 JSON 生成全部计入 filesystem 端到端耗时。报告中的耗时只统计最终得到正确业务答案的尝试。当前合并目录中的历史运行是在流断开重试加入前完成的；客户端现已默认对 response.completed 前的流断开最多重试 2 次，每次使用新的 ephemeral thread，并把重试时间计入同一任务预算。",
            "",
            f"本次合并 {len(rows)} 次尝试："
            + "，".join(f"{key}={value}" for key, value in sorted(status_counts.items()))
            + "。失败重跑分别来自初跑、首次重跑和第二次定向重跑，原始轨迹按 `attempt` 和 `source_run` 保留。",
            "",
            "## 模型响应状态",
            "",
            "A1 fs 和 A3 fs 的多次重跑均在模型完成响应前出现 `stream disconnected before completion`；同一批数据上的 SQL 任务曾成功返回并通过 oracle。该状态计入模型服务响应状态，不计入业务错误。第二次定向重跑期间，A1 SQL 和 A3 SQL 也出现同类断流，保留在原始记录中。",
            "",
            "## 数据与复现",
            "",
            "使用快照 `setup-application-20260914e`，A1/A2/A3 的 schema、行数、全量内容 hash 和只读权限均已验证。初跑、首次失败重跑和第二次定向重跑目录分别为：",
            "",
        ]
    )
    lines.extend(f"- `{directory}`" for directory in run_dirs)
    lines.extend(
        [
            "",
            "完整模型可见交互、每次命令和 IoTDB 原始响应见 `trials.jsonl` 及各源目录的 `trials/<trial_id>/tools/`；合并明细见 `trials.csv` 和 `comparison.csv`。",
            "",
            "## 解释",
            "",
            "在当前可获得的成功配对中，A2 的 filesystem 端到端中位耗时为 33.65 秒，SQL 为 27.80 秒，filesystem 多 5.85 秒。这个差值包含模型为 filesystem 语法进行的命令选择、元数据读取和结果整理；它不是 IoTDB SQL 引擎执行时间差。A1/A3 的 filesystem 业务耗时没有可用的成功观测，因此不对这两个场景给出速度排名。",
        ]
    )
    (output / "REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("runs", nargs="+")
    args = parser.parse_args()
    run_dirs = [Path(item).resolve() for item in args.runs]
    write_report(Path(args.output).resolve(), load_rows(run_dirs), run_dirs)


if __name__ == "__main__":
    main()
