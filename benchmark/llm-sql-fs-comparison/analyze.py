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

"""Report actual Codex trials, preserving failures and evidence uncertainty."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path
import random
import statistics


BOOTSTRAP_SAMPLES = 2000
BOOTSTRAP_SEED = 20260913
ARMS = ("sql", "filesystem")
TOKEN_KEYS = {
    "input_tokens": "inputTokens",
    "cached_input_tokens": "cachedInputTokens",
    "output_tokens": "outputTokens",
    "reasoning_output_tokens": "reasoningOutputTokens",
    "total_tokens": "totalTokens",
}
TRIAL_FIELDS = [
    "trial_id",
    "pair_id",
    "task_id",
    "scale",
    "information",
    "repeat",
    "arm",
    "order",
    "original_status",
    "status",
    "answer_correct",
    "original_reason",
    "evaluation_reason",
    "review_applied",
    "task_wall_ms",
    "tool_wall_ms",
    "non_tool_wall_ms",
    "cli_process_ms",
    "model_api_ms",
    "tool_attempts",
    "tool_errors",
    "model_responses",
    "model_requests",
    *TOKEN_KEYS,
]
COMPARISON_FIELDS = [
    "scope",
    "task_id",
    "scale",
    "information",
    "sql_trials",
    "filesystem_trials",
    "sql_successes",
    "filesystem_successes",
    "sql_success_rate",
    "filesystem_success_rate",
    "complete_pairs",
    "both_success",
    "sql_only_success",
    "filesystem_only_success",
    "neither_success",
    "pending_pairs",
    "incomplete_pairs",
    "sql_median_seconds",
    "filesystem_median_seconds",
    "geometric_ratio_sql_over_fs",
    "bootstrap95_low",
    "bootstrap95_high",
    "eligible_task_count",
]
LICENSE = """<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->
"""


def _read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _finite(value):
    try:
        return type(value) in {int, float} and math.isfinite(value)
    except OverflowError:
        return False


def load_results(directory, review_path=None):
    """Validate identities and apply explicit review decisions without rewriting raw data."""
    metadata = _read_json(directory / "run.json")
    trial_file = directory / "trials.jsonl"
    rows = []
    if trial_file.exists():
        for number, line in enumerate(
            trial_file.read_text(encoding="utf-8").splitlines(), 1
        ):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"trials.jsonl 第 {number} 行不完整；请在本次写入完成后重试") from exc
            if not isinstance(row, dict):
                raise ValueError(f"trials.jsonl 第 {number} 行不是对象")
            rows.append(row)
    identities = set()
    pair_arms = set()
    for row in rows:
        for field in ("trial_id", "pair_id", "task_id", "status", "arm"):
            if not isinstance(row.get(field), str) or not row[field]:
                raise ValueError(f"trial 缺少有效的 {field}")
        if row["trial_id"] in identities or (row["pair_id"], row["arm"]) in pair_arms:
            raise ValueError("重复 trial_id 或 pair/arm；拒绝静默覆盖记录")
        if row["arm"] not in ARMS:
            raise ValueError("未知访问模式")
        identities.add(row["trial_id"])
        pair_arms.add((row["pair_id"], row["arm"]))
        for field in (
            "task_wall_ms",
            "tool_wall_ms",
            "non_tool_wall_ms",
            "cli_process_ms",
            "model_api_ms",
        ):
            value = row.get(field)
            if value is not None and (not _finite(value) or value < 0):
                raise ValueError(f"{row['trial_id']} 的 {field} 无效")
        if row["status"] == "success" and (
            row.get("answer_correct") is not True
            or not _finite(row.get("task_wall_ms"))
            or row["task_wall_ms"] <= 0
        ):
            raise ValueError("success 必须具有正确答案及正的实际任务耗时")
        row["original_status"] = row["status"]
        row["original_reason"] = row.get("evaluation_reason", "")
        row["review_applied"] = False
        usage = row.get("usage") or {}
        for output, source in TOKEN_KEYS.items():
            value = usage.get(source)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("token 必须是非负整数或 null")
            row[output] = value

    review_path = review_path or directory / "evidence_review.json"
    reviews = _read_json(review_path) if review_path.exists() else {}
    if not isinstance(reviews, dict) or set(reviews) - identities:
        raise ValueError("审核文件必须仅包含本次已有 trial_id 的对象映射")
    by_id = {row["trial_id"]: row for row in rows}
    for trial_id, review in reviews.items():
        row = by_id[trial_id]
        if (
            row["status"] != "evidence_pending"
            or not isinstance(review, dict)
            or review.get("status")
            not in {"success", "insufficient_evidence", "evidence_pending"}
            or not isinstance(review.get("reason"), str)
            or not review["reason"].strip()
        ):
            raise ValueError(f"{trial_id} 必须是有明确理由的 evidence_pending 审核")
        if review["status"] == "success" and (
            row.get("answer_correct") is not True
            or not _finite(row.get("task_wall_ms"))
            or row["task_wall_ms"] <= 0
        ):
            raise ValueError("审核不能把无正确答案或无有效耗时的记录改为成功")
        row.update(
            status=review["status"],
            evaluation_reason=review["reason"],
            review_applied=True,
        )
    return metadata, rows, reviews


def paired_rows(rows):
    pairs = defaultdict(dict)
    for row in rows:
        pairs[row["pair_id"]][row["arm"]] = row
    for arms in pairs.values():
        if len(arms) == 2:
            sql, fs = arms["sql"], arms["filesystem"]
            for field in (
                "task_id",
                "scale",
                "information",
                "repeat",
                "database",
                "target_table",
            ):
                if sql.get(field) != fs.get(field):
                    raise ValueError(f"{sql['pair_id']} 的 {field} 在两侧不一致")
    return pairs


def paired_log_ratios(rows):
    groups = defaultdict(list)
    for arms in paired_rows(rows).values():
        if len(arms) != 2 or any(row["status"] != "success" for row in arms.values()):
            continue
        sql, fs = arms["sql"], arms["filesystem"]
        key = (sql.get("scale", "S"), sql["task_id"])
        groups[key].append(math.log(sql["task_wall_ms"] / fs["task_wall_ms"]))
    return groups


def interval(groups, label):
    """Fixed tasks; resample complete pairs within each task, then weight tasks equally."""
    if not groups:
        return None, None, None
    groups = [groups[key] for key in sorted(groups)]
    point = math.exp(statistics.fmean(statistics.fmean(values) for values in groups))
    seed = (
        int.from_bytes(hashlib.sha256(label.encode()).digest()[:8], "big")
        ^ BOOTSTRAP_SEED
    )
    rng = random.Random(seed)
    estimates = []
    for _ in range(BOOTSTRAP_SAMPLES):
        means = [
            statistics.fmean(values[rng.randrange(len(values))] for _ in values)
            for values in groups
        ]
        estimates.append(math.exp(statistics.fmean(means)))
    estimates.sort()
    return point, estimates[49], estimates[1949]


def summary(rows, scope, task_id="", scale="", information=""):
    arms = {arm: [row for row in rows if row["arm"] == arm] for arm in ARMS}
    result = {
        "scope": scope,
        "task_id": task_id,
        "scale": scale,
        "information": information,
    }
    for arm, records in arms.items():
        successes = sum(row["status"] == "success" for row in records)
        result[arm + "_trials"] = len(records)
        result[arm + "_successes"] = successes
        result[arm + "_success_rate"] = successes / len(records) if records else None
    cells = Counter()
    matched = []
    for pair in paired_rows(rows).values():
        if len(pair) != 2:
            cells["incomplete_pairs"] += 1
            continue
        cells["complete_pairs"] += 1
        if any(row["status"] == "evidence_pending" for row in pair.values()):
            cells["pending_pairs"] += 1
            continue
        successful = {arm for arm, row in pair.items() if row["status"] == "success"}
        if len(successful) == 2:
            cells["both_success"] += 1
            matched.append(pair)
        elif successful == {"sql"}:
            cells["sql_only_success"] += 1
        elif successful == {"filesystem"}:
            cells["filesystem_only_success"] += 1
        else:
            cells["neither_success"] += 1
    for field in (
        "complete_pairs",
        "both_success",
        "sql_only_success",
        "filesystem_only_success",
        "neither_success",
        "pending_pairs",
        "incomplete_pairs",
    ):
        result[field] = cells[field]
    for arm in ARMS:
        result[arm + "_median_seconds"] = (
            statistics.median(pair[arm]["task_wall_ms"] / 1000 for pair in matched)
            if matched
            else None
        )
    groups = paired_log_ratios(rows)
    point, low, high = interval(groups, f"{scope}/{task_id}/{scale}/{information}")
    result.update(
        geometric_ratio_sql_over_fs=point,
        bootstrap95_low=low,
        bootstrap95_high=high,
        eligible_task_count=len(groups),
    )
    return result


def summarize(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[
            (row.get("scale", "S"), row["task_id"], row.get("information", ""))
        ].append(row)
    comparisons = [
        summary(grouped[key], "task", task_id=key[1], scale=key[0], information=key[2])
        for key in sorted(grouped)
    ]
    for condition in sorted({row.get("information", "") for row in rows}):
        comparisons.append(
            summary(
                [row for row in rows if row.get("information", "") == condition],
                "information",
                information=condition,
            )
        )
    comparisons.append(summary(rows, "overall"))
    return comparisons


def _fmt(value, digits=3):
    return "—" if value is None else f"{value:.{digits}f}"


def _percent(value):
    return "—" if value is None else f"{100 * value:.1f}%"


def _mean(rows, key, divisor=1):
    values = [row[key] / divisor for row in rows if row.get(key) is not None]
    return statistics.fmean(values) if values else None


def _cell(value):
    return str(value).replace("|", r"\|").replace("\n", " ")


def write_report(directory, metadata, rows, comparisons, reviews):
    overall = comparisons[-1]
    counts = Counter(row["status"] for row in rows)
    planned = metadata.get("planned_trials")
    complete = metadata.get("status") == "completed" and (
        planned is None or planned == len(rows)
    )
    pending = counts["evidence_pending"]
    provisional = not complete or bool(pending)
    actual_models = sorted(
        {row["resolved_model"] for row in rows if row.get("resolved_model")}
    )
    providers = sorted(
        {row["model_provider"] for row in rows if row.get("model_provider")}
    )
    models_text = ", ".join(actual_models) or str(metadata.get("model", "未记录"))
    provider_text = ", ".join(providers) or str(metadata.get("model_provider", "未记录"))
    invalid_protocol = "protocol_invalid" in str(metadata.get("status", ""))
    lines = [
        LICENSE,
        "# Codex 操作 IoTDB：SQL 与 fs 任务耗时报告",
        "",
        f"报告状态：**{'阶段性，尚不能作为最终比较结论' if provisional else '本轮执行及证据审核已完成'}**。",
        f"本目录已记录 {len(rows)} 次真实模型任务，计划 {planned if planned is not None else '未记录'} 次；"
        f"成功 {counts['success']} 次，记录内总成功率 {_percent(counts['success'] / len(rows) if rows else None)}，"
        f"证据待审 {pending} 次。运行器状态为 {metadata.get('status', '未记录')}。",
        "",
        "## 实际比较对象",
        "",
        f"使用本地 Codex 客户端调用 {models_text}，模型 provider 为 {provider_text}。"
        "本地指客户端运行位置；记录没有证明模型权重在本机运行。"
        f"推理强度为 {metadata.get('reasoning_effort', '未记录')}，Codex 版本为 {metadata.get('codex_version', '未记录')}。",
        "同一模型通过统一 iotdb_command 工具自主生成命令、读取真实输出、纠错并生成最终 JSON。"
        "SQL 和 fs 共用 IoTDB SQL 后端；fs 内部生成 SQL、额外读取及客户端计算都是被测工作流的一部分。",
        f"每表 {metadata.get('rows_per_table', '未记录')} 行，每设备 {metadata.get('points_per_device', '未记录')} 个时间点。"
        "每对共享表角色置换、只读快照与任务目标；两侧采用相同预热且分别使用全新模型会话。"
        "每次工具调用启动独立 CLI 进程。",
        "工具隔离须同时核对顶层 Codex 工具配置和嵌套工具目录；仅检查 ALL_TOOLS 列表不足以证明没有其他工具可用。"
        "具体核验记录和协议限制见 run.json 附录，未记录的隔离措施不能视为已验证。",
        "",
        "## 成功率与配对耗时",
        "",
        "成功须同时通过完整答案 oracle 和数据库观察证据检查。"
        "错误、超时、基础设施失败和证据待审均留在运行成功率分母中；耗时比率仅使用两侧均成功的配对。",
        "",
        "| 模式 | 已记录任务 | 成功 | 运行成功率 |",
        "| --- | ---: | ---: | ---: |",
    ]
    if invalid_protocol:
        lines[3:3] = [
            "**协议无效：运行元数据标记 protocol_invalid。该轮不能用于 SQL/fs 优劣判断，"
            "耗时比率与配对中位数已留空；成功状态仅表示原记录的评分，不构成合规试验结果。**",
            "",
        ]
    for arm in ARMS:
        lines.append(
            f"| {arm} | {overall[arm+'_trials']} | {overall[arm+'_successes']} | "
            f"{_percent(overall[arm+'_success_rate'])} |"
        )
    lines.extend(
        [
            "",
            "| 任务/规模 | 信息条件 | SQL/fs 成功率 | 双成功/已成对 | SQL/fs 中位秒 | T_sql/T_fs 几何比 | 95% bootstrap |",
            "| --- | --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for item in comparisons:
        if item["scope"] != "task":
            continue
        lines.append(
            f"| {item['task_id']}/{item['scale']} | {item['information']} | "
            f"{_percent(item['sql_success_rate'])} / {_percent(item['filesystem_success_rate'])} | "
            f"{item['both_success']}/{item['complete_pairs']} | "
            f"{_fmt(item['sql_median_seconds'])} / {_fmt(item['filesystem_median_seconds'])} | "
            f"{_fmt(item['geometric_ratio_sql_over_fs'])} | "
            f"{_fmt(item['bootstrap95_low'])}–{_fmt(item['bootstrap95_high'])} |"
        )
    lines.extend(
        [
            "",
            "中位耗时使用同一批双成功配对。比值大于 1 表示 SQL 用时更长；"
            "fs 相对节省比例为 1−1/比值。不同任务按任务等权汇总，避免较容易成功的任务获得更多权重。",
            "",
            "| 汇总条件 | 有双成功配对的任务数 | 任务等权几何比 | 95% bootstrap |",
            "| --- | ---: | ---: | --- |",
        ]
    )
    for item in comparisons:
        if item["scope"] != "task":
            lines.append(
                f"| {item['information'] or '全部已执行任务'} | {item['eligible_task_count']} | "
                f"{_fmt(item['geometric_ratio_sql_over_fs'])} | "
                f"{_fmt(item['bootstrap95_low'])}–{_fmt(item['bootstrap95_high'])} |"
            )
    lines.extend(
        [
            "",
            "任务数只包含至少一个双成功配对的任务；没有成功配对的任务不产生比率。"
            "存在证据待审、未完成计划或某任务无成功配对时，该汇总不代表完整计划的总体优势。",
            "",
            "## 失败与证据状态",
            "",
            "| 配对结果 | 对数 |",
            "| --- | ---: |",
        ]
    )
    for key, label in (
        ("both_success", "双方成功"),
        ("sql_only_success", "仅 SQL 成功"),
        ("filesystem_only_success", "仅 fs 成功"),
        ("neither_success", "双方均未成功"),
        ("pending_pairs", "至少一侧证据待审，未纳入上述四格"),
        ("incomplete_pairs", "仅有一侧记录，未纳入上述四格"),
    ):
        lines.append(f"| {label} | {overall[key]} |")
    lines.extend(["", "| 状态 | SQL | fs |", "| --- | ---: | ---: |"])
    by_arm = {
        arm: Counter(row["status"] for row in rows if row["arm"] == arm) for arm in ARMS
    }
    for status in sorted(counts):
        lines.append(
            f"| {status} | {by_arm['sql'][status]} | {by_arm['filesystem'][status]} |"
        )
    failed = [row for row in rows if row["status"] != "success"]
    if failed:
        lines.extend(["", "| 未成功/待审任务 | 状态 | 原因 |", "| --- | --- | --- |"])
        for row in failed:
            reason = (
                row.get("evaluation_reason")
                or row.get("error")
                or row.get("execution_status", "")
            )
            lines.append(
                f"| {_cell(row['trial_id'])} | {_cell(row['status'])} | {_cell(reason)} |"
            )
    lines.extend(
        [
            "",
            f"已应用 {len(reviews)} 条显式证据审核。审核仅修改派生 CSV/报告的有效状态；"
            "原始 trials.jsonl 和各 trial/result.json 保留。命令语法可能暴露模式，审核不能称为完全盲审。",
            "",
            "## 时间、工具及 token",
            "",
            "下表使用各模式全部已记录任务，反映实际运行成本，不能直接替代双成功配对的耗时比较。" "均值仅使用已记录字段；缺失值保持为空并报告覆盖数。",
            "",
            "| 模式 | 任务均值秒 | 工具均值秒 | CLI均值秒 | 非工具均值秒 | 工具尝试均值 | 工具错误总数 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for arm in ARMS:
        records = [row for row in rows if row["arm"] == arm]
        values = [
            _fmt(_mean(records, key, 1000))
            for key in (
                "task_wall_ms",
                "tool_wall_ms",
                "cli_process_ms",
                "non_tool_wall_ms",
            )
        ]
        errors = [
            row["tool_errors"] for row in records if row.get("tool_errors") is not None
        ]
        lines.append(
            f"| {arm} | {' | '.join(values)} | {_fmt(_mean(records, 'tool_attempts'), 2)} | "
            f"{sum(errors) if errors else '—'} |"
        )
        coverage = ", ".join(
            f"{key}={sum(row.get(key) is not None for row in records)}/{len(records)}"
            for key in (
                "task_wall_ms",
                "tool_wall_ms",
                "cli_process_ms",
                "non_tool_wall_ms",
            )
        )
        # Add coverage below the table, after both arm rows.
        metadata.setdefault("_analysis_coverage", {})[arm] = coverage
    lines.extend(
        [
            "",
            "| 模式 | token记录覆盖 | input均值 | cached input均值 | output均值 | reasoning output均值 | total总和 |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for arm in ARMS:
        records = [row for row in rows if row["arm"] == arm]
        token_totals = [
            row["total_tokens"]
            for row in records
            if row.get("total_tokens") is not None
        ]
        values = [
            _fmt(_mean(records, key), 1)
            for key in (
                "input_tokens",
                "cached_input_tokens",
                "output_tokens",
                "reasoning_output_tokens",
            )
        ]
        lines.append(
            f"| {arm} | {len(token_totals)}/{len(records)} | {' | '.join(values)} | "
            f"{sum(token_totals) if token_totals else '—'} |"
        )
    lines.extend(
        [
            "",
            "时间覆盖："
            + "；".join(
                f"{arm}: {text}"
                for arm, text in metadata.get("_analysis_coverage", {}).items()
            )
            + "。",
            "task_wall_ms 为任务开始至最终回答接收的墙钟时间，不包含准备数据、预热及事后评分。"
            "tool_wall_ms 包括校验、CLI 执行和工具结果处理；cli_process_ms 是其子集，不能再相加。",
            "model_api_ms 不能从本地 Codex 事件精确拆出时保持 null。non_tool_wall_ms 包括模型计算、"
            "网络、服务排队和 Codex 编排，不是纯推理时间。本文不据残差归因某个内部组件。",
            "cached input 和 reasoning output 是子计数，不能在 input/output/total 上再相加。"
            "新会话不意味着没有 provider prefix cache，实际缓存 token 已列出。",
            "",
            "## 统计解释与限制",
            "",
            f"比率先在每任务内取配对 log(T_sql/T_fs) 的均值，再对任务等权平均并取指数。"
            f"95% 区间以固定种子 {BOOTSTRAP_SEED}，在每个固定任务内对双成功配对重采样 {BOOTSTRAP_SAMPLES} 次，"
            "保持两侧对应关系。该区间仅描述已执行固定任务的重复波动，不推广到其他业务任务；"
            "只有一对时区间退化为点值，不能解释为没有不确定性。",
            "首轮每任务 10 对属于可行性 pilot。不能据此可靠评估 P95/P99，不能把重复执行当成独立业务问题。"
            "双方成功样本的结果也可能有选择偏差，应与成功率、失败四格和未完成任务一起解释。",
            "相同 SQL 后端不保证完整任务用时相同：模型可能选择不同命令数、探索路径、数据返回量或纠错方式。"
            "fs 的格式化、统计和元数据查询也影响工具路径；除已记录时间分解外，不声称确定的内部耗时因果。",
            "",
            "## 实际配置及设计偏离",
            "",
            "以下附录直接取自 run.json；冻结模型参数、计时限制、缓存、工具边界及实现偏离应以这些实际记录为准。"
            "未记录的参数不能视为已成功控制。fixtures 和 source_hashes 完整保存在 run.json。",
            "",
        ]
    )
    appendix = {
        key: value
        for key, value in metadata.items()
        if key not in {"fixtures", "source_hashes", "_analysis_coverage"}
    }
    lines.extend(
        "    " + line
        for line in json.dumps(appendix, ensure_ascii=False, indent=2).splitlines()
    )
    lines.extend(
        [
            "",
            "文件索引：[逐任务明细](./trials.csv)、[配对汇总](./comparison.csv)、"
            "[运行元数据](./run.json)、[原始任务记录](./trials.jsonl)。"
            "每条命令和原生输出保存在 trials/<trial_id>/tools/<序号>/call.json 及 stdout.txt/stderr.txt。",
        ]
    )
    (directory / "REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def analyze(result_dir, evidence_review=None):
    directory = Path(result_dir).resolve()
    review_path = Path(evidence_review).resolve() if evidence_review else None
    if review_path is not None and not review_path.is_file():
        raise ValueError("指定的审核文件不存在")
    metadata, rows, reviews = load_results(directory, review_path)
    comparisons = summarize(rows)
    if "protocol_invalid" in str(metadata.get("status", "")):
        for result in comparisons:
            for field in (
                "sql_median_seconds",
                "filesystem_median_seconds",
                "geometric_ratio_sql_over_fs",
                "bootstrap95_low",
                "bootstrap95_high",
            ):
                result[field] = None
            result["eligible_task_count"] = 0
    _write_csv(directory / "trials.csv", rows, TRIAL_FIELDS)
    _write_csv(directory / "comparison.csv", comparisons, COMPARISON_FIELDS)
    write_report(directory, metadata, rows, comparisons, reviews)
    return {
        "trials": len(rows),
        "successes": sum(row["status"] == "success" for row in rows),
        "pending": sum(row["status"] == "evidence_pending" for row in rows),
        "report": str(directory / "REPORT.md"),
    }


def main():
    parser = argparse.ArgumentParser(description="从真实 Codex trial 记录生成 SQL/fs 对比报告")
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--evidence-review", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(analyze(args.result_dir, args.evidence_review), ensure_ascii=False)
    )


if __name__ == "__main__":
    main()
