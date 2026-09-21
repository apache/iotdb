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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Offline verification semantics. Never expose this module or gold to agents."""
from collections import deque
from datetime import datetime, timedelta, timezone
import hashlib
import math
import re

import numpy as np
import pandas as pd


def epoch(value):
    return int(pd.Timestamp(value, tz="UTC").value // 1_000_000)


def bounds(task, private=False):
    """Return half-open millisecond bounds; private args are diagnostic only."""
    text = task["meta"]["args"]["time"]
    hidden = re.findall(r"Timestamp\('([^']+)'\)", text)
    if hidden:
        if len(hidden) != 2:
            raise ValueError("Invalid private time range")
        if private:
            return epoch(hidden[0]), epoch(hidden[1]) + 1
        dates = re.search(
            r"(\d{4}-\d{2}-\d{2}) to (\d{4}-\d{2}-\d{2})", task["question"]
        )
        if not dates:
            raise ValueError("Public date range absent")
        return epoch(dates[1]), epoch(dates[2]) + 86_400_000
    if not re.fullmatch(r"\d{4}(?:-\d{2})?", text) or text not in task["question"]:
        raise ValueError("Unsupported public period")
    year, month = int(text[:4]), int(text[5:]) if len(text) == 7 else 1
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = (
        datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        if len(text) == 4 or month == 12
        else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    )
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def timestamp(ms):
    return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def source_frame(path, channel):
    frame = pd.read_csv(
        path, dtype={str(channel): "float64"}, float_precision="round_trip"
    )
    if set(frame.columns) != {"timestamp", str(channel)}:
        raise ValueError("This migration requires the audited single-channel files")
    times = (
        pd.to_datetime(frame["timestamp"], utc=True)
        .to_numpy(dtype="datetime64[ns]")
        .astype("int64")
    )
    if np.any(times % 1_000_000):
        raise ValueError("Sub-millisecond source would lose precision")
    return pd.DataFrame(
        {
            "time": times // 1_000_000,
            "channel_id": str(channel),
            "value": frame[str(channel)].to_numpy(),
        }
    )


def read_output(path):
    frame = pd.read_csv(path, dtype={"channel_id": str}, float_precision="round_trip")
    frame.columns = [s.lower() for s in frame.columns]
    # IoTDB's CSV exporter adds literal quotes around STRING fields.
    frame["channel_id"] = frame["channel_id"].str.strip('"')
    frame["time"] = frame["time"].astype("int64")
    frame["value"] = frame["value"].astype("float64")
    return frame[["time", "channel_id", "value"]]


def fingerprint(frame):
    if frame.empty or frame["channel_id"].nunique() != 1:
        raise ValueError("Expected a nonempty single-channel series")
    if np.any(np.diff(frame["time"].to_numpy()) <= 0):
        raise ValueError("Rows must be in strictly increasing timestamp order")
    records = np.empty(len(frame), dtype=[("time", "<i8"), ("value", "<f8")])
    records["time"] = frame["time"]
    records["value"] = frame["value"]
    records["value"][np.isnan(records["value"])] = np.nan
    digest = hashlib.sha256(
        str(frame["channel_id"].iloc[0]).encode() + b"\0" + records.tobytes()
    ).hexdigest()
    return {
        "rows": len(frame),
        "nulls": int(frame["value"].isna().sum()),
        "min_time": int(frame["time"].min()),
        "max_time": int(frame["time"].max()),
        "sha256": digest,
    }


def scoped(frame, limits):
    lo, hi = limits
    return frame[(frame.time >= lo) & (frame.time < hi)].reset_index(drop=True)


def window_size(task, times):
    steps = np.diff(times)
    if not len(steps) or np.any(steps != steps[0]) or steps[0] <= 0:
        raise ValueError("Rolling contract requires a regular, nonempty series")
    days = int(re.fullmatch(r"(\d+)D", task["meta"]["args"]["window"])[1])
    numerator = days * 86_400_000
    if numerator % int(steps[0]):
        raise ValueError("Window is not an integral sample count")
    return numerator // int(steps[0])


def rolling_composition(values, width, metric):
    """Generic linear-time rolling primitives, independent of pandas oracle."""
    v = np.asarray(values, dtype=np.longdouble)
    valid = np.isfinite(v)
    v = np.where(valid, v, 0)
    count = np.r_[0, np.cumsum(valid)]
    count = count[width:] - count[:-width]
    sums = np.r_[np.longdouble(0), np.cumsum(v)]
    sums = sums[width:] - sums[:-width]
    if "average" in metric:
        out = sums / width
    elif "variance" in metric:
        # Center before accumulating squares to avoid catastrophic cancellation.
        centered = v - np.mean(v[valid])
        sq = np.r_[np.longdouble(0), np.cumsum(centered * centered)]
        sm = np.r_[np.longdouble(0), np.cumsum(centered)]
        out = ((sq[width:] - sq[:-width]) - (sm[width:] - sm[:-width]) ** 2 / width) / (
            width - 1
        )
    elif metric == "largest range":
        low, high, result = deque(), deque(), []
        for i, value in enumerate(v):
            while low and low[0] <= i - width:
                low.popleft()
            while high and high[0] <= i - width:
                high.popleft()
            while low and v[low[-1]] >= value:
                low.pop()
            while high and v[high[-1]] <= value:
                high.pop()
            low.append(i)
            high.append(i)
            if i >= width - 1:
                result.append(v[high[0]] - v[low[0]])
        out = np.asarray(result, dtype=np.longdouble)
    else:
        raise ValueError(metric)
    out[count < width] = np.nan
    return np.r_[np.full(width - 1, np.nan), out]


def solve(task, frame, composition=False):
    """Reference answer; composition consumes only data obtained via FS."""
    args, family = task["meta"]["args"], task["subtask"]
    t, v = frame.time.to_numpy(), frame.value.to_numpy()
    if not len(t):
        return None
    if family == "Global Aggregation":
        clean = v[np.isfinite(v)]
        if not len(clean):
            return None
        op = args["agg"]
        if op == "average":
            result = (
                math.fsum(map(float, clean)) / len(clean)
                if composition
                else float(np.mean(clean))
            )
        elif op == "median":
            ordered = sorted(clean)
            result = (
                (ordered[(len(clean) - 1) // 2] + ordered[len(clean) // 2]) / 2
                if composition
                else np.median(clean)
            )
        else:
            result = {
                "minimum": np.min(clean),
                "maximum": np.max(clean),
                "range": np.max(clean) - np.min(clean),
            }[op]
        return float(result)
    if family == "Temporal Localization":
        action = args["action"]
        if "maximum" in action or "minimum" in action:
            pos = np.nanargmax(v) if "maximum" in action else np.nanargmin(v)
        elif action.startswith("first rise above"):
            candidates = np.flatnonzero(v > float(args["threshold_high"]))
            if not len(candidates):
                return None
            pos = candidates[0]
        elif action.startswith("last fall below"):
            candidates = np.flatnonzero(v < float(args["threshold_low"]))
            if not len(candidates):
                return None
            pos = candidates[-1]
        else:
            raise ValueError(action)
        return timestamp(t[pos])
    if family == "Interval Discovery":
        mask = v > float(args["threshold"])
        starts = np.flatnonzero(mask & ~np.r_[False, mask[:-1]])
        ends = np.flatnonzero(mask & ~np.r_[mask[1:], False])
        if not len(starts):
            return None
        # Strictly regular source cadence is already audited; null breaks a run.
        pos = int(np.argmax(t[ends] - t[starts]))
        return [timestamp(t[starts[pos]]), timestamp(t[ends[pos]])]
    if family == "Sliding Window":
        width, metric = window_size(task, t), args["metric"]
        if width > len(v):
            return None
        if composition:
            values = rolling_composition(v, width, metric)
        else:
            rolling = pd.Series(v).rolling(width, min_periods=width)
            if "average" in metric:
                values = rolling.mean().to_numpy()
            elif "variance" in metric:
                values = rolling.var(ddof=1).to_numpy()
            else:
                values = (rolling.max() - rolling.min()).to_numpy()
        if not np.any(np.isfinite(values)):
            return None
        end = int(
            np.nanargmin(values)
            if metric.startswith("lowest")
            else np.nanargmax(values)
        )
        return [timestamp(t[end - width + 1]), timestamp(t[end])]
    raise ValueError(family)


def answer_equal(task, left, right):
    if left is None or right is None:
        return left is None and right is None
    if task["eval_metric"] == "rel_acc":
        return f"{float(left):.3f}" == f"{float(right):.3f}"
    return left == right


def sql_for(task, database, limits, times):
    args, family = task["meta"]["args"], task["subtask"]
    channel = str(task["channel"])
    if not re.fullmatch(r"[A-Za-z0-9_]+", channel) or not re.fullmatch(
        r"[a-z0-9_]+", database
    ):
        raise ValueError("Invalid mapped identifier")
    lo, hi = limits
    base = f"SELECT time,value FROM {database}.raw_data WHERE channel_id='{channel}' AND time>={lo} AND time<{hi}"
    if family == "Global Aggregation":
        expr = {
            "minimum": "min(value)",
            "maximum": "max(value)",
            "average": "avg(value)",
            "range": "max(value)-min(value)",
            "median": "percentile(value,0.5)",
        }[args["agg"]]
        return f"SELECT {expr} AS answer FROM ({base})"
    if family == "Temporal Localization":
        action = args["action"]
        predicate = "value IS NOT NULL"
        order = "value DESC,time ASC" if "maximum" in action else "value ASC,time ASC"
        if action.startswith("first rise above"):
            predicate, order = f"value>{float(args['threshold_high'])}", "time ASC"
        elif action.startswith("last fall below"):
            predicate, order = f"value<{float(args['threshold_low'])}", "time DESC"
        return f"SELECT cast(time AS INT64) AS answer FROM ({base}) WHERE {predicate} ORDER BY {order} LIMIT 1"
    if family == "Interval Discovery":
        condition = f"value>{float(args['threshold'])}"
        marked = (
            f"SELECT time,CASE WHEN {condition} THEN 1 ELSE 0 END AS ok FROM ({base})"
        )
        grouped = f"SELECT time,ok,sum(CASE WHEN ok=0 THEN 1 ELSE 0 END) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp FROM ({marked})"
        runs = f"SELECT min(time) AS st,max(time) AS en FROM ({grouped}) WHERE ok=1 GROUP BY grp"
        return f"SELECT cast(st AS INT64) AS start_ms,cast(en AS INT64) AS end_ms FROM ({runs}) ORDER BY cast(en AS INT64)-cast(st AS INT64) DESC,st ASC LIMIT 1"
    width, metric = window_size(task, times), args["metric"]
    over = f"OVER (ORDER BY time ROWS BETWEEN {width-1} PRECEDING AND CURRENT ROW)"
    expr = (
        f"avg(value) {over}"
        if "average" in metric
        else (
            f"var_samp(value) {over}"
            if "variance" in metric
            else f"max(value) {over}-min(value) {over}"
        )
    )
    windowed = f"SELECT time,min(time) {over} AS st,count(value) {over} AS n,{expr} AS score FROM ({base})"
    order = "ASC" if metric.startswith("lowest") else "DESC"
    return f"SELECT cast(st AS INT64) AS start_ms,cast(time AS INT64) AS end_ms FROM ({windowed}) WHERE n={width} ORDER BY score {order},time ASC LIMIT 1"
