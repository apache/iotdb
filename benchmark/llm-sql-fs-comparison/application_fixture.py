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

"""Deterministic application fixtures and independent business oracles.

The fixtures are intentionally small enough for a smoke run but retain the
failure modes that make an operational time-series task meaningful: gaps,
duplicates, quality flags, state changes, local time conversion and meter
rollbacks.  The oracle is computed from the source rows, never from a model
query.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
SCENARIOS = ("A1", "A2", "A3")
_A1_BASE = 1_700_000_000_000
_A2_BASE = 1_700_100_000_000
_A3_BASE = 1_700_200_000_000
_A1_SAMPLE_MS = 300_000
_A2_SAMPLE_MS = 600_000
_A3_HOUR_MS = 3_600_000


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def scenario_rows(scenario: str) -> dict[str, list[dict[str, Any]]]:
    if scenario == "A1":
        # Five-minute samples over a 30-minute incident window.  pump_02 has
        # a running overheat and vibration interval; pump_03 is stopped while
        # hot and must not be raised as an operational incident.
        base = _A1_BASE
        sensors = []
        assets = []
        for pump in range(1, 5):
            name = f"pump_{pump:02d}"
            assets.append(
                {
                    "pump_id": name,
                    "line": "L1" if pump < 3 else "L2",
                    "area": "north" if pump % 2 else "south",
                }
            )
            for i in range(7):
                t = base + i * 300_000
                temp = 72 + pump + i
                vib = 3.0 + pump / 10
                state = "RUNNING"
                # Two separated overheat observations exercise interval
                # splitting (the points at i=2 and i=4 are 10 minutes apart).
                if pump == 2 and i in (2, 4):
                    temp, vib = 88 + i, 8.0 + i / 10
                if pump == 3 and 2 <= i <= 5:
                    temp, vib, state = 92 + i, 9.2, "STOPPED"
                sensors.append(
                    {
                        "time": t,
                        "pump_id": name,
                        "temperature_c": temp,
                        "vibration_rms": vib,
                        "run_state": state,
                        "quality": "OK",
                        "upload_id": f"{name}-{i}-a",
                    }
                )
        # A short gap and a duplicate are explicit quality observations for
        # the task; they are not silently repaired in the expected answer.
        sensors = [
            r
            for r in sensors
            if not (r["pump_id"] == "pump_04" and r["time"] == base + 900_000)
        ]
        duplicate = dict(
            next(
                r
                for r in sensors
                if r["pump_id"] == "pump_02" and r["time"] == base + 600_000
            )
        )
        duplicate["upload_id"] = "pump_02-2-b"
        sensors.append(duplicate)
        return {"pump_readings": sensors, "pump_assets": assets}
    if scenario == "A2":
        # UTC rows around a shipment window.  Local task times are Asia/Shanghai.
        base = _A2_BASE
        readings = []
        boxes = [
            {"box_id": f"box-{i}", "shipment_id": "B-2047", "route_tz": "Asia/Shanghai"}
            for i in (1, 2, 3)
        ]
        for box in boxes:
            for i in range(9):
                t = base + i * 600_000
                temp = 5.0
                status = "ONLINE"
                quality = "OK"
                # A normal point at i=3 separates two high-temperature
                # excursions for the same box.
                if box["box_id"] == "box-1" and i in (2, 4):
                    temp = 9.5
                if box["box_id"] == "box-2" and i in (5, 6):
                    temp = -0.5
                if box["box_id"] == "box-3" and i in (3, 4):
                    temp, status, quality = None, "OFFLINE", "MISSING"
                readings.append(
                    {
                        "time": t,
                        "box_id": box["box_id"],
                        "shipment_id": "B-2047",
                        "temperature_c": temp,
                        "status": status,
                        "quality": quality,
                        "upload_id": f"{box['box_id']}-{i}-a",
                    }
                )
        # repeated upload of one point; the task requires de-duplication.
        duplicate = dict(
            next(
                r
                for r in readings
                if r["box_id"] == "box-1" and r["time"] == base + 1_200_000
            )
        )
        duplicate["upload_id"] = "box-1-2-b"
        readings.append(duplicate)
        return {"shipment_readings": readings, "shipments": boxes}
    if scenario == "A3":
        base = _A3_BASE
        meters, baselines, floors = [], [], []
        for floor in range(1, 5):
            name = f"floor_{floor:02d}"
            floors.append(
                {
                    "floor_id": name,
                    "building": "B1",
                    "use_type": "office" if floor < 4 else "retail",
                }
            )
            cumulative_energy = Decimal("0")
            for i in range(24):
                t = base + i * _A3_HOUR_MS
                power = Decimal("40") + floor * 3 + (10 if 9 <= i < 18 else 0)
                baseline_power = Decimal("39") + floor * 3 + (8 if 9 <= i < 18 else 0)
                if floor == 4 and 14 <= i <= 16:
                    power += 30  # largest operational anomaly
                cumulative_energy += power  # one hourly sample, so kWh == kW
                meter = cumulative_energy
                if floor == 3 and i == 13:
                    meter = Decimal("1")  # meter replacement rollback
                    cumulative_energy = meter
                meters.append(
                    {
                        "time": t,
                        "floor_id": name,
                        "active_power_kw": float(power),
                        "energy_kwh": float(meter),
                        "occupancy": 1 if 9 <= i < 18 else 0,
                        "quality": "OK",
                    }
                )
                baselines.append(
                    {
                        "time": t,
                        "floor_id": name,
                        "baseline_power_kw": float(baseline_power),
                    }
                )
        return {
            "meter_readings": meters,
            "floor_baselines": baselines,
            "floor_assets": floors,
        }
    raise ValueError(f"unknown scenario: {scenario}")


def _unique(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    seen = set()
    result = []
    for row in sorted(rows, key=lambda r: tuple(r[k] for k in keys)):
        key = tuple(row[k] for k in keys)
        if key not in seen:
            seen.add(key)
            result.append(row)
    return result


def _contiguous_groups(
    rows: list[dict[str, Any]], max_gap_ms: int
) -> list[list[dict[str, Any]]]:
    """Split timestamped observations when the gap exceeds the sampling interval.

    The interval rule is inclusive: observations exactly ``max_gap_ms`` apart
    remain in the same incident.  The input is copied into time order so the
    oracle does not depend on the database's output order.
    """
    if not rows:
        return []
    ordered = sorted(rows, key=lambda row: row["time"])
    groups = [[ordered[0]]]
    for row in ordered[1:]:
        if row["time"] - groups[-1][-1]["time"] <= max_gap_ms:
            groups[-1].append(row)
        else:
            groups.append([row])
    return groups


def expected_application_answer(task_id: str) -> dict[str, Any]:
    rows = scenario_rows(task_id[:2])
    if task_id == "A1-TRIAGE":
        raw_readings = rows["pump_readings"]
        readings = _unique(raw_readings, ("pump_id", "time"))
        incidents = []
        pumps = sorted({r["pump_id"] for r in raw_readings})
        expected_times = set(
            range(_A1_BASE, _A1_BASE + _A1_SAMPLE_MS * 7, _A1_SAMPLE_MS)
        )
        missing = sum(
            len(expected_times - {r["time"] for r in readings if r["pump_id"] == pump})
            for pump in pumps
        )
        duplicates = len(raw_readings) - len(readings)
        for pump in pumps:
            active = [
                r
                for r in readings
                if r["pump_id"] == pump
                and r["run_state"] == "RUNNING"
                and (r["temperature_c"] > 85 or r["vibration_rms"] > 7.5)
            ]
            for group in _contiguous_groups(active, _A1_SAMPLE_MS):
                incidents.append(
                    {
                        "pump_id": pump,
                        "start": group[0]["time"],
                        "end": group[-1]["time"],
                        "peak_temperature_c": max(r["temperature_c"] for r in group),
                        "peak_vibration_rms": max(r["vibration_rms"] for r in group),
                    }
                )
        incidents.sort(
            key=lambda r: (
                -(r["end"] - r["start"]),
                -r["peak_temperature_c"],
                r["pump_id"],
                r["start"],
            )
        )
        return {
            "incident_window": {"start": _A1_BASE, "end": _A1_BASE + _A1_SAMPLE_MS * 6},
            "priority_pumps": incidents[:3],
            "evidence": [
                {"pump_id": r["pump_id"], "time": r["start"]} for r in incidents[:3]
            ],
            "data_quality": {"missing": missing, "duplicates": duplicates},
        }
    if task_id == "A2-COMPLIANCE":
        readings = _unique(rows["shipment_readings"], ("box_id", "time"))
        excursions = []
        offline = []
        for box in sorted({r["box_id"] for r in readings}):
            rs = [r for r in readings if r["box_id"] == box]
            hot = [
                r
                for r in rs
                if r["temperature_c"] is not None and r["temperature_c"] > 8
            ]
            cold = [
                r
                for r in rs
                if r["temperature_c"] is not None and r["temperature_c"] < 2
            ]
            missing = [
                r for r in rs if r["temperature_c"] is None or r["status"] == "OFFLINE"
            ]
            for kind, values in (("high", hot), ("low", cold)):
                for group in _contiguous_groups(values, _A2_SAMPLE_MS):
                    excursions.append(
                        {
                            "box_id": box,
                            "kind": kind,
                            "start": group[0]["time"],
                            "end": group[-1]["time"],
                            "peak_c": (max if kind == "high" else min)(
                                r["temperature_c"] for r in group
                            ),
                        }
                    )
            for group in _contiguous_groups(missing, _A2_SAMPLE_MS):
                offline.append(
                    {"box_id": box, "start": group[0]["time"], "end": group[-1]["time"]}
                )
        non_compliant = bool(excursions or offline)
        review_list = {}
        for item in excursions:
            reason = "high_temperature" if item["kind"] == "high" else "low_temperature"
            review_list[(item["box_id"], reason)] = {
                "box_id": item["box_id"],
                "reason": reason,
            }
        for item in offline:
            review_list[(item["box_id"], "sensor_offline")] = {
                "box_id": item["box_id"],
                "reason": "sensor_offline",
            }
        ordered_review = [review_list[key] for key in sorted(review_list)]
        return {
            "shipment_id": "B-2047",
            "compliance": "NON_COMPLIANT" if non_compliant else "COMPLIANT",
            "excursions": excursions,
            "offline_periods": offline,
            "review_list": ordered_review,
        }
    if task_id == "A3-ENERGY":
        meters = _unique(rows["meter_readings"], ("floor_id", "time"))
        baselines = {
            (row["floor_id"], row["time"]): row["baseline_power_kw"]
            for row in rows["floor_baselines"]
        }
        ranking = []
        for floor in sorted({r["floor_id"] for r in meters}):
            rs = [r for r in meters if r["floor_id"] == floor]
            # Pick the earliest hour when a floor has tied deviations.  This
            # makes the evidence row deterministic without changing ranking.
            anomaly = max(
                rs,
                key=lambda r: (
                    abs(r["active_power_kw"] - baselines[(floor, r["time"])]),
                    -r["time"],
                ),
            )
            baseline = baselines[(floor, anomaly["time"])]
            deviation = abs(anomaly["active_power_kw"] - baseline)
            ranking.append(
                {
                    "floor_id": floor,
                    "anomaly_hour": anomaly["time"],
                    "actual_power_kw": anomaly["active_power_kw"],
                    "baseline_power_kw": round(baseline, 6),
                    "reason": "peak_power" if deviation else "within_baseline",
                }
            )
        ranking.sort(
            key=lambda r: (
                -abs(r["actual_power_kw"] - r["baseline_power_kw"]),
                r["floor_id"],
            )
        )
        reconstructed = []
        for floor in sorted({r["floor_id"] for r in meters}):
            rs = sorted(
                (r for r in meters if r["floor_id"] == floor), key=lambda r: r["time"]
            )
            for previous, current in zip(rs, rs[1:]):
                if current["energy_kwh"] < previous["energy_kwh"]:
                    reconstructed.append(
                        {
                            "floor_id": floor,
                            "time": current["time"],
                            "energy_kwh": current["active_power_kw"],
                        }
                    )
        return {
            "date": _iso(_A3_BASE)[:10],
            "floor_ranking": ranking[:3],
            "hourly_evidence": ranking[:3],
            "reconstructed_intervals": reconstructed,
        }
    raise ValueError(f"unknown task: {task_id}")


def fixture_hash(scenario: str) -> str:
    payload = json.dumps(
        scenario_rows(scenario),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


SCHEMA = {
    "pump_readings": [
        ("pump_id", "STRING", "TAG"),
        ("temperature_c", "DOUBLE", "FIELD"),
        ("vibration_rms", "DOUBLE", "FIELD"),
        ("run_state", "STRING", "FIELD"),
        ("quality", "STRING", "FIELD"),
        ("upload_id", "STRING", "TAG"),
    ],
    "pump_assets": [
        ("pump_id", "STRING", "TAG"),
        ("line", "STRING", "FIELD"),
        ("area", "STRING", "FIELD"),
    ],
    "shipment_readings": [
        ("box_id", "STRING", "TAG"),
        ("shipment_id", "STRING", "TAG"),
        ("temperature_c", "DOUBLE", "FIELD"),
        ("status", "STRING", "FIELD"),
        ("quality", "STRING", "FIELD"),
        ("upload_id", "STRING", "TAG"),
    ],
    "shipments": [
        ("box_id", "STRING", "TAG"),
        ("shipment_id", "STRING", "FIELD"),
        ("route_tz", "STRING", "FIELD"),
    ],
    "meter_readings": [
        ("floor_id", "STRING", "TAG"),
        ("active_power_kw", "DOUBLE", "FIELD"),
        ("energy_kwh", "DOUBLE", "FIELD"),
        ("occupancy", "INT32", "FIELD"),
        ("quality", "STRING", "FIELD"),
    ],
    "floor_baselines": [
        ("floor_id", "STRING", "TAG"),
        ("baseline_power_kw", "DOUBLE", "FIELD"),
    ],
    "floor_assets": [
        ("floor_id", "STRING", "TAG"),
        ("building", "STRING", "FIELD"),
        ("use_type", "STRING", "FIELD"),
    ],
}


def fixture_sql(database: str, scenario: str) -> list[str]:
    """Return isolated CREATE/INSERT statements for one application snapshot."""
    if not database.startswith("llm_app_benchmark_"):
        raise ValueError("application fixture database prefix required")
    rows_by_table = scenario_rows(scenario)
    statements = [f"CREATE DATABASE {database}"]
    for table, rows in rows_by_table.items():
        definitions = ", ".join(
            f"{name} {kind} {category}" for name, kind, category in SCHEMA[table]
        )
        statements.append(f"CREATE TABLE {database}.{table} ({definitions})")
        columns = [name for name, _, _ in SCHEMA[table]]
        for row in rows:
            values = []
            insert_columns = ["time"] + columns
            for name in insert_columns:
                value = row.get(name, 0) if name == "time" else row[name]
                if value is None:
                    values.append("null")
                elif isinstance(value, str):
                    values.append("'" + value.replace("'", "''") + "'")
                else:
                    values.append(
                        str(value).lower() if isinstance(value, bool) else str(value)
                    )
            statements.append(
                f"INSERT INTO {database}.{table} ({', '.join(insert_columns)}) VALUES ({', '.join(values)})"
            )
    return statements
