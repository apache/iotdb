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

"""Import all selected L1 data using official tools; verify without an LLM."""
import argparse
import fcntl
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import time
import traceback

import pandas as pd

from equivalence import (
    answer_equal,
    bounds,
    fingerprint,
    read_output,
    scoped,
    solve,
    source_frame,
    sql_for,
    timestamp,
)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    )
    temporary.replace(path)


def cli_rows(text):
    lines = [
        line.strip()
        for line in text.splitlines()
        if line.startswith("|") and line.endswith("|")
    ]
    if not lines:
        if "Total line number = 0" in text:
            return []
        raise ValueError("CLI table not found")
    keys = [s.strip() for s in lines[0][1:-1].split("|")]
    return [
        dict(zip(keys, [s.strip() for s in line[1:-1].split("|")]))
        for line in lines[1:]
    ]


class Migration:
    def __init__(self, args):
        self.args = args
        self.home, self.root = Path(args.iotdb_home), Path(args.output)
        self.code_hashes = {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [Path(__file__), Path(__file__).with_name("equivalence.py")]
        }
        self.root.mkdir(parents=True, exist_ok=True)
        self.env = dict(
            os.environ,
            IOTDB_HOME=str(self.home),
            JAVA_TOOL_OPTIONS="-Xmx1g -Duser.timezone=UTC",
        )
        self.common = [
            "-h",
            args.host,
            "-p",
            str(args.port),
            "-u",
            args.username,
            "-pw",
            os.environ.get("IOTDB_PASSWORD", "root"),
            "-sql_dialect",
            "table",
        ]
        spec = importlib.util.spec_from_file_location(
            "source_evaluator", Path(args.sonar_root) / "sonar_ts/evaluator.py"
        )
        self.evaluator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.evaluator)

    def run(self, command, directory, name, timeout=300):
        directory.mkdir(parents=True, exist_ok=True)
        out, err = directory / (name + ".stdout"), directory / (name + ".stderr")
        started = time.monotonic()
        with out.open("w") as stdout, err.open("w") as stderr:
            result = subprocess.run(
                command, stdout=stdout, stderr=stderr, env=self.env, timeout=timeout
            )
        command = list(command)
        if "-pw" in command:
            command[command.index("-pw") + 1] = "<redacted>"
        save(
            directory / (name + ".command.json"),
            {
                "argv": command,
                "exit_code": result.returncode,
                "elapsed_seconds": time.monotonic() - started,
            },
        )
        # CLI may emit query errors with a zero process exit code.
        with out.open() as stream:
            prefix = stream.read(20000)
        messages = [line for line in prefix.splitlines() if line.startswith("Msg:")]
        bad_message = any(
            line != "Msg: The statement is executed successfully." for line in messages
        )
        if result.returncode or bad_message or "Exception:" in prefix:
            raise RuntimeError(
                f"{name} failed: {prefix[-1800:]} {err.read_text()[-600:]}"
            )
        return out

    def sql(self, sql, directory, name):
        (directory / (name + ".sql")).write_text(sql + ";\n")
        return self.run(
            [str(self.home / "sbin/start-cli.sh"), *self.common, "-e", sql],
            directory,
            name,
        )

    def sql_answer(self, task, database, limits, frame, directory, name):
        query = sql_for(task, database, limits, scoped(frame, limits).time.to_numpy())
        result = cli_rows(self.sql(query, directory, name).read_text())
        if not result:
            return None
        row = result[0]
        if "start_ms" in row:
            return [timestamp(row["start_ms"]), timestamp(row["end_ms"])]
        if row["answer"] == "null":
            return None
        return (
            float(row["answer"])
            if task["eval_metric"] == "rel_acc"
            else timestamp(row["answer"])
        )

    def tsfile_fingerprint(self, path):
        from tsfile import TsFileReader

        with TsFileReader(str(path)) as reader:
            with reader.query_table(
                "raw_data", ["channel_id", "value"], -(2**63) + 1, 2**63 - 1
            ) as result:
                chunks = []
                while result.next():
                    chunks.append(result.read_data_frame(max_row_num=50000))
                frame = pd.concat(chunks, ignore_index=True)
        frame.columns = [str(c).lower() for c in frame.columns]
        return fingerprint(frame[["time", "channel_id", "value"]])

    def task(self, index, task):
        if not re.fullmatch(r"L1_T[1-4]_[A-Za-z_]+_[0-9]+", task["id"]):
            raise ValueError("Invalid source task identifier")
        directory = self.root / "tasks" / task["id"]
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            return self._task(index, task)

    def _task(self, index, task):
        directory = self.root / "tasks" / task["id"]
        directory.mkdir(parents=True, exist_ok=True)
        record = directory / "result.json"
        source = (Path(self.args.data_root) / task["ts_data_path"]).resolve()
        if not source.is_relative_to(Path(self.args.data_root).resolve()):
            raise ValueError("Source CSV outside dataset root")
        original_sha = hashlib.sha256(source.read_bytes()).hexdigest()
        if record.exists() and not self.args.recheck:
            previous = json.loads(record.read_text())
            if previous["source_sha256"] != original_sha:
                raise ValueError("Source CSV changed; use a new namespace")
            if (
                previous.get("code_sha256") == self.code_hashes
                and previous["status"] in {"verified", "answer_mismatch"}
            ) or (self.args.import_only and previous["status"] == "imported"):
                return previous
        database = self.args.prefix + task["id"].rsplit("_", 1)[1]
        result = {
            "task_id": task["id"],
            "source_index": index,
            "database": database,
            "family": task["subtask"],
            "source_sha256": original_sha,
            "code_sha256": self.code_hashes,
            "status": "error",
            "experiment_ready": False,
        }
        try:
            frame = source_frame(source, task["channel"])
            expected = fingerprint(frame)
            state = directory / "schema.json"
            intent = directory / "import-intent.json"
            identity = {
                "database": database,
                "source_sha256": original_sha,
                "fingerprint": expected,
                "schema": "time TIMESTAMP TIME,channel_id STRING TAG,value DOUBLE FIELD",
            }
            if state.exists():
                if json.loads(state.read_text()) != identity:
                    raise ValueError(
                        "Existing import identity differs; use a new namespace"
                    )
            else:
                if intent.exists() and json.loads(intent.read_text()) != identity:
                    raise ValueError("Existing import intent differs")
                if not intent.exists():
                    databases = cli_rows(
                        self.sql(
                            "SHOW DATABASES", directory, "databases-before-create"
                        ).read_text()
                    )
                    exists = any(database in row.values() for row in databases)
                    # An earlier interrupted attempt is owned only if its recorded
                    # source hash and database mapping match this exact source.
                    owned = (
                        record.exists()
                        and json.loads(record.read_text()).get("database") == database
                        and json.loads(record.read_text()).get("source_sha256")
                        == original_sha
                    )
                    if exists and not owned:
                        raise ValueError(
                            "Refusing to adopt an unowned existing database"
                        )
                    save(intent, identity)
                self.sql(
                    f"CREATE DATABASE IF NOT EXISTS {database};CREATE TABLE IF NOT EXISTS {database}.raw_data (channel_id STRING TAG,value DOUBLE FIELD)",
                    directory,
                    "schema",
                )
                schema = cli_rows(
                    self.sql(
                        f"DESC {database}.raw_data", directory, "validate-schema"
                    ).read_text()
                )
                if [
                    (r["ColumnName"], r["DataType"], r["Category"]) for r in schema
                ] != [
                    ("time", "TIMESTAMP", "TIME"),
                    ("channel_id", "STRING", "TAG"),
                    ("value", "DOUBLE", "FIELD"),
                ]:
                    raise ValueError("Existing table schema differs")
                save(state, identity)
            normalized = directory / "normalized.csv"
            if not normalized.exists():
                frame.to_csv(normalized, index=False, float_format="%.17g", na_rep="")
            marker = directory / "import.complete.json"
            if (
                marker.exists()
                and json.loads(marker.read_text()).get("rows") != expected["rows"]
            ):
                counts = cli_rows(
                    self.sql(
                        f"SELECT count(*) AS n FROM {database}.raw_data",
                        directory,
                        "resume-count",
                    ).read_text()
                )
                if int(counts[0]["n"]) != expected["rows"]:
                    marker.rename(
                        directory / f"import-unverified-{time.time_ns()}.json"
                    )
                else:
                    save(
                        marker,
                        {"source_sha256": original_sha, "rows": expected["rows"]},
                    )
            if not marker.exists():
                failed = directory / "failed"
                failed.mkdir(exist_ok=True)
                self.run(
                    [
                        str(self.home / "tools/import-data.sh"),
                        *self.common,
                        "-ft",
                        "csv",
                        "-db",
                        database,
                        "-table",
                        "raw_data",
                        "-s",
                        str(normalized),
                        "-fd",
                        str(failed),
                        "-batch",
                        "10000",
                        "-tn",
                        "1",
                        "-tp",
                        "ms",
                        "-tz",
                        "+00:00",
                    ],
                    directory,
                    "import",
                    600,
                )
                if any(failed.iterdir()):
                    raise ValueError("Import produced rejected rows")
                counts = cli_rows(
                    self.sql(
                        f"SELECT count(*) AS n FROM {database}.raw_data",
                        directory,
                        "import-count",
                    ).read_text()
                )
                if int(counts[0]["n"]) != expected["rows"]:
                    raise ValueError("Imported row count mismatch")
                save(marker, {"source_sha256": original_sha, "rows": expected["rows"]})
            if self.args.import_only:
                counts = cli_rows(
                    self.sql(
                        f"SELECT count(*) AS n FROM {database}.raw_data",
                        directory,
                        "import-count",
                    ).read_text()
                )
                if int(counts[0]["n"]) != expected["rows"]:
                    marker.rename(
                        directory / f"import-unverified-{time.time_ns()}.json"
                    )
                    raise ValueError("Imported row count mismatch")
                result["data"] = {"source": expected}
                result["status"] = "imported"
                save(record, result)
                return result
            export = directory / "sql-export"
            export.mkdir(exist_ok=True)
            self.run(
                [
                    str(self.home / "tools/export-data.sh"),
                    *self.common,
                    "-ft",
                    "csv",
                    "-db",
                    database,
                    "-table",
                    "raw_data",
                    "-t",
                    str(export),
                    "-pfn",
                    "rows",
                    "-q",
                    f"SELECT cast(time AS INT64) AS time,channel_id,value FROM {database}.raw_data ORDER BY time",
                    "-tf",
                    "timestamp",
                    "-lpf",
                    "10000000",
                ],
                directory,
                "sql-export",
                600,
            )
            parts = sorted(export.glob("*.csv"))
            if len(parts) != 1:
                raise ValueError("Expected one complete SQL CSV export")
            sql_data = fingerprint(read_output(parts[0]))
            fs_command = [
                str(self.home / "sbin/start-cli.sh"),
                *self.common,
                "--access_mode",
                "filesystem",
                "-e",
            ]
            fs_file = self.run(
                [*fs_command, f"cat -f csv /{database}/raw_data.csv"],
                directory,
                "fs-full",
                600,
            )
            fs_frame = read_output(fs_file)
            fs_data = fingerprint(fs_frame)
            result["data"] = {"source": expected, "sql": sql_data, "fs": fs_data}
            if not expected == sql_data == fs_data:
                raise ValueError("Full data fingerprint mismatch")
            tsdir = directory / "tsfile"
            tsdir.mkdir(exist_ok=True)
            self.run(
                [
                    str(self.home / "tools/export-data.sh"),
                    *self.common,
                    "-ft",
                    "tsfile",
                    "-db",
                    database,
                    "-table",
                    "raw_data",
                    "-t",
                    str(tsdir),
                    "-pfn",
                    "raw_data",
                ],
                directory,
                "tsfile-export",
                600,
            )
            files = list(tsdir.glob("*.tsfile"))
            if len(files) != 1:
                raise ValueError("Expected exactly one TsFile")
            tf = self.tsfile_fingerprint(files[0])
            result["tsfile"] = {
                "path": str(files[0]),
                "bytes": files[0].stat().st_size,
                "sha256": hashlib.sha256(files[0].read_bytes()).hexdigest(),
                "data": tf,
            }
            if tf != expected:
                raise ValueError("TsFile full data fingerprint mismatch")
            public, private = bounds(task), bounds(task, private=True)
            lo, hi = public
            fs_scoped_file = self.run(
                [
                    *fs_command,
                    f"cat -f csv --start {lo} --end {hi-1} --tag-filter channel_id eq {task['channel']} /{database}/raw_data.csv",
                ],
                directory,
                "fs-scoped",
                600,
            )
            fs_scoped = read_output(fs_scoped_file)
            if fingerprint(fs_scoped) != fingerprint(scoped(frame, public)):
                raise ValueError("FS public time-range mismatch")
            oracle = solve(task, scoped(frame, public))
            sql = self.sql_answer(
                task, database, public, frame, directory, "answer-public"
            )
            fs = solve(task, fs_scoped, composition=True)
            hidden_oracle = solve(task, scoped(frame, private))
            hidden_sql = (
                self.sql_answer(
                    task, database, private, frame, directory, "answer-private"
                )
                if private != public
                else sql
            )
            gold = task["ground_truth"]

            def score(answer):
                value = f"{answer:.3f}" if isinstance(answer, float) else answer
                return self.evaluator.score_one(task["eval_metric"], value, gold)[0]

            result["answers"] = {
                "public_bounds_ms": public,
                "private_bounds_ms": private,
                "oracle_public": oracle,
                "sql_public": sql,
                "fs_composed_public": fs,
                "oracle_private": hidden_oracle,
                "sql_private": hidden_sql,
                "gold": gold,
                "sql_oracle_equal": answer_equal(task, sql, oracle),
                "fs_oracle_equal": answer_equal(task, fs, oracle),
                "private_sql_oracle_equal": answer_equal(
                    task, hidden_sql, hidden_oracle
                ),
                "public_gold_equal": answer_equal(task, oracle, gold),
                "private_gold_equal": answer_equal(task, hidden_oracle, gold),
                "public_official_score": score(oracle),
                "private_official_score": score(hidden_oracle),
                "sql_official_score": score(sql),
                "fs_official_score": score(fs),
            }
            result["status"] = (
                "verified"
                if all(
                    result["answers"][k]
                    for k in [
                        "sql_oracle_equal",
                        "fs_oracle_equal",
                        "private_sql_oracle_equal",
                    ]
                )
                else "answer_mismatch"
            )
            result["fs_capability"] = (
                "cat_plus_external_generic_composition_not_native_fs"
            )
            result["experiment_ready"] = False
        except Exception as exc:
            result["error"] = str(exc)
            (directory / "error.txt").write_text(traceback.format_exc())
        save(record, result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["iotdb-home", "sonar-root", "data-root", "output"]:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=32867)
    parser.add_argument("--username", default="root")
    parser.add_argument("--prefix", default="nlqts_v1_")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--ids", nargs="*")
    parser.add_argument("--recheck", action="store_true")
    parser.add_argument("--import-only", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.prefix):
        parser.error("Invalid database prefix")
    migration = Migration(args)
    tasks = [
        (i, t)
        for i, t in enumerate(
            json.loads((Path(args.sonar_root) / "nlqtsbench/tasks.json").read_text())
        )
        if t["level"] == 1 and (not args.ids or t["id"] in args.ids)
    ]
    if args.limit:
        tasks = tasks[: args.limit]
    rows = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(migration.task, i, t) for i, t in tasks]
        for future in as_completed(futures):
            row = future.result()
            rows.append(row)
            print(
                json.dumps(
                    {
                        "completed": len(rows),
                        "total": len(tasks),
                        "task": row["task_id"],
                        "status": row["status"],
                        "error": row.get("error"),
                    }
                ),
                flush=True,
            )
    rows.sort(key=lambda r: r["source_index"])
    save(
        Path(args.output) / "migration-results.json",
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "model_used": False,
            "records": rows,
        },
    )
    return 1 if any(r["status"] == "error" for r in rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
