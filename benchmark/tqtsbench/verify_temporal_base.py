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

"""Verify temporal FS operators against native benchmark SQL without an LLM."""
import argparse
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    results = []

    def run(label, package, command, fs=True):
        folder = args.output / label
        folder.mkdir(exist_ok=True)
        creds = json.loads((args.root / "results/tqts-discovery-pilot-20261118/credentials" / (package + ".json")).read_text())
        java = str(Path(os.environ["JAVA_HOME"]) / "bin/java")
        spill = folder / "private-spill"
        spill.mkdir(mode=0o700, exist_ok=True)
        argv = ["/usr/bin/time", "-v", "-o", str(folder / "resources.txt"), java, "-Xms64m", "-Xmx512m", "-XX:MaxDirectMemorySize=128m", "-Djava.io.tmpdir=" + str(spill), "--add-opens=java.base/java.lang=ALL-UNNAMED", "-Dlogback.configurationFile=" + str(args.root / "runtime/iotdb/conf/logback-cli.xml"), "-cp", str(args.jar) + ":" + str(args.root / "runtime/iotdb/lib/*"), "org.apache.iotdb.cli.Cli", "-h", "127.0.0.1", "-p", "32867", "-u", creds["username"], "-pw", creds["password"], "-sql_dialect", "tree", "-disableISO8601", "-timeout", "280"]
        if fs:
            argv += ["--access_mode", "filesystem"]
        argv += ["-e", command]
        (folder / "command.txt").write_text(command)
        start = time.monotonic()
        with (folder / "stdout.txt").open("w") as out, (folder / "stderr.txt").open("w") as err:
            proc = subprocess.Popen(argv, stdout=out, stderr=err, start_new_session=True)
            try:
                code = proc.wait(timeout=300)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
                raise RuntimeError("Validation timed out: " + label)
        elapsed = time.monotonic() - start
        raw = (folder / "stdout.txt").read_text()
        metrics = {}
        for line in (folder / "resources.txt").read_text().splitlines():
            if ":" not in line:
                continue
            key, val = line.strip().rsplit(":", 1)
            if key == "Maximum resident set size (kbytes)":
                metrics["peak_rss_kib"] = float(val)
            if key in ["User time (seconds)", "System time (seconds)"]:
                metrics[key] = float(val)
        record = {"label": label, "wall_seconds": elapsed, "exit_code": code, "resources": metrics, "spill_files_after": len(list(spill.iterdir()))}
        results.append(record)
        (args.output / "progress.json").write_text(json.dumps(results, indent=2))
        print(json.dumps(record), flush=True)
        if code != 0 or "Msg:" in raw:
            raise RuntimeError("Validation failed: " + label)
        if record["spill_files_after"]:
            raise RuntimeError("Temporary files remained: " + label)
        if fs:
            return [json.loads(line) for line in raw.splitlines() if line.strip()]
        lines = [line for line in raw.splitlines() if line.startswith("|")]
        if not lines:
            return []
        split = lambda line: [cell.strip() for cell in line.strip().strip("|").split("|")]
        keys = split(lines[0])
        return [dict(zip(keys, split(line))) for line in lines[1:]]

    selection = json.loads((args.root / "results/tqts-discovery-pilot-20261118/selection.json").read_text())
    tasks = {t["id"]: t for t in selection}
    def gold(task_id):
        task = tasks[task_id]
        return run(task_id + "-sql", task["package"], task["gold_query"], False)

    for package, suffix, path, field, mode, associated in [
        ("iotdb_gas_dynamic", "001", "/experiment/ethylene_co", "sensor_01", "max", "sensor_02"),
        ("iotdb_ndbc", "005", "/station_41001", "atmp", "min", "wtmp"),
    ]:
        task_id = package + "-qa-" + suffix
        data = run(task_id + "-fs", package, f"extreme /root/{package}{path} --field {field} --mode {mode} -m {associated} -f ndjson")
        reference = gold(task_id)
        assert len(data) == len(reference) == 1
        values = [v for k,v in reference[0].items() if k.lower() != "time"]
        assert int(data[0]["observed_at"]) == int(values[0]), (data, reference)
        assert math.isclose(float(data[0]["value."+associated]), float(values[1]), abs_tol=1e-9), (data, reference)
        at = int(data[0]["observed_at"])
        # Inclusive endpoints, duplicate windows, empty window and output paging.
        command = f"extreme /root/{package}{path} --field {field} --mode {mode} -m {associated} --windows {at}:{at},{at}:{at},-2:-1 -f ndjson"
        windows = run(task_id + "-windows", package, command)
        assert len(windows) == 2 and [r["window_index"] for r in windows] == [0,1]
        assert all(r["value."+associated] == data[0]["value."+associated] for r in windows)
        assert run(task_id + "-page", package, command + " -n 1 --offset 1") == windows[1:]

    package = "iotdb_ndbc"
    begin, end, step = 1672531200000, 1672617600000, 3600000
    windows = ",".join(f"{lo}:{lo+step-1}" for lo in range(begin,end,step))
    command = f"gaps /root/{package}/station_42001 --field wspd --windows {windows} -f ndjson"
    data = run("ndbc-gaps-fs",package,command)
    reference = gold("iotdb_ndbc-qa-028")
    expected = [(int(r["Time"]), int(next(v for k,v in r.items() if k != "Time"))) for r in reference]
    assert [(int(r["observed_at"]),int(r["gap_ms"])) for r in data] == expected, (data,reference)
    assert len(data) == 48 and int(data[0]["previous_at"]) < begin
    assert all(int(r["observed_at"])-int(r["previous_at"]) == int(r["gap_ms"]) for r in data)
    assert run("ndbc-gaps-page",package,command + " -n 3 --offset 2") == data[2:5]
    assert run("ndbc-gap-single-boundary",package,f"gaps /root/{package}/station_42001 --field wspd --start {begin} --end {begin} -f ndjson")[0]["gap_ms"] == "600000"

    package = "iotdb_pvdaq"
    path = "/root/iotdb_pvdaq/system_1200/metric_4199"
    command = f"sessions {path} --field value --gap-ms 14400000 -f ndjson"
    data = run("pvdaq-sessions-fs",package,command)
    reference = gold("iotdb_pvdaq-qa-006")
    assert len(data) == len(reference) == 2, (data,reference)
    for row, ref in zip(data,reference):
        assert int(row["session_start"]) == int(ref["Time"])
        assert int(row["session_end"]) == int(ref["__endTime"])
        assert math.isclose(float(row["avg"]), float(ref["mean_frequency"]),rel_tol=1e-10,abs_tol=1e-9), (row,ref)
    assert run("pvdaq-sessions-page",package,command+" -n 1 --offset 1") == data[1:]
    # Clipping is before segmentation, not a filter over complete sessions.
    low, high = int(data[0]["session_end"]), int(data[1]["session_start"])
    clipped = run("pvdaq-sessions-clipped",package,command+f" --start {low} --end {high}")
    assert len(clipped)==2 and all(int(r["count"])==1 for r in clipped)
    # Gap exactly at the threshold joins; one ms less splits. Real observations,
    # independent native SQL GROUP BY SESSION, not only a synthetic unit test.
    for threshold, count in [(high-low,1),(high-low-1,2)]:
        out = run(f"pvdaq-threshold-{threshold}-fs",package,f"sessions {path} --field value --gap-ms {threshold} --start {low} --end {high} -f ndjson")
        ref = run(f"pvdaq-threshold-{threshold}-sql",package,f"SELECT __endTime, avg(value) AS av FROM root.iotdb_pvdaq.system_1200.metric_4199 WHERE time >= {low} AND time <= {high} GROUP BY SESSION({threshold}ms)",False)
        assert len(out)==len(ref)==count
        for row,r in zip(out,ref):
            assert int(row["session_start"])==int(r["Time"]) and int(row["session_end"])==int(r["__endTime"])
            assert math.isclose(float(row["avg"]),float(r["av"]),abs_tol=1e-9)
    summary = {"status":"passed","jar_sha256":hashlib.sha256(args.jar.read_bytes()).hexdigest(),"heap_mib":512,"cases":results,"scope":"Four target original Tree gold queries plus inclusive/duplicate/empty windows, output pagination, outside-window predecessor, clipping and exact session-threshold SQL checks. Table entity isolation, NULL and INT64 boundaries additionally unit-tested; no claim of full SQL semantic coverage."}
    (args.output/"summary.json").write_text(json.dumps(summary,indent=2))
    print("VALIDATION PASSED",flush=True)


if __name__ == "__main__":
    main()
