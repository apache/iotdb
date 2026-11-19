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

"""Deterministic read-only regression on imported data; never invokes an LLM."""
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

    gas = "/root/iotdb_gas_dynamic/experiment/ethylene_co"
    data = run("gas-full-scalar", "iotdb_gas_dynamic", f"stats {gas} -m sensor_01 --aggregates count,min,max,avg,first,last -f ndjson")[0]
    gold = run("gas-full-sql", "iotdb_gas_dynamic", "SELECT count(sensor_01) AS c, min_value(sensor_01) AS lo, max_value(sensor_01) AS hi, avg(sensor_01) AS av, first_value(sensor_01) AS f, last_value(sensor_01) AS l FROM root.iotdb_gas_dynamic.experiment.ethylene_co", False)[0]
    for left, right in [("count", "c"), ("min", "lo"), ("max", "hi"), ("avg", "av"), ("first", "f"), ("last", "l")]:
        assert math.isclose(float(data[left]), float(gold[right]), rel_tol=1e-8, abs_tol=1e-9), (left, data[left], gold[right])
    page = run("gas-narrow-page", "iotdb_gas_dynamic", f"head {gas} -m sensor_01 -m sensor_02 --start 5020520 --end 5020520 -n 1 -f ndjson")
    assert len(page) == 1 and float(page[0]["sensor_01"]) == 3553.64 and float(page[0]["sensor_02"]) == -4.39
    ranges = [(0, 10000), (5000000, 5100000), (5000000, 5100000), (-1000, -1)]
    batch = run("gas-window-batch", "iotdb_gas_dynamic", f"stats {gas} -m sensor_01 --windows " + ",".join(f"{lo}:{hi}" for lo, hi in ranges) + " --aggregates count,min,max,first,last,null_count -f ndjson")
    for i, (lo, hi) in enumerate(ranges):
        single = run(f"gas-window-{i}", "iotdb_gas_dynamic", f"stats {gas} -m sensor_01 --start {lo} --end {hi} --aggregates count,min,max,first,last,null_count -f ndjson")[0]
        assert {k:v for k,v in batch[i].items() if not k.startswith("window_")} == single
    pv = "/root/iotdb_pvdaq/system_1199/metric_2745"
    start, width = 1559347200000, 1209600000
    windows = [(start+i*width,start+(i+1)*width-1) for i in range(8)]
    batch = run("pvdaq-eight-windows", "iotdb_pvdaq", f"stats {pv} -m value --windows " + ",".join(f"{lo}:{hi}" for lo, hi in windows) + " --aggregates count,first,last,min_time,max_time -f ndjson")
    for i, (lo, hi) in enumerate(windows):
        single = run(f"pvdaq-window-{i}", "iotdb_pvdaq", f"stats {pv} -m value --start {lo} --end {hi} --aggregates count,first,last,min_time,max_time -f ndjson")[0]
        assert {k:v for k,v in batch[i].items() if not k.startswith("window_")} == single
    assert float(batch[1]["first"]) == 27 and math.isclose(float(batch[1]["last"]), 47.566667)
    ndbc = "/root/iotdb_ndbc/station_42001"
    lo, hi = 1672531200000, 1672617599999
    rows = run("ndbc-day-rows", "iotdb_ndbc", f"cat {ndbc} -m wspd --start {lo} --end {hi} -f ndjson")
    stats = run("ndbc-day-median", "iotdb_ndbc", f"stats {ndbc} -m wspd --start {lo} --end {hi} --aggregates count,median -f ndjson")[0]
    values = [float(r["wspd"]) for r in rows if r["wspd"] is not None]
    assert int(stats["count"]) == len(values) and float(stats["median"]) == statistics.median(values)
    # Larger exact median forces disk runs; check its rank against independent SQL counts.
    median = run("gas-full-exact-median", "iotdb_gas_dynamic", f"stats {gas} -m sensor_01 --aggregates count,median -f ndjson")[0]
    m = str(Decimal(str(median["median"])))
    lower = run("gas-median-rank-lower", "iotdb_gas_dynamic", f"SELECT count(sensor_01) AS c FROM root.iotdb_gas_dynamic.experiment.ethylene_co WHERE sensor_01 < {m}", False)[0]
    upper = run("gas-median-rank-upper", "iotdb_gas_dynamic", f"SELECT count(sensor_01) AS c FROM root.iotdb_gas_dynamic.experiment.ethylene_co WHERE sensor_01 > {m}", False)[0]
    assert int(lower["c"]) <= int(median["count"]) // 2 and int(upper["c"]) <= int(median["count"]) // 2
    summary = {"status": "passed", "heap_mib": 512, "jar_sha256": hashlib.sha256(args.jar.read_bytes()).hexdigest(), "cases": results, "checks": ["full scalar statistics vs native SQL", "known extreme associated row", "four overlapping/duplicate/empty windows vs independent scans", "eight PVDAQ windows vs independent scans and original gold winner window", "NDBC exact median vs Python median", "full Gas median rank inequalities (even-count midpoint additionally covered by unit tests)"]}
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    print("VALIDATION PASSED", flush=True)


if __name__ == "__main__":
    main()
