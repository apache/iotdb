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

    def run(label, package, command, fs=True, dialect="tree", ddl=False):
        folder = args.output / label
        folder.mkdir(exist_ok=True)
        creds = json.loads((args.root / "results/tqts-discovery-pilot-20261118/credentials" / (package + ".json")).read_text()) if package else {"username": os.environ.get("IOTDB_VALIDATION_USER", "root"), "password": os.environ.get("IOTDB_VALIDATION_PASSWORD", "root")}
        java = str(Path(os.environ["JAVA_HOME"]) / "bin/java")
        spill = folder / "private-spill"
        spill.mkdir(mode=0o700, exist_ok=True)
        argv = ["/usr/bin/time", "-v", "-o", str(folder / "resources.txt"), java, "-Xms64m", "-Xmx512m", "-XX:MaxDirectMemorySize=128m", "-Djava.io.tmpdir=" + str(spill), "--add-opens=java.base/java.lang=ALL-UNNAMED", "-Dlogback.configurationFile=" + str(args.root / "runtime/iotdb/conf/logback-cli.xml"), "-cp", str(args.jar) + ":" + str(args.root / "runtime/iotdb/lib/*"), "org.apache.iotdb.cli.Cli", "-h", "127.0.0.1", "-p", "32867" if package else "33867", "-u", creds["username"], "-pw", creds["password"], "-sql_dialect", dialect, "-disableISO8601", "-timeout", "280"]
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
        if code != 0 or "Error" in raw or ("Msg:" in raw and not ddl):
            raise RuntimeError("Validation failed: " + label)
        if record["spill_files_after"]:
            raise RuntimeError("Temporary files remained: " + label)
        if ddl:
            return []
        if fs:
            return [json.loads(line) for line in raw.splitlines() if line.strip()]
        lines = [line for line in raw.splitlines() if line.startswith("|")]
        if not lines:
            return []
        split = lambda line: [cell.strip() for cell in line.strip().strip("|").split("|")]
        keys = split(lines[0])
        return [dict(zip(keys, split(line))) for line in lines[1:]]

    def close(a,b):
        assert math.isclose(float(a),float(b),rel_tol=1e-8,abs_tol=1e-8),(a,b)
    def fs(label,cmd,package=None,dialect="tree"):
        return run(label,package,cmd+" -f ndjson",True,dialect)
    def sql(label,query,package=None,dialect="tree",ddl=False):
        return run(label,package,query,False,dialect,ddl)
    # Separate synthetic namespaces: task accounts cannot access these fixtures.
    fixture="root.fs_window_validation_20261119"
    for device,rows in {"a":[(0,1),(10,3),(20,5)],"b":[(0,100),(30,80)],"c":[(5,-4),(15,-2)]}.items():
        if not sql("tree-exists-"+device,f"SHOW TIMESERIES {fixture}.{device}.x"):
            sql("tree-create-"+device,f"CREATE TIMESERIES {fixture}.{device}.x WITH DATATYPE=INT64,ENCODING=PLAIN",ddl=True)
        sql("tree-insert-"+device,f"INSERT INTO {fixture}.{device}(time,x) VALUES "+",".join(f"({t},{v})" for t,v in rows),ddl=True)
    native=fs("tree-grid-ranked",f"reduce /{fixture.replace('.', '/')} --field x --metrics count,sum,avg,min,max,first,last,delta,elapsed_ms,rate_per_ms --start 0 --end 39 --origin 0 --width 20 --rank-by delta --order desc")
    reference=sql("tree-grid-ranked-sql",f"SELECT count(x) AS c,sum(x) AS s,avg(x) AS av,min_value(x) AS lo,max_value(x) AS hi,first_value(x) AS f,last_value(x) AS l,last_value(x)-first_value(x) AS d,min_time(x) AS ft,max_time(x) AS lt FROM {fixture}.* GROUP BY ([0,40),20ms) HAVING count(x)>0 ALIGN BY DEVICE")
    expected={(r['Device'],r['Time']):r for r in reference}
    assert len(native)==len(expected)
    for row in native:
        ref=expected[(row['object'],row['window_start'])]
        for metric,key in [('count','c'),('sum','s'),('avg','av'),('min','lo'),('max','hi'),('first','f'),('last','l'),('delta','d'),('first_at','ft'),('last_at','lt')]:close(row[metric],ref[key])
    assert native[0]['object'].endswith('.a') and native[0]['window_start']=='0'
    page=fs("tree-top-page",f"reduce /{fixture.replace('.', '/')} --field x --metrics delta --start 0 --end 39 --origin 0 --width 20 --rank-by delta --top 3 -n 1 --offset 1")
    assert page[0]['object']==native[1]['object'] and page[0]['window_index']==native[1]['window_index']
    explicit=fs("tree-duplicate-empty",f"reduce /{fixture.replace('.', '/')}/a --field x --metrics delta --windows 0:19,0:19,100:200")
    assert len(explicit)==2 and explicit[0]['delta']==explicit[1]['delta']=='2'
    # Table fixture independently exercises true tag identities and SQL pooling.
    db="fs_window_validation_20261119"
    sql("table-create-db",f"CREATE DATABASE IF NOT EXISTS {db}",dialect="table",ddl=True)
    sql("table-create",f"CREATE TABLE IF NOT EXISTS {db}.events (entity STRING TAG, x DOUBLE FIELD)",dialect="table",ddl=True)
    sql("table-insert",f"INSERT INTO {db}.events(time,entity,x) VALUES (0,'a',1.0),(10,'a',3.0),(20,'a',5.0),(0,'b',100.0),(30,'b',80.0),(5,NULL,-4.0),(15,NULL,-2.0),(25,'b',NULL)",dialect="table",ddl=True)
    table_rank=fs("table-window-rank",f"reduce /{db}/events --field x --metrics delta --start 0 --end 39 --origin 0 --width 20 --rank-by delta --top 4 --offset 1 -n 2",dialect="table")
    table_rank_ref=sql("table-window-rank-sql",f"SELECT cast(date_bin(20ms,time) AS INT64) AS w,entity,last(x)-first(x) AS d FROM {db}.events WHERE time >= 0 AND time < 40 AND x IS NOT NULL GROUP BY date_bin(20ms,time),entity ORDER BY d DESC,w ASC,entity ASC NULLS FIRST LIMIT 2 OFFSET 1",dialect="table")
    assert len(table_rank)==len(table_rank_ref)==2
    for row,ref in zip(table_rank,table_rank_ref):
        assert row['window_start']==ref['w'] and row['tag.entity']==(None if ref['entity'].lower()=='null' else ref['entity'])
        close(row['delta'],ref['d'])
    table=fs("table-entity",f"reduce /{db}/events --field x --metrics count,sum,avg,min,max,first,last,delta,elapsed_ms,rate_per_ms --rank-by avg --start 0 --end 39",dialect="table")
    tref=sql("table-entity-sql",f"SELECT entity,count(x) AS c,sum(x) AS s,avg(x) AS av,min(x) AS lo,max(x) AS hi,first(x) AS f,last(x) AS l,last(x)-first(x) AS d,cast(min(time) AS INT64) AS ft,cast(max(time) AS INT64) AS lt FROM {db}.events WHERE x IS NOT NULL AND time >= 0 AND time < 40 GROUP BY entity ORDER BY av DESC,entity ASC NULLS FIRST",dialect="table")
    assert len(table)==len(tref)==3
    for row,ref in zip(table,tref):
        assert row['tag.entity']==(None if ref['entity'].lower()=='null' else ref['entity'])
        for metric,key in [('count','c'),('sum','s'),('avg','av'),('min','lo'),('max','hi'),('first','f'),('last','l'),('delta','d'),('first_at','ft'),('last_at','lt')]:close(row[metric],ref[key])
    pooled=fs("table-pooled",f"reduce /{db}/events --field x --group-by pooled --metrics count,sum,avg,min,max,first,last,delta,elapsed_ms,rate_per_ms --start 0 --end 39",dialect="table")[0]
    pref=sql("table-pooled-sql",f"SELECT count(x) AS c,sum(x) AS s,avg(x) AS av,min(x) AS lo,max(x) AS hi FROM {db}.events WHERE x IS NOT NULL AND time >= 0 AND time < 40",dialect="table")[0]
    for metric,key in [('count','c'),('sum','s'),('avg','av'),('min','lo'),('max','hi')]:close(pooled[metric],pref[key])
    endpoints=[]
    for direction in ['ASC','DESC']:
        endpoints.append(sql("table-endpoint-"+direction,f"SELECT cast(time AS INT64) AS t,entity,x FROM {db}.events WHERE x IS NOT NULL AND time >= 0 AND time < 40 ORDER BY time {direction},entity ASC NULLS FIRST LIMIT 1",dialect="table")[0])
    for row,prefix in zip(endpoints,['first','last']):close(pooled[prefix],row['x']);close(pooled[prefix+'_at'],row['t'])
    close(pooled['delta'],float(endpoints[1]['x'])-float(endpoints[0]['x']))
    close(pooled['rate_per_ms'],float(pooled['delta'])/float(pooled['elapsed_ms']))
    treepool=fs("tree-pooled",f"reduce /{fixture.replace('.', '/')} --field x --group-by pooled --metrics count,sum,avg,first,last,delta")[0]
    for metric in ['count','sum','avg','first','last','delta']:close(treepool[metric],pooled[metric])
    # Calendar DST and month-end boundaries use independently specified epoch oracles.
    grid=fs("dst-grid","windows --start 1710046800000 --end 1710215999999 --origin 2024-03-10T00:00:00 --width 1 --unit day --timezone America/New_York")
    assert [(int(w['window_start']),int(w['nominal_end_exclusive'])) for w in grid]==[(1710046800000,1710129600000),(1710129600000,1710216000000)]
    sql("table-dst-insert",f"INSERT INTO {db}.events(time,entity,x) VALUES (1710046800000,'dst',10.0),(1710129599999,'dst',20.0),(1710129600000,'dst',30.0),(1710215999999,'dst',50.0)",dialect="table",ddl=True)
    dst=fs("table-dst-reduce",f"reduce /{db}/events --field x --metrics count,delta --start 1710046800000 --end 1710215999999 --origin 2024-03-10T00:00:00 --width 1 --unit day --timezone America/New_York",dialect="table")
    for i,(lo,hi) in enumerate([(1710046800000,1710129600000),(1710129600000,1710216000000)]):
        ref=sql("table-dst-sql-"+str(i),f"SELECT count(x) AS c,last(x)-first(x) AS d FROM {db}.events WHERE time >= {lo} AND time < {hi}",dialect="table")[0]
        close(dst[i]['count'],ref['c']);close(dst[i]['delta'],ref['d'])
    # Real data: endpoints across every generated window and global entity ranking.
    cmapss=fs("cmapss-grid-delta","reduce /root/iotdb_cmapss/train/fd001/engine_1 --field sensor_03 --metrics count,first,last,delta,elapsed_ms,rate_per_ms --start 1000 --end 192999 --origin 1000 --width 24000","iotdb_cmapss")
    cref=sql("cmapss-grid-delta-sql","SELECT count(sensor_03) AS c,first_value(sensor_03) AS f,last_value(sensor_03) AS l,last_value(sensor_03)-first_value(sensor_03) AS d,min_time(sensor_03) AS ft,max_time(sensor_03) AS lt FROM root.iotdb_cmapss.train.fd001.engine_1 GROUP BY ([1000,193000),24s)","iotdb_cmapss")
    assert len(cmapss)==len(cref)==8
    for row,ref in zip(cmapss,cref):
        for metric,key in [('count','c'),('first','f'),('last','l'),('delta','d'),('first_at','ft'),('last_at','lt')]:close(row[metric],ref[key])
    pv=fs("pvdaq-grid-rank","reduce /root/iotdb_pvdaq/system_1199 --field value --metrics first,last,delta --start 1559347200000 --end 1569887999999 --origin 2019-06-01T00:00:00 --width 14 --unit day --timezone UTC --rank-by delta --top 5","iotdb_pvdaq")
    pref=sql("pvdaq-grid-rank-sql","SELECT first_value(value) AS f,last_value(value) AS l,last_value(value)-first_value(value) AS d FROM root.iotdb_pvdaq.system_1199.* GROUP BY ([1559347200000,1569888000000),1209600000ms) HAVING count(value)>0 ALIGN BY DEVICE","iotdb_pvdaq")
    # This server's Tree aggregate ORDER BY drops groups (diagnostics retained).
    # Compare all SQL-produced aggregates and sort them independently; actual native
    # SQL ranking equivalence is checked above in the Table date_bin query.
    pref=sorted(pref,key=lambda r:(-Decimal(r["d"]),int(r["Time"]),r["Device"]))[:5]
    assert len(pv)==len(pref)==5
    for row,ref in zip(pv,pref):
        assert row['object']==ref['Device'] and row['window_start']==ref['Time'],(row,ref)
        for metric,key in [('first','f'),('last','l'),('delta','d')]:close(row[metric],ref[key])
    summary={"status":"passed","jar_sha256":hashlib.sha256(args.jar.read_bytes()).hexdigest(),"heap_mib":512,"cases":results,"tree_sort_oracle":"Tree aggregate ORDER BY dropped groups on both original and fresh isolated processes. Real Tree top-k uses all unsorted SQL aggregates plus independent deterministic sort; actual SQL ordered-window top/offset equivalence tested with Table date_bin. Diagnostics retained; benchmark gold untouched.","scope":"Tree and Table synthetic SQL differential including pooled observations, NULL TAG/target, ranking/endpoints, DST and pagination; real Cmapss window deltas and PVDAQ global entity/window ranking. Empty FS groups omitted; no universal SQL NULL/tie or NL interpretation equivalence."}
    (args.output/"summary.json").write_text(json.dumps(summary,indent=2))
    print("VALIDATION PASSED",flush=True)

if __name__=="__main__":main()
