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

"""Run the frozen four-arm matrix serially with rotated arm order."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    root,output=args.root.resolve(),args.output.resolve()
    source=Path(__file__).resolve().parent
    baseline=root/"results/tqts-discovery-pilot-20261118"
    previous=root/"results/tqts-fs-temporal-20261119/both"
    validation=json.loads((output/"validation/summary.json").read_text())
    jar=root/"runtime/iotdb/lib/iotdb-cli-2.0.11-SNAPSHOT.jar"
    assert validation["status"]=="passed"
    assert hashlib.sha256(jar.read_bytes()).hexdigest()==validation["jar_sha256"]
    tasks=json.loads((baseline/"selection.json").read_text())
    arms=["grid-base","grid-only","reduce-only","grid-reduce"]
    plan=[{"question":t["id"],"arm":arms[(i+j)%4]} for i,t in enumerate(tasks) for j in range(4)]
    plan_path=output/"schedule.json"
    if plan_path.exists():
        assert json.loads(plan_path.read_text())==plan
    else:
        plan_path.write_text(json.dumps(plan,indent=2))
    def command(arm):
        return [sys.executable,str(source/"run_fs_pilot.py"),"--root",str(root),"--sql-baseline",str(baseline),"--fs-baseline",str(previous),"--output",str(output/arm),"--features",arm]
    for arm in arms:
        subprocess.run(command(arm)+["--prepare-only"],check=True)
    completed=[]
    for item in plan:
        arm,qid=item["arm"],item["question"]
        print("CELL",arm,qid,flush=True)
        started=time.time()
        subprocess.run(command(arm)+["--task-id",qid,"--run"],check=True)
        trial=output/arm/"trials"/qid
        result=json.loads((trial/"dsh_result.json").read_text())
        policy=json.loads((trial/"policy-audit.json").read_text())
        assert policy["passed"], "Tool-policy mismatch"
        completed.append({**item,"status":result["status"],"task_wall_ms":result.get("task_wall_ms"),"wrapper_seconds":time.time()-started})
        (output/"progress.json").write_text(json.dumps(completed,indent=2))
        if result["status"]=="infrastructure_error" and (result.get("turn_end_reason") or {}).get("kind")!="max-tokens":
            raise RuntimeError("Stop on infrastructure error; preserve prior results without selective reruns")
    (output/"completed.json").write_text(json.dumps({"cells":len(completed),"completed_at_epoch":time.time()},indent=2))
    print("ABLATION COMPLETE",flush=True)


if __name__=="__main__":
    main()
