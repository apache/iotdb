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

"""Sample only experiment FS descendants and host memory; never save command lines."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time


def stat(pid):
    text=(Path("/proc")/str(pid)/"stat").read_text().rsplit(") ",1)[1].split()
    return {"pid":int(pid),"ppid":int(text[1]),"state":text[0],"start_ticks":text[19],"rss_pages":int(text[21]),"cpu_ticks":int(text[11])+int(text[12])}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--run-pid",type=int,required=True)
    parser.add_argument("--runner",required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    original=stat(args.run_pid)["start_ticks"]
    with args.output.open("a") as out:
        while True:
            try:
                if stat(args.run_pid)["start_ticks"]!=original:break
            except (FileNotFoundError,ProcessLookupError):break
            processes={};roots=set()
            for p in Path("/proc").iterdir():
                if not p.name.isdigit():continue
                try:
                    value=stat(p.name);processes[value["pid"]]=value
                    # Inspect locally for membership only; credentials/argv are never persisted.
                    if args.runner.encode() in (p/"cmdline").read_bytes():roots.add(value["pid"])
                except (OSError,ValueError,IndexError):continue
            # Exclude this monitor, whose argv itself includes the runner path.
            import os
            roots.discard(os.getpid())
            selected=set(roots)
            while True:
                added={pid for pid,v in processes.items() if v["ppid"] in selected}-selected
                if not added:break
                selected.update(added)
            memory={}
            for line in Path("/proc/meminfo").read_text().splitlines():
                key,value=line.split(":",1)
                if key in {"MemAvailable","MemFree","SwapFree"}:memory[key+"_kib"]=int(value.split()[0])
            record={"at":datetime.now(timezone.utc).isoformat(),"memory":memory,"processes":[processes[pid] for pid in sorted(selected) if pid in processes]}
            out.write(json.dumps(record)+"\n");out.flush()
            time.sleep(5)


if __name__=="__main__":main()
