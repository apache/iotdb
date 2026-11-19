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

"""Offline two-arm analysis; frozen designated-table serializer and original matcher."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys

from final_table_submission import serialize_final_table


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--output",type=Path,required=True)
    p.add_argument("--sql-baseline",type=Path,required=True)
    p.add_argument("--snapshot",type=Path,required=True)
    p.add_argument("--contracts",type=Path,required=True)
    args=p.parse_args()
    read=lambda path:json.loads(path.read_text())
    sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
    snapshot=read(args.snapshot/"code-snapshot.json")
    for entry in snapshot["files"]:
        assert sha(args.snapshot/entry["path"])==entry["sha256"],entry["path"]
    os.environ["TSDB_RESOURCE_GUARD"]="0"
    sys.path.insert(0,str(args.snapshot/"src"))
    from tsdb_benchmark.results import MatchPolicy, compare_outputs
    contracts={q["id"]:q for q in read(args.contracts)["questions"]}
    arms=["grid-reduce","composition"]
    protocols={arm:read(args.output/arm/"protocol.json") for arm in arms}
    integrity={k:len({json.dumps(v.get(k),sort_keys=True) for v in protocols.values()})==1 for k in ["limits","selection_sha256","settings_sha256","cli_jar_sha256","source_sha256","final_submission"]}
    integrity["features"]=protocols[arms[0]]["features"]==["temporal","extreme","grid","reduce"] and protocols[arms[1]]["features"]==["temporal","extreme","grid","reduce","composition"]
    for ctx in (args.output/arms[0]/"public-context").glob("*.json"):
        integrity[ctx.name]=len({sha(args.output/arm/"public-context"/ctx.name) for arm in arms})==1
    assert all(integrity.values()),integrity
    data={}
    for arm in arms:
        folder=args.output/arm
        subprocess.run([sys.executable,str(Path(__file__).with_name("analyze_fs_pilot.py")),"--output",str(folder),"--sql-baseline",str(args.sql_baseline)],check=True,stdout=subprocess.DEVNULL)
        rows=read(folder/"summary.json")["rows"]
        for row in rows:
            trial=folder/"trials"/row["id"]
            q=contracts[row["id"]]
            gold=args.sql_baseline/"validation"/row["id"]/"call-24d7f03d8dc3c3666969"/"stdout.txt"
            assert sha(gold)==q["gold_stdout_sha256"]
            evaluation={"track":"FS final-table extension; original comparator", "final_sha256":sha(trial/"dsh_result.json"), "gold_sha256":sha(gold)}
            try:
                candidate=serialize_final_table(row["final"])
                matched,detail=compare_outputs(gold.read_text(),candidate,MatchPolicy.from_dict(q["match_policy"]))
                evaluation.update(matched=matched,detail=detail,failure_stage=None if matched else "result_mismatch")
                (trial/"submitted-final-table.json").write_text(candidate)
            except ValueError as e:
                evaluation.update(matched=False,detail=str(e),failure_stage="submission_invalid")
            (trial/"evaluation.json").write_text(json.dumps(evaluation,indent=2))
            row["evaluation"]=evaluation
            ops=Counter(); prior=Counter(); dup=[]; verifications=[]; reuse=[]; errors=[]
            for call in row["calls"]:
                a=call["arguments"]
                signature=json.dumps({"kind":call["kind"],"arguments":a},sort_keys=True)
                prior[signature]+=1
                if a.get("command")!="frame":continue
                ops[a.get("op","missing")]+=1
                result_path=folder/call["output_ref"]/"result.json"
                payload=json.loads(read(result_path).get("content","{}")) if result_path.exists() else {}
                if call["is_error"]:errors.append({"callId":call["callId"],"arguments":a,"error":payload})
                if payload.get("duplicate_request"):dup.append({"callId":call["callId"],"op":a.get("op"),"scan_performed":payload.get("scan_performed")})
                if a.get("op")=="verify":verifications.append({"callId":call["callId"],**payload.get("verification",{})})
                if payload.get("reused"):reuse.append({"callId":call["callId"],"op":a.get("op"),"scan_performed":payload.get("scan_performed")})
            row["frame"]={"operations":dict(ops),"errors":errors,"duplicates":dup,"verifications":verifications,"reuse":reuse}
            row["identical_request_repeats"]=sum(max(0,n-1) for n in prior.values())
        summary={k:sum(r[k] for r in rows) for k in ["wall_seconds","model_seconds","tool_interval_sum_seconds","tool_attempts","fs_calls","context_calls","tool_errors","transport_retries","model_requests","discovery_fs_calls","identical_request_repeats","heap_oom_calls","truncated_outputs"]}
        for key in ["totalTokens","inputTokens","cachedInputTokens","outputTokens","reasoningTokens"]:summary[key]=sum(r["usage"].get(key,0) for r in rows)
        denom=summary["inputTokens"]+summary["cachedInputTokens"]
        summary.update(recorded=len(rows),planned=8,matched=sum(r["evaluation"]["matched"] for r in rows),median_wall_seconds=statistics.median(r["wall_seconds"] for r in rows),median_total_tokens=statistics.median(r["usage"].get("totalTokens",0) for r in rows),cache_hit_ratio=summary["cachedInputTokens"]/denom if denom else None)
        summary["frame_adoption_questions"]=sum(bool(r["frame"]["operations"]) for r in rows)
        summary["frame_operations"]=dict(sum((Counter(r["frame"]["operations"]) for r in rows),Counter()))
        summary["rows"]=rows
        data[arm]=summary
    (args.output/"comparison.json").write_text(json.dumps({"integrity":integrity,"arms":data,"limitations":"16 stochastic development cells; bundle ablation; original-matcher FS extension, not original SQL execution track; no post-hoc answer repair"},ensure_ascii=False,indent=2))
    print(json.dumps({a:{k:v for k,v in d.items() if k!="rows"} for a,d in data.items()},ensure_ascii=False,indent=2))


if __name__=="__main__":main()
