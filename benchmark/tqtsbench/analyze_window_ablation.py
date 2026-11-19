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

"""Read-only analysis of the fixed temporal/extreme capability matrix."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--sql-baseline",type=Path,required=True)
    args=parser.parse_args()
    read=lambda p:json.loads(p.read_text())
    arms=["grid-base","grid-only","reduce-only","grid-reduce"]
    protocols={a:read(args.output/a/"protocol.json") for a in arms}
    integrity={key:len({json.dumps(p.get(key),sort_keys=True) for p in protocols.values()})==1 for key in ["limits","selection_sha256","settings_sha256","cli_jar_sha256","source_sha256"]}
    integrity["feature_gates"]=all(protocols[a]["features"]==f for a,f in [("grid-base",["temporal","extreme"]),("grid-only",["temporal","extreme","grid"]),("reduce-only",["temporal","extreme","reduce"]),("grid-reduce",["temporal","extreme","grid","reduce"])])
    for context in (args.output/"grid-base/public-context").glob("*.json"):
        integrity[context.name]=len({hashlib.sha256((args.output/a/"public-context"/context.name).read_bytes()).hexdigest() for a in arms})==1
    assert all(integrity.values()),integrity
    data={}
    for arm in arms:
        folder=args.output/arm
        subprocess.run([sys.executable,str(Path(__file__).with_name("analyze_fs_pilot.py")),"--output",str(folder),"--sql-baseline",str(args.sql_baseline)],check=True,stdout=subprocess.DEVNULL)
        summary=read(folder/"summary.json")
        summary["scope"]="Contemporaneous same-question, same-budget FS capability ablation; one trial per cell; no causal generalization from a single stochastic matrix."
        (folder/"summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2))
        rows=summary["rows"]
        result={k:sum(r[k] for r in rows) for k in ["wall_seconds","model_seconds","tool_interval_sum_seconds","tool_attempts","fs_calls","context_calls","tool_errors","transport_retries","heap_oom_calls","truncated_outputs","model_requests"]}
        for k in ["totalTokens","inputTokens","cachedInputTokens","outputTokens","reasoningTokens"]:
            result[k]=sum(r["usage"].get(k,0) for r in rows)
        denom=result["inputTokens"]+result["cachedInputTokens"]
        result["cache_hit_ratio"]=result["cachedInputTokens"]/denom if denom else None
        result["median_wall_seconds"]=statistics.median(r["wall_seconds"] for r in rows) if rows else None
        result["completed"]=len(rows)
        commands=Counter()
        for row in rows:commands.update(row["commands"])
        result["commands"]=dict(commands)
        result["rows"]=[{k:r[k] for k in ["id","status","wall_seconds","model_seconds","tool_interval_sum_seconds","tool_attempts","usage","commands","final","final_text","tool_errors","truncated_outputs","policy_audit"]} for r in rows]
        review=folder/"answer-review.json"
        if review.exists():result["answer_review"]=read(review)
        data[arm]=result
    result={"integrity":integrity,"arms":data,"limitations":"One stochastic trial per question/arm; rotated order does not control provider or prefix-cache variation. Manual answer review, not official execution accuracy. Token total includes cached input; reasoning is included in output."}
    (args.output/"comparison.json").write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps({a:{k:v for k,v in r.items() if k not in {"rows","answer_review"}} for a,r in data.items()},ensure_ascii=False,indent=2))


if __name__=="__main__":main()
