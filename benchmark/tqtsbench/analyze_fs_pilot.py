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

"""Summarize FS trace bottlenecks without re-running any database query."""
import argparse
from collections import Counter
import json
from pathlib import Path
import statistics

from analyze_pilot import final_object


def read(path):
    return json.loads(Path(path).read_text())


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--sql-baseline",type=Path,required=True)
    args=parser.parse_args()
    rows=[]
    baseline={r["id"]:r for r in read(args.sql_baseline/"summary.json")["rows"]}
    for task in read(args.output/"selection.json"):
        trial=args.output/"trials"/task["id"]
        if not (trial/"dsh_result.json").exists():continue
        r=read(trial/"dsh_result.json")
        generation_limits=set()
        dispatch_errors=[]
        for log in (trial/"dsh-session").rglob("session.v3.jsonl"):
            for line in log.open():
                event=json.loads(line)
                data=event.get("data",{})
                header=data.get("header",{})
                value=header.get("config",{}).get("maxTokens")
                if value is not None:
                    generation_limits.add(value)
                if event.get("type")=="tool/result" and data.get("error"):
                    message=data.get("message",{})
                    texts=[]
                    for block in message.get("content",[]):
                        texts.extend(c.get("text","") for c in block.get("content",[]) if isinstance(c,dict))
                    dispatch_errors.append({"callId":message.get("source",{}).get("callId"),"step":data.get("step"),"error":data["error"],"message":"\n".join(texts)})
        events=[json.loads(line) for line in (trial/"events-summary.jsonl").read_text().splitlines()]
        steps={e["callId"]:e.get("step") for e in events if e.get("type")=="tool/call"}
        calls=[]
        for f in sorted((trial/"tool-output").glob("*/request.json"),key=lambda p:p.stat().st_mtime):
            request=read(f)
            result=read(f.parent/"result.json") if (f.parent/"result.json").exists() else {}
            payload=json.loads(result.get("content","{}"))
            metrics=read(f.parent/"resources.json") if (f.parent/"resources.json").exists() else {}
            stderr=(f.parent/"stderr.txt").read_text(errors="replace") if (f.parent/"stderr.txt").exists() else ""
            empty = not result.get("isError",True) and (payload.get("rows")==[] or (request["arguments"].get("command")=="grep" and payload.get("stdout")==""))
            calls.append({**request,"elapsed_ms":result.get("elapsedMs"),"is_error":result.get("isError",True),"incomplete":not bool(result),"heap_oom":"OutOfMemoryError" in stderr,"validation_error":bool(payload.get("error")) and not (f.parent/"native-command.txt").exists(),"empty_result":empty,"timed_out":payload.get("timed_out",False),"truncated":payload.get("truncated",False),"returned_rows":payload.get("returned_rows"),"response_chars":len(result.get("content","")),"resources":metrics,"output_ref":str(f.parent.relative_to(args.output))})
            calls[-1]["step"]=steps.get(request["callId"])
        fs=[c for c in calls if c["kind"]=="fs"]
        commands=Counter(c["arguments"].get("command") for c in fs)
        end=r.get("turn_end_reason") or {}
        row={"id":task["id"],"difficulty":task["difficulty"],"question":task["question"],"status":r["status"],"completion_class":"generation_budget_exhausted" if end.get("kind")=="max-tokens" else r["status"],"turn_end_reason":end,"final":final_object(r.get("final_text","")),"final_text":r.get("final_text",""),"wall_seconds":r["task_wall_ms"]/1000,"model_seconds":r.get("model_api_ms",0)/1000,"tool_interval_sum_seconds":r.get("tool_wall_ms",0)/1000,"tool_attempts":r.get("tool_attempts",0),"admitted_tools":len(calls),"context_calls":len(calls)-len(fs),"fs_calls":len(fs),"commands":dict(commands),"tool_errors":sum(c["is_error"] for c in calls),"tool_timeouts":sum(c["timed_out"] for c in calls),"incomplete_tool_outputs":sum(c["incomplete"] for c in calls),"truncated_outputs":sum(c["truncated"] for c in calls),"response_chars":sum(c["response_chars"] for c in calls),"peak_cli_rss_kib":max((c["resources"].get("peak_rss_kib",0) for c in calls),default=0),"model_requests":r.get("model_requests",0),"transport_retries":r.get("retry_count",0),"usage":r.get("usage",{}),"cache_hit_ratio":r.get("cache_hit_ratio"),"policy_audit":read(trial/"policy-audit.json"),"calls":calls}
        b=baseline[task["id"]]
        row["heap_oom_calls"]=sum(c["heap_oom"] for c in calls)
        row["validation_errors"]=sum(c["validation_error"] for c in calls)
        row["empty_results"]=sum(c["empty_result"] for c in calls)
        row["discovery_fs_calls"]=sum(commands[k] for k in ["ls","find","tree","schema","meta","stat","file"])
        row["actual_generation_max_tokens"]=sorted(generation_limits)
        call_ids={c["callId"] for c in calls}
        row["pre_dispatch_rejections"]=[e for e in dispatch_errors if e["callId"] not in call_ids]
        row["sdk_tool_errors"]=r.get("tool_errors",0)
        row["sql_baseline"]={k:b.get(k) for k in ["wall_seconds","tool_attempts","context_calls","sql_calls","tool_errors","empty_sql_results","usage","completion_class"]}
        rows.append(row)
    summary={"scope":"Same-task FS bottleneck comparison with deliberately asymmetric resource budgets; not equal-budget performance","expected_trials":8,"recorded_trials":len(rows),"completion_counts":dict(Counter(r["completion_class"] for r in rows)),"total_tokens":sum(r["usage"].get("totalTokens",0) for r in rows),"total_tool_attempts":sum(r["tool_attempts"] for r in rows),"total_admitted_tools":sum(r["admitted_tools"] for r in rows),"median_wall_seconds":statistics.median(r["wall_seconds"] for r in rows) if rows else None,"median_total_tokens":statistics.median(r["usage"].get("totalTokens",0) for r in rows) if rows else None,"rows":rows}
    (args.output/"summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in summary.items() if k!="rows"},ensure_ascii=False))


if __name__=="__main__":main()
