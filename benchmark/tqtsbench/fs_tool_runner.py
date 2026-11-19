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

"""Controlled Tree FS adapter; invokes native FS commands, never model SQL."""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time

from tool_runner import context_page

COMMANDS = ("help", "ls", "find", "tree", "schema", "meta", "stats", "count", "cat", "head", "tail", "stat", "file", "grep", "cut", "paste", "join", "wc")
AGGREGATES = ("count", "min", "max", "sum", "avg", "median", "first", "last", "min_time", "max_time", "null_count")
READ = {"cat", "head"}


def integer(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"{name} must be an integer in [{low},{high}]")
    return value


def path_arg(value, package):
    scope = "/root/" + package
    if not isinstance(value, str) or not re.fullmatch(r"/[A-Za-z_0-9/-]+", value):
        raise ValueError("Use an absolute virtual Tree path with simple identifier segments")
    if value != scope and not value.startswith(scope + "/"):
        raise ValueError("Path is outside the assigned database")
    if any(x in {"", ".", ".."} for x in value.split("/")[1:]):
        raise ValueError("Path must be canonical")
    return value


def help_text(scope):
    return {
        "scope": scope,
        "commands": {
            "help": "No other parameters. Returns this complete controlled contract.",
            "ls": "List direct children of path (default assigned root).",
            "find": "Recursively find objects: path, optional pattern (filename glob), type=f|d, max_depth=1..12 (default 6).",
            "tree": "Display hierarchy: path, max_depth=1..12 (default 3).",
            "schema": "Field types: path, optional measurements (array of names).",
            "meta": "Underlying metadata: path.",
            "stats": "Statistics: path, optional measurements, start_ms, end_ms, aggregates, windows. Bounds inclusive epoch milliseconds. aggregates subset of count,min,max,sum,avg,median,first,last,min_time,max_time,null_count; omit for complete native statistics including first,last,min_time,max_time. min_time/max_time denote earliest/latest non-null observation, NOT argmin/argmax timestamps. windows is an array of 1..256 objects {start_ms,end_ms}, inclusive, mutually exclusive with top-level time bounds. Overlap and duplicates are allowed; output includes zero-based window_index, window_start, window_end. Each device is scanned once for all windows. Scalar statistics stream; exact median may use private temporary disk, never exposed to the model.",
            "count": "Native count: path, optional measurements. No time filter on count; use stats for filtered count.",
            "cat/head": "Read page: path, measurements, start_ms, end_ms, limit=1..2000 (default 200), offset>=0 (default 0). Time bounds inclusive. Structured NDJSON rows and next_offset; follow identical scope/filter with next_offset. Null is JSON null. Missing columns are not numerical zero.",
            "tail": "Last rows in chronological order: path, limit=1..2000 (default 10). No filters or offset.",
            "stat/file": "Describe object: path.",
            "grep": "Search native virtual contents: path, pattern (required regex). Returns matching lines. No numeric expression evaluation.",
            "cut": "Select native CSV text fields: path, fields (required positive index/range list, e.g. 1,3-5). Delimiter is comma.",
            "paste": "Paste native contents side-by-side: paths (2..8 paths).",
            "join": "Native text join: paths (exactly 2), left_field/right_field positive column indices (default 1); comma delimiter. Inputs must satisfy native join ordering/format requirements.",
            "wc": "Native byte count: path. This is not a row count."
        },
        "path_mapping": "root.db.device.measurement maps to /root/db/device/measurement. Tree paths are virtual, with no .csv/.tsfile suffix added. Devices/directories can expose several measurements. Paths remain inside assigned root.",
        "limits": "120 admitted tool calls per task shared with dataset_context; 180 seconds per FS process; 8 GiB Java heap; model output capped at 40000 characters. Raw output retained offline and never exposed as spill paths.",
        "scope_note": "Only native read-only FS commands are exposed. No sql escape, shell, arbitrary local files, writes or export. Native multi-window statistics are supported; session and ranking are not added by this adapter. Aggregation remains in the CLI, with streaming range reads."
    }


def build_command(args, package):
    if not isinstance(args, dict):
        raise ValueError("Tool arguments must be an object")
    cmd = args.get("command")
    if cmd not in COMMANDS:
        raise ValueError("Unknown command; use help")
    allowed = {"command"}
    if cmd != "help":
        allowed.add("paths" if cmd in {"paste", "join"} else "path")
    if cmd in READ | {"stats", "schema", "count"}:
        allowed.add("measurements")
    if cmd in READ | {"stats"}:
        allowed |= {"start_ms", "end_ms"}
    if cmd in READ | {"tail"}:
        allowed.add("limit")
    if cmd in READ:
        allowed.add("offset")
    if cmd == "stats":
        allowed |= {"aggregates", "windows"}
    if cmd in {"find", "grep"}:
        allowed.add("pattern")
    if cmd in {"tree", "find"}:
        allowed.add("max_depth")
    if cmd == "find":
        allowed.add("type")
    if cmd == "cut":
        allowed.add("fields")
    if cmd == "join":
        allowed |= {"left_field", "right_field"}
    if set(args) - allowed:
        raise ValueError("Unsupported parameters for " + cmd + ": " + ",".join(sorted(set(args)-allowed)))
    if cmd == "help":
        return None
    if cmd in {"paste", "join"}:
        paths = args.get("paths")
        if not isinstance(paths, list) or not 2 <= len(paths) <= (2 if cmd == "join" else 8):
            raise ValueError("paths requires 2 entries for join or 2..8 for paste")
        paths = [path_arg(p, package) for p in paths]
    else:
        paths = [path_arg(args.get("path", "/root/"+package), package)]
    parts = [cmd]
    if cmd in {"schema", "meta", "stats", "count", "cat", "head"}:
        parts += ["-f", "ndjson"]
    elif cmd == "tail":
        parts += ["--format", "ndjson"]
    if "measurements" in args:
        names = args["measurements"]
        if not isinstance(names, list) or not 1 <= len(names) <= 64 or any(not isinstance(n,str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9.]*", n) for n in names):
            raise ValueError("measurements must be 1..64 identifier names")
        for name in names:
            parts += ["-m", name]
    for key, flag in [("start_ms","--start"),("end_ms","--end")]:
        if key in args:
            parts += [flag, str(integer(args[key], key, -(2**63), 2**63-1))]
    if "start_ms" in args and "end_ms" in args and args["start_ms"] > args["end_ms"]:
        raise ValueError("end_ms must be >= start_ms")
    if "windows" in args:
        windows = args["windows"]
        if "start_ms" in args or "end_ms" in args:
            raise ValueError("windows excludes top-level start_ms/end_ms")
        if not isinstance(windows, list) or not 1 <= len(windows) <= 256:
            raise ValueError("windows requires 1..256 ranges")
        ranges = []
        for window in windows:
            if not isinstance(window, dict) or set(window) != {"start_ms", "end_ms"}:
                raise ValueError("Each window requires exactly start_ms and end_ms")
            start = integer(window["start_ms"], "window.start_ms", -(2**63), 2**63-1)
            end = integer(window["end_ms"], "window.end_ms", -(2**63), 2**63-1)
            if start > end:
                raise ValueError("Window end_ms must be >= start_ms")
            ranges.append(f"{start}:{end}")
        parts += ["--windows", ",".join(ranges)]
    if cmd in READ | {"tail"}:
        limit = integer(args.get("limit",10 if cmd=="tail" else 200), "limit",1,2000)
        parts += ["-n", str(limit + (1 if cmd in READ else 0))]
    if cmd in READ:
        parts += ["--offset", str(integer(args.get("offset",0),"offset",0,100000000))]
    if "aggregates" in args:
        values = args["aggregates"]
        if not isinstance(values,list) or not values or any(v not in AGGREGATES for v in values):
            raise ValueError("aggregates must be a nonempty subset of " + ",".join(AGGREGATES))
        parts += ["--aggregates", ",".join(values)]
    if cmd == "tree":
        parts += ["-L",str(integer(args.get("max_depth",3),"max_depth",1,12))]
    if cmd == "find":
        parts += paths
        parts += ["-maxdepth",str(integer(args.get("max_depth",6),"max_depth",1,12))]
        if "type" in args:
            if args["type"] not in ("f","d"):
                raise ValueError("type must be f or d")
            parts += ["-type",args["type"]]
    if cmd in {"grep","find"}:
        pattern = args.get("pattern")
        if cmd == "grep" and pattern is None:
            raise ValueError("grep requires pattern")
        if pattern is not None:
            if not isinstance(pattern,str) or len(pattern)>256 or any(c in pattern for c in "\n\r;|<>\"'"):
                raise ValueError("pattern must be bounded and cannot contain command separators or quotes")
            parts += (["-E",pattern] if cmd == "grep" else ["-name",pattern])
    if cmd == "cut":
        fields = args.get("fields", "")
        if not isinstance(fields,str) or not re.fullmatch(r"[1-9][0-9]*(?:-[1-9][0-9]*)?(?:,[1-9][0-9]*(?:-[1-9][0-9]*)?)*",fields):
            raise ValueError("fields must be positive CSV column indices/ranges")
        parts += ["-d",",","-f",fields]
    if cmd == "join":
        parts += ["-t",",","-1",str(integer(args.get("left_field",1),"left_field",1,10000)),"-2",str(integer(args.get("right_field",1),"right_field",1,10000))]
    if cmd == "wc":
        parts += ["-c"]
    if cmd != "find":
        parts += paths
    return " ".join(shlex.quote(p) for p in parts)


def resource_metrics(text):
    values = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.strip().rsplit(":",1)
        for source, target in [("Maximum resident set size (kbytes)","peak_rss_kib"),("User time (seconds)","cpu_user_seconds"),("System time (seconds)","cpu_system_seconds")]:
            if key == source:
                values[target] = float(value.strip())
    return values


def execute(config, request):
    args, kind = request["arguments"], request["kind"]
    folder = Path(config["output"]) / ("call-"+hashlib.sha256(request["callId"].encode()).hexdigest()[:20])
    folder.mkdir(parents=True, exist_ok=False)
    (folder/"request.json").write_text(json.dumps(request,ensure_ascii=False,indent=2))
    start = time.monotonic()
    spill_dir = None
    try:
        if kind == "context":
            payload = context_page(json.loads(Path(config["context"]).read_text()), args)
        elif kind == "fs":
            command = build_command(args,config["package"])
            if command is None:
                payload = help_text("/root/"+config["package"])
            else:
                (folder/"native-command.txt").write_text(command)
                creds=json.loads(Path(config["credentials"]).read_text())
                argv=["/usr/bin/time","-v","-o",str(folder/"resources.txt"),config["cli"],"-h","127.0.0.1","-p","32867","-u",creds["username"],"-pw",creds["password"],"-sql_dialect","tree","--access_mode","filesystem","-disableISO8601","-e",command]
                with tempfile.TemporaryDirectory(prefix="iotdb-fs-", dir=folder) as spill_dir:
                    env={**os.environ,"_JAVA_OPTIONS":"-Xms128m -Xmx8g -XX:MaxDirectMemorySize=512m -Djava.io.tmpdir=" + shlex.quote(spill_dir)}
                    timed_out=False
                    with (folder/"stdout.txt").open("w") as out, (folder/"stderr.txt").open("w") as err:
                        proc=subprocess.Popen(argv,stdout=out,stderr=err,env=env,start_new_session=True)
                        def terminate(_signal, _frame):
                            try:
                                os.killpg(proc.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            proc.wait()
                            raise SystemExit(143)
                        previous = signal.signal(signal.SIGTERM, terminate)
                        try:
                            proc.wait(timeout=180)
                        except subprocess.TimeoutExpired:
                            timed_out=True
                            os.killpg(proc.pid,signal.SIGKILL)
                            proc.wait()
                        finally:
                            signal.signal(signal.SIGTERM, previous)
                page_command = args["command"] in READ | {"tail"}
                if page_command and (folder/"stdout.txt").stat().st_size > 8*1024*1024:
                    raise ValueError("Page output exceeds 8 MiB; select fewer measurements")
                with (folder/"stdout.txt").open(errors="replace") as stream:
                    raw=stream.read() if page_command else stream.read(40001)
                stderr=(folder/"stderr.txt").read_text(errors="replace")
                metrics=resource_metrics((folder/"resources.txt").read_text()) if (folder/"resources.txt").exists() else {}
                (folder/"resources.json").write_text(json.dumps(metrics))
                error=timed_out or proc.returncode not in ([0,1] if args["command"]=="grep" else [0])
                payload={"stdout":raw[:40000],"stderr":"\n".join(line for line in stderr.splitlines() if not line.startswith("Picked up _JAVA_OPTIONS:"))[:2000],"truncated":len(raw)>40000,"is_error":error,"exit_code":proc.returncode,"timed_out":timed_out}
                if not error and args["command"] in READ | {"tail"}:
                    rows=[json.loads(line) for line in raw.splitlines() if line.strip()]
                    limit=args.get("limit",10 if args["command"]=="tail" else 200)
                    selected=rows[:limit]
                    while selected and len(json.dumps(selected,ensure_ascii=False))>38000:
                        selected.pop()
                    if rows and not selected:
                        raise ValueError("One row exceeds output budget; select fewer measurements")
                    if args["command"]=="tail" and len(selected)<len(rows):
                        raise ValueError("Tail page exceeds output budget; use a smaller limit")
                    offset=args.get("offset",0)
                    more=args["command"] in READ and len(selected)<len(rows)
                    payload={"rows":selected,"offset":offset,"returned_rows":len(selected),"has_more":more,"next_offset":offset+len(selected) if more else None,"is_error":False}
                if payload.get("truncated"):
                    payload["continuation"]="Narrow the path or parameters. Raw output is retained offline and cannot be opened by the agent."
        else:
            raise ValueError("Only context and fs requests are permitted")
    except Exception as exc:
        payload={"is_error":True,"error":str(exc)}
    content = json.dumps(payload, ensure_ascii=False)
    if spill_dir is not None:
        content = content.replace(spill_dir, "<private temporary storage>")
    result={"content":content,"elapsedMs":(time.monotonic()-start)*1000,"isError":payload.get("is_error",False)}
    (folder/"result.json").write_text(json.dumps(result,ensure_ascii=False,indent=2))
    return result


if __name__ == "__main__":
    os.umask(0o077)
    print(json.dumps(execute(json.loads(Path(sys.argv[1]).read_text()),json.load(sys.stdin)),ensure_ascii=False))
