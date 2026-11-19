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

"""Bounded dataset-context retrieval and read-only IoTDB tree SQL."""
import argparse
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path


def validate_sql(sql, package):
    if not isinstance(sql, str) or len(sql) > 16000:
        raise ValueError("SQL must be a bounded string")
    sql = sql.strip().rstrip(";").strip()
    # Inspect syntax outside quoted literals; database ACLs are the second boundary.
    code = re.sub(r"'([^']|'')*'", "''", sql)
    if any(x in code for x in (";", "--", "/*", "*/")):
        raise ValueError("Only one statement, without SQL comments, is allowed")
    if re.search(r"\b(INTO|INSERT|DELETE|UPDATE|CREATE|ALTER|DROP|GRANT|REVOKE|LOAD|REMOVE|SET|CALL)\b", code, re.I):
        raise ValueError("Only read-only queries are allowed")
    if not re.match(r"^(SELECT\b|SHOW (TIMESERIES|DEVICES|CHILD PATHS|CHILD NODES|VERSION)\b|COUNT (TIMESERIES|DEVICES|NODES)\b)", code, re.I):
        raise ValueError("Allowed: SELECT, SHOW TIMESERIES/DEVICES/CHILD PATHS/CHILD NODES/VERSION, COUNT TIMESERIES/DEVICES/NODES")
    paths = re.findall(r"\broot(?:\.[A-Za-z_0-9*`-]+)*", code, re.I)
    scope = "root." + package
    if not paths and code.upper() != "SHOW VERSION":
        raise ValueError("Use an explicit root path inside " + scope)
    if any(p != scope and not p.startswith(scope + ".") for p in paths):
        raise ValueError("Query is outside the assigned database")
    return sql


def flatten(value, path=""):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from flatten(child, path + "/" + str(key).replace("~", "~0").replace("/", "~1"))
    elif isinstance(value, list):
        for i, child in enumerate(value):
            yield from flatten(child, path + "/" + str(i))
    else:
        yield {"path": path, "value": value}


def context_page(context, args):
    prefix = args.get("pointer", "")
    query = args.get("query", "").casefold()
    offset, limit = args.get("offset", 0), args.get("limit", 20)
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 40:
        raise ValueError("offset >= 0; limit must be 1..40")
    rows = [r for r in flatten(context) if (not prefix or r["path"] == prefix or r["path"].startswith(prefix.rstrip("/") + "/")) and (not query or query in json.dumps(r, ensure_ascii=False).casefold())]
    page = rows[offset:offset+limit]
    while len(json.dumps(page, ensure_ascii=False).encode()) > 24000 and page:
        page.pop()
    if not page and offset < len(rows):
        raise ValueError("One metadata value exceeds the response bound")
    return {"entries": page, "matched": len(rows), "offset": offset, "next_offset": offset+len(page) if offset+len(page)<len(rows) else None}


def execute(config, request):
    args = request["arguments"]
    kind = request["kind"]
    root = Path(config["output"])
    call = hashlib.sha256(request["callId"].encode()).hexdigest()[:20]
    out = root / ("call-" + call)
    out.mkdir(parents=True, exist_ok=False)
    (out / "request.json").write_text(json.dumps(request, ensure_ascii=False, indent=2))
    start = time.monotonic()
    try:
        if kind == "context":
            payload = context_page(json.loads(Path(config["context"]).read_text()), args)
        elif kind == "sql":
            sql = validate_sql(args["sql"], config["package"])
            # Dedicated read-only per-package credentials, never sent to the model.
            creds = json.loads(Path(config["credentials"]).read_text())
            argv = [config["cli"], "-h", "127.0.0.1", "-p", "32867", "-u", creds["username"], "-pw", creds["password"], "-sql_dialect", "tree", "-disableISO8601", "-e", sql]
            proc = subprocess.run(argv, capture_output=True, text=True, timeout=45)
            (out / "stdout.txt").write_text(proc.stdout)
            (out / "stderr.txt").write_text(proc.stderr)
            error = proc.returncode != 0 or bool(re.search(r"(?:Error (?:code|message)|Exception|SQL execution error)", proc.stdout+proc.stderr, re.I))
            payload = {"stdout": proc.stdout[:24000], "stderr": proc.stderr[:2000], "truncated": len(proc.stdout)>24000, "is_error": error, "exit_code": proc.returncode}
            if payload["truncated"]:
                payload["continuation"] = "Result truncated. Narrow the query or use LIMIT/OFFSET. Full raw output is retained for offline analysis, not available to the agent."
        else:
            raise ValueError("Unknown tool")
    except subprocess.TimeoutExpired:
        # TimeoutExpired includes argv, which contains the private database password.
        payload = {"is_error": True, "error": "SQL query exceeded the 45 second execution limit"}
    except Exception as exc:
        payload = {"is_error": True, "error": str(exc)}
    result = {"content": json.dumps(payload, ensure_ascii=False), "elapsedMs": (time.monotonic()-start)*1000, "isError": payload.get("is_error", False)}
    (out / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    import sys
    config = json.loads(Path(sys.argv[1]).read_text())
    print(json.dumps(execute(config, json.load(sys.stdin)), ensure_ascii=False))
