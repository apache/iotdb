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

"""Split B2 (empty result or path error) calls into path misses and predicate-empty results.

Static check against each package's schema.sql (CREATE [ALIGNED] TIMESERIES), no
database query. For each B2 SQL call:
  type_confusion    a FROM path is a measurement (series) used as a device;
  pattern_miss      the objects exist but the path pattern misses them: a FROM pattern
                    with ** where the stem is the device (IoTDB ** is one or more
                    levels), or a SHOW/COUNT prefix without .** (exact match);
  device_miss       a FROM path matches no device (wrong name or level), or a SHOW/COUNT
                    pattern matches nothing at all;
  measurement_miss  FROM devices exist but a selected measurement exists under none of them;
  predicate_empty   all paths exist and the statement has a WHERE/time filter;
  other             paths exist, no filter (e.g. aggregate over an empty range) or path error text;
  unparsed          the statement could not be parsed by these rules.
Only type_confusion, pattern_miss, device_miss and measurement_miss are path/object misses (hierarchy access); predicate_empty is an
empty filter result on known objects.
"""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re

CREATE = re.compile(r"CREATE\s+(?:ALIGNED\s+)?TIMESERIES\s+(root[\w.]*)\s*\(([^)]*)\)", re.I)
SINGLE = re.compile(r"CREATE\s+TIMESERIES\s+(root[\w.]+)\s+(?:WITH|\w+\s*=)", re.I)
KEYWORDS = {"as", "time", "and", "or", "not", "null", "true", "false", "desc", "asc", "distinct"}


def load_schema(path):
    series = set()
    text = Path(path).read_text()
    for device, body in CREATE.findall(text):
        for col in body.split(","):
            if col.strip():
                series.add(device + "." + col.split()[0])
    for full in SINGLE.findall(text):
        series.add(full)
    devices = {s.rsplit(".", 1)[0] for s in series}
    return series, devices


def pattern_regex(path):
    """IoTDB path pattern: * is one level, ** is one or more levels."""
    parts = [("[^.]+" if x == "*" else r"[^.]+(?:\.[^.]+)*" if x == "**" else re.escape(x.strip("`"))) for x in path.split(".")]
    return re.compile(r"\.".join(parts))


def split_top(text):
    out, depth, cur = [], 0, ""
    for ch in text:
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            out.append(cur); cur = ""
        else:
            cur += ch
    return out + [cur]


def measurements(select):
    """Relative series names in a SELECT list (dotted names kept); '*' for a bare star."""
    names = set()
    for item in split_top(select):
        item = re.sub(r"\bAS\s+\S+\s*$", "", item.strip(), flags=re.I)
        item = re.sub(r"'[^']*'", "", item)
        if item.strip() == "*" or re.fullmatch(r"\w+\(\s*\*\s*\)", item.strip()):
            names.add("*")
        for m in re.finditer(r"(?<![\w.])([A-Za-z_]\w*(?:\.[A-Za-z_*]\w*)*)\b(?!\s*\()", item):
            name = m.group(1)
            if name.lower() not in KEYWORDS and not name.startswith("root"):
                names.add(name)
    return names or {"*"}


def classify(sql, error, series, devices):
    code = re.sub(r"'([^']|'')*'", "''", sql.strip().rstrip(";"))
    show = re.match(r"^(SHOW|COUNT)\s+(\w+)(?:\s+\w+)?\s+(root[\w.*`]*)", code, re.I)
    if show:
        # SHOW/COUNT match the pattern exactly: a bare prefix lists nothing below it.
        path = show.group(3).strip("`")
        pool = devices if show.group(2).upper() == "DEVICES" else series
        rx = pattern_regex(path)
        if any(rx.fullmatch(x) for x in pool):
            return "other"
        stem = re.sub(r"(\.\*{1,2})+$", "", path)
        return "pattern_miss" if any(x.startswith(stem + ".") for x in pool) else "device_miss"
    m = re.match(r"^SELECT\s+(.*?)\s+FROM\s+(.*?)(?:\s+(WHERE|GROUP|ORDER|LIMIT|FILL|ALIGN|HAVING|SLIMIT|OFFSET)\b(.*))?$", code, re.I | re.S)
    if not m:
        return "unparsed"
    select, from_part = m.group(1), m.group(2)
    paths = [x.strip().strip("`") for x in from_part.split(",")]
    if not all(x.lower().startswith("root") for x in paths):
        return "unparsed"
    wanted = measurements(select)
    for path in paths:
        stem = re.sub(r"(\.\*{1,2})+$", "", path)
        if stem in series:
            return "type_confusion"
        if not any(pattern_regex(path).fullmatch(d) for d in devices):
            if "**" in path and any(pattern_regex(path.replace(".**", "")).fullmatch(d) or d.startswith(stem + ".") for d in devices | {stem}):
                return "pattern_miss"
            return "device_miss"
    for w in wanted:
        if not any(pattern_regex(path + "." + w).fullmatch(x) for path in paths for x in series):
            return "measurement_miss"
    if error:
        return "other"
    return "predicate_empty" if re.search(r"\bWHERE\b|GROUP\s+BY\s*\(", code, re.I) else "other"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bottlenecks", type=Path, required=True, help="bottlenecks.json from analyze_bottlenecks.py")
    p.add_argument("--packages", type=Path, required=True, help="TQTS benchmark/packages directory")
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    data = json.loads(args.bottlenecks.read_text())
    schemas = {}
    rows, counts, by_question = [], Counter(), defaultdict(Counter)
    for c in data["calls"]:
        if "B2" not in c["classes"] or c.get("kind") != "sql":
            continue
        package = "iotdb_" + c["question_id"].split("iotdb_", 1)[1].split("-qa-")[0]
        if package not in schemas:
            schemas[package] = load_schema(args.packages / package / "schema.sql")
        verdict = classify(c["arguments"].get("sql", ""), c["error"], *schemas[package])
        counts[(verdict, c["category"])] += 1
        by_question[c["question_id"]][verdict] += 1
        rows.append(dict(question_id=c["question_id"], ref=c["ref"], category=c["category"], verdict=verdict,
                         sql=c["arguments"].get("sql", "")))
    (args.output / "b2.json").write_text(json.dumps(dict(rules=__doc__, calls=rows,
        counts={f"{v}/{cat}": n for (v, cat), n in sorted(counts.items())},
        by_question={q: dict(v) for q, v in sorted(by_question.items())}), ensure_ascii=False, indent=1) + "\n")
    total = Counter()
    for (v, _), n in counts.items():
        total[v] += n
    print(json.dumps(dict(total=dict(total), by_category={f"{v}/{cat}": n for (v, cat), n in sorted(counts.items())}), indent=1))


if __name__ == "__main__":
    main()
