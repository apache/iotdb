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

"""Pre-registered, gold-feature selection of the FSView phase-1 bottleneck subset.

Rule version ``tqts-bottleneck-subset-v1``, frozen before any model call.

Inputs are only the question text, ``gold_query``, ``difficulty`` and the tree
metadata (device paths and field names in ``expected-materialization.json.gz``).
No model answer, trace or earlier run outcome is read.

Pool
    All questions of iotdb_cmapss, iotdb_gas_dynamic, iotdb_ndbc and iotdb_pvdaq,
    minus the eight development questions in ``DEV_EXCLUDED``.

Gold-query features
    String literals are blanked first. A SELECT is split at top-level clause
    keywords. Each identifier in SELECT/WHERE/GROUP BY/HAVING/ORDER BY that is not
    a function name or keyword is a suffix path; it is joined to every FROM path
    and split into device (all but the last component) and measurement (last
    component). Devices are checked against the package device list; a ``*``/``**``
    device component is expanded against that list. A ``*`` is an arithmetic
    operator when the next non-blank character is not ``.``, ``,`` or ``)`` (or end
    of clause). SHOW TIMESERIES paths are split the same way; SHOW CHILD NODES/PATHS
    and SHOW DEVICES are metadata navigation. Recorded features include FROM path
    count and kind, concrete and expanded device counts, siblings under one parent,
    device depth below the database, measurement count, device/measurement
    wildcards, ALIGN BY DEVICE, GROUP BY LEVEL/time/sliding/SESSION/VARIATION,
    FILL, HAVING, ORDER BY ... LIMIT, function names (aggregates, max_by/min_by,
    TOP_K/BOTTOM_K, M4, DIFF/derivative) and CASE.

Anchoring test (does the question name the device path verbatim?)
    Text is normalised by lower-casing and turning ``_ - ‐ ‑ ‒ – — ― / ' ’`` into
    spaces. Every device-path component below ``root.<database>`` that contains no
    wildcard is anchored iff one of:

    A. exact: the normalised component occurs in the question with alphanumeric
       boundaries on both sides (``station_41001`` <- "station 41001");
    B. word prefix: the component is a single alphabetic word and starts a word in
       the question (``train`` <- "training");
    C. stem number: the component is ``<letters>[_]<digits>``; the integer (leading
       zeros ignored) appears as a standalone number, and either it has at least
       three digits or the letter stem starts a word in the question
       (``engine_2`` <- "engines 1 and 2").

    Components are walked from the database down and the walk stops at the first
    wildcard component. ``unanchored_levels`` counts distinct (level, component)
    pairs that fail the test at a branching level, i.e. where the parent node has at
    least two children in the tree (a single-child level needs no choice; such
    misses are kept separately as ``unanchored_nonbranching``).

Classes (exclusive; priority breadth > hierarchy > control > other)
    breadth   any of: at least 2 distinct devices after wildcard expansion
              (siblings or not); a wildcard in a device component; a measurement
              wildcard inside a function argument or a partial glob such as
              ``sensor_*``; ALIGN BY DEVICE; GROUP BY LEVEL.
    hierarchy not breadth, and unanchored_levels >= 1 or the statement is SHOW
              metadata navigation without a device wildcard.
    control   not breadth or hierarchy; a SELECT on exactly one existing device
              whose components are all anchored.
    other     anything else (reported, never selected).

Selection
    Targets 12 breadth, 8 hierarchy, 10 control. Each class is filled by
    round-robin over packages, the package order being rotated by the class index
    (breadth starts at cmapss, hierarchy at gas_dynamic, control at ndbc). An
    exhausted package is skipped and its share goes to the next ones. In the chosen
    package the difficulty with the fewest picks in this class and package is used,
    ties broken by fewest picks in this class overall, then easy < medium < hard.
    In that cell the question with the smallest sha256("<seed>|<id>") is taken.

Optional gold check (``--verify-gold``)
    Each selected gold query runs once through the IoTDB CLI with the per-package
    read-only accounts, as in the 2026-11-18 validation (trailing ``;`` removed,
    tree dialect, ``-disableISO8601``). A question passes when exit code is 0, no
    error marker is printed and at least one row is returned. A failing question
    is replaced by the next question of the same (class, package, difficulty) cell
    in hash order; replacements are recorded. Only row counts and output hashes
    are stored, not the results.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import time

RULE_VERSION = "tqts-bottleneck-subset-v1"
PACKAGES = ("iotdb_cmapss", "iotdb_gas_dynamic", "iotdb_ndbc", "iotdb_pvdaq")
DIFFICULTIES = ("easy", "medium", "hard")
CLASS_ORDER = ("breadth", "hierarchy", "control")
DEFAULT_TARGETS = {"breadth": 12, "hierarchy": 8, "control": 10}
DEFAULT_SEED = "fsview-phase1-bottleneck-20261120"
DEV_EXCLUDED = frozenset({
    "iotdb_cmapss-qa-013", "iotdb_cmapss-qa-030",
    "iotdb_gas_dynamic-qa-001", "iotdb_gas_dynamic-qa-021",
    "iotdb_ndbc-qa-005", "iotdb_ndbc-qa-028",
    "iotdb_pvdaq-qa-006", "iotdb_pvdaq-qa-040",
})

CLAUSE_RE = re.compile(
    r"\b(FROM|WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|FILL|LIMIT|OFFSET|SLIMIT|SOFFSET|"
    r"ALIGN\s+BY\s+DEVICE|ALIGN\s+BY\s+TIME|DISABLE\s+ALIGN)\b", re.I)
EXPR_CLAUSES = ("SELECT", "WHERE", "GROUP BY", "HAVING", "ORDER BY")
TOKEN_RE = re.compile(r"(?<![\w.*#])([A-Za-z_*][\w*]*(?:\.[A-Za-z_*][\w*]*)*)")
KEYWORDS = frozenset("""
SELECT FROM WHERE AND OR NOT IN IS NULL NULLS FIRST LAST ASC DESC CASE WHEN THEN ELSE
END GROUP BY ORDER HAVING LIMIT OFFSET SLIMIT SOFFSET FILL PREVIOUS LINEAR ALIGN DEVICE
TIME TIMESTAMP LEVEL LIKE REGEXP BETWEEN TRUE FALSE __ENDTIME AS DISABLE
""".split())
AGGREGATES = frozenset("""
count sum avg min_value max_value first_value last_value min_time max_time extreme
stddev stddev_pop stddev_samp variance var_pop var_samp max_by min_by mode
count_if time_duration
""".split())
DIFF_FUNCS = frozenset({"diff", "difference", "derivative", "non_negative_difference",
                        "non_negative_derivative", "time_difference"})
DASHES = "_-‐‑‒–—―/'’"
WRITE_RE = re.compile(r"\b(INTO|INSERT|DELETE|UPDATE|CREATE|ALTER|DROP|GRANT|REVOKE|LOAD|REMOVE|SET|CALL)\b", re.I)
READ_RE = re.compile(r"^(SELECT\b|SHOW (TIMESERIES|DEVICES|CHILD PATHS|CHILD NODES)\b|COUNT (TIMESERIES|DEVICES|NODES)\b)", re.I)


def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def strip_literals(sql):
    return re.sub(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"", "''", sql)


def paren_depths(text):
    depth, out = 0, []
    for ch in text:
        if ch == ")":
            depth -= 1
        out.append(depth)
        if ch == "(":
            depth += 1
    return out


def split_clauses(code):
    """Return {clause name: text} for a SELECT, splitting only at paren depth 0."""
    depths = paren_depths(code)
    body = re.match(r"\s*SELECT\b", code, re.I)
    start, name, clauses = body.end(), "SELECT", {}
    for m in CLAUSE_RE.finditer(code, start):
        if depths[m.start()] != 0:
            continue
        clauses[name] = clauses.get(name, "") + " " + code[start:m.start()]
        name, start = re.sub(r"\s+", " ", m.group(1).upper()), m.end()
    clauses[name] = clauses.get(name, "") + " " + code[start:]
    return {k: v.strip().rstrip(";").strip() for k, v in clauses.items()}


def mark_operator_stars(text):
    """Replace arithmetic '*' by '#', keeping wildcard stars."""
    out, i = [], 0
    while i < len(text):
        if text[i] != "*":
            out.append(text[i])
            i += 1
            continue
        j = i
        while j < len(text) and text[j] == "*":
            j += 1
        rest = text[j:].lstrip()
        wildcard = rest == "" or rest[0] in ".,)"
        out.append(text[i:j] if wildcard else " # ")
        i = j
    return "".join(out)


def norm_text(s):
    s = s.lower()
    for ch in DASHES:
        s = s.replace(ch, " ")
    return re.sub(r"\s+", " ", s).strip()


def anchor_rule(component, qnorm):
    c = norm_text(component)
    if re.search(r"(?<![a-z0-9])" + re.escape(c) + r"(?![a-z0-9])", qnorm):
        return "exact"
    if re.fullmatch(r"[a-z]+", c) and re.search(r"(?<![a-z0-9])" + re.escape(c), qnorm):
        return "word_prefix"
    m = re.fullmatch(r"([a-z]+)_?0*(\d+)", component.lower())
    if m:
        stem, num = m.groups()
        if re.search(r"(?<![0-9.,])0*" + num + r"(?![0-9]|[.,][0-9])", qnorm) and (
                len(num) >= 3 or re.search(r"(?<![a-z0-9])" + stem, qnorm)):
            return "stem_number"
    return None


def match_components(pattern, comps):
    if not pattern:
        return not comps
    head = pattern[0]
    if head == "**":
        return any(match_components(pattern[1:], comps[k:]) for k in range(1, len(comps) + 1))
    if not comps:
        return False
    regex = "^" + re.escape(head).replace(r"\*", "[^.]*") + "$"
    return bool(re.match(regex, comps[0])) and match_components(pattern[1:], comps[1:])


class Tree:
    def __init__(self, package, info):
        self.db = "root." + package
        self.fields = {d: set(v.get("fields", {})) for d, v in info["devices"].items()}
        self.devices = sorted(self.fields)
        self.children = defaultdict(set)
        for d in self.devices:
            comps = d.split(".")
            for k in range(1, len(comps)):
                self.children[".".join(comps[:k])].add(comps[k])

    def child_count(self, node):
        return len(self.children.get(node, ()))

    def expand(self, device_pattern, measurement=None):
        pat = device_pattern.split(".")
        hits = [d for d in self.devices if match_components(pat, d.split("."))]
        if measurement and "*" not in measurement:
            hits = [d for d in hits if measurement in self.fields[d]]
        return hits

    def is_prefix(self, path):
        return any(d.startswith(path + ".") for d in self.devices)

    def below_db(self, path):
        comps = path.split(".")
        return comps[len(self.db.split(".")):]


def path_tokens(text):
    """Yield (token, inside_function) for identifier paths of an expression clause."""
    text = mark_operator_stars(re.sub(r"\bAS\s+[A-Za-z_]\w*", " ", text, flags=re.I))
    depths = paren_depths(text)
    funcs = []
    tokens = []
    for m in TOKEN_RE.finditer(text):
        tok = m.group(1)
        rest = text[m.end():].lstrip()
        if rest.startswith("(") and "." not in tok and "*" not in tok:
            funcs.append(tok.lower())
            continue
        if tok.upper() in KEYWORDS:
            continue
        tokens.append((tok, depths[m.start()] > 0))
    return tokens, funcs


def extract_features(q, package, tree):
    sql = q["gold_query"]
    code = strip_literals(sql).strip().rstrip(";").strip()
    qnorm = norm_text(q["question"])
    upper = re.sub(r"\s+", " ", code.upper())
    kind_m = re.match(r"(SELECT|SHOW TIMESERIES|SHOW CHILD NODES|SHOW CHILD PATHS|SHOW DEVICES|COUNT \w+)\b", upper)
    kind = kind_m.group(1) if kind_m else "UNKNOWN"
    f = {"id": q["id"], "package": package, "difficulty": q["difficulty"], "intents": q.get("intents", []),
         "statement_kind": kind, "statement_count": len([s for s in code.split(";") if s.strip()])}
    refs, funcs, unresolved = [], [], []
    from_paths, from_kinds = [], []
    meas_wild_fn = meas_glob = select_star_bare = False
    navigated = []
    clauses = {}
    if kind == "SELECT":
        clauses = split_clauses(code)
        from_paths = [p.strip() for p in clauses.get("FROM", "").split(",") if p.strip()]
        for p in from_paths:
            from_kinds.append("device_pattern" if "*" in p else "device" if p in tree.fields
                              else "prefix" if tree.is_prefix(p) else "unknown")
        if re.fullmatch(r"\*", clauses.get("SELECT", "")) or re.match(r"\*\s*,", clauses.get("SELECT", "")) \
                or re.search(r",\s*\*\s*(,|$)", clauses.get("SELECT", "")):
            select_star_bare = True
        for name in EXPR_CLAUSES:
            if name not in clauses:
                continue
            toks, fn = path_tokens(clauses[name])
            funcs += fn
            for tok, in_fn in toks:
                last = tok.split(".")[-1]
                if "*" in last and last != "*" and last != "**":
                    meas_glob = True
                if "*" in last and in_fn:
                    meas_wild_fn = True
                if tok.lower().startswith("root."):
                    refs.append(tok)
                else:
                    refs += [p + "." + tok for p in from_paths]
    elif kind == "SHOW TIMESERIES":
        refs.append(code.split()[-1])
    elif kind in ("SHOW CHILD NODES", "SHOW CHILD PATHS", "SHOW DEVICES"):
        navigated.append(code.split()[-1])

    concrete, patterns, expanded, measurements = set(), set(), set(), set()
    for full in refs:
        comps = full.split(".")
        dev, meas = ".".join(comps[:-1]), comps[-1]
        if "*" in dev:
            patterns.add(dev)
            hits = tree.expand(dev, meas)
            expanded.update(hits)
            if not hits:
                unresolved.append(full)
        elif dev in tree.fields:
            concrete.add(dev)
            expanded.add(dev)
            if "*" not in meas:
                if meas in tree.fields[dev]:
                    measurements.add(meas)
                else:
                    unresolved.append(full)
        else:
            unresolved.append(full)
    # A FROM device may be referenced only through SELECT * / count(*); keep it.
    for p, k in zip(from_paths, from_kinds):
        if k == "device":
            concrete.add(p)
            expanded.add(p)
        elif k == "device_pattern":
            patterns.add(p)
            expanded.update(tree.expand(p))
    # Path components that the agent must locate.
    anchor_paths = sorted(concrete | patterns | set(navigated))
    comp_seen = {}
    for path in anchor_paths:
        parent = tree.db
        for level, comp in enumerate(tree.below_db(path), start=1):
            if "*" in comp:
                break
            if (level, comp) not in comp_seen:
                comp_seen[(level, comp)] = (anchor_rule(comp, qnorm), tree.child_count(parent) >= 2)
            parent += "." + comp
    components = [{"level": lv, "component": c, "anchored": r is not None, "rule": r, "branching": b}
                  for (lv, c), (r, b) in sorted(comp_seen.items())]
    parents = Counter(d.rsplit(".", 1)[0] for d in expanded)
    group_by = clauses.get("GROUP BY", "")
    order_by = clauses.get("ORDER BY", "")
    # "([start, end), interval[, sliding step])"; group(1) holds what follows the range.
    time_window = re.match(r"\(\s*[\[(][^\])]*[\])]\s*,(.*)\)\s*$", group_by)
    funcs_set = sorted(set(funcs))
    f.update({
        "from_paths": from_paths,
        "from_kinds": from_kinds,
        "n_from_paths": len(from_paths),
        "devices_concrete": sorted(concrete),
        "n_devices_concrete": len(concrete),
        "device_patterns": sorted(patterns),
        "n_devices_expanded": len(expanded),
        "max_siblings": max(parents.values()) if parents else 0,
        "n_parents": len(parents),
        "device_depths": sorted({len(tree.below_db(d)) for d in concrete | patterns}),
        "navigated_nodes": navigated,
        "measurements": sorted(measurements),
        "n_measurements": len(measurements),
        "measurements_verbatim": sum(1 for m in measurements if anchor_rule(m, qnorm) == "exact"),
        "device_wildcard": bool(patterns),
        "double_star": any("**" in p for p in patterns),
        "measurement_wildcard_in_function": meas_wild_fn,
        "measurement_glob": meas_glob,
        "select_star_bare": select_star_bare,
        "align_by_device": "ALIGN BY DEVICE" in clauses,
        "group_by_level": bool(re.search(r"\bLEVEL\b", group_by, re.I)),
        "group_by_time": bool(time_window),
        "sliding_window": bool(time_window) and "," in time_window.group(1),
        "group_by_session": bool(re.match(r"SESSION\b", group_by, re.I)),
        "group_by_variation": bool(re.match(r"VARIATION\b", group_by, re.I)),
        "group_by_condition_or_count": bool(re.match(r"(CONDITION|COUNT)\b", group_by, re.I)),
        "fill": "FILL" in clauses,
        "having": "HAVING" in clauses,
        "order_by_limit": bool(order_by) and "LIMIT" in clauses,
        "order_by_value": bool(order_by) and bool(re.sub(r"\b(TIME|DEVICE|ASC|DESC|NULLS|FIRST|LAST)\b|,", "", order_by, flags=re.I).strip()),
        "limit": "LIMIT" in clauses,
        "functions": funcs_set,
        "aggregation": any(fn in AGGREGATES for fn in funcs_set),
        "max_min_by": any(fn in ("max_by", "min_by") for fn in funcs_set),
        "top_bottom_k": any(fn in ("top_k", "bottom_k") for fn in funcs_set),
        "m4": "m4" in funcs_set,
        "diff_like": any(fn in DIFF_FUNCS for fn in funcs_set),
        "case_when": bool(re.search(r"\bCASE\s+WHEN\b", code, re.I)),
        "device_components": components,
        "unanchored_levels": sum(1 for c in components if not c["anchored"] and c["branching"]),
        "unanchored_nonbranching": sum(1 for c in components if not c["anchored"] and not c["branching"]),
        "unresolved_refs": sorted(set(unresolved)),
    })
    breadth = []
    if len(expanded) >= 2:
        breadth.append("multi_device_siblings" if f["max_siblings"] >= 2 else "multi_device_cousins")
    if patterns:
        breadth.append("device_wildcard")
    if meas_wild_fn or meas_glob:
        breadth.append("measurement_wildcard")
    if f["align_by_device"]:
        breadth.append("align_by_device")
    if f["group_by_level"]:
        breadth.append("group_by_level")
    hierarchy = []
    if f["unanchored_levels"]:
        hierarchy.append("unanchored_levels")
    if kind.startswith("SHOW") and not patterns:
        hierarchy.append("metadata_navigation")
    if breadth:
        cls = "breadth"
    elif hierarchy:
        cls = "hierarchy"
    elif kind == "SELECT" and len(concrete) == 1 and len(expanded) == 1 and not unresolved:
        cls = "control"
    else:
        cls = "other"
    f.update({"breadth_reasons": breadth, "hierarchy_reasons": hierarchy, "class": cls})
    return f


def hash_key(seed, qid):
    return hashlib.sha256((seed + "|" + qid).encode()).hexdigest()


def build_cells(features, seed):
    cells = defaultdict(list)
    for f in features:
        if f["class"] in CLASS_ORDER:
            cells[(f["class"], f["package"], f["difficulty"])].append(f["id"])
    for key in cells:
        cells[key].sort(key=lambda qid: hash_key(seed, qid))
    return cells


def select(features, seed, targets):
    cells = build_cells(features, seed)
    picks = []
    for ci, cls in enumerate(CLASS_ORDER):
        order = PACKAGES[ci % len(PACKAGES):] + PACKAGES[:ci % len(PACKAGES)]
        per_pkg = defaultdict(Counter)
        per_cls = Counter()
        taken = 0
        while taken < targets[cls]:
            progressed = False
            for pkg in order:
                if taken >= targets[cls]:
                    break
                avail = [d for d in DIFFICULTIES if cells.get((cls, pkg, d))]
                if not avail:
                    continue
                d = min(avail, key=lambda x: (per_pkg[pkg][x], per_cls[x], DIFFICULTIES.index(x)))
                qid = cells[(cls, pkg, d)].pop(0)
                picks.append({"id": qid, "class": cls, "package": pkg, "difficulty": d,
                              "pick_rank": len(picks) + 1, "hash_key": hash_key(seed, qid)})
                per_pkg[pkg][d] += 1
                per_cls[d] += 1
                taken += 1
                progressed = True
            if not progressed:
                break
    return picks, cells


def run_gold(sql, package, cli, cred_dir, timeout):
    stmt = sql.strip().rstrip(";").strip()
    code = strip_literals(stmt)
    if WRITE_RE.search(code) or not READ_RE.match(code) or ";" in code:
        return {"ok": False, "error": "not a single read-only statement"}
    creds = json.loads((Path(cred_dir) / (package + ".json")).read_text())
    argv = [cli, "-h", "127.0.0.1", "-p", "32867", "-u", creds["username"], "-pw", creds["password"],
            "-sql_dialect", "tree", "-disableISO8601", "-e", stmt]
    start = time.monotonic()
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        # The exception text contains argv and the password; do not keep it.
        return {"ok": False, "error": "timeout", "timeout_s": timeout}
    elapsed = (time.monotonic() - start) * 1000
    out = proc.stdout + proc.stderr
    error = proc.returncode != 0 or bool(re.search(r"(?:Error (?:code|message)|Exception|SQL execution error|Msg: \d+)", out, re.I))
    m = re.findall(r"Total line number = (\d+)", proc.stdout)
    rows = int(m[-1]) if m else (0 if "Empty set" in proc.stdout else None)
    return {"ok": (not error) and bool(rows), "exit_code": proc.returncode, "is_error": error, "rows": rows,
            "elapsed_ms": round(elapsed, 1), "stdout_bytes": len(proc.stdout.encode()),
            "stdout_sha256": hashlib.sha256(proc.stdout.encode()).hexdigest(),
            "error_excerpt": (out[-600:] if error else "")}


def verify_and_replace(picks, cells, qa_by_id, args):
    checks, replacements = {}, []
    final = []
    for p in picks:
        current = p
        while True:
            qid = current["id"]
            res = run_gold(qa_by_id[qid]["gold_query"], current["package"], args.cli, args.credentials_dir, args.gold_timeout)
            checks[qid] = res
            if res["ok"]:
                final.append(current)
                break
            queue = cells.get((current["class"], current["package"], current["difficulty"]), [])
            if not queue:
                current = dict(current, gold_failed=True)
                final.append(current)
                replacements.append({"failed": qid, "replacement": None})
                break
            nxt = queue.pop(0)
            replacements.append({"failed": qid, "replacement": nxt})
            current = dict(current, id=nxt, hash_key=hash_key(args.seed, nxt), replaces=qid)
    return final, checks, replacements


def table(rows_key, cols_key, items, rows, cols):
    counts = Counter((rows_key(x), cols_key(x)) for x in items)
    head = "| | " + " | ".join(cols) + " | total |\n|---|" + "---|" * (len(cols) + 1) + "\n"
    body = ""
    for r in rows:
        vals = [counts[(r, c)] for c in cols]
        body += "| " + r + " | " + " | ".join(str(v) for v in vals) + " | " + str(sum(vals)) + " |\n"
    totals = [sum(counts[(r, c)] for r in rows) for c in cols]
    body += "| total | " + " | ".join(str(v) for v in totals) + " | " + str(sum(totals)) + " |\n"
    return head + body


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--packages-dir", required=True, help="TQTS benchmark/packages directory")
    ap.add_argument("--materialization", required=True, help="expected-materialization.json.gz")
    ap.add_argument("--out", required=True, help="output directory (new or containing only this script)")
    ap.add_argument("--seed", default=DEFAULT_SEED)
    ap.add_argument("--breadth", type=int, default=DEFAULT_TARGETS["breadth"])
    ap.add_argument("--hierarchy", type=int, default=DEFAULT_TARGETS["hierarchy"])
    ap.add_argument("--control", type=int, default=DEFAULT_TARGETS["control"])
    ap.add_argument("--verify-gold", action="store_true", help="execute selected gold queries read-only")
    ap.add_argument("--cli", help="IoTDB start-cli.sh path (with --verify-gold)")
    ap.add_argument("--credentials-dir", help="directory with <package>.json read-only accounts")
    ap.add_argument("--gold-timeout", type=int, default=120)
    args = ap.parse_args()
    targets = {"breadth": args.breadth, "hierarchy": args.hierarchy, "control": args.control}

    out = Path(args.out)
    out.mkdir(parents=False, exist_ok=True)
    script = Path(__file__).resolve()
    script_copy = out / script.name
    names = ["selection.json", "features-all.json", "README.md"]
    if any((out / n).exists() for n in names):
        raise SystemExit("output files already exist in " + str(out))
    if script_copy.resolve() != script:
        shutil.copyfile(script, script_copy)
    script_sha = sha256_file(script)

    mat_raw = Path(args.materialization).read_bytes()
    mat = json.loads(gzip.decompress(mat_raw))
    inputs = {"materialization": {"path": str(Path(args.materialization)), "sha256": hashlib.sha256(mat_raw).hexdigest()}}
    features, qa_by_id, totals = [], {}, Counter()
    for pkg in PACKAGES:
        qa_path = Path(args.packages_dir) / pkg / "qa.json"
        inputs[pkg + "/qa.json"] = {"path": str(qa_path), "sha256": sha256_file(qa_path)}
        tree = Tree(pkg, mat[pkg])
        for q in json.loads(qa_path.read_text())["qa"]:
            totals[pkg] += 1
            if q["id"] in DEV_EXCLUDED:
                continue
            qa_by_id[q["id"]] = q
            features.append(extract_features(q, pkg, tree))
    features.sort(key=lambda f: (PACKAGES.index(f["package"]), f["id"]))

    picks, cells = select(features, args.seed, targets)
    checks, replacements = None, []
    if args.verify_gold:
        if not (args.cli and args.credentials_dir):
            raise SystemExit("--verify-gold needs --cli and --credentials-dir")
        picks, checks, replacements = verify_and_replace(picks, cells, qa_by_id, args)
    feat_by_id = {f["id"]: f for f in features}
    now = datetime.now(timezone.utc).isoformat()
    selection = {
        "rule_version": RULE_VERSION, "created_at": now, "seed": args.seed, "targets": targets,
        "script_sha256": script_sha, "inputs": inputs, "dev_excluded": sorted(DEV_EXCLUDED),
        "pool_size": len(features), "question_totals": dict(totals),
        "pool_class_counts": dict(Counter(f["class"] for f in features)),
        "gold_check": None if checks is None else {"checks": checks, "replacements": replacements},
        "selected": [dict(p, question=qa_by_id[p["id"]]["question"], features=feat_by_id[p["id"]])
                     for p in sorted(picks, key=lambda p: (CLASS_ORDER.index(p["class"]), PACKAGES.index(p["package"]), p["id"]))],
    }
    with open(out / "selection.json", "x") as fh:
        json.dump(selection, fh, ensure_ascii=False, indent=2)
    with open(out / "features-all.json", "x") as fh:
        json.dump({"rule_version": RULE_VERSION, "script_sha256": script_sha, "features": features}, fh, ensure_ascii=False, indent=2)

    short = {p: p.replace("iotdb_", "") for p in PACKAGES}
    pool = [f for f in features]
    sel = [feat_by_id[p["id"]] for p in picks]
    classes = list(CLASS_ORDER) + ["other"]
    lines = [
        "# TQTS bottleneck subset (FSView phase 1)", "",
        "Rule `%s`, created %s by `%s` (sha256 `%s`). Seed `%s`. No model was called; only question text, "
        "gold_query, difficulty and tree metadata were read." % (RULE_VERSION, now, script.name, script_sha, args.seed), "",
        "## Rule", "", "```text", __doc__.strip(), "```", "",
        "## Inputs", "",
    ]
    lines += ["- `%s`: `%s`" % (k, v["sha256"]) for k, v in inputs.items()]
    lines += ["", "Pool: %d questions (%s) minus %d development questions = %d." % (
        sum(totals.values()), ", ".join("%s %d" % (short[p], totals[p]) for p in PACKAGES), len(DEV_EXCLUDED), len(pool)), "",
        "## Pool: class x package", "", table(lambda f: f["class"], lambda f: short[f["package"]], pool, classes, [short[p] for p in PACKAGES]),
        "## Pool: class x difficulty", "", table(lambda f: f["class"], lambda f: f["difficulty"], pool, classes, list(DIFFICULTIES)),
        "## Selection: class x package", "", table(lambda f: f["class"], lambda f: short[f["package"]], sel, list(CLASS_ORDER), [short[p] for p in PACKAGES]),
        "## Selection: class x difficulty", "", table(lambda f: f["class"], lambda f: f["difficulty"], sel, list(CLASS_ORDER), list(DIFFICULTIES)),
        "## Selection: package x difficulty", "", table(lambda f: short[f["package"]], lambda f: f["difficulty"], sel, [short[p] for p in PACKAGES], list(DIFFICULTIES)),
        "## Selected questions", "",
        "| id | class | difficulty | reasons | devices (expanded) | unanchored levels | gold check |",
        "|---|---|---|---|---|---|---|"]
    for p in selection["selected"]:
        f = p["features"]
        reasons = ",".join(f["breadth_reasons"] if p["class"] == "breadth" else f["hierarchy_reasons"]) or "-"
        gc = "-" if checks is None else ("ok, %s rows" % checks[p["id"]]["rows"] if checks[p["id"]]["ok"] else "FAILED")
        lines.append("| %s | %s | %s | %s | %d | %d | %s |" % (p["id"], p["class"], p["difficulty"], reasons,
                                                          f["n_devices_expanded"], f["unanchored_levels"], gc))
    lines += ["", "Shortfalls against targets: " + (", ".join(
        "%s %d/%d" % (c, sum(1 for p in picks if p["class"] == c), targets[c]) for c in CLASS_ORDER
        if sum(1 for p in picks if p["class"] == c) < targets[c]) or "none") + "."]
    if checks is not None:
        lines += ["", "Gold check: %d executed, %d passed; replacements: %s." % (
            len(checks), sum(1 for c in checks.values() if c["ok"]),
            json.dumps(replacements) if replacements else "none")]
    lines += ["", "Files: `selection.json` (selected ids with features), `features-all.json` (all pool questions), "
              "`%s` (this script). Model answers and earlier run outcomes were not read." % script.name, ""]
    with open(out / "README.md", "x") as fh:
        fh.write("\n".join(lines))
    print(json.dumps({"selected": len(picks), "pool": len(pool), "pool_classes": selection["pool_class_counts"],
                      "out": str(out)}))


if __name__ == "__main__":
    main()
