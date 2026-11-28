#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import re, sys, collections

LICENSE = '''--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
'''

# Usage: extract.py <server log> <corpus dir> <server version> <app>=<client version>...
log, outdir, pgv = sys.argv[1], sys.argv[2], sys.argv[3]
versions = dict(a.split('=',1) for a in sys.argv[4:])
# application names may contain spaces (Power BI: "Mashup Engine")
start = re.compile(r'^(\d{4}-\d\d-\d\d) \d\d:\d\d:\d\d\.\d+ \w+ \[\d+\] app=(.*?) user=\S* db=\S* (LOG|DETAIL|ERROR|STATEMENT|FATAL):  (.*)$')
captured = None
records = []
for line in open(log, encoding='utf-8', errors='replace'):
    line = line.rstrip('\n')
    m = start.match(line)
    if m:
        captured = captured or m.group(1)
        records.append([m.group(2), m.group(3), m.group(4)])
    elif records and not re.match(r'^\d{4}-\d\d-\d\d ', line):
        records[-1][2] += '\n' + line
# skip the setup (it ran as psql too): everything up to the final ANALYZE
cut = max(i for i, r in enumerate(records) if r[1] == 'LOG' and r[2].strip() == 'statement: ANALYZE;')
records = records[cut + 1:]
per = collections.OrderedDict()
for app, kind, text in records:
    if app not in versions:
        continue
    if kind == 'LOG':
        m = re.match(r'(?:statement|execute [^:]*): (.*)', text, re.S)
        if m:
            per.setdefault(app, []).append([m.group(1).strip(), None])
    elif kind == 'DETAIL' and text.startswith('parameters:') and per.get(app):
        per[app][-1][1] = text
for app, stmts in per.items():
    with open(f'{outdir}/{app}.sql', 'w') as f:
        f.write(LICENSE)
        f.write(f'-- Client: {app} {versions[app]}\n-- Server: PostgreSQL {pgv} (docker postgres:14-alpine), log_statement=all\n')
        f.write(f'-- Captured: {captured} (T001, spec 012); schema refdb/other from corpus/README.md\n')
        collapsed = []
        for sql, params in stmts:
            if collapsed and collapsed[-1][0] == sql and collapsed[-1][1] == params:
                collapsed[-1][2] += 1
            else:
                collapsed.append([sql, params, 1])
        for sql, params, times in collapsed:
            f.write('-- next\n')
            if times > 1:
                f.write(f'-- executed {times} times in a row (portal fetches or repeated executions)\n')
            if params:
                f.write(f'-- {params}\n')
            f.write(sql + '\n')
    print(app, len(stmts), 'statements,', len(collapsed), 'entries')
