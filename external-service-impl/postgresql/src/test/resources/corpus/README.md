<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Client SQL corpus (spec 012, T001)

What PostgreSQL clients actually send, recorded against a real PostgreSQL 14 with
`log_statement=all`. The SQL translation and the catalog tables are built and tested against these
statements (golden tests, T034/T044), not against guesses.

One file per client, `<client>.sql`: a header with client and server versions and the capture
date, then the statements in order, each after a `-- next` line. Bound parameters of the extended
protocol follow as a `-- parameters: ...` comment; repeated identical executions (portal fetches,
repeated prepared statements) are collapsed into one entry with a count.

| File | Client | What was done |
|---|---|---|
| `psql.sql` | psql 14.25 | `\l`, `\dn`, `\dt refdb.*`, `\d refdb.sensors`, `\d+ refdb.events`, `\dt *.*`, one aggregation |
| `pgjdbc.sql` | pgJDBC 42.7.4 (DBeaver's driver) | `DatabaseMetaData` catalogs, schemas, tables, columns, primary keys, indexes, imported keys, type info; a plain query; a prepared statement executed 6 times; `setFetchSize(1000)` over 100,000 rows |
| `psycopg.sql` | psycopg 3.3.9 | `version()`, plain query, parameterised query 6 times, a binary-result query, a server-side cursor over 100,000 rows |
| `powerbi.sql` | Power BI Desktop | **to be recorded** (owner, Windows), see below |
| `excel.sql`, `metabase.sql` | Excel, Metabase | to be recorded |

## Expected outcomes

`expected/<client>/<n>.sql` holds, for the n-th statement of `<client>.sql`, what the adapter does
with it: the table-model SQL it runs, `-- session command`, or `-- refused <SQLSTATE>: <message>`.
`TranslatorGoldenTest` (T034) compares them; after a deliberate change run it with
`-Dgolden.update=true` and review the diff. `EmbeddedPostgreSqlIT` runs every translated statement
without parameters on the engine.

## Repeating a recording

The scripts are in `services/postgresql/src/test/corpus-capture/`:

```shell
docker run -d --name pg14-corpus -e POSTGRES_PASSWORD=<secret> -p 127.0.0.1:54314:5432 \
  postgres:14-alpine -c log_statement=all \
  -c "log_line_prefix=%m [%p] app=%a user=%u db=%d "
docker exec -i pg14-corpus psql -U postgres < services/postgresql/src/test/corpus-capture/setup.sql
# clients: give each its own application_name (psql: PGAPPNAME=psql)
java -cp postgresql-42.7.4.jar services/postgresql/src/test/corpus-capture/PgJdbcCapture.java <password file>
python3 services/postgresql/src/test/corpus-capture/psycopg_capture.py <password file>   # psycopg 3 installed
docker logs pg14-corpus > pg14.log 2>&1
python3 services/postgresql/src/test/corpus-capture/extract.py pg14.log \
  services/postgresql/src/test/resources/corpus 14.25 psql=14.25 pgjdbc=42.7.4 psycopg=3.3.9
```

`extract.py` skips everything up to the final `ANALYZE` of `setup.sql` and writes one file per
`application_name` given on the command line.

## Power BI and Excel (Windows)

Power BI's connector sets no `application_name` of its own that we can rely on, so record it alone:
start a fresh container (as above, but published on an address the Windows machine reaches, for
example `-p <mac-ip>:54314:5432`), run `setup.sql`, then on Windows:

1. Power BI Desktop → Get data → PostgreSQL database → server `<mac-ip>:54314`, database
   `iotdb`, Import → user `postgres` and the password. When asked about encryption, accept the
   unencrypted connection (the recording container has no certificate).
2. Navigator: expand `refdb` and `other`, select `refdb.sensors` and `refdb.events`, load.
3. A second report with **DirectQuery** on `refdb.sensors`: card with the last `temperature`; line
   chart of average `temperature` by `time` with the date hierarchy, drilled down year → quarter →
   month → day; clustered bar of average `temperature` by `machine`; table and matrix by `plant`,
   `line`, `machine`; slicer on `machine`; relative date filter (last 7 days) on `time`; a Top N
   filter (top 3 machines by average `temperature`).
4. Refresh once, then close Power BI.
5. Excel: Data → Get Data → From Database → From PostgreSQL Database, same server, load
   `refdb.events`.

Then `docker logs` into a file and run `extract.py` with the application names found in the log
(`grep -o 'app=[^ ]*' pg14.log | sort | uniq -c`). An empty name works too (`=<version>`); it is
written to `.sql`, so rename that file to `powerbi.sql` or `excel.sql`. Record the Power BI Desktop
and Excel versions in the header.
