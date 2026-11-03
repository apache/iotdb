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

# ImportWAL

`ImportWAL` replays WAL entries into a running IoTDB instance. It supports tree-model inserts and
deletes, table-model inserts, table-model deletes that can be converted to SQL, and memtable
snapshots. WAL files are read in version order within each parent directory. When several WAL
directories are supplied, the directories can be replayed in parallel.

## Prerequisites

- Start the target IoTDB instance before running the tool.
- Use the `tools/import-wal.sh` launcher on Linux or macOS, or
  `tools/windows/import-wal.bat` on Windows, from an IoTDB distribution.
- The launcher needs a Java runtime. The Windows launcher requires `JAVA_HOME`; the shell launcher
  uses `JAVA_HOME` when it is set and otherwise looks for `java` on `PATH`.
- Use a consistent copy of the WAL directory. The tool does not roll back operations that have
  already been sent to IoTDB when a later operation or file fails.

## Usage

```bash
tools/import-wal.sh --file <wal-file-or-directory> [options]
```

```bat
tools\windows\import-wal.bat --file <wal-file-or-directory> [options]
```

`--file` may name one `.wal` file or a directory. A directory is searched recursively for files with
the `.wal` suffix (case-insensitive). Files are grouped by their parent directory and sorted by WAL
version, then by file name.

Run `tools/import-wal.sh --help` (or the Windows equivalent) to print the same option list as the
current binary.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `-f, --file <path>` | required | A WAL file or a directory containing WAL files. |
| `-h, --host <host>` | `127.0.0.1` | Target IoTDB host. |
| `-p, --port <port>` | `6667` | Target IoTDB RPC port. |
| `-u, --username <name>` | `root` | Target IoTDB user name. |
| `-pw, --password <password>` | prompted | Target IoTDB password. It is read from the terminal when omitted. |
| `-db, --database <name>` | inferred when possible | Target table-model database. An explicit value applies to every WAL directory and takes precedence over inference. |
| `--skip_db_confirmation` | off | Accept every inferred table-model database without prompting. The explicit `--database` value still takes precedence. |
| `-os, --on_success <none\|delete>` | `none` | Keep source WAL files, or delete files that were completely replayed after the import succeeds. Files with skipped entries or corruption are retained. |
| `-tn, --thread_num <count>` | `1` | Number of WAL directories to replay concurrently. Files in one directory remain ordered. |
| `--on_delete <ask\|execute\|skip\|terminate>` | `ask` | Policy for tree-model and table-model delete entries. |
| `--on_object <ask\|skip\|terminate>` | `ask` | Policy for `ObjectNode` entries. `execute` is not supported. |
| `--on_unsupported <ask\|skip\|terminate>` | `ask` | Policy for unsupported entries, including table deletes that cannot be converted to SQL. |
| `--on_corrupted <ask\|skip\|terminate>` | `ask` | Policy for a truncated or corrupted WAL file. Skipping abandons the rest of that file. |
| `--help` | | Print command-line help. |

For unattended imports, specify a policy for every operation that may require a decision. An
`ask` policy without an interactive terminal terminates the operation (or fails for a corrupted
file), so automation should use `execute`, `skip`, or `terminate` as appropriate.

## Table-model database selection

When `--database` is omitted, the tool examines each WAL file's immediate parent directory. IoTDB
WAL directories are normally named `<database>-<regionId>`, for example:

```text
wal/
  factory-0/
    wal-00000000000000000001.wal
  factory-east-2/
    wal-00000000000000000002.wal
```
The final `-<regionId>` is removed, so the inferred databases are `factory` and `factory-east`.
The database name must satisfy IoTDB's table-model database naming rules. Directories that do not
match the pattern are left without an inferred database; table-model entries from such a directory
require `--database`.

For every inferred directory, an interactive run asks whether to replay into that database:

```text
... [y] yes, [a] accept all inferred databases, [N] quit:
```

Enter `a` or `all` to accept the remaining inferred database names for this import. Each directory
still uses its own inferred database. Use `--skip_db_confirmation` to suppress these prompts.

## Interactive policies

With the default `ask` policies, the following responses are available:

- Delete entries: `e`/`execute`, `s`/`skip`, `a`/`all` to execute all deletes for the current data
  model, `l`/`skip_all` to skip all deletes for the current data model, or `q`/`quit` to stop.
- `ObjectNode` and other unsupported entries: `s`/`skip`, `l`/`skip_all`, or `q`/`quit`.
- Corruption: `s`/`skip` to skip the current file, `l`/`skip_all` to skip all corrupted files, or
  `q`/`quit` to stop.

Already replayed operations are not rolled back when a corrupted file is skipped. Skipped entries
and skipped corrupted files are reported in the final summary and their source files are retained.

## Examples

Replay a tree-model WAL directory and keep the source files:

```bash
tools/import-wal.sh --file /backup/iotdb/wal/root-0 --username root --password secret
```

Replay table-model WAL directories using the database names inferred from their directory names:

```bash
tools/import-wal.sh \
  --file /backup/iotdb/wal \
  --password secret \
  --skip_db_confirmation \
  --on_delete skip \
  --on_object skip \
  --on_unsupported skip \
  --on_corrupted skip \
  --thread_num 4
```

Run a non-interactive import with one explicit target database and delete only fully replayed WAL
files:

```bash
tools/import-wal.sh \
  --file /backup/iotdb/wal/factory-0 \
  --database factory \
  --password secret \
  --on_delete execute \
  --on_object skip \
  --on_unsupported terminate \
  --on_corrupted terminate \
  --on_success delete
```

The process exits with `0` after a successful import and `1` when argument parsing, replay, or
source-file handling fails.

# ExportData

`tools/export-data.sh` (Windows: `tools/windows/export-data.bat`) exports Tree or Table results
as CSV, SQL or TsFile. Run `-help csv`, `-help sql` or `-help tsfile` for the format's options.

## Tree measurement files

```bash
tools/export-data.sh -h 127.0.0.1 -p 6667 -u root -pw secret \
  -ft csv -t ./export -mf ./measurements.txt \
  -start_time 2026-10-14T00:00:00+08:00 -end_time 2026-10-14T23:59:59+08:00
```

`-mf/--measurement_file` accepts a UTF-8 file containing one IoTDB path pattern per line:

```text
# Exact paths and IoTDB patterns can be combined.
root.factory.device01.temperature
root.factory.*.pre*
root.factory.**
```

A first-line BOM, blank lines, surrounding whitespace and lines beginning with `#` are accepted.
The entire file is validated before any server query. Each distinct pattern is resolved with
`SHOW TIMESERIES`; `*`, `**` and `pre*` use IoTDB path syntax. Actual measurements are merged and
deduplicated in first-discovery order, then exported sequentially. Each unmatched pattern produces
a warning that names the pattern and its line, such as
`WARNING: No measurements matched root.factory.missing.* (measurement file line 5).`; the log also
records the source file. The command fails if no actual measurements remain.
`-mf` is available only in Tree mode and cannot be combined with `-q`.

Each measurement uses its actual full path as the output filename. CSV and SQL are split by
`-lpf/--lines_per_file` (default 10,000 rows); TsFile produces one file per measurement:

```text
export/root.factory.device01.temperature_0.csv
export/root.factory.device01.temperature_1.csv
export/root.factory.device01.temperature.tsfile
```

The existing `-pfn/--prefix_file_name` supplies an optional extra prefix in measurement-file mode:
`-pfn backup` produces `backup_root.factory.device01.temperature_0.csv`. No default `dump` prefix
is added in this mode, and `-f` is not an alias. Query and Table exports retain names such as
`dump0_0.csv`, `dump0_0.sql` and `dump0.tsfile`. Characters that are unsafe in filenames are encoded
as `%HH`. Oversized filenames and case/normalization collisions fail before exporting data.

`-start_time` and `-end_time` form an inclusive time interval. Values may be integer timestamps in
the server's precision or date-time strings interpreted in the session's timezone (`-tz`). A
single bound is supported; a reversed or invalid interval fails before data queries. These bounds
also apply to automatic Table exports. Queries, whether given with `-q` or at the Tree prompt, run
as written: the bounds are ignored and reported as a `WARNING`.
A measurement with no rows in the interval still counts as completed; CSV/SQL create no data file
for it. SQL exports contain INSERT statements for reimport into compatible existing schema;
TsFile retains measurement types, encoding, compression and device alignment.

## Queries and Table exports

```bash
tools/export-data.sh -ft csv -t ./export -q 'SELECT * FROM root.factory.**'
tools/export-data.sh -sql_dialect table -db factory -table readings \
  -ft tsfile -t ./export -start_time 100 -end_time 200
tools/export-data.sh -sql_dialect table -db factory \
  -ft sql -t ./export -q 'SELECT * FROM readings WHERE time >= 100'
```

Tree mode prompts for a query when neither `-mf` nor `-q` is supplied. Table mode automatically
exports the selected table or all tables in `-db`. Each query in `-q` writes its own files
(`dump0_0.csv`, `dump1_0.csv`, ...). A Table query exported as SQL or TsFile is written back to the
table it reads, so it must select `*` or column names from a single table of `-db`: aliases,
expressions, `FILL`, joins, subqueries and CTEs would write values that the table does not hold.
The query must return the time column and at least one FIELD column, and TsFile queries must
include every TAG column to preserve device identity. All queries are checked against their tables
before the first one runs. Like the time bounds, `-table` does not apply to queries and is reported
as a `WARNING`.

SQL output quotes strings and dates in single quotes, doubling a quote inside them. A Tree query
row that spans several devices becomes one SQL INSERT per device that has values in the row, and
columns without a value are left out. These statements stay in the same file and count as one data
row for `-lpf`, `exported_data_rows` and `file_rows`. `ALIGN BY DEVICE` and Table rows write `NULL`
for a missing value. With `-dt true`, CSV headers in both models add each value column's type, such
as `s1(DOUBLE)`.

## Progress, logs and failures

Every invocation writes a UTF-8 append log and reports progress every two seconds, including while
waiting for queries. When Tree mode prompts for a query, logging and progress start after the query
is entered. The default log path uses the command's startup time:
`<target_dir>/log_export_data_yyyyMMddHHmmss.log`. Override it with `-l/--export_data_log <path>`.
The output directory is created automatically; an overridden log's parent directory must exist.

The console is for people. In a terminal one progress line is redrawn in place, cutting the current
measurement from the left to fit the terminal width. When output goes to a file, `nohup` or a
container log, or when the progress line would not fit the terminal, each update is a line of its
own:

```text
Exporting 59%  121/204  3,535,481 rows  00:00:02  ETA 00:00:01  ...line19.s0
[59%] 121/204 measurements, 3,535,481 rows, elapsed 00:00:02, ETA 00:00:01, current root.factory.line19.s0
```

The shell launcher passes the terminal width to the tool as `COLUMNS`. The Windows launcher does
not, so 80 columns are assumed there unless `COLUMNS` is set.

The console also shows the start, warnings and the log path, then ends with one summary line such
as `Export completely! Rows: 5, files: 1, elapsed: 00:00:00.` Query and Table modes have no total,
so they show rows, elapsed time and rows per second instead of a percentage and ETA. The launcher
applies `conf/logback-tool.xml` when present, so library logging shows errors only.

The log is for programs: JSON Lines, one event object per line, with counts as JSON numbers. Each
event starts with `progress_id` (the operating system process ID), `timestamp` (ISO 8601 with
offset), `model` (`tree` or `table`), `status` (`RUNNING`, `SUCCESS`, `FAILED` or `CANCELLED`),
`exported_data_rows`, `elapsed_time` (`HH:mm:ss`) and `output`, the file being written (omitted
until a task opens one), followed by `event`, `level`, `phase` and the event's details. Events
include `START`, `DISCOVERY`, `WARNING`, `TASK_START`, `TASK_COMPLETE`, `FILE_START`,
`FILE_COMPLETE`, `PROGRESS` and one `FINISH`. The log records source patterns, queries, output
files, rows, completed files, elapsed time and errors; command credentials are excluded. For
example, `jq -c 'select(.event == "FINISH")' export/log_export_data_*.log` prints each result.

Measurement-file mode adds `exported_measurement_num`, `total_measurement_num`, `exported_percent`
and `estimated_remaining` between `exported_data_rows` and `elapsed_time`. A measurement completes
only after all output files and its query handle close successfully. `estimated_remaining` uses the
average measurement duration (`HH:mm:ss`). Measurements differ in size, so it stays `UNKNOWN`, and
the console shows `ETA --`, until 5% of the measurements, and at least three, have completed. No
extra `COUNT(*)` scan is made, and no `-progress` option is needed.

The command returns `0` on success and `1` for invalid arguments, input, queries, output or log
errors; failures record `FAILED`. Ctrl+C or `SIGTERM` records `CANCELLED`. The export stops at the
next row and closes its query and session; a server call that is still blocked after three seconds
is closed. The process then exits with the signal's status (`130` for Ctrl+C, `143` for `SIGTERM`),
which `FINISH` reports as `exit_code`. Already generated files are retained for inspection, and
incomplete files are not marked `FILE_COMPLETE`.
