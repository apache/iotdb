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
