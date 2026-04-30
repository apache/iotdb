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

# IoTDB CLI Filesystem Mode Design

## Goal

Extend the IoTDB CLI with an explicit filesystem access mode that lets users browse IoTDB
metadata through directory-like commands while preserving the existing SQL CLI behavior by
default.

Filesystem-mode command behavior and output must strictly follow standard Unix/POSIX filesystem
command semantics wherever an equivalent command exists. A deviation is treated as a bug unless it
is explicitly documented as a temporary compatibility exception.

Filesystem mode is read-only by default. Table mode also exposes a minimal, opt-in write loop
behind `--fs_write_mode enabled`; unsupported or unsafe write paths must fail with filesystem-style
errors. The architecture must still leave clear extension points for richer future write operations
such as creating tables, altering schema objects, or writing data rows.

## Non-Goals

- Do not implement a FUSE or operating-system-level mount.
- Do not change the default SQL CLI behavior.
- Do not make filesystem commands available implicitly in the existing SQL mode.
- Do not bypass server-side SQL, permissions, dialect handling, timeout handling, or SSL handling.
- Do not expose broad write operations by default. Writes are disabled unless
  `--fs_write_mode enabled` is set, and the current writable surface is intentionally limited to
  table-mode database creation, table drop, and same-database table rename.

## Compatibility Requirements

Backward compatibility is a hard requirement.

- Existing CLI invocations without a new access-mode argument must behave as they do today.
- Existing SQL mode command parsing, result printing, pagination, `help`, `import`, `set`, `show`,
  `exit`, and `quit` behavior must remain unchanged unless explicitly running in filesystem mode.
- Existing login arguments remain valid: `-h`, `-p`, `-u`, `-pw`, `-timeout`, `-sql_dialect`, and
  SSL options.
- No new short option should be introduced, because short options such as `-p` already have
  established meanings in CLI and tool scripts.
- The new mode must be selected explicitly with a long option such as
  `--access_mode filesystem`; the default remains `sql`.
- Filesystem writes must be selected explicitly with `--fs_write_mode enabled`; the default remains
  `disabled`.
- In filesystem mode, `-e` executes a filesystem command such as `ls /`, not a SQL statement. This
  distinction must be documented in help output.

## Agentic Collaboration Policy

All follow-up work on this feature may use subagents to accelerate analysis, implementation, and
verification. Subagents are allowed by default, as long as their tasks are scoped and reviewed.

Appropriate subagent work includes:

- Read-only codebase exploration, such as checking existing CLI parser patterns, provider behavior,
  or test conventions.
- Independent design review, such as checking whether a proposed command still matches Unix
  filesystem semantics.
- Focused implementation tasks with explicit ownership of disjoint files or modules.
- Focused verification tasks, such as running a specific test class or reviewing whether output
  remains Unix-like.
- Documentation review, such as checking that design, plan, and implementation notes remain
  consistent.

Subagents must not be used as unbounded background workers. Each delegated task should include:

- The exact files or subsystem to inspect or edit.
- Whether the task is read-only or may write files.
- The expected output, such as a summary, changed file list, failing test, or patch.
- A reminder that git operations require explicit user confirmation.
- A reminder not to revert unrelated workspace changes.

When multiple subagents are used in parallel, their write scopes must be disjoint. The main agent
remains responsible for reviewing their results, integrating changes, running Spotless where code
was edited, and running the focused verification commands.

## Entry Point

Add a new access mode to the existing `Cli` entry point:

- `--access_mode sql`: default. Runs the current SQL CLI path.
- `--access_mode filesystem`: runs the new filesystem shell after the existing authentication and
  JDBC connection setup.
- `--fs_write_mode disabled`: default. Filesystem write commands return read-only errors.
- `--fs_write_mode enabled`: enables the table-mode mutation provider. Tree-mode writes still use
  the unsupported mutation provider.

The current `Cli` class remains responsible for command-line option parsing and connection setup.
After the mode is known, it dispatches to either the existing SQL command loop or the new
filesystem shell.

The filesystem shell still uses `IoTDBConnection` and JDBC `Statement` internally. It does not add a
new client protocol.

## Object Model

Filesystem mode exposes an IoTDB metadata view. A filesystem path maps to a typed IoTDB metadata
node. The node type drives command behavior and future write semantics.

### Common Node Types

- `VIRTUAL_ROOT`: synthetic `/` root.
- `UNKNOWN`: a path that cannot be resolved to an IoTDB object.

### Tree Model Mapping

When `sql_dialect=tree`, filesystem paths map to the tree metadata hierarchy.

| Filesystem Path | IoTDB Object | Node Type | Discovery |
| --- | --- | --- | --- |
| `/` | Virtual root | `VIRTUAL_ROOT` | Fixed |
| `/root` | Tree root | `TREE_ROOT` | Fixed |
| `/root/<db>` | Database | `TREE_DATABASE` | `SHOW DATABASES <path>` |
| `/root/<...>` | Internal path | `TREE_INTERNAL_PATH` | `SHOW CHILD PATHS <path>` |
| `/root/<...>/<device>` | Device | `TREE_DEVICE` | `SHOW DEVICES <path>` |
| `/root/<...>/<measurement>` | Timeseries | `TREE_TIMESERIES` | `SHOW TIMESERIES <path>` |

Tree paths are converted from slash-separated filesystem paths to dot-separated IoTDB paths. For
example, `/root/sg/d1/s1` maps to `root.sg.d1.s1`.

A tree path can be ambiguous: the same textual path can be an internal path and a device. The
resolver should use staged checks and return the most specific metadata it can prove. `ls` focuses
on children, while `stat` performs more detailed resolution.

### Table Model Mapping

When `sql_dialect=table`, filesystem paths use a CSV-first sidecar model. A database is exposed as
a directory. Each table is exposed as a data regular file named `<table>.csv`, with adjacent
sidecar regular files named `<table>.schema` and `<table>.meta`.

The table data file is the primary path for user data operations:

- `/<database>/<table>.csv`: table rows as CSV-like tabular content.
- `/<database>/<table>.schema`: sidecar schema content rendered as CSV from
  `DESC <database>.<table> DETAILS`, preserving the column names and values returned by IoTDB.
- `/<database>/<table>.meta`: sidecar table metadata rendered as CSV from the table row returned by
  `SHOW TABLES DETAILS FROM <database>`, preserving the column names and values returned by IoTDB.

The legacy column-oriented path `/<database>/<table>/<column>` is not the primary table-model
filesystem abstraction. It may remain as a compatibility path for existing read behavior during a
migration period, or be documented as the migration source when moving users to the sidecar model.

| Filesystem Path | IoTDB Object | Node Type | Discovery |
| --- | --- | --- | --- |
| `/` | Virtual root | `VIRTUAL_ROOT` | `SHOW DATABASES` |
| `/<database>` | Database directory | `TABLE_DATABASE` | `SHOW DATABASES` |
| `/<database>/<table>.csv` | Table data regular file | `TABLE_DATA_FILE` | `SHOW TABLES FROM <database>` |
| `/<database>/<table>.schema` | Schema sidecar regular file | `TABLE_SCHEMA_FILE` | Exists via `SHOW TABLES FROM <database>`; content via `DESC <database>.<table> DETAILS` |
| `/<database>/<table>.meta` | Metadata sidecar regular file | `TABLE_META_FILE` | Exists via `SHOW TABLES FROM <database>`; content via `SHOW TABLES DETAILS FROM <database>` |
| `/<database>/<table>/<column>` | Legacy column compatibility path | `TABLE_COLUMN` | `DESC <database>.<table> DETAILS` |

Table-model devices from `SHOW DEVICES FROM <table>` are not part of the first version's base path
hierarchy because they are data-instance-oriented rather than schema-container-oriented. They can
be added later as a virtual directory such as `/<database>/<table>/.devices`.

## Path Rules

- `/` is always the filesystem root.
- `.` and `..` are supported.
- Relative paths are resolved against the current directory.
- Attempts to navigate above `/` resolve to `/`.
- Tree-model paths must begin at `/root` for real IoTDB metadata.
- Table-model paths use `/database/table.csv`, `/database/table.schema`, and
  `/database/table.meta`. The database component is a directory; the table data and sidecar paths
  are regular files.
- Legacy table-model paths of the form `/database/table/column` are compatibility paths or
  migration inputs, not the primary table filesystem model.
- Wildcard paths are not treated as filesystem nodes in the first version. Users should run
  wildcard SQL through normal SQL mode.
- SQL escaping and identifier quoting must be centralized in provider helper methods. Command
  implementations must not hand-build SQL strings for IoTDB identifiers.

## Supported Command Reference

Filesystem mode commands are implemented in-process by `FilesystemCommandParser` and
`FilesystemShell`. They are not delegated to `/bin/ls`, `/bin/cat`, `/bin/cut`, or
`java.nio.file.FileSystem`, but their visible syntax and output should match Unix command
semantics wherever the same command exists. Provider support can still vary by dialect and path.

| Command | Description | Example |
| --- | --- | --- |
| `pwd` | Print the current filesystem path. | `pwd` |
| `ls [-a|-l|-la] [path]` | List child names, one per line. `-a` includes `.` and `..`; `-l` uses long listing output. | `ls -a /db` |
| `ll [-a] [path]` | Long listing alias. Uses read-only permissions by default: directories as `dr-xr-xr-x`, files as `-r--r--r--`. | `ll -a /db` |
| `cd <path>` | Change the current directory only if the target is a directory node. | `cd /db` |
| `stat [path]` | Print filesystem-style metadata, including path, Unix type, and provider metadata. | `stat /db/table.csv` |
| `cat <path>...` | Print one or more readable paths sequentially. Table `.csv`, `.schema`, and `.meta` sidecars print CSV lines; legacy table/column paths print tab-separated values. | `cat /db/table.csv` |
| `head [-n lines] <path>` | Print the first rows or text lines for a readable path. Short numeric form such as `head -5 <path>` is also parsed. | `head -n 5 /db/table.csv` |
| `tail [-n lines] <path>` | Print the last rows or text lines where the provider supports tail. Table `.csv` uses `ORDER BY time DESC LIMIT n` internally and returns original order. | `tail -n 5 /db/table.csv` |
| `wc -l <path>` | Print logical row or line count plus path. Only `-l` is supported. | `wc -l /db/table.csv` |
| `grep <pattern> <path>` | Print lines or rows containing the literal pattern. This is substring matching, not regular-expression matching. | `grep spricoder /db/table.csv` |
| `find [path] [-name name]` | Recursively list the starting path and descendants whose node name exactly matches `name`; without `-name`, it prints all visited paths. | `find /db -name table.csv` |
| `less <path>` | Current implementation prints readable content like `cat` with the default read limit; it is not an interactive pager. | `less /db/table.csv` |
| `more <path>` | Current implementation prints readable content like `cat` with the default read limit; it is not an interactive pager. | `more /db/table.schema` |
| `file <path>` | Print the Unix type for the path: `directory`, `regular file`, or `unknown`. | `file /db/table.meta` |
| `du <path>` | Print logical count plus path, using the provider count operation. | `du /db/table.csv` |
| `cut -d<delimiter> -f<fields> <path>` | Apply Unix delimiter-based field selection to each line. The delimiter must be one character. Field lists and closed ranges such as `2,3` and `1-2` are supported. | `cut -d, -f2,3 /db/table.csv` |
| `paste <path>...` | Read multiple file-like paths side by side. Table mode currently supports legacy same-table column paths and optimizes them to one SQL projection. | `paste /db/table/key /db/table/value` |
| `tree [-L depth] [path]` | Recursively print descendants with indentation and names only. `-L` limits recursion depth. | `tree -L 2 /db` |
| `mkdir <path>` | Write-gated command. With table mode and `--fs_write_mode enabled`, `mkdir /db` creates a database. Otherwise it returns a read-only or unsupported error. | `mkdir /newdb` |
| `rm <path>` | Write-gated command. With table mode and `--fs_write_mode enabled`, only `rm /db/table.csv` is allowed and maps to table drop. | `rm /db/table.csv` |
| `mv <source> <target>` | Write-gated command. With table mode and `--fs_write_mode enabled`, only same-database table CSV rename is allowed. | `mv /db/t1.csv /db/t2.csv` |
| `tee -a <path>` | Write-gated append command. With table mode and `--fs_write_mode enabled`, only `tee -a /db/table.csv` appends CSV records as rows. | `tee -a /db/table.csv` |
| `help` | Print filesystem-mode help. | `help` |
| `exit` / `quit` | Exit filesystem mode. | `exit` |

The parser currently recognizes `sql <statement>`, but `FilesystemShell` does not execute it yet.
Raw SQL should be run in the default SQL access mode.

## Command Mapping

### Tree Model

| Operation | SQL/API Mapping |
| --- | --- |
| `ls /` | Return fixed child `root/`. |
| `ls /root` | `SHOW CHILD PATHS root`. |
| `ls /root/sg` | `SHOW CHILD PATHS root.sg` for child directories and `SHOW TIMESERIES root.sg.*` for one-level measurement leaves. |
| `stat /root/sg` | Check `SHOW DATABASES root.sg`, then child/device/timeseries metadata if needed. |
| `stat /root/sg/d1` | Check `SHOW DEVICES root.sg.d1` and `SHOW TIMESERIES root.sg.d1`. |
| `cat /root/sg/d1/s1` | `SHOW TIMESERIES root.sg.d1.s1`, formatted as schema text. |
| `tree /root/sg -L 2` | Repeated one-level `SHOW CHILD PATHS` calls with a depth limit. |

### Table Model

| Operation | SQL/API Mapping |
| --- | --- |
| `ls /` | `SHOW DATABASES`. |
| `ls /db` | `SHOW TABLES FROM db`, formatted as `table.csv`, `table.schema`, and `table.meta` entries for each table. |
| `stat /db` | `SHOW DATABASES DETAILS`, filtered to the database. |
| `stat /db/table.csv` | `SHOW TABLES FROM db`, filtered to the table and rendered as filesystem metadata for the data file. |
| `stat /db/table.schema` | `SHOW TABLES FROM db`, filtered to the table and rendered as filesystem metadata for the schema sidecar. |
| `stat /db/table.meta` | `SHOW TABLES FROM db`, filtered to the table and rendered as filesystem metadata for the metadata sidecar. |
| `cat /db/table.csv` | `SELECT * FROM db.table LIMIT <limit>`, formatted as CSV records. |
| `cut -d, -f2,3 /db/table.csv` | Delimiter-based text field projection over the CSV records. |
| `tee -a /db/table.csv` | Parse CSV input with Apache Commons CSV, validate columns and required `time`, then execute chunked `INSERT INTO db.table(...) VALUES ...`. |
| `cat /db/table.schema` | `DESC db.table DETAILS`, formatted as CSV with IoTDB result columns preserved. |
| `cat /db/table.meta` | `SHOW TABLES DETAILS FROM db`, filtered to the table and formatted as CSV with IoTDB result columns preserved. |
| `stat /db/table/col` | Legacy compatibility: `DESC db.table DETAILS`, filtered to the column. |
| `cat /db/table/col` | Legacy compatibility: `SELECT col FROM db.table LIMIT <limit>`. |

## Unix Output Semantics

Filesystem mode must keep command output aligned with standard Unix command behavior. Avoid
exposing internal implementation types or Java debug-style structures in normal command output.

- `ls` prints child names only. The baseline implementation should use one entry per line, matching
  `ls -1`; it must not introduce comma-separated output or database-specific listing dialects.
- `ls -a` and `ll -a` include `.` and `..` before normal entries.
- `tree` prints the hierarchy with indentation and names only.
- `cat` prints regular file content without Java object formatting. For table data files this is
  CSV; for legacy compatibility table/column paths this remains tab-separated row values.
- `cut -d, -f2,3 /db/table.csv` is the Unix-compatible way to project fields from table CSV
  content. It performs delimiter-based text cutting like Unix `cut`; it does not parse CSV quoting
  or introduce table-specific column-selection flags.
- `paste` prints multiple file-like paths side by side as tab-separated values.
- `less` and `more` are currently non-interactive read aliases with the default read limit.
- `stat` is the command that may expose typed metadata, because Unix `stat` is explicitly about
  object metadata.
- `mkdir`, `rm`, and `mv` are write-gated. In default mode they report a read-only filesystem.
  When enabled, unsupported levels must report an invalid filesystem write operation instead of
  falling through to broad SQL execution.
- Error output should follow a command-prefixed style such as `cat: <message>` or
  `cd: <path>: Not a directory`.

This means provider-internal node types such as `TABLE_DATABASE`, `TABLE_COLUMN`, or
`TREE_DATABASE` must not appear in `ls` or `tree` output. Similarly, `SqlRow.asMap().toString()`
must not be used for `cat` or `paste` output.

## Completion Semantics

Interactive completion must be mode-aware:

- SQL mode keeps the existing SQL completer.
- Filesystem mode installs `FilesystemShell.createCompleter()`.
- At the first word, filesystem completion suggests filesystem commands.
- At later words, filesystem completion lists children from the relevant path, filters by prefix,
  and appends `/` to directory candidates.
- Completion failures are ignored so TAB never interrupts the interactive session.

## Read Semantics

Table-mode read behavior is currently:

- `cat /db/table.csv` maps to `SELECT * FROM db.table LIMIT <limit>`.
- `cat /db/table.schema` maps to `DESC db.table DETAILS` and preserves IoTDB result columns in CSV.
- `cat /db/table.meta` maps to `SHOW TABLES DETAILS FROM db`, filters to the table row, and
  preserves IoTDB result columns in CSV.
- Legacy `cat /db/table/column` may map to `SELECT column FROM db.table LIMIT <limit>` during the
  migration period.
- Legacy `paste /db/table/col1 /db/table/col2` may map to
  `SELECT col1, col2 FROM db.table LIMIT <limit>` when all paths are columns from the same table.

Although `paste` is implemented through one optimized SQL query for table mode, the user-facing
semantics remain Unix-like: users pass multiple file paths to a standard Unix command rather than
using a database-specific `select` command or `cat --columns` dialect.

For CSV-first table files, multi-column projection should prefer Unix `cut` syntax such as
`cut -d, -f2,3 /db/table.csv`. The implementation may later optimize this internally, but the
public interface must remain the standard `cut` form.

In interactive filesystem mode, a single command failure must not terminate the CLI session. For
example, if `cat time` is resolved from `/testtest` to `/testtest/time`, table mode treats that as a
table path and may receive a server error such as `550: Table 'testtest.time' does not exist`. That
error should be printed as `cat: 550: ...`, then the prompt should continue.

## Append Data Write Semantics

Table data writes use Unix append semantics over the table CSV file:

```bash
tee -a /db/table.csv
```

Only append writes are in scope for the first data-write implementation. Truncation, overwrite,
random writes, partial row deletion, and shell redirection such as `>> /db/table.csv` are out of
scope because filesystem mode is an IoTDB CLI command surface, not an operating-system mount.

Append writes are allowed only when all of these conditions hold:

- CLI is running with `--access_mode filesystem`.
- CLI is running with `--sql_dialect table`.
- CLI is running with `--fs_write_mode enabled`.
- Target path is exactly a table data file of the form `/<database>/<table>.csv`.
- Command is `tee -a`; `tee` without `-a` is rejected.

The sidecar files remain read-only metadata views:

- `tee -a /db/table.schema` is rejected.
- `tee -a /db/table.meta` is rejected.
- Legacy column paths such as `/db/table/col` are not writable append targets.
- Tree-mode paths remain non-writable even when `--fs_write_mode enabled` is set.

### Input Modes

Non-interactive mode reads CSV from standard input until EOF, then submits once:

```bash
printf 'time,key,value\n1,spricoder,2.0\n' | \
  start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root \
  --sql_dialect table --access_mode filesystem --fs_write_mode enabled \
  -e 'tee -a /db/table.csv'
```

Interactive mode enters an append buffer:

```text
IoTDB:fs> tee -a /db/table.csv
time,key,value
1,spricoder,2.0
2,spricoder,1.5
:wq
```

The append buffer supports a minimal Vim-style command set:

- `:wq`: validate, submit, and exit the append buffer after a successful write.
- `:q!`: discard the buffer and exit.
- `:q`: exit only when the buffer is empty; otherwise print guidance to use `:wq` or `:q!`.

The first version intentionally does not support `:w`, because writing while keeping the buffer
open creates ambiguity around repeated submissions and buffer clearing.

### CSV Input Rules

Append input is CSV and is parsed with a proven CSV parser, not by ad hoc string splitting.

- Both header and headerless input are supported.
- Header input may provide a subset of columns, but it must include `time`.
- Headerless input must provide all columns in the current `/db/table.csv` output order.
- Every appended record must explicitly provide `time`.
- `time` must not be empty and must not be `\N`.
- `\N` represents SQL `NULL` for non-time columns.
- Empty fields are not automatically treated as `NULL`.
- Client-side validation checks path, write mode, CSV shape, known columns, and required `time`.
- IoTDB remains responsible for type validation, timestamp-format validation, permissions, and
  final insert semantics.

### SQL Mapping And Failure Semantics

The provider should map validated append input to table-model SQL:

```sql
INSERT INTO db.table(col1,col2,...) VALUES (...), (...), ...
```

Implementation rules:

- Use `DESC db.table DETAILS` or an equivalent schema query to get the table columns and output
  order.
- Use SQL identifier rendering and SQL literal escaping helpers; never concatenate raw user values
  into SQL.
- Convert `\N` to `NULL`.
- Submit rows in fixed-size chunks, for example 1000 records per `INSERT`, to avoid oversized SQL
  statements.
- Client-side validation must pass for the full buffer before any write is attempted.
- If a server-side insert fails, the CLI does not compensate or retry automatically. Server state is
  whatever IoTDB actually committed.
- In interactive mode, a server-side failure keeps the append buffer so the user can correct input
  or abandon it with `:q!`.
- In non-interactive mode, a failure prints a command-prefixed error and exits with failure.

## Proposed Code Structure

New code should live under `org.apache.iotdb.cli.fs`.

- `FilesystemShell`: filesystem-mode command loop and `-e` single-command execution.
- `command/*`: command parsing and command value objects for filesystem commands such as `pwd`,
  `ls`, `cd`, `stat`, `cat`, `cut`, `paste`, `tree`, and `help`.
- `path/FsPath`: path normalization and resolution for absolute and relative paths.
- `node/FsNode`, `node/FsNodeType`, `node/FsNodeMetadata`: typed metadata model.
- `provider/FilesystemSchemaProvider`: schema and data read provider interface.
- `provider/FilesystemMutationProvider`: write-gated mutation provider interface.
- `provider/TableCsvAppendPlanner` or equivalent helper: CSV append validation and SQL planning.
- `provider/TreeFilesystemSchemaProvider`: tree-model SQL mapping.
- `provider/TableFilesystemSchemaProvider`: table-model SQL mapping.
- `provider/TableFilesystemMutationProvider`: opt-in table-model write mapping.
- `provider/UnsupportedFilesystemMutationProvider`: mutation provider used when writes are disabled
  at the provider layer or unsupported by the dialect.
- `sql/SqlExecutor`: JDBC statement execution and result extraction helpers.
- `print/FilesystemPrinter`: text output for filesystem commands.

Existing SQL CLI code should not be broadly refactored. The implementation should only add the
small hooks needed to select filesystem mode, install the mode-specific completer, and route
filesystem commands through the provider layer. Raw SQL execution remains the responsibility of SQL
access mode until a filesystem-mode `sql` command is explicitly implemented.

## Provider Interface Direction

Filesystem mode separates reads from mutations. The schema provider owns read operations:

- `list(FsPath path)`
- `describe(FsPath path)`
- `read(FsPath path)`
- `readLines(FsPath path)` for text sidecars such as `.csv`, `.schema`, and `.meta`
- `tail(FsPath path)` / `tailLines(FsPath path)` where the provider supports tail
- `count(FsPath path)` where the provider supports logical count
- `read(List<FsPath> paths)` for provider-optimized multi-path reads such as legacy table-column
  `paste`

The mutation provider owns the current write-gated operations:

- `mkdir(FsPath path)`
- `remove(FsPath path)`
- `move(FsPath source, FsPath target)`
- `append(FsPath path, List<String> csvLines)` or an equivalent table-mode append method for
  `tee -a /db/table.csv`

When writes are disabled, `FilesystemShell` rejects `mkdir`, `rm`, and `mv` before calling the
mutation provider. When writes are enabled, table mode uses `TableFilesystemMutationProvider`; tree
mode still uses `UnsupportedFilesystemMutationProvider`.

The current table-model write boundary is intentionally narrow:

- `mkdir /db` creates a database directory.
- `rm /db/table.csv` drops the table data file and therefore the table.
- `mv /db/t1.csv /db/t2.csv` renames a table within the same database.
- `tee -a /db/table.csv` appends CSV records as table rows.

The sidecar files are metadata views, not independently writable objects:

- `rm /db/table.schema` and `rm /db/table.meta` are forbidden.
- `mv /db/table.schema ...` and `mv /db/table.meta ...` are forbidden.
- `rm /db` is forbidden; database deletion must remain an explicit SQL operation or a separately
  designed filesystem command with stronger safeguards.
- Cross-database table rename, such as `mv /db1/t.csv /db2/t.csv`, is forbidden.

## Dependency Strategy

Use existing, proven dependencies already present in the CLI module:

- `commons-cli` for startup argument parsing.
- JLine for terminal input, history, and autosuggestion.
- JDBC and IoTDB SQL for metadata access.
- Existing IoTDB result printing if a future filesystem-mode raw SQL command is added.

Do not add a shell framework, FUSE dependency, or local filesystem abstraction library for the
first version. The only custom path logic should be the IoTDB-specific mapping from slash paths to
tree/table metadata identifiers.

## Error Semantics

- A missing path returns `No such path`.
- Running directory-only commands on leaf nodes returns `Not a directory`.
- Running leaf-only commands on directories returns `Is a directory`.
- `cd` updates the current directory only after successful resolution.
- `tree` uses a default depth limit to avoid accidentally scanning large schemas.
- Command errors do not close the JDBC connection or exit the shell.

## Parallel Work Guidance

The filesystem mode is intentionally decomposed into path, command, provider, shell, and CLI
dispatch layers so future work can be developed and verified in parallel.

All operations may use subagents for acceleration. Parallel work must still respect ownership
boundaries:

- Path tasks own `fs/path` and path tests.
- Command parser tasks own `fs/command` and parser tests.
- Provider tasks own one provider package slice and matching provider tests.
- Shell output and command-loop tasks own `FilesystemShell` and shell tests.
- CLI dispatch and compatibility tasks own `Cli`, `AbstractCli`, and CLI tests.
- Documentation tasks own the design and plan files under `docs/superpowers`.

Read-only design review can run in parallel with local implementation. Implementation subagents may
run in parallel only when their write scopes are disjoint. Verification subagents may run focused
tests and summarize output while implementation continues, but they should not modify files unless
explicitly reassigned as implementation workers.

The main session remains responsible for integrating results, resolving conflicts, preserving SQL
mode backward compatibility, enforcing Unix output semantics, running Spotless after code edits,
and running the focused verification suite before claiming completion.

## Testing Strategy

Unit tests should cover the behavior without needing a live IoTDB instance wherever possible.

- `FsPath` tests for absolute paths, relative paths, `.`, `..`, empty input, and attempts to move
  above root.
- Command parser tests for valid and invalid forms of all supported shell commands, including
  listing options, `head`/`tail` limits, `wc -l`, `find -name`, `cut`, `paste`, write-gated
  commands, and the parser-only `sql` form.
- Provider tests with a mocked `SqlExecutor`, verifying tree-mode path-to-SQL mapping.
- Provider tests with a mocked `SqlExecutor`, verifying table-mode path-to-SQL mapping, CSV
  sidecars, IoTDB-preserved `.schema`/`.meta` content, multi-column legacy reads, and table
  mutation restrictions.
- CLI option tests extending existing CLI unit coverage for default `access_mode`, filesystem
  mode, invalid mode values, and `fs_write_mode`.
- Shell tests proving SQL mode remains the default and filesystem mode dispatches to
  `FilesystemShell`, keeps single-command failures inside the prompt loop, and preserves Unix-like
  output.

Integration tests against a real IoTDB cluster can be added after the unit-tested shell behavior is
stable.

## Risks and Mitigations

- Tree path ambiguity: use staged resolution and return the most specific proven node type.
- Large schema scans: limit `tree` depth by default and list only one level in `ls`.
- Identifier escaping bugs: centralize escaping in provider helpers.
- Backward compatibility regressions: make `sql` the default access mode and keep SQL-mode command
  processing unchanged.
- Write support complexity: keep writes opt-in and narrow, and route object semantics through typed
  paths and mutation providers rather than broad command-specific SQL construction.
