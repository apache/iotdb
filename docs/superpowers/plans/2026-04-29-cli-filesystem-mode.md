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

# CLI Filesystem Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an explicit filesystem mode to IoTDB CLI while preserving default SQL CLI behavior.
Filesystem mode is read-only by default; table mode has an opt-in minimal write loop behind
`--fs_write_mode enabled`.

**Architecture:** Add a small `org.apache.iotdb.cli.fs` subsystem with path parsing, command parsing, typed nodes, and tree/table schema providers backed by JDBC SQL. Existing `Cli` keeps ownership of startup parsing and dispatches to filesystem mode only when `--access_mode filesystem` is passed.

**Tech Stack:** Java 8, JUnit 4, Mockito, commons-cli, JLine, JDBC, existing IoTDB CLI test patterns.

---

## File Structure

- Modify `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/AbstractCli.java`: add access-mode option constants and parsing helpers.
- Modify `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/Cli.java`: route to SQL or filesystem mode.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/path/FsPath.java`: normalize filesystem paths.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/command/FilesystemCommand.java`: command value object.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/command/FilesystemCommandParser.java`: parse filesystem commands.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/node/FsNodeType.java`: node type enum.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/node/FsNode.java`: typed filesystem node.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/sql/SqlExecutor.java`: minimal JDBC query abstraction.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/sql/SqlRow.java`: row data object for tests and providers.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/FilesystemSchemaProvider.java`: schema and data read provider interface.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/FilesystemMutationProvider.java`: write-gated mutation provider interface.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/TreeFilesystemSchemaProvider.java`: tree SQL mapping.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/TableFilesystemSchemaProvider.java`: table SQL mapping.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/TableFilesystemMutationProvider.java`: table write mapping.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/UnsupportedFilesystemMutationProvider.java`: unsupported mutation provider.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/FilesystemShell.java`: command execution surface and interactive shell.
- Test `iotdb-client/cli/src/test/java/org/apache/iotdb/cli/fs/path/FsPathTest.java`.
- Test `iotdb-client/cli/src/test/java/org/apache/iotdb/cli/fs/command/FilesystemCommandParserTest.java`.
- Test `iotdb-client/cli/src/test/java/org/apache/iotdb/cli/fs/provider/TreeFilesystemSchemaProviderTest.java`.
- Test `iotdb-client/cli/src/test/java/org/apache/iotdb/cli/fs/provider/TableFilesystemSchemaProviderTest.java`.
- Modify `iotdb-client/cli/src/test/java/org/apache/iotdb/cli/AbstractCliTest.java`: access-mode defaults and validation.

## Current Implementation Notes

These notes capture follow-up implementation experience for quickly resuming this branch.

- The current filesystem mode commands are implemented in-process by `FilesystemCommandParser` and
  `FilesystemShell`; they do not call `/bin/ls`, `/bin/cat`, or `java.nio.file.FileSystem`.
- `FilesystemCommandParser` currently parses `sql <statement>`, but `FilesystemShell` does not
  execute it and will report `Unsupported filesystem command: SQL`. Raw SQL should be run in SQL
  access mode until this command is explicitly implemented.
- Keep user-visible output strictly aligned with standard Unix/POSIX filesystem command semantics.
  Any deviation is a bug unless it is explicitly documented as a temporary compatibility exception.
  - `ls` prints names only. The baseline output should be one entry per line, matching `ls -1`; do
    not use comma-separated output.
  - `ls -a` and `ll -a` include `.` and `..`; `ll` reuses `ls` option parsing as the long-listing
    alias.
  - `tree` prints indented names only.
  - `cat /db/table.csv` prints CSV records. Legacy compatibility table/column paths and `paste`
    continue to print tab-separated values.
  - `cut -d, -f2,3 /db/table.csv` is the Unix-compatible multi-field projection form for CSV-first
    table files. It is delimiter-based text cutting, not CSV quote parsing and not a database
    column-selection dialect.
  - `less` and `more` are non-interactive read aliases today; they print readable content with the
    default read limit.
  - `stat` is the place to show metadata.
- Do not add filesystem command dialects such as `cat --columns` or `select`. Multi-column reads
  should use `cut` for CSV-first table files; legacy column paths may still use
  `paste /db/table/col1 /db/table/col2`.
- Table provider can optimize Unix-looking commands internally:
  - Table mode is CSV-first: a database is a directory, and each table is exposed as
    `/db/table.csv` with `/db/table.schema` and `/db/table.meta` sidecar regular files.
  - `ls /db` should list `table.csv`, `table.schema`, and `table.meta` entries for each table.
  - `cat /db/table.csv` -> `SELECT * FROM db.table LIMIT 20`, formatted as CSV.
  - `cut -d, -f2,3 /db/table.csv` -> delimiter-based field selection over the CSV records.
  - `cat /db/table.schema` -> `DESC db.table DETAILS`, formatted as CSV with IoTDB result columns
    preserved.
  - `cat /db/table.meta` -> `SHOW TABLES DETAILS FROM db`, filtered to the table and formatted as
    CSV with IoTDB result columns preserved.
  - Legacy `/db/table/column` paths may remain as compatibility paths or migration sources.
  - Legacy `cat /db/table/col` -> `SELECT col FROM db.table LIMIT 20`
  - Legacy `paste /db/table/col1 /db/table/col2` -> `SELECT col1, col2 FROM db.table LIMIT 20`
- Table-mode write boundaries are intentionally narrow and only active with
  `--fs_write_mode enabled`:
  - `mkdir /db` creates a database.
  - `rm /db/table.csv` drops a table.
  - `mv /db/t1.csv /db/t2.csv` renames a table inside the same database.
  - `tee -a /db/table.csv` appends CSV records as table rows.
  - Forbid `rm` or `mv` of `/db/table.schema` and `/db/table.meta`.
  - Forbid `rm /db`.
  - Forbid cross-database rename such as `mv /db1/t.csv /db2/t.csv`.
- Tree-mode writes remain unsupported even when `--fs_write_mode enabled` is set.
- Filesystem completion is mode-aware after login: filesystem mode installs
  `FilesystemShell.createCompleter()`, which completes command names at the first word and path
  children later, appending `/` to directories.
- Interactive filesystem command errors must be handled at the single-command loop level. A
  `SQLException` from `FilesystemShell.execute()` should print `<command>: <message>` and continue
  the prompt, not bubble out to `receiveCommands()` and exit the CLI.
- The observed `cat time` exit came from path resolution and error bubbling: from `/testtest`,
  `cat time` resolves to `/testtest/time`; table mode interpreted that as table `testtest.time`;
  the server returned `550`; the unchecked propagation exited the CLI. Keep a regression test for
  this behavior.

## Data Append Design

The first table data-write implementation supports only append semantics over CSV table files:

```bash
tee -a /db/table.csv
```

Design decisions already approved:

- Only append writes are supported. Truncate, overwrite, random writes, row deletion, and operating
  system redirection such as `>> /db/table.csv` are out of scope.
- The target must be a table-mode data file `/<database>/<table>.csv`.
- `tee` without `-a`, sidecar files, legacy column paths, and tree-mode paths are rejected.
- Writes require `--access_mode filesystem --sql_dialect table --fs_write_mode enabled`.
- Non-interactive mode reads CSV from stdin until EOF and then submits.
- Interactive mode enters an append buffer. `:wq` validates and submits, `:q!` discards, and `:q`
  exits only if the buffer is empty.
- The first version does not support `:w`.
- CSV parsing should use Apache Commons CSV or another proven parser already available to the
  module.
- Header and headerless input are both supported.
- Header input may write a subset of columns but must include `time`.
- Headerless input must provide all columns in the current `/db/table.csv` output order.
- Every record must explicitly provide `time`; `time` cannot be empty or `\N`.
- `\N` is the explicit SQL `NULL` marker for non-time columns. Empty fields are not automatically
  NULL.
- Client-side validation covers path, write mode, CSV shape, known columns, and required `time`.
- IoTDB performs type validation, timestamp-format validation, permissions checks, and final insert
  semantics.
- Validated records map to SQL of the form
  `INSERT INTO db.table(col1,col2,...) VALUES (...), (...), ...`.
- Insert statements should be chunked, for example 1000 records per statement.
- The full buffer must pass client validation before any insert is attempted.
- Server-side insert failure is not compensated or retried automatically. Interactive mode keeps the
  buffer; non-interactive mode exits with failure.

## Supported Command Quick Reference

| Command | Description | Example |
| --- | --- | --- |
| `pwd` | Print the current filesystem path. | `pwd` |
| `ls [-a|-l|-la] [path]` | List child names; `-a` includes dot entries and `-l` enables long listing. | `ls -la /db` |
| `ll [-a] [path]` | Long listing alias with read-only permissions in output. | `ll -a /db` |
| `cd <path>` | Change directory when the target is a directory node. | `cd /db` |
| `stat [path]` | Print filesystem-style metadata for a node. | `stat /db/table.csv` |
| `cat <path>...` | Print readable paths sequentially. | `cat /db/table.csv` |
| `head [-n lines] <path>` | Print the first rows or text lines; short form such as `-5` is accepted. | `head -n 5 /db/table.csv` |
| `tail [-n lines] <path>` | Print the last rows or text lines where supported. | `tail -n 5 /db/table.csv` |
| `wc -l <path>` | Print logical count and path. | `wc -l /db/table.csv` |
| `grep <pattern> <path>` | Print rows or lines containing a literal substring. | `grep spricoder /db/table.csv` |
| `find [path] [-name name]` | Recursively print matching paths; `-name` is exact node-name matching. | `find /db -name table.csv` |
| `less <path>` | Non-interactive read alias using the default read limit. | `less /db/table.csv` |
| `more <path>` | Non-interactive read alias using the default read limit. | `more /db/table.schema` |
| `file <path>` | Print `directory`, `regular file`, or `unknown`. | `file /db/table.meta` |
| `du <path>` | Print logical count and path using provider count. | `du /db/table.csv` |
| `cut -d<delimiter> -f<fields> <path>` | Delimiter-based Unix field selection; supports lists and closed ranges. | `cut -d, -f2,3 /db/table.csv` |
| `paste <path>...` | Print multiple readable paths side by side; table mode supports legacy same-table column paths. | `paste /db/table/key /db/table/value` |
| `tree [-L depth] [path]` | Print descendants with indentation and names only. | `tree -L 2 /db` |
| `mkdir <path>` | Write-gated; in table mode with writes enabled, creates a database. | `mkdir /newdb` |
| `rm <path>` | Write-gated; in table mode with writes enabled, only table CSV drop is allowed. | `rm /db/table.csv` |
| `mv <source> <target>` | Write-gated; in table mode with writes enabled, only same-database table CSV rename is allowed. | `mv /db/t1.csv /db/t2.csv` |
| `tee -a <path>` | Write-gated; in table mode with writes enabled, appends CSV input to a table data file. | `tee -a /db/table.csv` |
| `help` | Print filesystem-mode help. | `help` |
| `exit` / `quit` | Exit filesystem mode. | `exit` |

## Subagent Usage Notes

All later work on this feature may use subagents to accelerate execution. Prefer subagents when the
work can be split into independent, bounded tasks; keep tightly coupled or immediately blocking
work in the main session.

Good subagent tasks for this branch:

- Read-only exploration of one subsystem, for example parser behavior in
  `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/command`.
- Read-only review of Unix command semantics before adding or changing a filesystem command.
- Implementing one isolated slice with a disjoint write set, for example only provider tests and
  provider code for a table-mode read behavior.
- Running or reviewing one focused test group while the main session works on a different slice.
- Reviewing docs for consistency after implementation changes.
- Assigning natural layers independently: `FsPath`, command parser, tree provider, table provider,
  shell output, CLI dispatch, and docs.
- Checking whether `ls`, `tree`, `cat`, `paste`, and `stat` still match the design document's Unix
  output semantics.
- Reviewing whether SQL mode remains the default and whether existing SQL CLI behavior is still
  isolated from filesystem mode.
- Inspecting exception paths, especially interactive filesystem commands where one failed command
  must not exit the shell.
- Reviewing Maven output to distinguish real test failures from sandbox or Develocity noise.

Avoid subagents for:

- Changes that require editing the same files in parallel.
- Immediate blocking work where the next local step depends on the result.
- Broad refactors across CLI, provider, shell, and docs at the same time.
- Final integration decisions touching startup compatibility in `Cli.java` or `AbstractCli.java`.
- Cross-cutting behavior that simultaneously changes parser, shell, provider, and tests.
- Root-cause analysis where the fix direction is still unclear and requires one coherent debugging
  thread.
- Merging multiple subagent results, resolving file conflicts, or deciding whether to expand the
  test scope.
- Any git operation. Git status, diff, add, commit, branch, push, or reset still require explicit
  user confirmation before running.

Subagent task categories:

- Read-only subagents may inspect code, docs, tests, and command output. Use them for current-state
  analysis, risk review, Unix semantic checks, and test coverage gap analysis.
- Implementation subagents may edit only the files explicitly assigned to them. Give each worker a
  single package or feature slice, such as table-provider reads plus matching provider tests.
- Verification subagents run specified commands, summarize results, and identify likely causes of
  failures. They must not change files unless explicitly reassigned.

The main session owns final decisions, conflict resolution, and the final verification story.

When dispatching a subagent, include this checklist in the prompt:

- State whether the task is read-only or may edit files.
- Name the exact files or package the subagent owns.
- Explicitly say: `Do not run git commands. Do not inspect git status or git diff unless the user
  explicitly approves it.`
- Tell implementation subagents that they are not alone in the codebase, must not revert unrelated
  edits, and must accommodate changes made by others.
- Require a final summary with changed files, tests run, and any residual risk.
- For implementation work, require TDD: write or update the failing test first, verify red, then
  implement.
- For verification work, require read-only behavior: report the command, result, failure summary,
  and suspected root cause without modifying files.

The main session must review subagent output before finalizing. For code changes, run
`mvn spotless:apply -o -nsu` before compile/test, then run the focused and broader filesystem-mode
test commands listed below.

## Fast Test And Build Notes

- After code edits and before compile/test, run Spotless first:

  ```bash
  mvn spotless:apply -o -nsu
  ```

- Prefer running Maven from `iotdb-client/cli` for focused CLI work:

  ```bash
  cd iotdb-client/cli
  ```

- Prefer offline/no-snapshot-update mode when dependencies are already local:

  ```bash
  mvn test -o -nsu -Dtest=FilesystemShellTest,CliFilesystemModeTest
  ```

- The broader filesystem-mode focused suite is:

  ```bash
  mvn test -o -nsu -Dtest=AbstractCliTest,CliFilesystemModeTest,JlineUtilsTest,FsPathTest,FilesystemCommandParserTest,TreeFilesystemSchemaProviderTest,TableFilesystemSchemaProviderTest,TableFilesystemMutationProviderTest,FilesystemShellTest,JdbcSqlExecutorTest
  ```

- Maven/Develocity may print `Operation not permitted` stack traces for writes under
  `~/.m2/.develocity` in the sandbox. Check the final Maven result. In observed runs,
  `spotless:apply` still ended with `BUILD SUCCESS` despite those Develocity warnings.

- A good TDD sequence for this area:
  1. Add or update focused tests in parser, shell, and provider layers.
  2. Run the focused tests and verify the expected red failure.
  3. Implement the minimal code.
  4. Run `mvn spotless:apply -o -nsu`.
  5. Run the focused test command.
  6. Run the broader filesystem-mode suite above.

- Git operations require explicit user confirmation before running any `git` command.

### Task 1: Path Model

- [ ] **Step 1: Write failing `FsPathTest`**

Create tests for absolute paths, relative paths, `.`, `..`, repeated slashes, empty input, and not moving above root.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl iotdb-client/cli -Dtest=FsPathTest`
Expected: compilation failure because `FsPath` does not exist.

- [ ] **Step 3: Implement `FsPath`**

Implement only normalization, resolution, `isRoot`, `getSegments`, `getFileName`, and `toString`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl iotdb-client/cli -Dtest=FsPathTest`
Expected: all `FsPathTest` tests pass.

### Task 2: Command Parser

- [ ] **Step 1: Write failing `FilesystemCommandParserTest`**

Cover `pwd`, `ls`, `ls /root`, `cd ..`, `stat`, `cat /x`, `tree -L 2 /root`, `sql SHOW DATABASES`, `help`, `exit`, invalid `tree -L bad`, and unknown commands.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl iotdb-client/cli -Dtest=FilesystemCommandParserTest`
Expected: compilation failure because command classes do not exist.

- [ ] **Step 3: Implement parser classes**

Implement a small parser using whitespace tokenization with special handling for `sql`, whose body preserves the rest of the input.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl iotdb-client/cli -Dtest=FilesystemCommandParserTest`
Expected: all command parser tests pass.

### Task 3: Tree Provider SQL Mapping

- [ ] **Step 1: Write failing `TreeFilesystemSchemaProviderTest`**

Use a mocked `SqlExecutor` to verify `list(/)`, `list(/root)`, `list(/root/sg)`, `describe(/root/sg/d1/s1)`, and `read(/root/sg/d1/s1)` issue the expected SQL and return typed nodes.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl iotdb-client/cli -Dtest=TreeFilesystemSchemaProviderTest`
Expected: compilation failure because provider classes do not exist.

- [ ] **Step 3: Implement provider and node model**

Implement read-only provider methods needed by the tests, using centralized path-to-tree-SQL conversion.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl iotdb-client/cli -Dtest=TreeFilesystemSchemaProviderTest`
Expected: all tree provider tests pass.

### Task 4: Table Provider SQL Mapping

- [ ] **Step 1: Write failing `TableFilesystemSchemaProviderTest`**

Use a mocked `SqlExecutor` to verify `list(/)`, `list(/db)`, `describe(/db/table.csv)`,
`read(/db/table.csv)`, `describe(/db/table.schema)`, `readLines(/db/table.schema)`,
`describe(/db/table.meta)`, and `readLines(/db/table.meta)` issue expected SQL and return typed nodes
or text lines.
Also cover legacy `/db/table/col` paths if compatibility behavior remains enabled.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl iotdb-client/cli -Dtest=TableFilesystemSchemaProviderTest`
Expected: compilation failure or missing behavior.

- [ ] **Step 3: Implement table provider**

Implement CSV-first database/table sidecar mapping with centralized identifier rendering:
`/db/table.csv` is the table data regular file, `/db/table.schema` is the schema sidecar regular
file, and `/db/table.meta` is the metadata sidecar regular file. Keep `/db/table/column` only as a
legacy compatibility path or migration source.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl iotdb-client/cli -Dtest=TableFilesystemSchemaProviderTest`
Expected: all table provider tests pass.

### Task 5: CLI Access Mode Dispatch

- [ ] **Step 1: Extend `AbstractCliTest` with failing access-mode tests**

Cover default `sql`, accepted `filesystem`, and rejected invalid access mode.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl iotdb-client/cli -Dtest=AbstractCliTest`
Expected: failure because access-mode constants/helpers do not exist.

- [ ] **Step 3: Implement access-mode option and shell dispatch hook**

Add the long option and route filesystem mode to `FilesystemShell` without changing default SQL mode.

- [ ] **Step 4: Run focused tests**

Run: `mvn test -pl iotdb-client/cli -Dtest=AbstractCliTest,CliFilesystemModeTest,JlineUtilsTest,FsPathTest,FilesystemCommandParserTest,TreeFilesystemSchemaProviderTest,TableFilesystemSchemaProviderTest,TableFilesystemMutationProviderTest,FilesystemShellTest,JdbcSqlExecutorTest`
Expected: all focused tests pass.

### Task 6: Verification

- [ ] **Step 1: Run CLI module unit tests**

Run: `mvn test -o -nsu -Dtest=AbstractCliTest,CliFilesystemModeTest,JlineUtilsTest,FsPathTest,FilesystemCommandParserTest,TreeFilesystemSchemaProviderTest,TableFilesystemSchemaProviderTest,TableFilesystemMutationProviderTest,FilesystemShellTest,JdbcSqlExecutorTest` from `iotdb-client/cli`.
Expected: focused filesystem-mode unit tests pass, or any unrelated pre-existing failure is
documented with output.

- [ ] **Step 2: Run formatting**

Run: `mvn spotless:apply -o -nsu` from `iotdb-client/cli`.
Expected: formatting applied without errors.

- [ ] **Step 3: Re-run focused tests after formatting**

Run: `mvn test -o -nsu -Dtest=AbstractCliTest,CliFilesystemModeTest,JlineUtilsTest,FsPathTest,FilesystemCommandParserTest,TreeFilesystemSchemaProviderTest,TableFilesystemSchemaProviderTest,TableFilesystemMutationProviderTest,FilesystemShellTest,JdbcSqlExecutorTest` from `iotdb-client/cli`.
Expected: all focused tests pass.
