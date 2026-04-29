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

**Goal:** Add an explicit read-only filesystem mode to IoTDB CLI while preserving default SQL CLI behavior.

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
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/FilesystemSchemaProvider.java`: read-only provider interface.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/TreeFilesystemSchemaProvider.java`: tree SQL mapping.
- Create `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/TableFilesystemSchemaProvider.java`: table SQL mapping.
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
- Keep user-visible output Unix-like:
  - `ls` prints names only.
  - `tree` prints indented names only.
  - `cat` and `paste` print tab-separated values.
  - `stat` is the place to show metadata.
- Do not add filesystem command dialects such as `cat --columns` or `select`. Multi-column reads use
  `paste /db/table/col1 /db/table/col2`.
- Table provider can optimize Unix-looking commands internally:
  - `cat /db/table` -> `SELECT * FROM db.table LIMIT 20`
  - `cat /db/table/col` -> `SELECT col FROM db.table LIMIT 20`
  - `paste /db/table/col1 /db/table/col2` -> `SELECT col1, col2 FROM db.table LIMIT 20`
- Interactive filesystem command errors must be handled at the single-command loop level. A
  `SQLException` from `FilesystemShell.execute()` should print `<command>: <message>` and continue
  the prompt, not bubble out to `receiveCommands()` and exit the CLI.
- The observed `cat time` exit came from path resolution and error bubbling: from `/testtest`,
  `cat time` resolves to `/testtest/time`; table mode interpreted that as table `testtest.time`;
  the server returned `550`; the unchecked propagation exited the CLI. Keep a regression test for
  this behavior.

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
  mvn test -o -nsu -Dtest=AbstractCliTest,CliFilesystemModeTest,FsPathTest,FilesystemCommandParserTest,TreeFilesystemSchemaProviderTest,TableFilesystemSchemaProviderTest,FilesystemShellTest,JdbcSqlExecutorTest
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

Use a mocked `SqlExecutor` to verify `list(/)`, `list(/db)`, `list(/db/table)`, `describe(/db/table/col)`, and `read(/db/table/col)` issue expected SQL and return typed nodes.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl iotdb-client/cli -Dtest=TableFilesystemSchemaProviderTest`
Expected: compilation failure or missing behavior.

- [ ] **Step 3: Implement table provider**

Implement database/table/column mapping with centralized identifier rendering.

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

Run: `mvn test -pl iotdb-client/cli -Dtest=AbstractCliTest,FsPathTest,FilesystemCommandParserTest,TreeFilesystemSchemaProviderTest,TableFilesystemSchemaProviderTest`
Expected: all focused tests pass.

### Task 6: Verification

- [ ] **Step 1: Run CLI module unit tests**

Run: `mvn test -pl iotdb-client/cli`
Expected: CLI module unit tests pass, or any unrelated pre-existing failure is documented with output.

- [ ] **Step 2: Run formatting**

Run: `mvn spotless:apply -pl iotdb-client/cli`
Expected: formatting applied without errors.

- [ ] **Step 3: Re-run focused tests after formatting**

Run: `mvn test -pl iotdb-client/cli -Dtest=AbstractCliTest,FsPathTest,FilesystemCommandParserTest,TreeFilesystemSchemaProviderTest,TableFilesystemSchemaProviderTest`
Expected: all focused tests pass.
