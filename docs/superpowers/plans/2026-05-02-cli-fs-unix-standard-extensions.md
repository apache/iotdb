# CLI Filesystem Unix Standard Extensions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend CLI filesystem mode with a first slice of standard Unix-style database operations.

**Architecture:** Keep command parsing in `FilesystemCommandParser`, command dispatch in `FilesystemShell`, and database mutations behind `FilesystemMutationProvider`. Table-mode mutations map to SQL through `TableFilesystemMutationProvider`; tree-mode writes remain unsupported through `UnsupportedFilesystemMutationProvider`.

**Tech Stack:** Java, JUnit 4, Mockito, Maven module `iotdb-client/cli`.

---

### Task 1: Add Standard Command Parsing

**Files:**
- Modify: `iotdb-client/cli/src/test/java/org/apache/iotdb/cli/fs/command/FilesystemCommandParserTest.java`
- Modify: `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/command/FilesystemCommand.java`
- Modify: `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/command/FilesystemCommandParser.java`

- [ ] Write failing tests for `rmdir <path>`, `rm -r <path>`, `cp <source> <target>`, and `ls -R [path]`.
- [ ] Run `mvn -pl iotdb-client/cli -Dtest=FilesystemCommandParserTest test` and verify the new tests fail because commands are not implemented.
- [ ] Implement the minimal parser changes.
- [ ] Re-run the parser test and verify it passes.

### Task 2: Add Shell Dispatch

**Files:**
- Modify: `iotdb-client/cli/src/test/java/org/apache/iotdb/cli/fs/FilesystemShellTest.java`
- Modify: `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/FilesystemShell.java`
- Modify: `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/FilesystemMutationProvider.java`
- Modify: `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/UnsupportedFilesystemMutationProvider.java`

- [ ] Write failing shell tests proving `rmdir`, `rm -r`, and `cp` call the mutation provider only when writes are enabled, and `ls -R` recursively lists children.
- [ ] Run `mvn -pl iotdb-client/cli -Dtest=FilesystemShellTest test` and verify the tests fail for missing behavior.
- [ ] Implement minimal shell dispatch and provider interface methods.
- [ ] Re-run the shell test and verify it passes.

### Task 3: Add Table Mutation SQL

**Files:**
- Modify: `iotdb-client/cli/src/test/java/org/apache/iotdb/cli/fs/provider/TableFilesystemMutationProviderTest.java`
- Modify: `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/TableFilesystemMutationProvider.java`

- [ ] Write failing provider tests for dropping a database through `rmdir`/recursive remove and copying `/db/t1.schema` to `/db/t2.schema`.
- [ ] Run `mvn -pl iotdb-client/cli -Dtest=TableFilesystemMutationProviderTest test` and verify failures.
- [ ] Implement minimal table mutation SQL: `DROP DATABASE <db>` and `CREATE TABLE <target> LIKE <source>`.
- [ ] Re-run provider tests and verify they pass.

### Task 4: Regression Verification

**Files:**
- Existing fs-mode tests.

- [ ] Run `mvn -pl iotdb-client/cli -Dtest=FilesystemCommandParserTest,FilesystemShellTest,TableFilesystemMutationProviderTest,CliFilesystemModeTest test`.
- [ ] Fix only failures caused by this change.
