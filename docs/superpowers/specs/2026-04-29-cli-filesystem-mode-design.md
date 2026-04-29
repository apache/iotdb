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

The first version is read-only. The architecture must still leave clear extension points for
future write operations such as creating databases, creating tables, dropping schema objects, or
writing data.

## Non-Goals

- Do not implement a FUSE or operating-system-level mount.
- Do not change the default SQL CLI behavior.
- Do not make filesystem commands available implicitly in the existing SQL mode.
- Do not bypass server-side SQL, permissions, dialect handling, timeout handling, or SSL handling.
- Do not implement write operations in the first version.

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

When `sql_dialect=table`, filesystem paths map to relational schema objects.

| Filesystem Path | IoTDB Object | Node Type | Discovery |
| --- | --- | --- | --- |
| `/` | Virtual root | `VIRTUAL_ROOT` | `SHOW DATABASES` |
| `/<database>` | Database | `TABLE_DATABASE` | `SHOW DATABASES` |
| `/<database>/<table>` | Table or view | `TABLE_TABLE` / `TABLE_VIEW` | `SHOW TABLES DETAILS FROM <database>` |
| `/<database>/<table>/<column>` | Column | `TABLE_COLUMN` | `DESC <database>.<table> DETAILS` |

Table-model devices from `SHOW DEVICES FROM <table>` are not part of the first version's base path
hierarchy because they are data-instance-oriented rather than schema-container-oriented. They can
be added later as a virtual directory such as `/<database>/<table>/.devices`.

## Path Rules

- `/` is always the filesystem root.
- `.` and `..` are supported.
- Relative paths are resolved against the current directory.
- Attempts to navigate above `/` resolve to `/`.
- Tree-model paths must begin at `/root` for real IoTDB metadata.
- Table-model paths use `/database/table/column`.
- Wildcard paths are not treated as filesystem nodes in the first version. Users can run wildcard
  SQL through `sql <statement>`.
- SQL escaping and identifier quoting must be centralized in provider helper methods. Command
  implementations must not hand-build SQL strings for IoTDB identifiers.

## Commands

The first version supports a compact read-only command set.

| Command | Behavior |
| --- | --- |
| `pwd` | Print the current filesystem path. |
| `ls [path]` | List child nodes for a directory. |
| `cd <path>` | Change current directory if the target is a directory node. |
| `stat [path]` | Print node type and metadata. |
| `cat <path>` | Print file-like schema content for a leaf node. |
| `paste <path>...` | Print multiple file-like paths side by side, following Unix `paste` semantics. |
| `tree [path] [-L depth]` | Recursively list children with an explicit or default depth limit. |
| `sql <statement>` | Execute a raw SQL statement through the existing SQL result printer. |
| `help` | Print filesystem-mode help. |
| `exit` / `quit` | Exit filesystem mode. |

Unsupported write-oriented commands can be reserved for future use and return a clear read-only
message if introduced before write support.

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
| `ls /db` | `SHOW TABLES FROM db`. |
| `ls /db/table` | `DESC db.table`. |
| `stat /db` | `SHOW DATABASES DETAILS`, filtered to the database. |
| `stat /db/table` | `SHOW TABLES DETAILS FROM db`, filtered to the table. |
| `stat /db/table/col` | `DESC db.table DETAILS`, filtered to the column. |
| `cat /db/table/col` | `DESC db.table DETAILS`, formatted as schema text for the column. |

## Unix Output Semantics

Filesystem mode should keep command output close to standard Unix command behavior. Avoid exposing
internal implementation types or Java debug-style structures in normal command output.

- `ls` prints child names only, one entry per line.
- `tree` prints the hierarchy with indentation and names only.
- `cat` prints row content as tab-separated values, one row per line.
- `paste` prints multiple file-like paths side by side as tab-separated values.
- `stat` is the command that may expose typed metadata, because Unix `stat` is explicitly about
  object metadata.
- Error output should follow a command-prefixed style such as `cat: <message>` or
  `cd: <path>: Not a directory`.

This means provider-internal node types such as `TABLE_DATABASE`, `TABLE_COLUMN`, or
`TREE_DATABASE` must not appear in `ls` or `tree` output. Similarly, `SqlRow.asMap().toString()`
must not be used for `cat` or `paste` output.

## Read Semantics

Table-mode read behavior is currently:

- `cat /db/table` maps to `SELECT * FROM db.table LIMIT <limit>`.
- `cat /db/table/column` maps to `SELECT column FROM db.table LIMIT <limit>`.
- `paste /db/table/col1 /db/table/col2` maps to
  `SELECT col1, col2 FROM db.table LIMIT <limit>` when all paths are columns from the same table.

Although `paste` is implemented through one optimized SQL query for table mode, the user-facing
semantics remain Unix-like: users pass multiple file paths to a standard Unix command rather than
using a database-specific `select` command or `cat --columns` dialect.

In interactive filesystem mode, a single command failure must not terminate the CLI session. For
example, if `cat time` is resolved from `/testtest` to `/testtest/time`, table mode treats that as a
table path and may receive a server error such as `550: Table 'testtest.time' does not exist`. That
error should be printed as `cat: 550: ...`, then the prompt should continue.

## Proposed Code Structure

New code should live under `org.apache.iotdb.cli.fs`.

- `FilesystemShell`: filesystem-mode command loop and `-e` single-command execution.
- `command/*`: command parsing and command handlers for `pwd`, `ls`, `cd`, `stat`, `cat`, `tree`,
  `sql`, and `help`.
- `path/FsPath`: path normalization and resolution for absolute and relative paths.
- `node/FsNode`, `node/FsNodeType`, `node/FsNodeMetadata`: typed metadata model.
- `provider/FilesystemSchemaProvider`: read-only provider interface.
- `provider/TreeFilesystemSchemaProvider`: tree-model SQL mapping.
- `provider/TableFilesystemSchemaProvider`: table-model SQL mapping.
- `sql/SqlExecutor`: JDBC statement execution and result extraction helpers.
- `print/FilesystemPrinter`: text output for filesystem commands.

Existing SQL CLI code should not be broadly refactored. The implementation should only add the
small hooks needed to select filesystem mode and to let `sql <statement>` reuse existing SQL result
printing.

## Provider Interface Direction

The first version needs read-only operations:

- `resolve(FsPath path)`
- `list(FsPath path)`
- `describe(FsPath path)`
- `read(FsPath path)`

The interface should be shaped so future writes can be added without changing command parsing:

- `create(FsPath path, CreateOptions options)`
- `delete(FsPath path, DeleteOptions options)`
- `rename(FsPath source, FsPath target)`
- `write(FsPath path, FsWriteContent content, WriteOptions options)`

The first version exposes only read methods in the public provider interface. Write operation names
remain documented here as future extension points, and command handlers for unsupported write-like
commands return a read-only error instead of calling provider methods.

## Dependency Strategy

Use existing, proven dependencies already present in the CLI module:

- `commons-cli` for startup argument parsing.
- JLine for terminal input, history, and autosuggestion.
- JDBC and IoTDB SQL for metadata access.
- Existing IoTDB result printing where raw SQL output is required.

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
- Command parser tests for valid and invalid forms of `ls`, `cd`, `stat`, `cat`, `tree -L`, `sql`,
  `help`, `exit`, and `quit`.
- Provider tests with a mocked `SqlExecutor`, verifying tree-mode path-to-SQL mapping.
- Provider tests with a mocked `SqlExecutor`, verifying table-mode path-to-SQL mapping.
- CLI option tests extending existing CLI unit coverage for default `access_mode`, filesystem
  mode, and invalid mode values.
- Shell tests proving SQL mode remains the default and filesystem mode dispatches to
  `FilesystemShell`.

Integration tests against a real IoTDB cluster can be added after the unit-tested shell behavior is
stable.

## Risks and Mitigations

- Tree path ambiguity: use staged resolution and return the most specific proven node type.
- Large schema scans: limit `tree` depth by default and list only one level in `ls`.
- Identifier escaping bugs: centralize escaping in provider helpers.
- Backward compatibility regressions: make `sql` the default access mode and keep SQL-mode command
  processing unchanged.
- Future write support complexity: route all object semantics through typed nodes and providers,
  not through command-specific SQL construction.
