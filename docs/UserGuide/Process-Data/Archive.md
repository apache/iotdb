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

# Archive Data

The archive data tools consist of 5 Cli commands: `set`, `cancel`, `pause`, `continue`, and `show`. Users may use
archive tools to create archive tasks, these archive tasks start at the user specified date, and archives expired data (
timestamp before expire time) into a target directory specified by user, the user can then perform other operations such
as `pause` on the tasks.

## SQL statements

### Show Archive Tasks

Show the data archive tasks.

#### Syntax

```sql
SHOW ALL ARCHIVE
SHOW ARCHIVE ON <storage_group>
```

- `<storage_group>` specifies the storage group to show archive task on.

#### Example Result

```sql
+-------+---------------------------+-------------+------+---------------------------+---------------+----------------+
|task id|                submit time|storage group|status|                 start time|expire time(ms)|target directory|
+-------+---------------------------+-------------+------+---------------------------+---------------+----------------+
|      0|2022-1-1T00:00:00.000+08:00|      root.ln| READY|2023-1-1T00:00:00.000+08:00|         360000|            /tmp|
+-------+---------------------------+-------------+------+---------------------------+---------------+----------------+
```

### Set Data Archive Task

User submit data archive task.

#### Syntax

```sql
SET ARCHIVE TO <storage_group> <start_time> <ttl> <target_dir>
SET ARCHIVE TO storage_group=<storage_group> start_time=<start_time> ttl=<ttl> target_dir=<target_dir>
```

- `<storage_group>` specifies the storage group to show archive task on.
- `<start_time>` specifies the date to start the archive task.
- `<ttl>` specifies the expire time for task, data with `timestamp < now - ttl` are archived, units in milliseconds.
- `<target_dir>` specifies the target directory to move the archived data, uses string for the path.

#### Example

```sql
SET ARCHIVE TO storage_group=root.ln start_time=2023-01-01 ttl=360000 target_dir="/tmp"
SET ARCHIVE TO root.ln 2023-01-01 360000 "/tmp"
```

#### Tips

- `A=` (such as `storage_group=`) in the Cli commands can be omitted, the order after omission must be the same as the
  above.
- The start time is in ISO 8601 format, so information such as hour/minute/second can be omitted, and it is set to 0 by
  default after being omitted.
- `SET` command is able to submit migration tasks for all storage groups by parameters like `root.ln.**`.

### Cancel Archive Task

Stop and delete the data archive task. (Note: data that has been archived will not be put back into the database)

#### Syntax

```sql
CANCEL ARCHIVE <task_id>
CANCEL ARCHIVE ON <storage_group>
```

- `<task_id>` specifies the id of archive task to cancel.
- `<storage_group>` specifies the storage group to cancel archive task, if many exist cancel the one with the lowest
  start time.

#### Example

```sql
CANCEL ARCHIVE 0
CANCEL ARCHIVE ON root.ln
```

### Pause Archive Task

Suspend the data migration task, run the `RESUME` command to resume the task.

#### Syntax

```sql
PAUSE ARCHIVE <task_id>
PAUSE ARCHIVE ON <storage_group>
```

- `<task_id>` specifies the id of archive task to pause.
- `<storage_group>` specifies the storage group to pause archive task, if many exist cancel the one with the lowest
  start time.

#### Example

```sql
PAUSE ARCHIVE 0
PAUSE ARCHIVE ON root.ln
```

### Resume Archive Task

Resume suspended data archive tasks.

#### Syntax

```sql
RESUME ARCHIVE <task_id>
RESUME ARCHIVE ON <storage_group>
```

- `<task_id>` specifies the id of archive task to resume.
- `<storage_group>` specifies the storage group to resume archive task, if many exist cancel the one with the lowest
  start time.

#### Example

```sql
RESUME ARCHIVE 0
RESUME ARCHIVE ON root.ln
```

## System Parameter Configuration

| Name                 | Description                                                           | Data Type | Default Value |
| :------------------- | --------------------------------------------------------------------- | --------- | ------------- |
| `archive_thread_num` | The number of threads in the thread pool that executes archive tasks. | int       | 2             |
