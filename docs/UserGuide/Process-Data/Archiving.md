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

# Data Archiving

The data archiving tools consist of 5 Cli commands: `set`, `cancel`, `pause`, `continue`, and `show`. Users may use
archiving tools to create archiving tasks, these archiving tasks start at the user specified date, and archives expired
data (timestamp before expire time) into a target directory specified by user, the user can then perform other
operations such as `pause` on the tasks.

## SQL statements

### Show Archiving Tasks

Show the data archiving tasks.

#### Syntax

```sql
SHOW ARCHIVING
SHOW ALL ARCHIVING
SHOW ARCHIVING ON <storage_group>
SHOW ALL ARCHIVING ON <storage_group>
```

- `<storage_group>` specifies the storage group to show archiving task on.
- `all` By default, only tasks in the READY, RUNNING, and PAUSED states are returned. You can view tasks in other states by adding the all parameter.
#### Example Result

```sql
+-------+---------------------------+-------------+------+---------------------------+---------------+----------------+
|task id|                submit time|storage group|status|                 start time|expire time(ms)|target directory|
+-------+---------------------------+-------------+------+---------------------------+---------------+----------------+
|      0|2022-1-1T00:00:00.000+08:00|      root.ln| READY|2023-1-1T00:00:00.000+08:00|         360000|            /tmp|
+-------+---------------------------+-------------+------+---------------------------+---------------+----------------+
```

### Set Data Archiving Task

User submit data archiving task.

#### Syntax

```sql
SET ARCHIVING TO <storage_group> <start_time> <ttl> <target_dir>
SET ARCHIVING TO storage_group=<storage_group> start_time=<start_time> ttl=<ttl> target_dir=<target_dir>
```

- `<storage_group>` specifies the storage group to show archiving task on.
- `<start_time>` specifies the date to start the archiving task.
- `<ttl>` specifies the expire time for task, data with `timestamp < now - ttl` are archived, units in milliseconds.
- `<target_dir>` specifies the target directory to move the archived data, uses string for the path.

#### Example

```sql
SET ARCHIVING TO storage_group=root.ln start_time=2023-01-01 ttl=360000 target_dir="/tmp"
SET ARCHIVING TO root.ln 2023-01-01 360000 "/tmp"
```

#### Tips

- `A=` (such as `storage_group=`) in the Cli commands can be omitted, the order after omission must be the same as the
  above.
- The start time is in ISO 8601 format, so information such as hour/minute/second can be omitted, and it is set to 0 by
  default after being omitted.
- `SET` command is able to submit migration tasks for all storage groups by parameters like `root.ln.**`.

### Cancel Archiving Task

Stop and delete the data archiving task. (Note: data that has been archived will not be put back into the database)

#### Syntax

```sql
CANCEL ARCHIVING <task_id>
CANCEL ARCHIVING ON <storage_group>
```

- `<task_id>` specifies the id of archiving task to cancel.
- `<storage_group>` specifies the storage group to cancel archiving task, if many exist cancel the one with the lowest
  start time.

#### Example

```sql
CANCEL ARCHIVING 0
CANCEL ARCHIVING ON root.ln
```

### Pause Archiving Task

Suspend the data migration task.

#### Syntax

```sql
PAUSE ARCHIVING <task_id>
PAUSE ARCHIVING ON <storage_group>
```

- `<task_id>` specifies the id of archiving task to pause.
- `<storage_group>` specifies the storage group to pause archiving task, if many exist cancel the one with the lowest
  start time.

#### Example

```sql
PAUSE ARCHIVING 0
PAUSE ARCHIVING ON root.ln
```

### Resume Archiving Task

Resume suspended data archiving tasks.

#### Syntax

```sql
RESUME ARCHIVING <task_id>
RESUME ARCHIVING ON <storage_group>
```

- `<task_id>` specifies the id of archiving task to resume.
- `<storage_group>` specifies the storage group to resume archiving task, if many exist cancel the one with the lowest
  start time.

#### Example

```sql
RESUME ARCHIVING 0
RESUME ARCHIVING ON root.ln
```

## System Parameter Configuration

| Name                   | Description                                                             | Data Type | Default Value |
|:-----------------------|-------------------------------------------------------------------------| --------- | ------------- |
| `archiving_thread_num` | The number of threads in the thread pool that executes archiving tasks. | int       | 2             |
