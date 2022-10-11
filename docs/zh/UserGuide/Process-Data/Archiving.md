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

# 数据归档

数据归档功能提供 5 个 Cli 命令：包括查看、提交、取消、暂停和继续归档任务。
用户可以创建归档任务，这些归档任务由用户指定的的启动时间，并归档过期数据到用户指定的目录。

## SQL 语句

### 查看数据归档任务

显示数据归档任务。

#### 语法

```sql
SHOW ARCHIVING
SHOW ALL ARCHIVING
SHOW ARCHIVING ON <storage_group>
SHOW ALL ARCHIVING ON <storage_group>
```

- `<storage_group>` 返回指定存储组上的任务参数以及状态。
- `all` 默认只返回处于 READY、RUNNING、PAUSED 状态的任务，可以通过添加 all 参数查看其他状态的任务

#### 结果示例

```sql
+-------+---------------------------+-------------+------+---------------------------+---------------+----------------+
|task id|                submit time|storage group|status|                 start time|expire time(ms)|target directory|
+-------+---------------------------+-------------+------+---------------------------+---------------+----------------+
|      0|2022-1-1T00:00:00.000+08:00|      root.ln| READY|2023-1-1T00:00:00.000+08:00|         360000|            /tmp|
+-------+---------------------------+-------------+------+---------------------------+---------------+----------------+
```

### 提交数据归档任务

用户提交数据归档任务。

#### 语法

```sql
SET ARCHIVING TO <storage_group> <start_time> <ttl> <target_dir>
SET ARCHIVING TO storage_group=<storage_group> start_time=<start_time> ttl=<ttl> target_dir=<target_dir>
```

- `<storage_group>` 指定的归档的存储组。
- `<start_time>` 归档任务开始执行的时间。
- `<ttl>` 数据过期时长，当数据的时间辍 `timestamp < now - ttl` 则为过期数据，单位为毫秒。
- `<target_dir>` 数据文件被归档存储的目标路径，使用字符串指定路径。

#### 示例

```sql
SET ARCHIVING TO storage_group=root.ln start_time=2023-01-01 ttl=360000 target_dir="/tmp"
SET ARCHIVING TO root.ln 2023-01-01 360000 "/tmp"
```

#### 提示

- 指令中的 `A=` （比如 `storage_group=`）可以省略，省略后顺序必须和上述一致。
- 开始时间使用 ISO 8601 格式，因此可以省略时/分/秒等信息，省略后默认设成 0。
- 可以提交全部存储组的归档任务，使用类似 `root.ln.**`。

### 取消数据归档任务

停止并取消数据归档任务。（注意：已经被归档的数据不会被放回数据库中）

#### 语法

```sql
CANCEL ARCHIVING <task_id>
CANCEL ARCHIVING ON <storage_group>
```

- `<task_id>` 归档任务的索引号。
- `<storage_group>` 取消归档任务的存储组，如果存在多个则取启动时间最早的任务。

#### 示例

```sql
CANCEL ARCHIVING 0
CANCEL ARCHIVING ON root.ln
```

### 暂停数据归档任务

将正在运行的数据归档任务挂起。

#### 语法

```sql
PAUSE ARCHIVING <task_id>
PAUSE ARCHIVING ON <storage_group>
```

- `<task_id>` 归档任务的索引号。
- `<storage_group>` 暂停归档任务的存储组，如果存在多个则取启动时间最早的任务。

#### 示例

```sql
PAUSE ARCHIVING 0
PAUSE ARCHIVING ON root.ln
```

### 继续数据归档任务

让挂起的数据归档任务重新执行。

#### 语法

```sql
RESUME ARCHIVING <task_id>
RESUME ARCHIVING ON <storage_group>
```

- `<task_id>` 归档任务的索引号。
- `<storage_group>` 继续归档任务的存储组，如果存在多个则取启动时间最早的任务。

#### 示例

```sql
RESUME ARCHIVING 0
RESUME ARCHIVING ON root.ln
```

## 系统参数配置

| 参数名                    | 描述                     | 数据类型 | 默认值 |
|:-----------------------| ------------------------ | -------- | ------ |
| `archiving_thread_num` | 数据归档任务使用的线程数 | int      | 2      |
