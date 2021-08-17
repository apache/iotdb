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
## 运维命令

### FLUSH

将指定存储组的内存缓存区Memory Table的数据持久化到磁盘上，并将数据文件封口。

```sql
IoTDB> FLUSH 
IoTDB> FLUSH root.ln
IoTDB> FLUSH root.sg1,root.sg2
```

### MERGE

合并顺序和乱序数据。当前IoTDB支持使用如下两种SQL手动触发数据文件的合并：

* `MERGE` 仅重写重复的Chunk，整理速度快，但是最终磁盘会存在多余数据。
* `FULL MERGE` 将需要合并的顺序和乱序文件的所有数据都重新写一份，整理速度慢，最终磁盘将不存在无用的数据。

```sql
IoTDB> MERGE
IoTDB> FULL MERGE
```

### CLEAR CACHE

手动清除chunk, chunk metadata和timeseries metadata的缓存，在内存资源紧张时，可以通过此命令，释放查询时缓存所占的内存空间。

```sql
IoTDB> CLEAR CACHE
```

### SET STSTEM TO READONLY / WRITABLE

手动设置系统为只读或者可写入状态。

```sql
IoTDB> SET SYSTEM TO READONLY
IoTDB> SET SYSTEM TO WRITABLE
```

### SCHEMA SNAPSHOT

为了加快 IoTDB 重启速度，用户可以手动触发创建 schema 的快照，从而避免服务器从 mlog 文件中恢复。
```sql
IoTDB> CREATE SNAPSHOT FOR SCHEMA
```

### 中止查询

当使用 IoTDB 时，您可能会遇到以下情形：输入了一个查询，但是由于其包含的数据量过大或是其他原因，导致长时间无法返回结果，但是迫于生产环境无法中止该命令，只能被迫等待。

从 0.12 版本开始，IoTDB 对执行时间过长的查询给出了两种解决方案：查询超时和查询中止。

#### 查询超时

对于执行时间过长的查询，IoTDB 将强行中断该查询，并抛出超时异常，如下所示：

```sql
IoTDB> select * from root;
Msg: 701 Current query is time out, please check your statement or modify timeout parameter.
```

系统默认的超时时间为 60000 ms，可以在配置文件中通过 `query_timeout_threshold` 参数进行自定义配置。

如果您使用 JDBC 或 Session，还支持对单个查询设置超时时间（单位为 ms）：

```java
((IoTDBStatement) statement).executeQuery(String sql, long timeoutInMS)
session.executeQueryStatement(String sql, long timeout)
```

如果不配置超时时间参数或将超时时间设置为 0，将使用服务器端默认的超时时间.

#### 查询中止

除了被动地等待查询超时外，IoTDB 还支持主动地中止查询，命令为：

```sql
KILL QUERY <queryId>
```

通过指定 `queryId` 可以中止指定的查询，而如果不指定 `queryId`，将中止所有正在执行的查询。

为了获取正在执行的查询 id，用户可以使用 `show query processlist` 命令，该命令将显示所有正在执行的查询列表，结果形式如下：

| Time | queryId | statement |
| ---- | ------- | --------- |
|      |         |           |

其中 statement 最大显示长度为 64 字符。对于超过 64 字符的查询语句，将截取部分进行显示。
