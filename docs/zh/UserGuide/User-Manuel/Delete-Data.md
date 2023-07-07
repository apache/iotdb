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

# 删除数据

用户使用 [DELETE 语句](../Reference/SQL-Reference.md) 可以删除指定的时间序列中符合时间删除条件的数据。在删除数据时，用户可以选择需要删除的一个或多个时间序列、时间序列的前缀、时间序列带、*路径对某一个时间区间内的数据进行删除。

在 JAVA 编程环境中，您可以使用 JDBC API 单条或批量执行 DELETE 语句。

## 单传感器时间序列值删除

以测控 ln 集团为例，存在这样的使用场景：

wf02 子站的 wt02 设备在 2017-11-01 16:26:00 之前的供电状态出现多段错误，且无法分析其正确数据，错误数据影响了与其他设备的关联分析。此时，需要将此时间段前的数据删除。进行此操作的 SQL 语句为：

```sql
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

如果我们仅仅想要删除 2017 年内的在 2017-11-01 16:26:00 之前的数据，可以使用以下 SQL:
```sql
delete from root.ln.wf02.wt02.status where time>=2017-01-01T00:00:00 and time<=2017-11-01T16:26:00;
```

IoTDB 支持删除一个时间序列任何一个时间范围内的所有时序点，用户可以使用以下 SQL 语句指定需要删除的时间范围：
```sql
delete from root.ln.wf02.wt02.status where time < 10
delete from root.ln.wf02.wt02.status where time <= 10
delete from root.ln.wf02.wt02.status where time < 20 and time > 10
delete from root.ln.wf02.wt02.status where time <= 20 and time >= 10
delete from root.ln.wf02.wt02.status where time > 20
delete from root.ln.wf02.wt02.status where time >= 20
delete from root.ln.wf02.wt02.status where time = 20
```

需要注意，当前的删除语句不支持 where 子句后的时间范围为多个由 OR 连接成的时间区间。如下删除语句将会解析出错：
```
delete from root.ln.wf02.wt02.status where time > 4 or time < 0
Msg: 303: Check metadata error: For delete statement, where clause can only contain atomic
expressions like : time > XXX, time <= XXX, or two atomic expressions connected by 'AND'
```

如果 delete 语句中未指定 where 子句，则会删除时间序列中的所有数据。
```sql
delete from root.ln.wf02.wt02.status
```

## 多传感器时间序列值删除    

当 ln 集团 wf02 子站的 wt02 设备在 2017-11-01 16:26:00 之前的供电状态和设备硬件版本都需要删除，此时可以使用含义更广的 [路径模式（Path Pattern）](../Basic-Concept/Data-Model-and-Terminology.md) 进行删除操作，进行此操作的 SQL 语句为：


```sql
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```

需要注意的是，当删除的路径不存在时，IoTDB 不会提示路径不存在，而是显示执行成功，因为 SQL 是一种声明式的编程方式，除非是语法错误、权限不足等，否则都不认为是错误，如下所示。

```sql
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
Msg: The statement is executed successfully.
```

## 删除时间分区 （实验性功能）
您可以通过如下语句来删除某一个 database 下的指定时间分区：

```sql
DELETE PARTITION root.ln 0,1,2
```

上例中的 0,1,2 为待删除时间分区的 id，您可以通过查看 IoTDB 的数据文件夹找到它，或者可以通过计算`timestamp / partitionInterval`（向下取整）,
手动地将一个时间戳转换为对应的 id，其中的`partitionInterval`可以在 IoTDB 的配置文件中找到（如果您使用的版本支持时间分区）。

请注意该功能目前只是实验性的，如果您不是开发者，使用时请务必谨慎。

## 数据存活时间（TTL）

IoTDB 支持对 database 级别设置数据存活时间（TTL），这使得 IoTDB 可以定期、自动地删除一定时间之前的数据。合理使用 TTL
可以帮助您控制 IoTDB 占用的总磁盘空间以避免出现磁盘写满等异常。并且，随着文件数量的增多，查询性能往往随之下降，
内存占用也会有所提高。及时地删除一些较老的文件有助于使查询性能维持在一个较高的水平和减少内存资源的占用。

TTL的默认单位为毫秒，如果配置文件中的时间精度修改为其他单位，设置ttl时仍然使用毫秒单位。

### 设置 TTL

设置 TTL 的 SQL 语句如下所示：
```
IoTDB> set ttl to root.ln 3600000
```
这个例子表示在`root.ln`数据库中，只有3600000毫秒，即最近一个小时的数据将会保存，旧数据会被移除或不可见。
```
IoTDB> set ttl to root.sgcc.** 3600000
```
支持给某一路径下的 database 设置TTL，这个例子表示`root.sgcc`路径下的所有 database 设置TTL。
```
IoTDB> set ttl to root.** 3600000
```
表示给所有 database 设置TTL。

### 取消 TTL

取消 TTL 的 SQL 语句如下所示：

```
IoTDB> unset ttl to root.ln
```

取消设置 TTL 后， database `root.ln`中所有的数据都会被保存。
```
IoTDB> unset ttl to root.sgcc.**
```

取消设置`root.sgcc`路径下的所有 database 的 TTL 。
```
IoTDB> unset ttl to root.**
```

取消设置所有 database 的 TTL 。

### 显示 TTL

显示 TTL 的 SQL 语句如下所示：

```
IoTDB> SHOW ALL TTL
IoTDB> SHOW TTL ON StorageGroupNames
```

SHOW ALL TTL 这个例子会给出所有 database 的 TTL。
SHOW TTL ON root.ln,root.sgcc,root.DB 这个例子会显示指定的三个 database 的 TTL。
注意：没有设置 TTL 的 database 的 TTL 将显示为 null。

```
IoTDB> show all ttl
+-------------+-------+
|     database|ttl(ms)|
+-------------+-------+
|      root.ln|3600000|
|    root.sgcc|   null|
|      root.DB|3600000|
+-------------+-------+
```