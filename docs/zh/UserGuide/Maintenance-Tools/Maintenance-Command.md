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
# 运维命令

## FLUSH

将指定 database 的内存缓存区 Memory Table 的数据持久化到磁盘上，并将数据文件封口。在集群模式下，我们提供了持久化本节点的指定 database 的缓存、持久化整个集群指定 database 的缓存命令。

注意：此命令客户端不需要手动调用，IoTDB 有 wal 保证数据安全，IoTDB 会选择合适的时机进行 flush。
如果频繁调用 flush 会导致数据文件很小，降低查询性能。

```sql
IoTDB> FLUSH 
IoTDB> FLUSH ON LOCAL
IoTDB> FLUSH ON CLUSTER
IoTDB> FLUSH root.ln
IoTDB> FLUSH root.sg1,root.sg2 ON LOCAL
IoTDB> FLUSH root.sg1,root.sg2 ON CLUSTER
```

## CLEAR CACHE


手动清除chunk, chunk metadata和timeseries metadata的缓存，在内存资源紧张时，可以通过此命令，释放查询时缓存所占的内存空间。在集群模式下，我们提供了清空本节点缓存、清空整个集群缓存命令。

```sql
IoTDB> CLEAR CACHE
IoTDB> CLEAR CACHE ON LOCAL
IoTDB> CLEAR CACHE ON CLUSTER
```


## SET SYSTEM TO READONLY / RUNNING

手动设置系统为正常运行、只读状态。在集群模式下，我们提供了设置本节点状态、设置整个集群状态的命令，默认对整个集群生效。

```sql
IoTDB> SET SYSTEM TO RUNNING
IoTDB> SET SYSTEM TO READONLY ON LOCAL
IoTDB> SET SYSTEM TO READONLY ON CLUSTER
```

## 终止查询

IoTDB 支持设置 Session 连接超时和查询超时时间，并支持手动终止正在执行的查询。

### Session 超时

Session 超时控制何时关闭空闲 Session。空闲 Session 指在一段时间内没有发起任何操作的 Session。

Session 超时默认未开启。可以在配置文件中通过 `dn_session_timeout_threshold` 参数进行配置。

### 查询超时

对于执行时间过长的查询，IoTDB 将强行中断该查询，并抛出超时异常，如下所示：

```sql
IoTDB> select * from root.**;
Msg: 701 Current query is time out, please check your statement or modify timeout parameter.
```

系统默认的超时时间为 60000 ms，可以在配置文件中通过 `query_timeout_threshold` 参数进行自定义配置。

如果您使用 JDBC 或 Session，还支持对单个查询设置超时时间（单位为 ms）：

```java
((IoTDBStatement) statement).executeQuery(String sql, long timeoutInMS)
session.executeQueryStatement(String sql, long timeout)
```

> 如果不配置超时时间参数或将超时时间设置为负数，将使用服务器端默认的超时时间。 
> 如果超时时间设置为0，则会禁用超时功能。

### 查询终止

尚未支持
