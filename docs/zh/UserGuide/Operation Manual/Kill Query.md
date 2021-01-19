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

# 中止查询

当使用 IoTDB 时，您可能会遇到以下情形：输入了一个查询，但是由于其包含的数据量过大或是其他原因，导致长时间无法返回结果，但是迫于生产环境无法中止该命令，只能被迫等待。

从 0.12 版本开始，IoTDB 对执行时间过长的查询给出了两种解决方案：查询超时和查询中止。

## 查询超时

对于执行时间过长的查询，IoTDB 将强行中断该查询，并抛出超时异常，如图所示：

![image](https://user-images.githubusercontent.com/34242296/104586593-a224aa00-56a0-11eb-9c52-241dcdb68ecb.png)

系统默认的超时时间为 60000 ms，可以在配置文件中通过 `query_time_threshold` 参数进行自定义配置。

如果您使用 JDBC 或 Session，还支持对单个查询设置超时时间（单位为 ms）：

```
E.g. ((IoTDBStatement) statement).executeQuery(String sql, long timeoutInMS)
E.g. session.executeQueryStatement(String sql, long timeout)
```

如果不配置超时时间参数或将超时时间设置为 0，将使用服务器端默认的超时时间.

## 查询中止

除了被动地等待查询超时外，IoTDB 还支持主动地中止查询，命令为：

```
KILL QUERY <queryId>
```

通过指定 `queryId` 可以中止指定的查询，而如果不指定 `queryId`，将中止所有正在执行的查询。

为了获取正在执行的查询 id，用户可以使用 `show query processlist` 命令，该命令将显示所有正在执行的查询列表，结果形式如下：

| Time | queryId | statement |
| ---- | ------- | --------- |
|      |         |           |

其中 statement 最大显示长度为 64 字符。对于超过 64 字符的查询语句，将截取部分进行显示。