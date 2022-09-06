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

<!-- TOC -->

## 常见问题

**如何查询我的 IoTDB 版本**

有几种方法可以识别您使用的 IoTDB 版本：

* 启动 IoTDB 的命令行界面：

```
> ./start-cli.sh -p 6667 -pw root -u root -h localhost
 _____       _________  ______   ______    
|_   _|     |  _   _  ||_   _ `.|_   _ \   
  | |   .--.|_/ | | \_|  | | `. \ | |_) |  
  | | / .'`\ \  | |      | |  | | |  __'.  
 _| |_| \__. | _| |_    _| |_.' /_| |__) | 
|_____|'.__.' |_____|  |______.'|_______/  version x.x.x
```

* 检查 pom.xml 文件：

```
<version>x.x.x</version>
```

* 使用 JDBC API:

```
String iotdbVersion = tsfileDatabaseMetadata.getDatabaseProductVersion();
```

* 使用命令行接口：

```
IoTDB> show version
show version
+---------------+
|version        |
+---------------+
|x.x.x          |
+---------------+
Total line number = 1
It costs 0.241s
```

**在哪里可以找到 IoTDB 的日志**

假设您的根目录是：

```shell
$ pwd
/workspace/iotdb

$ ls -l
server/
cli/
pom.xml
Readme.md
...
```

假如 `$IOTDB_HOME = /workspace/iotdb/server/target/iotdb-server-{project.version}`

假如 `$IOTDB_CLI_HOME = /workspace/iotdb/cli/target/iotdb-cli-{project.version}`

在默认的设置里，logs 文件夹会被存储在```IOTDB_HOME/logs```。您可以在```IOTDB_HOME/conf```目录下的```logback.xml```文件中修改日志的级别和日志的存储路径。

**在哪里可以找到 IoTDB 的数据文件**

在默认的设置里，数据文件（包含 TsFile，metadata，WAL）被存储在```IOTDB_HOME/data```文件夹。

**如何知道 IoTDB 中存储了多少时间序列**

使用 IoTDB 的命令行接口：

```
IoTDB> show timeseries root
```

在返回的结果里，会展示`Total timeseries number`，这个数据就是 IoTDB 中 timeseries 的数量。

在当前版本中，IoTDB 支持直接使用命令行接口查询时间序列的数量：

```
IoTDB> count timeseries root
```

如果您使用的是 Linux 操作系统，您可以使用以下的 Shell 命令：

```
> grep "0,root" $IOTDB_HOME/data/system/schema/mlog.txt |  wc -l
>   6
```

**可以使用 Hadoop 和 Spark 读取 IoTDB 中的 TsFile 吗？**

是的。IoTDB 与开源生态紧密结合。IoTDB 支持 [Hadoop](https://github.com/apache/iotdb/tree/master/hadoop), [Spark](https://github.com/apache/iotdb/tree/master/spark) 和 [Grafana](https://github.com/apache/iotdb/tree/master/grafana) 可视化工具。

**IoTDB 如何处理重复的数据点**

一个数据点是由一个完整的时间序列路径（例如：```root.vehicle.d0.s0```) 和时间戳唯一标识的。如果您使用与现有点相同的路径和时间戳提交一个新点，那么 IoTDB 将更新这个点的值，而不是插入一个新点。 

**我如何知道具体的 timeseries 的类型**

在 IoTDB 的命令行接口中使用 SQL ```SHOW TIMESERIES <timeseries path>```:

例如：如果您想知道所有 timeseries 的类型 \<timeseries path> 应该为 `root`。上面的 SQL 应该修改为：

```
IoTDB> show timeseries root
```

如果您想查询一个指定的时间序列，您可以修改 \<timeseries path> 为时间序列的完整路径。比如：

```
IoTDB> show timeseries root.fit.d1.s1
```

您还可以在 timeseries 路径中使用通配符：

```
IoTDB> show timeseries root.fit.d1.*
```

**如何更改 IoTDB 的客户端时间显示格式**

IoTDB 客户端默认显示的时间是人类可读的（比如：```1970-01-01T08:00:00.001```)，如果您想显示是时间戳或者其他可读格式，请在启动命令上添加参数```-disableISO8601```:

```
> $IOTDB_CLI_HOME/sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root -disableISO8601
```
