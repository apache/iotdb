<!--

```
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
```

-->

# 经常问的问题

## 如何识别我的IoTDB版本？

有几种方法可以识别您正在使用的IoTDB版本：

* 启动IoTDB的命令行界面：

```
> ./start-client.sh -p 6667 -pw root -u root -h localhost
 _____       _________  ______   ______    
|_   _|     |  _   _  ||_   _ `.|_   _ \   
  | |   .--.|_/ | | \_|  | | `. \ | |_) |  
  | | / .'`\ \  | |      | |  | | |  __'.  
 _| |_| \__. | _| |_    _| |_.' /_| |__) | 
|_____|'.__.' |_____|  |______.'|_______/  version x.x.x
```

* 检查pom.xml文件：

```
<version>x.x.x</version>
```

* 使用JDBC API：

```
String iotdbVersion = tsfileDatabaseMetadata.getDatabaseProductVersion();
```

* 使用命令行界面：

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

## 在哪里可以找到IoTDB日志？

假设您的根目录是：

```
$ pwd
/workspace/incubator-iotdb

$ ls -l
server/
client/
pom.xml
Readme.md
...
```

Let `$IOTDB_HOME = /workspace/incubator-iotdb/server/target/iotdb-server-{project.version}`

Let `$IOTDB_CLI_HOME = /workspace/incubator-iotdb/client/target/iotdb-client-{project.version}`

默认情况下，日志存储在IOTDB_HOME / logs下。 您可以通过在IOTDB_HOME / conf下配置logback.xml来更改日志级别和存储路径。

## 在哪里可以找到IoTDB数据文件？

默认设置下，数据文件（包括tsfile，元数据和WAL文件）存储在IOTDB_HOME / data下。

## 我如何知道IoTDB中存储了多少时间序列？

使用IoTDB的命令行界面：

```
IoTDB> show timeseries root
```

结果,将有一个声明显示 `Total timeseries number`, 此数字是IoTDB中的时间序列号。

在当前版本中，IoTDB支持查询时间序列数。 使用IoTDB的命令行界面：

```
IoTDB> count timeseries root
```

如果使用Linux，则可以使用以下shell命令：

```
> grep "0,root" $IOTDB_HOME/data/system/schema/mlog.txt |  wc -l
>   6
```

## 我可以使用Hadoop和Spark在IoTDB中读取TsFile吗？

是的。  IoTDB与开源生态系统紧密集成。IoTDB支持 [Hadoop](https://github.com/apache/incubator-iotdb/tree/master/hadoop), [Spark](https://github.com/apache/incubator-iotdb/tree/master/spark) 和[Grafana](https://github.com/apache/incubator-iotdb/tree/master/grafana) 可视化工具。

## IoTDB如何处理重复点？

数据点由完整的时间序列路径（例如```root.vehicle.d0.s0```）和时间戳唯一标识。 如果您提交的新点的路径和时间戳与现有点相同，则IoTDB将更新该点的值，而不是插入新点。

## 我如何知道特定时间序列的类型？

采用```SHOW TIMESERIES <timeseries path>``` IoTDB的命令行界面中的SQL：

例如，如果您想知道所有时间序列的类型，则\ <时间序列路径>应该是“ root”。 该语句将是：

```
IoTDB> show timeseries root
```

如果要查询特定的传感器，可以将```<timeseries path>```替换为传感器名称。 例如：

```
IoTDB> show timeseries root.fit.d1.s1
```

否则，您也可以在时间序列路径中使用通配符：

```
IoTDB> show timeseries root.fit.d1.*
```

## 如何更改IoTDB的客户端时间显示格式？

IoTDB的默认客户端时间显示格式是可读的 (例如 ```1970-01-01T08:00:00.001```), 如果要以时间戳记类型或其他可读格式显示时间，请添加参数 ```-disableIS08601``` 在启动命令中：

```
> $IOTDB_CLI_HOME/sbin/start-client.sh -h 127.0.0.1 -p 6667 -u root -pw root -disableIS08601
```

