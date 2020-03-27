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

# 快速开始

本简短指南将引导您完成使用IoTDB的基本过程。 有关更完整的指南，请访问我们的网站 [User Guide](/zh/document/V0.8.x/UserGuide/1-Overview/1-What%20is%20IoTDB.html).

## 先决条件

要使用IoTDB，您需要：

1. Java> = 1.8（请确保已设置环境路径）
2. 将最大打开文件数设置为65535，以避免出现“打开文件过多”的问题。

## 安装

IoTDB为您提供了三种安装方法，您可以参考以下建议，选择其中一种：

- 从源代码安装。 如果需要自己修改代码，则可以使用此方法。
- 从二进制文件安装。 从官方网站下载二进制文件。 这是推荐的方法，在此方法中，您将获得开箱即用的二进制发行包。（即将推出...）
- 使用Docker：dockerfile的路径为https://github.com/apache/incubator-iotdb/blob/master/docker/Dockerfile

在快速入门中，我们对安装IoTDB进行了简要介绍。 有关更多信息，请参阅《用户指南》第3章。

## 下载

您可以从以下位置下载二进制文件：
[这里](/download/)

## 构型

配置文件位于“ conf”文件夹下

- 环境配置模块 (`iotdb-env.bat`, `iotdb-env.sh`), 
- 系统配置模块 (`tsfile-format.properties`, `iotdb-engine.properties`)
- 日志配置模块 (`logback.xml`). 

有关更多信息，请参见 [Chapter3: Server](/zh/document/V0.8.x/UserGuide/3-Operation%20Manual/1-Sample%20Data.html) 

## 开始

您可以按照以下步骤测试安装，如果执行后没有错误，则说明安装已完成。

### 开始 IoTDB

用户可以通过sbin文件夹下的启动服务器脚本启动IoTDB。

```
# Unix/OS X
> sbin/start-server.sh

# Windows
> sbin\start-server.bat
```

### 使用IoTDB

#### 使用Cli

IoTDB提供了与服务器交互的不同方法，在这里我们介绍使用Cli工具插入和查询数据的基本步骤。

安装IoTDB后，有一个默认用户“ root”，其默认密码也是“ root”。 用户可以使用该默认用户登录Cli以使用IoTDB。  

Cli的启动脚本是文件夹sbin中的启动客户端脚本。 执行脚本时，用户应分配IP，PORT，USER_NAME和PASSWORD。 默认参数是“ -h 127.0.0.1 -p 6667 -u root -pw -root”。

这是启动Cli的命令：

```
# Unix/OS X
> sbin/start-client.sh -h 127.0.0.1 -p 6667 -u root -pw root

# Windows
> sbin\start-client.bat -h 127.0.0.1 -p 6667 -u root -pw root
```

命令行客户端是交互式的，因此，如果一切就绪，您应该会看到欢迎徽标和声明：

```
 _____       _________  ______   ______
|_   _|     |  _   _  ||_   _ `.|_   _ \
  | |   .--.|_/ | | \_|  | | `. \ | |_) |
  | | / .'`\ \  | |      | |  | | |  __'.
 _| |_| \__. | _| |_    _| |_.' /_| |__) |
|_____|'.__.' |_____|  |______.'|_______/  version x.x.x


IoTDB> login successfully
IoTDB>
```

#### IoTDB的基本命令

现在，让我们介绍创建时间序列，插入数据和查询数据的方法。
IoTDB中的数据按时间序列进行组织，在每个时间序列中都有一些数据-时间对，并且每个时间序列均由存储组拥有。 在定义时间序列之前，我们应该使用SET STORAGE GROUP定义存储组，这是一个示例： 

```
IoTDB> SET STORAGE GROUP TO root.ln
```

我们还可以使用SHOW STORAGE GROUP来检查创建的存储组：

```
IoTDB> SHOW STORAGE GROUP
+-----------------------------------+
|                      Storage Group|
+-----------------------------------+
|                            root.ln|
+-----------------------------------+
storage group number = 1
```

设置存储组后，我们可以使用CREATE TIMESERIES创建新的时间序列。 创建时间序列时，应定义其数据类型和编码方案。 我们创建两个时间序列，如下所示：

```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

为了查询特定的时间序列，我们可以使用SHOW TIMESERIES。 代表时间序列的路径。 它的默认值为null，这意味着查询系统中的所有时间序列（与使用“ SHOW TIMESERIES root”相同）。 以下是示例：

1. 查询系统中所有时间序列：

```
IoTDB> SHOW TIMESERIES
+-------------------------------+---------------+--------+--------+
|                     Timeseries|  Storage Group|DataType|Encoding|
+-------------------------------+---------------+--------+--------+
|       root.ln.wf01.wt01.status|        root.ln| BOOLEAN|   PLAIN|
|  root.ln.wf01.wt01.temperature|        root.ln|   FLOAT|     RLE|
+-------------------------------+---------------+--------+--------+
Total timeseries number = 2
```

1. 查询特定时间序列（root.ln.wf01.wt01.status）：

```
IoTDB> SHOW TIMESERIES root.ln.wf01.wt01.status
+------------------------------+--------------+--------+--------+
|                    Timeseries| Storage Group|DataType|Encoding|
+------------------------------+--------------+--------+--------+
|      root.ln.wf01.wt01.status|       root.ln| BOOLEAN|   PLAIN|
+------------------------------+--------------+--------+--------+
Total timeseries number = 1
```

插入时间序列数据是IoTDB的基本操作，您可以使用“ INSERT”命令完成此操作。 在插入之前，您应该分配时间戳记和后缀路径名：

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

我们刚刚插入的数据显示如下：

```
IoTDB> SELECT status FROM root.ln.wf01.wt01
+-----------------------+------------------------+
|                   Time|root.ln.wf01.wt01.status|
+-----------------------+------------------------+
|1970-01-01T08:00:00.100|                    true|
|1970-01-01T08:00:00.200|                   false|
+-----------------------+------------------------+
Total line number = 2
```

我们还可以像这样一次查询多个时间序列数据：

```
IoTDB> SELECT * FROM root.ln.wf01.wt01
+-----------------------+--------------------------+-----------------------------+
|                   Time|  root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------+--------------------------+-----------------------------+
|1970-01-01T08:00:00.100|                      true|                         null|
|1970-01-01T08:00:00.200|                     false|                        20.71|
+-----------------------+--------------------------+-----------------------------+
Total line number = 2
```

退出Cli的命令是：

```
IoTDB> quit
or
IoTDB> exit
```

有关IoTDB SQL支持的命令的更多信息，请参见 [SQL Reference](/zh/document/V0.8.x/UserGuide/5-IoTDB%20SQL%20Documentation/2-Reference.html).

### 停止IoTDB

可以使用ctrl-C或以下脚本停止服务器：

```
# Unix/OS X
> sbin/stop-server.sh

# Windows
> sbin\stop-server.bat
```

## 只建立客户端

在incubator-iotdb的根路径下：

```
> mvn clean package -pl client -am -DskipTests
```

构建后，IoTDB客户端将位于文件夹“ client / target / iotdb-client- {project.version}”下。