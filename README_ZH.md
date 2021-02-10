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
[English](./README.md) | [中文](./README_ZH.md)

# IoTDB
[![Build Status](https://www.travis-ci.org/apache/iotdb.svg?branch=master)](https://www.travis-ci.org/apache/iotdb)
[![coveralls](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)
[![GitHub release](https://img.shields.io/github/release/apache/iotdb.svg)](https://github.com/apache/iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb.svg)
![](https://img.shields.io/github/downloads/apache/iotdb/total.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macox%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/apache/iotdb.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/iotdb/context:java)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.iotdb/iotdb-parent/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.iotdb")

# 简介
IoTDB (Internet of Things Database) 是一个时序数据的数据管理系统，可以为用户提供数据收集、存储和分析等特定的服务。IoTDB由于其轻量级的结构、高性能和可用的特性，以及与Hadoop和Spark生态的无缝集成，满足了工业IoTDB领域中海量数据存储、高吞吐量数据写入和复杂数据分析的需求。

# 主要特点

IoTDB的主要特点如下:

1. 灵活的部署策略。IoTDB为用户提供了一个在云平台或终端设备上的一键安装工具，以及一个连接云平台和终端上的数据的数据同步工具。
2. 硬件成本低。IoTDB可以达到很高的磁盘存储压缩比。
3. 高效的目录结构。IoTDB支持智能网络设备对复杂时间序列数据结构的高效组织，同类设备对时间序列数据的组织，海量复杂时间序列数据目录的模糊搜索策略。
4. 高吞吐量读写。IoTDB支持数以百万计的低功耗设备的强连接数据访问、高速数据读写，适用于上述智能网络设备和混合设备。
5. 丰富的查询语义。IoTDB支持跨设备和测量的时间序列数据的时间对齐、时间序列字段的计算(频域转换)和时间维度的丰富聚合函数支持。
6. 学习成本非常低。IoTDB支持类似sql的语言、JDBC标准API和易于使用的导入/导出工具。
7. 与先进的开放源码生态系统的无缝集成。IoTDB支持分析生态系统，如Hadoop、Spark和可视化工具(如Grafana)。

有关IoTDB的最新信息，请访问[IoTDB官方网站](https://iotdb.apache.org/)。如果您在使用IoTDB时遇到任何问题或发现任何bug，请在[jira]中提交(https://issues.apache.org/jira/projects/IOTDB/issues)。

<!-- TOC -->

## 目录

- [IoTDB](#iotdb)
- [简介](#简介)
- [主要特点](#主要特点)
  - [目录](#目录)
- [快速开始](#快速开始)
  - [环境准备](#环境准备)
  - [安装](#安装)
    - [从源码构建](#从源码构建)
      - [配置](#配置)
  - [开始](#开始)
    - [启动 IoTDB](#启动-iotdb)
    - [使用 IoTDB](#使用-iotdb)
      - [使用 Cli 命令行](#使用-cli-命令行)
      - [基本的 IoTDB 命令](#基本的-iotdb-命令)
    - [停止 IoTDB](#停止-iotdb)
  - [只编译 server](#只编译-server)
  - [只编译 cli](#只编译-cli)
  - [导入导出CSV工具](#导入导出CSV工具)

<!-- /TOC -->

# 快速开始

这篇简短的指南将带您了解使用IoTDB的基本过程。如需更详细的介绍，请访问我们的网站[用户指南](http://iotdb.apache.org/zh/UserGuide/Master/Get%20Started/QuickStart.html)。

## 环境准备

要使用IoTDB，您需要:
1. Java >= 1.8 (目前 1.8、11和13 已经被验证可用。请确保环变量境路径已正确设置)。
2. Maven >= 3.6 (如果希望从源代码编译和安装IoTDB)。
3. 设置 max open files 为 65535，以避免"too many open files"错误。

## 安装

IoTDB提供了三种安装方法，您可以参考以下建议，选择最适合您的一种:

* 从源代码安装。如果需要自己修改代码，可以使用此方法。

* 从二进制文件安装。推荐的方法是从官方网站下载二进制文件，您将获得一个开箱即用的二进制发布包。

* 使用Docker: dockerfile的路径是https://github.com/apache/iotdb/tree/master/docker/src/main

在这篇《快速入门》中，我们简要介绍如何使用源代码安装IoTDB。如需进一步资料，请参阅《用户指南》第3章。

## 从源码构建

如果您使用Windows，请跳过此段。我们使用Thrift作为RPC模块来提供客户端-服务器间的通信和协议支持，因此在编译阶段我们需要使用Thrift 0.13.0
（或更高）编译器生成对应的Java代码。Thrift只提供了Windows下的二进制编译器，Unix下需要通过源码自行编译。但我们预先编译了一个Thrift编译器，并将其上传到了GitHub
，借助一个Maven插件，在编译时可以自动将其下载。该预编译的Thrift编译器在gcc8，Ubuntu, CentOS, MacOS下可以工作，但是在更低的gcc
版本以及其他操作系统上尚未确认。如果您在编译时发现了Thrift编译器相关的问题，请升级您的gcc版本或者依照Thrift
官方的指示自行编译编译器，并将编译器放置到目录`{project_root}\thrift\target\tools\thrift_0.12.0_0.13.0_linux.exe`。
如果您已经安装了一个兼容的Thrift编译器，您可以在运行Maven时通过以下参数指定使用您的编译器：`-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<YOUR LOCAL THRIFT BINARY FILE>`。
您也可以使用以下Maven参数来更换Thrift编译器的下载地址：`-Dthrift.download-url=<THE REMOTE URL FOR DOWNLOADING> -Dthrift.exec.absolute.path=<THE DOWNLOADED BINARY FILE NAME>`。
如果您对Maven足够熟悉，您也可以直接修改我们的根pom文件来避免每次编译都使用上述参数。
Thrift官方网址为：https://thrift.apache.org/

从 git 克隆源代码:

```
git clone https://github.com/apache/iotdb.git
```

默认的主分支是dev分支，如果你想使用某个发布版本x.x.x，请切换分支:

```
git checkout release/x.x.x
```

在 iotdb 根目录下执行 maven 编译:

```
> mvn clean package -DskipTests
```

执行完成之后，可以在**distribution/target/apache-iotdb-{project.version}-bin.zip**找到编译完成的二进制版本(包括服务器和客户端)

> 注意:"thrift/target/generated-sources/thrift" 和 "antlr/target/generated-sources/antlr4" 目录需要添加到源代码根中，以免在 IDE 中产生编译错误。

### 配置

配置文件在"conf"文件夹下
* 环境配置模块(`iotdb-env.bat`, `iotdb-env.sh`),
* 系统配置模块(`iotdb-engine.properties`)
* 日志配置模块(`logback.xml`)。

有关详细信息，请参见[Chapter3: Server](http://iotdb.apache.org/zh/UserGuide/Master/Server/Config%20Manual.html)。

## 开始

您可以通过以下步骤来测试安装，如果执行后没有返回错误，安装就完成了。

### 启动 IoTDB

可以通过运行 sbin 文件夹下的 start-server 脚本启动 IoTDB。

```
# Unix/OS X
> nohup sbin/start-server.sh >/dev/null 2>&1 &
or
> nohup sbin/start-server.sh -c <conf_path> -rpc_port <rpc_port> >/dev/null 2>&1 &

# Windows
> sbin\start-server.bat -c <conf_path> -rpc_port <rpc_port>
```
- "-c" and "-rpc_port" 都是可选的。
- 选项 "-c" 指定了配置文件所在的文件夹。
- 选项 "-rpc_port" 指定了启动的 rpc port。
- 如果两个选项同时指定，那么*rpc_port*将会覆盖*conf_path*下面的配置。

### 使用 IoTDB

#### 使用 Cli 命令行

IoTDB提供了与服务器交互的不同方式，这里我们将介绍使用 Cli 工具插入和查询数据的基本步骤。

安装 IoTDB 后，有一个默认的用户`root`，它的默认密码也是`root`。用户可以使用这个
默认用户登录 Cli 并使用 IoTDB。Cli 的启动脚本是 sbin 文件夹中的 start-cli 脚本。
在执行脚本时，用户应该指定 IP，端口，USER_NAME 和 密码。默认参数为`-h 127.0.0.1 -p 6667 -u root -pw root`。


下面是启动 Cli 的命令:

```
# Unix/OS X
> sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root

# Windows
> sbin\start-cli.bat -h 127.0.0.1 -p 6667 -u root -pw root
```

命令行客户端是交互式的，所以如果一切就绪，您应该看到欢迎标志和声明:

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

#### 基本的 IoTDB 命令

现在，让我们介绍创建 timeseries、插入数据和查询数据的方法。


IoTDB中的数据组织为 timeseries。每个 timeseries 包含多个`数据-时间`对，由一个存储组拥有。
在定义 timeseries 之前，我们应该先使用SET storage group来定义一个存储组，下面是一个例子:

```
IoTDB> SET STORAGE GROUP TO root.ln
```

我们也可以使用`SHOW STORAGE GROUP`来检查正在创建的存储组:

```
IoTDB> SHOW STORAGE GROUP
+-----------------------------------+
|                      Storage Group|
+-----------------------------------+
|                            root.ln|
+-----------------------------------+
storage group number = 1
```

在设置存储组之后，我们可以使用CREATE TIMESERIES来创建一个新的TIMESERIES。
在创建 timeseries 时，我们应该定义它的数据类型和编码方案。这里我们创建两个 timeseries:


```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

为了查询特定的timeseries，我们可以使用 `SHOW TIMESERIES <Path>`. <Path> 表示被查询的 timeseries 的路径. 默认值是`null`, 表示查询系统中所有的 timeseries (同`SHOW TIMESERIES root`). 
以下是一些示例:

1. 查询系统中所有 timeseries:

```
IoTDB> SHOW TIMESERIES
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
|                   timeseries  | alias|storage group|dataType|encoding|compression|tags|attributes|
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
|       root.ln.wf01.wt01.status|  null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
|  root.ln.wf01.wt01.temperature|  null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
Total timeseries number = 2
```

2. 查询指定的 timeseries(root.ln.wf01.wt01.status):

```
IoTDB> SHOW TIMESERIES root.ln.wf01.wt01.status
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
|                   timeseries  | alias|storage group|dataType|encoding|compression|tags|attributes|
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
|       root.ln.wf01.wt01.status|  null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
Total timeseries number = 1
```

插入 timeseries 数据是IoTDB的一个基本操作，你可以使用`INSERT` 命令来完成这个操作。
在插入之前，您应该指定时间戳和后缀路径名:

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

你刚才插入的数据会显示如下:

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

您还可以使用一条SQL语句查询多个 timeseries 数据:

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

你可以使用如下命令退出:

```
IoTDB> quit
or
IoTDB> exit
```

有关IoTDB SQL支持的命令的更多信息，请参见[SQL 参考文档](http://iotdb.apache.org/zh/UserGuide/Master/Operation%20Manual/SQL%20Reference.html)。

### 停止 IoTDB

server 可以使用 "ctrl-C" 或者执行下面的脚本:

```
# Unix/OS X
> sbin/stop-server.sh

# Windows
> sbin\stop-server.bat
```

## 只编译 server

在 iotdb 根目录下执行:

```
> mvn clean package -pl server -am -DskipTests
```

编译完成后, IoTDB server 将生成在: "server/target/iotdb-server-{project.version}".


## 只编译 cli

在 iotdb 根目录下执行:

```
> mvn clean package -pl cli -am -DskipTests
```

编译完成后, IoTDB cli 将生成在 "cli/target/iotdb-cli-{project.version}".

# 导入导出CSV工具

查看 [导入导出CSV工具](https://iotdb.apache.org/zh/UserGuide/Master/System%20Tools/CSV%20Tool.html)

# 常见编译错误
查看 [常见编译错误](https://iotdb.apache.org/zh/Development/ContributeGuide.html#%E5%B8%B8%E8%A7%81%E7%BC%96%E8%AF%91%E9%94%99%E8%AF%AF)

# 联系我们
### QQ群

* Apache IoTDB 交流群：659990460

### Wechat Group

* 添加好友 tietouqiao，我们会邀请您进群

获取更多内容，请查看 [加入社区](https://github.com/apache/iotdb/issues/1995) 
