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
[![Main Mac and Linux](https://github.com/apache/iotdb/actions/workflows/main-unix.yml/badge.svg)](https://github.com/apache/iotdb/actions/workflows/main-unix.yml)
[![Main Win](https://github.com/apache/iotdb/actions/workflows/main-win.yml/badge.svg)](https://github.com/apache/iotdb/actions/workflows/main-win.yml)<!--[![coveralls](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)-->
[![GitHub release](https://img.shields.io/github/release/apache/iotdb.svg)](https://github.com/apache/iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb.svg)
![](https://img.shields.io/github/downloads/apache/iotdb/total.svg)
![](https://img.shields.io/badge/platform-win%20%7C%20macos%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8+-blue.svg)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/apache/iotdb.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/iotdb/context:java)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.iotdb/iotdb-parent.svg)](https://central.sonatype.com/artifact/org.apache.iotdb/iotdb-parent)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/apacheiotdb/shared_invite/zt-3sh0ws7xf-JjVayYQt_RSUUHJ~iJOiSA)

# 简介

IoTDB (Internet of Things Database) 是一款[时序数据库管理系统](https://www.timecho.com/archives/48)，可以为用户提供数据收集、存储和分析等服务。IoTDB由于其轻量级架构、高性能和高可用的特性，以及与 Hadoop 和 Spark 生态的无缝集成，满足了工业 IoT 领域中海量数据存储、高吞吐量数据写入和复杂数据查询分析的需求。
更多功能请见[时序数据库IoTDB：功能详解与行业应用](https://www.timecho.com/archives/shi-xu-shu-ju-ku-iotdb-gong-neng-xiang-jie-yu-xing-ye-ying-yong)。

IoTDB 依赖 [TsFile](https://github.com/apache/tsfile) 项目，它是一种专门用于时序数据管理的文件格式. TsFile 仓库的 `iotdb` 分支被用来为 IoTDB master 分支部署 SNAPSHOT 版本.

# 主要特点

IoTDB的主要特点如下:

1. 灵活的部署策略。IoTDB为用户提供了一个在云平台或终端设备上的一键安装工具，以及一个连接云平台和终端上的数据的数据同步工具。
2. 硬件成本低。IoTDB可以达到很高的磁盘存储压缩比。
3. 高效的目录结构。IoTDB支持智能网络设备对复杂时间序列数据结构的高效组织，同类设备对时间序列数据的组织，海量复杂时间序列数据目录的模糊搜索策略。
4. 高吞吐量读写。IoTDB支持数以百万计的低功耗设备的强连接数据访问、高速数据读写，适用于上述智能网络设备和混合设备。
5. 丰富的查询语义。IoTDB支持跨设备和测量的时间序列数据的时间对齐、时间序列字段的计算(频域转换)和时间维度的丰富聚合函数支持。
6. 学习成本非常低。IoTDB支持类似sql的语言、JDBC标准API和易于使用的导入/导出工具。
7. 与先进的开放源码生态系统的无缝集成。IoTDB支持分析生态系统，如Hadoop、Spark和可视化工具(如Grafana)。

有关IoTDB的最新信息，请访问[IoTDB官方网站](https://www.timecho.com/)。

<!-- TOC -->

## 目录

- [IoTDB](#iotdb)
- [简介](#简介)
- [主要特点](#主要特点)
  - [目录](#目录)
- [快速开始](#快速开始)
  - [环境准备](#环境准备)
  - [安装](#安装)
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

这篇简短的指南将带您了解使用IoTDB的基本过程。如需更详细的介绍，请访问我们的网站[用户指南](https://www.timecho.com/docs/zh/UserGuide/latest/QuickStart/QuickStart_timecho.html)。


## 环境准备

要使用IoTDB，您需要:
1. Java >= 1.8 (推荐17，目前 1.8 到 25 已经被验证可用。请确保环变量境路径已正确设置)。
2. 设置 max open files 为 65535，以避免"too many open files"错误。
3. （可选） 将 somaxconn 设置为 65535 以避免系统在高负载时出现 "connection reset" 错误。
    ```
    # Linux
    > sudo sysctl -w net.core.somaxconn=65535
   
    # FreeBSD or Darwin
    > sudo sysctl -w kern.ipc.somaxconn=65535
    ```

## 安装

IoTDB提供了两种安装方法，您可以参考以下建议，选择最适合您的一种:

* 联系天谋商务获取 IoTDB 安装包，该方式提供了预编译的二进制可执行文件，开箱即用，适合快速部署和直接使用。

* 联系天谋商务获取 Docker 镜像，该方式支持容器化部署，适合需要灵活环境配置或快速迁移的场景。

如需更具体的安装方式，请参阅官网[用户指南](https://www.timecho.com/docs/zh/UserGuide/latest/QuickStart/QuickStart_timecho.html)。

## 开始

您可以通过以下步骤来测试安装，如果执行后没有返回错误，安装就完成了。

### 启动 IoTDB

可以通过运行 sbin 文件夹下的 start-standalone 脚本启动 1C1D IoTDB。

```
# Unix/OS X
> sbin/start-standalone.sh

# Windows
> sbin\start-standalone.bat
```

### 使用 IoTDB

#### 使用 Cli 命令行

IoTDB提供了与服务器交互的不同方式，这里我们将介绍使用 Cli 工具插入和查询数据的基本步骤。

安装 IoTDB 后，有一个默认的用户`root`，它的默认密码是`TimechoDB@2021`。用户可以使用这个
默认用户登录 Cli 并使用 IoTDB。Cli 的启动脚本是 sbin 文件夹中的 start-cli 脚本。
在执行脚本时，用户应该指定 IP，端口，USER_NAME 和 密码。默认参数为`-h 127.0.0.1 -p 6667 -u root -pw TimechoDB@2021`。


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


IoTDB中的数据组织为 timeseries。每个 timeseries 包含多个`数据-时间`对，由一个 database 拥有。
在定义 timeseries 之前，我们应该先使用 CREATE DATABASE 来创建一个数据库，下面是一个例子:

```
IoTDB> CREATE DATABASE root.ln
```

我们也可以使用`SHOW DATABASES`来检查已创建的数据库:

```
IoTDB> SHOW DATABASES
+--------+
|Database|
+--------+
| root.ln|
+--------+
Total line number = 1
```

在设置 database 之后，我们可以使用CREATE TIMESERIES来创建一个新的TIMESERIES。
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
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                   timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.temperature| null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|
|     root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 2
```

2. 查询指定的 timeseries(root.ln.wf01.wt01.status):

```
IoTDB> SHOW TIMESERIES root.ln.wf01.wt01.status
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|              timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 1
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
+-----------------------------+------------------------+
|                         Time|root.ln.wf01.wt01.status|
+-----------------------------+------------------------+
|1970-01-01T08:00:00.100+08:00|                    true|
|1970-01-01T08:00:00.200+08:00|                   false|
+-----------------------------+------------------------+
Total line number = 2
```

您还可以使用一条SQL语句查询多个 timeseries 数据:

```
IoTDB> SELECT * FROM root.ln.wf01.wt01
+-----------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+-----------------------------+-----------------------------+------------------------+
|1970-01-01T08:00:00.100+08:00|                         null|                    true|
|1970-01-01T08:00:00.200+08:00|                        20.71|                   false|
+-----------------------------+-----------------------------+------------------------+
Total line number = 2
```

如果需要修改 Cli 中的时区，您可以使用以下语句:

```
IoTDB> SET time_zone=+00:00
Time zone has set to +00:00
IoTDB> SHOW time_zone
Current time zone: Z
```

之后查询结果将会以更新后的新时区显示:

```
IoTDB> SELECT * FROM root.ln.wf01.wt01
+------------------------+-----------------------------+------------------------+
|                    Time|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+------------------------+-----------------------------+------------------------+
|1970-01-01T00:00:00.100Z|                         null|                    true|
|1970-01-01T00:00:00.200Z|                        20.71|                   false|
+------------------------+-----------------------------+------------------------+
Total line number = 2
```

你可以使用如下命令退出:

```
IoTDB> quit
or
IoTDB> exit
```

有关IoTDB SQL支持的命令的更多信息，请参见[用户指南](https://iotdb.apache.org/zh/UserGuide/latest/QuickStart/QuickStart.html)。

### 停止 IoTDB

server 可以使用 "ctrl-C" 或者执行下面的脚本:

```
# Unix/OS X
> sbin/stop-standalone.sh

# Windows
> sbin\stop-standalone.bat
```

# 数据导入导出工具

查看 [导入导出CSV工具](https://www.timecho.com/docs/zh/UserGuide/latest/Tools-System/Data-Import-Tool.html)

# 常见问题

查看 [常见问题](https://www.timecho.com/docs/zh/UserGuide/latest/FAQ/Frequently-asked-questions.html)


# 联系我们

### Wechat Group

* 请添加好友 `tietouqiao`