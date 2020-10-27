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

# 快速入门

## 概览 

- 快速入门
- 安装环境
- IoTDB安装
  - 从源代码生成
    - 配置文件	
- IoTDB试用
  - 启动IoTDB
  - 操作IoTDB
    - 使用Cli工具
    - IoTDB的基本操作
  - 停止IoTDB
- 单独打包服务器
- 单独打包客户端

<!-- /TOC -->

本文将介绍关于IoTDB使用的基本流程，如果需要更多信息，请浏览我们官网的[指引](../Overview/What%20is%20IoTDB.md).

## 安装环境

安装前需要保证设备上配有JDK>=1.8的运行环境，并配置好JAVA_HOME环境变量。

设置最大文件打开数为65535。

## IoTDB安装

IoTDB支持多种安装途径。用户可以使用三种方式对IoTDB进行安装——下载二进制可运行程序、使用源码、使用docker镜像。

* 使用源码：您可以从代码仓库下载源码并编译，具体编译方法见下方。

* 二进制可运行程序：请从Download页面下载最新的安装包，解压后即完成安装。

* 使用Docker镜像：dockerfile 文件位于 https://github.com/apache/iotdb/blob/master/docker/src/main

### IoTDB下载

您可以从这里下载程序：[下载](/Download/)

### 配置文件

配置文件在"conf"文件夹下，包括：

  * 环境配置模块 (`iotdb-env.bat`, `iotdb-env.sh`), 
  * 系统配置模块 (`iotdb-engine.properties`)
  * 日志配置模块 (`logback.xml`). 

想要了解更多，请浏览[Chapter3: Server](../Server/Download.md)
​	

## IoTDB试用

用户可以根据以下操作对IoTDB进行简单的试用，若以下操作均无误，则说明IoTDB安装成功。


### 启动IoTDB

用户可以使用sbin文件夹下的start-server脚本启动IoTDB。

Linux系统与MacOS系统启动命令如下：

```
> nohup sbin/start-server.sh >/dev/null 2>&1 &
or
> nohup sbin/start-server.sh -c <conf_path> -rpc_port <rpc_port> >/dev/null 2>&1 &
```

Windows系统启动命令如下：

```
> sbin\start-server.bat -c <conf_path> -rpc_port <rpc_port>
```
- "-c" and "-rpc_port" 都是可选的。
- 选项 "-c" 指定了配置文件所在的文件夹。
- 选项 "-rpc_port" 指定了启动的 rpc port。
- 如果两个选项同时指定，那么*rpc_port*将会覆盖*conf_path*下面的配置。


### 操作IoTDB

#### 使用Cli工具

IoTDB为用户提供多种与服务器交互的方式，在此我们介绍使用Cli工具进行写入、查询数据的基本步骤。

初始安装后的IoTDB中有一个默认用户：root，默认密码为root。用户可以使用该用户运行Cli工具操作IoTDB。Cli工具启动脚本为sbin文件夹下的start-cli脚本。启动脚本时需要指定运行ip、port、username和password。若脚本未给定对应参数,则默认参数为"-h 127.0.0.1 -p 6667 -u root -pw -root"

以下启动语句为服务器在本机运行，且用户未更改运行端口号的示例。

Linux系统与MacOS系统启动命令如下：

```
> sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root
```

Windows系统启动命令如下：

```
> sbin\start-cli.bat -h 127.0.0.1 -p 6667 -u root -pw root
```

启动后出现如图提示即为启动成功。

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

#### IoTDB的基本操作

在这里，我们首先介绍一下使用Cli工具创建时间序列、插入数据并查看数据的方法。

数据在IoTDB中的组织形式是以时间序列为单位，每一个时间序列中有若干个数据-时间点对，每一个时间序列属于一个存储组。在定义时间序列之前，要首先使用SET STORAGE GROUP语句定义存储组。SQL语句如下：

``` 
IoTDB> SET STORAGE GROUP TO root.ln
```

我们可以使用SHOW STORAGE GROUP语句来查看系统当前所有的存储组，SQL语句如下：

```
IoTDB> SHOW STORAGE GROUP
```

执行结果为：

```
+-----------------------------------+
|                      Storage Group|
+-----------------------------------+
|                            root.ln|
+-----------------------------------+
storage group number = 1
```

存储组设定后，使用CREATE TIMESERIES语句可以创建新的时间序列，创建时间序列时需要定义数据的类型和编码方式。此处我们创建两个时间序列，SQL语句如下：

```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

为了查看指定的时间序列，我们可以使用SHOW TIMESERIES \<Path\>语句，其中\<Path\>表示时间序列对应的路径，默认值为空，表示查看系统中所有的时间序列。下面是两个例子：

使用SHOW TIMESERIES语句查看系统中存在的所有时间序列，SQL语句如下：

``` 
IoTDB> SHOW TIMESERIES
```

执行结果为：

```
+-------------------------------+---------------+--------+--------+
|                     Timeseries|  Storage Group|DataType|Encoding|
+-------------------------------+---------------+--------+--------+
|       root.ln.wf01.wt01.status|        root.ln| BOOLEAN|   PLAIN|
|  root.ln.wf01.wt01.temperature|        root.ln|   FLOAT|     RLE|
+-------------------------------+---------------+--------+--------+
Total timeseries number = 2
```

查看具体的时间序列root.ln.wf01.wt01.status的SQL语句如下：

```
IoTDB> SHOW TIMESERIES root.ln.wf01.wt01.status
```

执行结果为：

```
+------------------------------+--------------+--------+--------+
|                    Timeseries| Storage Group|DataType|Encoding|
+------------------------------+--------------+--------+--------+
|      root.ln.wf01.wt01.status|       root.ln| BOOLEAN|   PLAIN|
+------------------------------+--------------+--------+--------+
Total timeseries number = 1
```


接下来，我们使用INSERT语句向root.ln.wf01.wt01.status时间序列中插入数据，在插入数据时需要首先指定时间戳和路径后缀名称：

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
```

我们也可以向多个时间序列中同时插入数据，这些时间序列同属于一个时间戳：

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

最后，我们查询之前插入的数据。使用SELECT语句我们可以查询指定的时间序列的数据结果，SQL语句如下：

```
IoTDB> SELECT status FROM root.ln.wf01.wt01
```

查询结果如下：

```
+-----------------------+------------------------+
|                   Time|root.ln.wf01.wt01.status|
+-----------------------+------------------------+
|1970-01-01T08:00:00.100|                    true|
|1970-01-01T08:00:00.200|                   false|
+-----------------------+------------------------+
Total line number = 2
```

我们也可以查询多个时间序列的数据结果，SQL语句如下：

```
IoTDB> SELECT * FROM root.ln.wf01.wt01
```

查询结果如下：

```
+-----------------------+--------------------------+-----------------------------+
|                   Time|  root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------+--------------------------+-----------------------------+
|1970-01-01T08:00:00.100|                      true|                         null|
|1970-01-01T08:00:00.200|                     false|                        20.71|
+-----------------------+--------------------------+-----------------------------+
Total line number = 2
```

输入quit或exit可退出Cli结束本次会话。

```
IoTDB> quit
```
或

```
IoTDB> exit
```

想要浏览更多IoTDB数据库支持的命令，请浏览[SQL Reference](../Operation%20Manual/SQL%20Reference.md).

### 停止IoTDB

用户可以使用$IOTDB_HOME/sbin文件夹下的stop-server脚本停止IoTDB。

Linux系统与MacOS系统停止命令如下：

```
> $sbin/stop-server.sh
```

Windows系统停止命令如下：

```
> $sbin\stop-server.bat
```

## 只建立客户端

在iotdb的根路径下：

```
> mvn clean package -pl cli -am -DskipTests
```

构建后，IoTDB客户端将位于文件夹“ cli / target / iotdb-cli- {project.version}”下。
