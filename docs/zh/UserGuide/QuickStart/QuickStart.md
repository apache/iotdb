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
# 快速上手
## 快速上手

本文将介绍关于 IoTDB 使用的基本流程，如果需要更多信息，请浏览我们官网的 [指引](../IoTDB-Introduction/What-is-IoTDB.md).

### 安装环境

安装前需要保证设备上配有 JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。

设置最大文件打开数为 65535。

### 安装步骤

IoTDB 支持多种安装途径。用户可以使用三种方式对 IoTDB 进行安装——下载二进制可运行程序、使用源码、使用 docker 镜像。

* 使用源码：您可以从代码仓库下载源码并编译，具体编译方法见下方。

* 二进制可运行程序：请从 [下载](https://iotdb.apache.org/Download/) 页面下载最新的安装包，解压后即完成安装。

* 使用 Docker 镜像：dockerfile 文件位于 [github](https://github.com/apache/iotdb/blob/master/docker/src/main)

### 软件目录结构

* sbin 启动和停止脚本目录
* conf 配置文件目录
*  tools 系统工具目录
*  lib 依赖包目录

### IoTDB 试用

用户可以根据以下操作对 IoTDB 进行简单的试用，若以下操作均无误，则说明 IoTDB 安装成功。

#### 启动 IoTDB

用户可以使用 sbin 文件夹下的 start-server 脚本启动 IoTDB。

Linux 系统与 MacOS 系统启动命令如下：

```
> nohup sbin/start-server.sh -f >/dev/null 2>&1 &
or
> nohup sbin/start-server.sh -b
```
可选参数:
- 默认不含任何参数时, iotdb 将在后台启动，并且不在控制台打印日志
- "-v": 查看iotdb版本
- "-f": 在控制台前台启动iotdb (v0.14前是默认设置)
- "-b": 在后台启动iotdb，控制台不打印日志
- "-p \<pidfile\>": 将pid保存到指定的文件中
- "-h": 查看帮助
- "printgc"(必须是最后一个参数): 打印GC日志 (从v0.14起，该参数将被-g取代)
- "-g": 打印GC日志
- "-c \<config folder\>": 设置IOTDB_CONF变量的值，从而修改配置文件所在文件夹
- "-D <a=b>": 设置Java的系统环境变量或其他参数
- "-H \<filePath\> 当OOM异常时存储堆快照到\<filePath\> (仅 Linux/Mac生效, 且要求iotdb内存小于16GB)"
- "-E <\filePath\> save error file of JVM to \<filePath\> (仅 Linux/Mac生效)"
- "-X \<command\> 等价于JVM中 -XX:\<command\>"
- 
Windows 系统启动命令如下：

```
> sbin\start-server.bat 
```

Windows 系统支持 -c, -v, -f, -b 四个参数。


#### 使用 Cli 工具

IoTDB 为用户提供多种与服务器交互的方式，在此我们介绍使用 Cli 工具进行写入、查询数据的基本步骤。

初始安装后的 IoTDB 中有一个默认用户：root，默认密码为 root。用户可以使用该用户运行 Cli 工具操作 IoTDB。Cli 工具启动脚本为 sbin 文件夹下的 start-cli 脚本。启动脚本时需要指定运行 ip、port、username 和 password。若脚本未给定对应参数，则默认参数为"-h 127.0.0.1 -p 6667 -u root -pw -root"

以下启动语句为服务器在本机运行，且用户未更改运行端口号的示例。

Linux 系统与 MacOS 系统启动命令如下：

```
> sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root
```

Windows 系统启动命令如下：

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

#### IoTDB 的基本操作

在这里，我们首先介绍一下使用 Cli 工具创建时间序列、插入数据并查看数据的方法。

数据在 IoTDB 中的组织形式是以时间序列为单位，每一个时间序列中有若干个数据-时间点对，每一个时间序列属于一个存储组。在定义时间序列之前，要首先使用 SET STORAGE GROUP 语句定义存储组。SQL 语句如下：

``` 
IoTDB> SET STORAGE GROUP TO root.ln
```

我们可以使用 SHOW STORAGE GROUP 语句来查看系统当前所有的存储组，SQL 语句如下：

```
IoTDB> SHOW STORAGE GROUP
```

执行结果为：

```
+-------------+
|storage group|
+-------------+
|      root.ln|
+-------------+
Total line number = 1
```

存储组设定后，使用 CREATE TIMESERIES 语句可以创建新的时间序列，创建时间序列时需要定义数据的类型和编码方式。此处我们创建两个时间序列，SQL 语句如下：

```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

为了查看指定的时间序列，我们可以使用 SHOW TIMESERIES \<Path\>语句，其中、<Path\>表示时间序列对应的路径，默认值为空，表示查看系统中所有的时间序列。下面是两个例子：

使用 SHOW TIMESERIES 语句查看系统中存在的所有时间序列，SQL 语句如下：

``` 
IoTDB> SHOW TIMESERIES
```

执行结果为：

```
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                   timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.temperature| null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|
|     root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 2
```

查看具体的时间序列 root.ln.wf01.wt01.status 的 SQL 语句如下：

```
IoTDB> SHOW TIMESERIES root.ln.wf01.wt01.status
```

执行结果为：

```
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|              timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 1
```

接下来，我们使用 INSERT 语句向 root.ln.wf01.wt01.status 时间序列中插入数据，在插入数据时需要首先指定时间戳和路径后缀名称：

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
```

我们也可以向多个时间序列中同时插入数据，这些时间序列同属于一个时间戳：

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

最后，我们查询之前插入的数据。使用 SELECT 语句我们可以查询指定的时间序列的数据结果，SQL 语句如下：

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

我们也可以查询多个时间序列的数据结果，SQL 语句如下：

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

输入 quit 或 exit 可退出 Cli 结束本次会话。

```
IoTDB> quit
```
或

```
IoTDB> exit
```

想要浏览更多 IoTDB 数据库支持的命令，请浏览 [SQL Reference](../Reference/SQL-Reference.md).

#### 停止 IoTDB

用户可以使用$IOTDB_HOME/sbin 文件夹下的 stop-server 脚本停止 IoTDB。

Linux 系统与 MacOS 系统停止命令如下：

```
> $sbin/stop-server.sh
```

Windows 系统停止命令如下：

```
> $sbin\stop-server.bat
```

#### IoTDB 的权限管理

初始安装后的 IoTDB 中有一个默认用户：root，默认密码为 root。该用户为管理员用户，固定拥有所有权限，无法被赋予、撤销权限，也无法被删除。

您可以通过以下命令修改其密码：
```
ALTER USER <username> SET PASSWORD <password>;
Example: IoTDB > ALTER USER root SET PASSWORD 'newpwd';
```

权限管理的具体内容可以参考：[权限管理](https://iotdb.apache.org/zh/UserGuide/V0.13.x/Administration-Management/Administration.html)

### 基础配置

配置文件在"conf"文件夹下，包括：

  * 环境配置模块 (`iotdb-env.bat`, `iotdb-env.sh`), 
  * 系统配置模块 (`iotdb-engine.properties`)
  * 日志配置模块 (`logback.xml`). 
