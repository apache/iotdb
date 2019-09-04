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

## 概览 

- 快速入门
 - 安装环境
    - IoTDB安装
    - IoTDB试用
        - 启动IoTDB
        - 操作IoTDB
            - 使用Cli/Shell工具
            - IoTDB的基本操作
        - 停止IoTDB
            - 使用stop-server脚本强制停止

<!-- /TOC -->

# 快速入门

## 安装环境

安装前需要保证设备上配有JDK>=1.8的运行环境，并配置好JAVA_HOME环境变量。

如需从源码进行编译，还需要Maven>=3.1的运行环境。

设置最大文件打开数为65535。

## IoTDB安装

IoTDB支持多种安装途径。用户可以使用三种方式对IoTDB进行安装——下载二进制可运行程序、使用源码、使用docker镜像。

* 二进制可运行程序：请从Download页面下载最新的安装包，解压后即完成安装。

* 使用源码：您可以从代码仓库下载源码并编译：`git clone https://github.com/apache/incubator-iotdb.git`, 并通过`mvn package -DskipTests` 进行编译。

* 使用Docker： dockerfile文件位于 https://github.com/apache/incubator-iotdb/blob/master/docker/Dockerfile

## IoTDB试用

用户可以根据以下操作对IoTDB进行简单的试用，若以下操作均无误，则说明IoTDB安装成功。

在后文中，记$IOTDB_HOME为IoTDB的安装目录路径，即上述iotdb子文件夹路径。

### 启动IoTDB

用户可以使用$IOTDB_HOME/bin文件夹下的start-server脚本启动IoTDB。

Linux系统与MacOS系统启动命令如下：

```
> $IOTDB_HOME/sbin/start-server.sh
```

Windows系统启动命令如下：
```
> $IOTDB_HOME\sbin\start-server.bat
```

当服务器输出log中包含ERROR输出时，服务器启动不成功。

### 操作IoTDB

#### 使用Cli/Shell工具

IoTDB为用户提供多种与服务器交互的方式，您可以选择使用Cli/Shell工具、Grafana可视化工具或JAVA API与IoTDB服务器进行数据写入与查询的交互操作。在此我们介绍使用Cli/Shell工具进行写入、查询数据的基本步骤。

初始安装后的IoTDB中有一个默认用户：root，默认密码为root。用户可以使用该用户运行Cli/Shell工具操作IoTDB。Cli/Shell工具启动脚本为$IOTDB_HOME/bin文件夹下的start-client脚本。启动脚本时需要指定运行IP和PORT。

以下启动语句为服务器在本机运行，且用户未更改运行端口号的示例。

Linux系统与MacOS系统启动命令如下：
```
> $IOTDB_HOME/sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root
```

Windows系统启动命令如下：
```
> $IOTDB_HOME\sbin\start-cli.bat -h 127.0.0.1 -p 6667 -u root
```

回车后输入root用户的密码，即可成功启动客户端。启动后出现如图提示即为启动成功。

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

Cli/Shell启动成功后，用户可使用该工具输入SQL命令操作IoTDB Server。

在这里，我们首先介绍一下使用Cli/Shell工具创建时间序列、插入数据并查看数据的方法。

数据在IoTDB中的组织形式是以时间序列为单位，每一个时间序列中有若干个数据-时间点对，存储结构为存储组。在定义时间序列之前，要首先使用SET STORAGE GROUP语句定义存储组。SQL语句如下：
``` 
IoTDB> SET STORAGE GROUP TO root.ln
```

执行成功后，Cli/Shell返回execute successfully表示执行成功。

存储组设定后，使用CREATE TIMESERIES语句可以创建新的时间序列，创建时间序列时需要定义数据的类型和编码形式。SQL语句如下：
```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
```

执行成功后，Cli/Shell返回execute successfully表示执行成功。
如果系统中已有被创建的存储组和时间序列，则系统会提示该部分已存在。如以下提示表示存储组已存在：
```
IoTDB> SET STORAGE GROUP TO root.ln
error: The path of root.ln already exist, it can't be set to the storage group
```

如下提示表示时间序列已存在：
```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
error: Timeseries root.ln.wf01.wt01.status already exist
```

创建时间序列后，我们使用SHOW TIMESERIES语句查看系统中存在的所有时间序列，SQL语句如下：

``` 
IoTDB> SHOW TIMESERIES
``` 

执行结果为：
``` 
===  Timeseries Tree  ===

{
	"root":{
		"ln":{
			"wf01":{
				"wt01":{
					"status":{
						"args":"{}",
						"StorageGroup":"root.ln",
						"DataType":"BOOLEAN",
						"Compressor":"UNCOMPRESSED",
						"Encoding":"PLAIN"
					}
				}
			}
		}
	}
}
```

我们可以尝试再创建一个时间序列，查看SHOW TIMESERIES的返回情况。SQL语句如下：
```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
IoTDB> SHOW TIMESERIES
``` 

执行结果为：
```
===  Timeseries Tree  ===

{
	"root":{
		"ln":{
			"wf01":{
				"wt01":{
					"temperature":{
						"args":"{}",
						"StorageGroup":"root.ln",
						"DataType":"FLOAT",
						"Compressor":"UNCOMPRESSED",
						"Encoding":"RLE"
					},
					"status":{
						"args":"{}",
						"StorageGroup":"root.ln",
						"DataType":"BOOLEAN",
						"Compressor":"UNCOMPRESSED",
						"Encoding":"PLAIN"
					}
				}
			}
		}
	}
}
```

为了查看指定的时间序列，我们可以使用SHOW TIMESERIES <Path>语句，查看时间序列root.ln.wf01.wt01.status的SQL语句如下：
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
timeseries number = 1
Execute successfully.
It costs 0.02s.
```

我们还可以使用SHOW STORAGE GROUP语句来查看系统当前所有的存储组，SQL语句如下：

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
Execute successfully.
It costs 0.001s
```

接下来，我们使用INSERT语句向root.ln.wf01.wt01.status时间序列中插入数据，在插入数据时需要首先指定时间戳和插入的传感器路径名称：
```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
execute successfully.
```

我们也可以向多个传感器中同时插入数据，这些传感器同属于一个时间戳：
```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
 execute successfully.
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
record number = 1
execute successfully.
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
```

输入quit或exit可退出Cli结束本次会话，Cli输出quit normally表示退出成功，操作语句与返回结果如下：
```
IoTDB> quit
quit normally
```

### 停止IoTDB
#### 使用stop-server脚本强制停止
用户可以使用$IOTDB_HOME/sbin文件夹下的stop-server脚本停止IoTDB（注意，此停止方式为强制停止，若希望安全停止IoTDB，请使用Jconsole工具的停止方法）。

Linux系统与MacOS系统停止命令如下：
```
> $IOTDB_HOME/sbin/stop-server.sh
```

Windows系统停止命令如下：
```
> $IOTDB_HOME\sbin\stop-server.bat
```
