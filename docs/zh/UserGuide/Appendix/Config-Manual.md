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

# 附录 1：配置参数

为方便 IoTDB Server 的配置与管理，IoTDB Server 为用户提供三种配置项，使得用户可以在启动服务或服务运行时对其进行配置。

三种配置项的配置文件均位于 IoTDB 安装目录：`$IOTDB_HOME/conf`文件夹下，其中涉及 server 配置的共有 2 个文件，分别为：`iotdb-env.sh`, `iotdb-engine.properties`。用户可以通过更改其中的配置项对系统运行的相关配置项进行配置。

配置文件的说明如下：

* `iotdb-env.sh`：环境配置项的默认配置文件。用户可以在文件中配置 JAVA-JVM 的相关系统配置项。

* `iotdb-engine.properties`：IoTDB 引擎层系统配置项的默认配置文件。用户可以在文件中配置 IoTDB 引擎运行时的相关参数，如 JDBC 服务监听端口 (`rpc_port`)、overflow 数据文件存储目录 (`overflow_data_dir`) 等。此外，用户可以在文件中配置 IoTDB 存储时 TsFile 文件的相关信息，如每次将内存中的数据写入到磁盘时的数据大小 (`group_size_in_byte`)，内存中每个列打一次包的大小 (`page_size_in_byte`) 等。

## 热修改配置项

为方便用户使用，IoTDB Server 为用户提供了热修改功能，即在系统运行过程中修改`iotdb-engine.properties`中部分配置参数并即时应用到系统中。下面介绍的参数中，改后
生效方式为`触发生效`的均为支持热修改的配置参数。

触发方式：客户端发送```load configuration```命令至 IoTDB Server，客户端的使用方式详见 [SQL 命令行终端（CLI）](https://iotdb.apache.org/zh/UserGuide/Master/CLI/Command-Line-Interface.html)

## 环境配置项

环境配置项主要用于对 IoTDB Server 运行的 Java 环境相关参数进行配置，如 JVM 相关配置。IoTDB Server 启动时，此部分配置会被传给 JVM。用户可以通过查看 `iotdb-env.sh`（或`iotdb-env.bat`) 文件查看环境配置项内容。详细配置项说明如下：

* JMX\_LOCAL

|名字|JMX\_LOCAL|
|:---:|:---|
|描述|JMX 监控模式，配置为 true 表示仅允许本地监控，设置为 false 的时候表示允许远程监控。如想在本地通过网络连接JMX Service，比如nodeTool.sh会尝试连接127.0.0.1:31999，请将JMX_LOCAL设置为false。|
|类型|枚举 String : “true”, “false”|
|默认值|true|
|改后生效方式|重启服务生效|

* JMX\_PORT

|名字|JMX\_PORT|
|:---:|:---|
|描述|JMX 监听端口。请确认该端口是不是系统保留端口并且未被占用。|
|类型|Short Int: [0,65535]|
|默认值|31999|
|改后生效方式|重启服务生效|

* MAX\_HEAP\_SIZE

|名字|MAX\_HEAP\_SIZE|
|:---:|:---|
|描述|IoTDB 启动时能使用的最大堆内存大小。|
|类型|String|
|默认值|取决于操作系统和机器配置。在 Linux 或 MacOS 系统下默认为机器内存的四分之一。在 Windows 系统下，32 位系统的默认值是 512M，64 位系统默认值是 2G。|
|改后生效方式|重启服务生效|

* HEAP\_NEWSIZE

|名字|HEAP\_NEWSIZE|
|:---:|:---|
|描述|IoTDB 启动时能使用的最小堆内存大小。|
|类型|String|
|默认值|取决于操作系统和机器配置。在 Linux 或 MacOS 系统下默认值为机器 CPU 核数乘以 100M 的值与 MAX\_HEAP\_SIZE 四分之一这二者的最小值。在 Windows 系统下，32 位系统的默认值是 512M，64 位系统默认值是 2G。|
|改后生效方式|重启服务生效|

## 系统配置项

系统配置项是 IoTDB Server 运行的核心配置，它主要用于设置 IoTDB Server 文件层和引擎层的参数，便于用户根据自身需求调整 Server 的相关配置，以达到较好的性能表现。系统配置项可分为两大模块：文件层配置项和引擎层配置项。用户可以通过`iotdb-engine.properties`, 文件查看和修改两种配置项的内容。在 0.7.0 版本中字符串类型的配置项大小写敏感。

### RPC配置

* rpc\_address

|名字| rpc\_address |
|:---:|:---|
|描述| rpc地址 |
|类型| String |
|默认值| 0.0.0.0|
|改后生效方式|重启服务生效|

* rpc\_port

|名字| rpc\_port |
|:---:|:---|
|描述| jdbc 服务监听端口。请确认该端口不是系统保留端口并且未被占用。|
|类型| Short Int : [0,65535] |
|默认值| 6667 |
|改后生效方式|重启服务生效|

* rpc\_thrift\_compression\_enable

|名字| rpc\_thrift\_compression\_enable |
|:---:|:---|
|描述| 是否启用 thrift 的压缩机制。|
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

* rpc\_advanced\_compression\_enable

|名字| rpc\_advanced\_compression\_enable |
|:---:|:---|
|描述| 是否启用 thrift 的自定制压缩机制。|
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

* rpc\_max\_concurrent\_client\_num

|名字| rpc\_max\_concurrent\_client\_num |
|:---:|:---|
|描述| 最大连接数。|
|类型| Short Int : [0,65535] |
|默认值| 65535 |
|改后生效方式|重启服务生效|

* thrift\_max\_frame\_size

|名字| thrift\_max\_frame\_size |
|:---:|:---|
|描述| RPC 请求/响应的最大字节数|
|类型| long |
|默认值| 536870912 （默认值512MB，应大于等于 512 * 1024 * 1024) |
|改后生效方式|重启服务生效|

* thrift\_init\_buffer\_size

|名字| thrift\_init\_buffer\_size |
|:---:|:---|
|描述| 字节数 |
|类型| long |
|默认值| 1024 |
|改后生效方式|重启服务生效|


### 写前日志配置

* enable\_wal

|名字| enable\_wal |
|:---:|:---|
|描述| 是否开启写前日志，默认值为 true 表示开启，配置成 false 表示关闭 |
|类型| Boolean|
|默认值| true |
|改后生效方式|触发生效|

* enable\_discard\_out\_of\_order\_data

|名字| enable\_discard\_out\_of\_order\_data |
|:---:|:---|
|描述| 是否丢弃乱序数据，默认值为 false 表示关闭 |
|类型| Boolean |
|默认值| false |
|改后生效方式|触发生效|

* flush\_wal\_threshold

|名字| flush\_wal\_threshold |
|:---:|:---|
|描述| 写前日志的条数达到该值之后，持久化到磁盘，有可能丢失至多 flush\_wal\_threshold 个操作 |
|类型| Int32 |
|默认值| 10000 |
|改后生效方式|触发生效|

* force\_wal\_period\_in\_ms

|名字| force\_wal\_period\_in\_ms |
|:---:|:---|
|描述| 写前日志定期持久化到磁盘的周期，单位毫秒，有可能丢失至多 force\_wal\_period\_in\_ms 毫秒的操作。 |
|类型| Int32 |
|默认值| 100 |
|改后生效方式|触发生效|

### 目录配置

* system\_dir

|名字| system\_dir |
|:---:|:---|
|描述| IoTDB 元数据存储路径，默认存放在和 sbin 目录同级的 data 目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。|
|类型| String |
|默认值| data/system（Windows：data\\system） |
|改后生效方式|触发生效|

* data\_dirs

|名字| data\_dirs |
|:---:|:---|
|描述| IoTDB 数据存储路径，默认存放在和 sbin 目录同级的 data 目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。|
|类型| String |
|默认值| data/data（Windows：data\\data） |
|改后生效方式|触发生效|

* multi\_dir\_strategy

|名字| multi\_dir\_strategy |
|:---:|:---|
|描述| IoTDB 在 tsfile\_dir 中为 TsFile 选择目录时采用的策略。可使用简单类名或类名全称。系统提供以下三种策略：<br>1. SequenceStrategy：IoTDB 按顺序从 tsfile\_dir 中选择目录，依次遍历 tsfile\_dir 中的所有目录，并不断轮循；<br>2. MaxDiskUsableSpaceFirstStrategy：IoTDB 优先选择 tsfile\_dir 中对应磁盘空余空间最大的目录；<br>3. MinFolderOccupiedSpaceFirstStrategy：IoTDB 优先选择 tsfile\_dir 中已使用空间最小的目录；<br>4. UserDfineStrategyPackage（用户自定义策略）<br>您可以通过以下方法完成用户自定义策略：<br>1. 继承 cn.edu.tsinghua.iotdb.conf.directories.strategy.DirectoryStrategy 类并实现自身的 Strategy 方法；<br>2. 将实现的类的完整类名（包名加类名，UserDfineStrategyPackage）填写到该配置项；<br>3. 将该类 jar 包添加到工程中。 |
|类型| String |
|默认值| MaxDiskUsableSpaceFirstStrategy |
|改后生效方式|触发生效|

* wal\_dir

|名字| wal\_dir |
|:---:|:---|
|描述| IoTDB 写前日志存储路径，默认存放在和 sbin 目录同级的 data 目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。|
|类型| String |
|默认值| data/wal（Windows：data\\wal） |
|改后生效方式|触发生效|

* tsfile\_storage\_fs

|名字| tsfile\_storage\_fs |
|:---:|:---|
|描述| Tsfile 和相关数据文件的存储文件系统。目前支持 LOCAL（本地文件系统）和 HDFS 两种 |
|类型| String |
|默认值| LOCAL |
|改后生效方式|仅允许在第一次启动服务前修改|

* core\_site\_path

|名字| core\_site\_path |
|:---:|:---|
|描述| 在 Tsfile 和相关数据文件存储到 HDFS 的情况下用于配置 core-site.xml 的绝对路径 |
|类型| String |
|默认值| /etc/hadoop/conf/core-site.xml |
|改后生效方式|重启服务生效|

* hdfs\_site\_path

|名字| hdfs\_site\_path |
|:---:|:---|
|描述| 在 Tsfile 和相关数据文件存储到 HDFS 的情况下用于配置 hdfs-site.xml 的绝对路径 |
|类型| String |
|默认值| /etc/hadoop/conf/hdfs-site.xml |
|改后生效方式|重启服务生效|

* hdfs\_ip

|名字| hdfs\_ip |
|:---:|:---|
|描述| 在 Tsfile 和相关数据文件存储到 HDFS 的情况下用于配置 HDFS 的 IP。**如果配置了多于 1 个 hdfs\_ip，则表明启用了 Hadoop HA**|
|类型| String |
|默认值|localhost |
|改后生效方式|重启服务生效|

* hdfs\_port

|名字| hdfs\_port |
|:---:|:---|
|描述| 在 Tsfile 和相关数据文件存储到 HDFS 的情况下用于配置 HDFS 的端口 |
|类型| String |
|默认值| 9000 |
|改后生效方式|重启服务生效|

* dfs\_nameservices

|名字| hdfs\_nameservices |
|:---:|:---|
|描述| 在使用 Hadoop HA 的情况下用于配置 HDFS 的 nameservices |
|类型| String |
|默认值| hdfsnamespace |
|改后生效方式|重启服务生效|

* dfs\_ha\_namenodes

|名字| hdfs\_ha\_namenodes |
|:---:|:---|
|描述| 在使用 Hadoop HA 的情况下用于配置 HDFS 的 nameservices 下的 namenodes |
|类型| String |
|默认值| nn1,nn2 |
|改后生效方式|重启服务生效|

* dfs\_ha\_automatic\_failover\_enabled

|名字| dfs\_ha\_automatic\_failover\_enabled |
|:---:|:---|
|描述| 在使用 Hadoop HA 的情况下用于配置是否使用失败自动切换 |
|类型| Boolean |
|默认值| true |
|改后生效方式|重启服务生效|

* dfs\_client\_failover\_proxy\_provider

|名字| dfs\_client\_failover\_proxy\_provider |
|:---:|:---|
|描述| 在使用 Hadoop HA 且使用失败自动切换的情况下配置失败自动切换的实现方式 |
|类型| String |
|默认值| org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider |
|改后生效方式|重启服务生效|

* hdfs\_use\_kerberos

|名字| hdfs\_use\_kerberos |
|:---:|:---|
|描述| 是否使用 kerberos 验证访问 hdfs |
|类型| String |
|默认值| false |
|改后生效方式|重启服务生效|

* kerberos\_keytab\_file_path

|名字| kerberos\_keytab\_file_path |
|:---:|:---|
|描述| kerberos keytab file 的完整路径 |
|类型| String |
|默认值| /path |
|改后生效方式|重启服务生效|

* kerberos\_principal

|名字| kerberos\_principal |
|:---:|:---|
|描述| Kerberos 认证原则 |
|类型| String |
|默认值| your principal |
|改后生效方式|重启服务生效|

### 存储引擎配置

* timestamp\_precision

|名字| timestamp\_precision |
|:---:|:---|
|描述| 时间戳精度，支持 ms、us、ns |
|类型|String |
|默认值| ms |
|改后生效方式|触发生效|

* default\_ttl

|名字| default\_ttl |
|:---:|:---|
|描述| 数据保留时间，会丢弃 now()-default\_ttl 之前的数据，单位 ms|
|类型| long |
|默认值| 36000000 |
|改后生效方式|重启服务生效|

* wal\_buffer\_size

|名字| wal\_buffer\_size |
|:---:|:---|
|描述| 写前日志的 buffer 大小 |
|类型|Int32|
|默认值| 16777216 |
|改后生效方式|触发生效|

* seq\_tsfile\_size

|名字| seq\_tsfile\_size |
|:---:|:---|
|描述| 每个顺序tsfile大小,单位：byte |
|类型|Int32|
|默认值| 1 |
|改后生效方式| 重启服务生效|

* unseq\_tsfile\_size

|名字| unseq\_tsfile\_size |
|:---:|:---|
|描述| 每个无序 tsfile 大小,单位：byte |
|类型| Int32 |
|默认值| 1 |
|改后生效方式| 重启服务生效|

* mlog\_buffer\_size

|名字| mlog\_buffer\_size |
|:---:|:---|
|描述| mlog 的 buffer 大小 |
|类型| Int32 |
|默认值| 1048576 |
|改后生效方式|触发生效|

* memtable\_size\_threshold

|名字| memtable\_size\_threshold |
|:---:|:---|
|描述| 内存缓冲区 memtable 阈值 |
|类型| Long |
|默认值| 1073741824 |
|改后生效方式|enable\_mem\_control 为 false 时生效、重启服务生效|

* enable\_timed\_flush\_seq\_memtable

|名字| enable\_timed\_flush\_seq\_memtable |
|:---:|:---|
|描述| 是否开启定时刷盘顺序 memtable |
|类型| Boolean |
|默认值| false |
|改后生效方式| 触发生效 |

* seq\_memtable\_flush\_interval\_in\_ms

|名字| seq\_memtable\_flush\_interval\_in\_ms |
|:---:|:---|
|描述| 当 memTable 的创建时间小于当前时间减去该值时，该 memtable 需要被刷盘 |
|类型| Int32 |
|默认值| 3600000 |
|改后生效方式| 触发生效 |

* seq\_memtable\_flush\_check\_interval\_in\_ms

|名字| seq\_memtable\_flush\_check\_interval\_in\_ms |
|:---:|:---|
|描述| 检查顺序 memtable 是否需要刷盘的时间间隔 |
|类型| Int32 |
|默认值| 600000 |
|改后生效方式| 触发生效 |

* enable\_timed\_flush\_unseq\_memtable

|名字| enable\_timed\_flush\_unseq\_memtable |
|:---:|:---|
|描述| 是否开启定时刷新乱序 memtable |
|类型| Boolean |
|默认值| true |
|改后生效方式| 触发生效 |

* unseq\_memtable\_flush\_interval\_in\_ms

|名字| unseq\_memtable\_flush\_interval\_in\_ms |
|:---:|:---|
|描述| 当 memTable 的创建时间小于当前时间减去该值时，该 memtable 需要被刷盘 |
|类型| Int32 |
|默认值| 3600000 |
|改后生效方式| 触发生效 |

* unseq\_memtable\_flush\_check\_interval\_in\_ms

|名字| unseq\_memtable\_flush\_check\_interval\_in\_ms |
|:---:|:---|
|描述| 检查乱序 memtable 是否需要刷盘的时间间隔 |
|类型| Int32 |
|默认值| 600000 |
|改后生效方式| 触发生效 |

* enable\_timed\_close\_tsfile

|名字| enable\_timed\_close\_tsfile |
|:---:|:---|
|描述| 是否开启定时关闭 tsfile |
|类型| Boolean |
|默认值| true |
|改后生效方式| 触发生效 |

* close\_tsfile\_interval\_after\_flushing\_in\_ms

|名字| close\_tsfile\_interval\_after\_flushing\_in\_ms |
|:---:|:---|
|描述| 当 tsfile 的上一个 memtable 刷盘时间小于当前时间减去该值且当前工作 memtable 为空时， 该 tsfile 需要被关闭 |
|类型| Int32 |
|默认值| 3600000 |
|改后生效方式| 触发生效 |

* close\_tsfile\_check\_interval\_in\_ms

|名字| close\_tsfile\_check\_interval\_in\_ms |
|:---:|:---|
|描述| 检查 tsfile 是否需要关闭的时间间隔 |
|类型| Int32 |
|默认值| 600000 |
|改后生效方式| 触发生效 |

* avg\_series\_point\_number\_threshold

|名字| avg\_series\_point\_number\_threshold |
|:---:|:---|
|描述| 内存中平均每个时间序列点数最大值，达到触发 flush |
|类型| Int32 |
|默认值| 10000 |
|改后生效方式|重启服务生效|

* concurrent\_flush\_thread

|名字| concurrent\_flush\_thread |
|:---:|:---|
|描述| 当 IoTDB 将内存中的数据写入磁盘时，最多启动多少个线程来执行该操作。如果该值小于等于 0，那么采用机器所安装的 CPU 核的数量。默认值为 0。|
|类型| Int32 |
|默认值| 0 |
|改后生效方式|重启服务生效|

* concurrent\_query\_thread

|名字| concurrent\_query\_thread |
|:---:|:---|
|描述| 当 IoTDB 对内存中的数据进行查询时，最多启动多少个线程来执行该操作。如果该值小于等于 0，那么采用机器所安装的 CPU 核的数量。默认值为 0。|
|类型| Int32 |
|默认值| 0 |
|改后生效方式|重启服务生效|

* chunk\_buffer\_pool\_enable

|名字| chunk\_buffer\_pool\_enable |
|:---:|:---|
|描述| 在将 memtable 序列化为内存中的字节时，是否开启由 IoTDB 而不是 JVM 接管内存管理，默认关闭。 |
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

* batch\_size

|名字| batch\_size |
|:---:|:---|
|描述| 服务器中每次迭代的数据量（数据条目，即不同时间戳的数量。） |
|类型| Int32 |
|默认值| 100000 |
|改后生效方式|重启服务生效|

* tag\_attribute\_total\_size

|名字| tag\_attribute\_total\_size |
|:---:|:---|
|描述| 每个时间序列标签和属性的最大持久化字节数 |
|类型| Int32 |
|默认值| 700 |
|改后生效方式|仅允许在第一次启动服务前修改|

* enable\_partial\_insert

|名字| enable\_partial\_insert |
|:---:|:---|
|描述| 在一次 insert 请求中，如果部分测点写入失败，是否继续写入其他测点。|
|类型| Boolean |
|默认值| true |
|改后生效方式|重启服务生效|

* enable\_mtree\_snapshot

|名字| enable\_mtree\_snapshot |
|:---:|:---|
|描述| 是否启用 MTree snapshot。 从 0.11.0 开始默认为 false。|
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

* mtree\_snapshot\_interval

|名字| mtree\_snapshot\_interval |
|:---:|:---|
|描述| 创建 MTree snapshot 时至少累积的 mlog 日志行数。单位为日志行数。|
|类型| Int32 |
|默认值| 100000 |
|改后生效方式|重启服务生效|

* mtree\_snapshot\_threhold\_time

|名字| mtree\_snapshot\_threhold\_time |
|:---:|:---|
|描述| MTree 修改的阈值间隔时间，若最后一次修改时间小于这个阈值，则不会创建 MTree snapshot。 单位：秒。 默认值：1 小时（3600 秒）|
|类型| Int32 |
|默认值| 3600 |
|改后生效方式|重启服务生效|

* virtual\_storage\_group\_num

|名字| virtual\_storage\_group\_num |
|:---:|:---|
|描述| 每个用户定义的虚拟存储组数 |
|类型| Int64 |
|默认值| 1 |
|改后生效方式|仅允许在第一次启动服务前修改|

* time\_index\_level

|名字| time\_index\_level |
|:---:|:---|
|描述| TimeIndex的级别，记录了TsFileResource的开始时间和结束时间。 目前，支持 DEVICE_TIME_INDEX 和 FILE_TIME_INDEX，第一次设置后无法更改。 |
|类型| String |
|默认值| DEVICE_TIME_INDEX |
|改后生效方式|触发生效|

### 内存控制配置

* enable\_mem\_control

|名字| enable\_mem\_control |
|:---:|:---|
|描述| 开启内存控制，避免爆内存 |
|类型| Boolean |
|默认值| true |
|改后生效方式|重启服务生效|

* write\_read\_schema\_free\_memory\_proportion

|名字| write\_read\_schema\_free\_memory\_proportion |
|:---:|:---|
|描述| 读写内存分配比率。 参数形式为a:b:c:d，其中a、b、c、d为整数。如“1:1:1:1”、“6:2:1:1”，如果你的写入压力大而读取压力小，请调整为“6:1:1:2” |
|类型| String |
|默认值| 4:3:1:2 |
|改后生效方式|重启服务生效|

* primary\_array\_size

|名字| primary\_array\_size |
|:---:|:---|
|描述| 数组池中的原始数组大小（每个数组的长度）|
|类型| Int32 |
|默认值| 32 |
|改后生效方式|重启服务生效|

* flush\_proportion

|名字| flush\_proportion |
|:---:|:---|
|描述| 调用flush disk的写入内存比例，默认0.4,若有极高的写入负载力（比如batch=1000），可以设置为低于默认值，比如0.2 |
|类型| Float |
|默认值| 0.4 |
|改后生效方式|重启服务生效|

* buffered\_arrays\_memory\_proportion

|名字| buffered\_arrays\_memory\_proportion |
|:---:|:---|
|描述| 为缓冲数组分配的写入内存比例，默认为0.6 |
|类型| Float |
|默认值| 0.6 |
|改后生效方式|重启服务生效|

* reject\_proportion

|名字| reject\_proportion |
|:---:|:---|
|描述| 拒绝插入的写入内存比例，默认0.8，若有极高的写入负载力（比如batch=1000）并且物理内存足够大，它可以设置为高于默认值，如0.9 |
|类型| Float |
|默认值| 0.8 |
|改后生效方式|重启服务生效|

* storage\_group\_report\_threshold

|名字| storage\_group\_report\_threshold |
|:---:|:---|
|描述| 如果存储组的内存（以字节byte为单位）增加超过此阈值，则向系统报告。默认值为16MB |
|类型| Int32 |
|默认值| 16777216 |
|改后生效方式|重启服务生效|

* max\_deduplicated\_path\_num

|名字| max\_deduplicated\_path\_num |
|:---:|:---|
|描述| 在一次查询中，允许最大重复路径的数据量，这只是一个建议值，实际限制是这个默认值和计算值的之间 |
|类型| Int32 |
|默认值| 1000 |
|改后生效方式|重启服务生效|

* check\_period\_when\_insert\_blocked

|名字| check\_period\_when\_insert\_blocked |
|:---:|:---|
|描述| 当插入被拒绝时，等待时间（以毫秒为单位）去再次检查系统，默认为50。若插入被拒绝，读取负载低，可以设置大一些。 |
|类型| Int32 |
|默认值| 50 |
|改后生效方式|重启服务生效|

* max\_waiting\_time\_when\_insert\_blocked

|名字| max\_waiting\_time\_when\_insert\_blocked |
|:---:|:---|
|描述| 当插入的等待时间（以毫秒为单位）超过此值时，抛出异常。默认为10000。若插入被拒绝，读取负载低，可以设置大一些。|
|类型| Int32 |
|默认值| 10000 |
|改后生效方式|重启服务生效|

* estimated\_series\_size

|名字| estimated\_series\_size |
|:---:|:---|
|描述| 在Mtree中一个时间序列的评估元数据大小（以字节byte为单位）。|
|类型| Int32 |
|默认值| 300 |
|改后生效方式|重启服务生效|

* io\_task\_queue\_size\_for\_flushing

|名字| io\_task\_queue\_size\_for\_flushing |
|:---:|:---|
|描述| ioTaskQueue 的大小。默认值为10。|
|类型| Int32 |
|默认值| 10 |
|改后生效方式|重启服务生效|

### 升级配置
 
* update\_thread\_num

|名字| update\_thread\_num |
|:---:|:---|
|描述| 当存在旧版本（V0.9.x/v1）数据时，会设置多少线程来执行升级任务，默认为1。版本小于等于V0.X时设置为1。|
|类型| Int32 |
|默认值| 1 |
|改后生效方式|重启服务生效|

### 查询配置

* default\_fill\_interval

|名字| default\_fill\_interval |
|:---:|:---|
|描述| 填充查询中使用的默认时间段，默认-1表示无限过去时间，以毫秒ms为单位 |
|类型| Int32 |
|默认值| -1 |
|改后生效方式|重启服务生效|

### 合并配置

* compaction\_strategy

|名字| compaction\_strategy |
|:---:|:---|
|描述| 默认开启合并，可根据需求开启合并或关闭合并（LEVEL_COMPACTION, NO_COMPACTION） |
|类型| String |
|默认值| LEVEL_COMPACTION |
|改后生效方式|重启服务生效|

* enable\_unseq_compaction

|名字| enable\_unseq_compaction |
|:---:|:---|
|描述| 是否将unseq文件合并为seq文件。仅当compaction_strategy为LEVEL_COMPACTION时有效。 |
|类型| Boolean |
|默认值| true |
|改后生效方式|仅开启合并后，重启服务生效|

* compaction\_interval

|名字| compaction\_interval |
|:---:|:---|
|描述| 设置延迟时间开始压缩任务，单位是：ms |
|类型| Int32 |
|默认值| 30000 |
|改后生效方式|仅开启合并后，重启服务生效|

* seq\_file\_num\_in\_each\_level

|名字| seq\_file\_num\_in\_each\_level |
|:---:|:---|
|描述| 顺序每层级文件最大数 |
|类型| Int32 |
|默认值| 6 |
|改后生效方式| 重启服务生效|

* seq\_level\_num

|名字| seq\_level\_num |
|:---:|:---|
|描述| 顺序最大层级数 |
|类型| Int32 |
|默认值| 3 |
|改后生效方式| 重启服务生效|

* unseq\_file\_num\_in\_each\_level

|名字| unseq\_file\_num\_in\_each\_level |
|:---:|:---|
|描述| 乱序每层级文件最大数 |
|类型| Int32 |
|默认值| 10 |
|改后生效方式| 重启服务生效|

* unseq\_level\_num

|名字| unseq\_level\_num |
|:---:|:---|
|描述| 乱序最大层级数 |
|类型| Int32 |
|默认值| 1 |
|改后生效方式| 重启服务生效|

* max\_select\_unseq\_file\_num\_in\_each\_unseq\_compaction

|名字| max\_select\_unseq\_file\_num\_in\_each\_unseq\_compaction |
|:---:|:---|
|描述| 每个无序压缩任务中的最大打开文件数，仅当compaction_strategy为LEVEL_COMPACTION时有效。此参数必须远小于操作系统控制的每个进程允许的最大打开文件数（大多数系统为65535）。|
|类型| Int32 |
|默认值| 2000 |
|改后生效方式| 重启服务生效|

* merge\_chunk\_point\_number

|名字| merge\_chunk\_point\_number |
|:---:|:---|
|描述| 在目标文件中chunk的平均点数值达到值时，将文件合并到顶层。仅当compaction_strategy为LEVEL_COMPACTION时有效。在合并过程中，如果一个chunk的点数少于该参数，则该chunk将被与其后续的块合并，即使它没有溢出，直到合并的chunk到达这个阈值和新的块将被刷新。若小于0时，此机制将被禁用。|
|类型| Int32 |
|默认值| 100000 |
|改后生效方式| 重启服务生效|

* merge\_page\_point\_number

|名字| merge\_page\_point\_number |
|:---:|:---|
|描述| 当一个页面的点数达到该值时，使用“追加合并”而不是“反序列化合并”。仅当compaction_strategy为LEVEL_COMPACTION时有效。|
|类型| Int32 |
|默认值| 100 |
|改后生效方式| 重启服务生效|

* merge\_chunk\_subthread\_num

|名字| merge\_chunk\_subthread\_num |
|:---:|:---|
|描述| 设置多少个线程来执行乱序合并块子任务，默认为 4。若小于等于0时设置为1。|
|类型| Int32 |
|默认值| 4 |
|改后生效方式| 重启服务生效|

* merge\_fileSelection\_time\_budget

|名字| merge\_fileSelection\_time\_budget |
|:---:|:---|
|描述| 若一个合并文件选择运行的时间超过这个时间，它将结束，并且当前的文件合并选择将用作为最终选择。当时间小于0 时，则表示时间是无边界的。单位：ms。|
|类型| Int32 |
|默认值| 30000 |
|改后生效方式| 重启服务生效|

* merge\_memory\_budget

|名字| merge\_memory\_budget |
|:---:|:---|
|描述| 一个合并任务可以使用多少内存（以字节为单位），默认为最大JVM内存的10%。这只是一个粗略的估计，从一个比较小的值开始，避免OOM。每个新的合并线程可能会占用这样的内存，所以merge_thread_num * merge_memory_budget是合并的预估总内存。|
|类型| Int32 |
|默认值| 2147483648 |
|改后生效方式| 重启服务生效|

* continue\_merge\_after\_reboot

|名字| continue\_merge\_after\_reboot |
|:---:|:---|
|描述| 重启后继续合并，当true时，在系统重启过程中检测到一些崩溃的合并，则此类合并将继续，为false时，则合并中未完成的部分将不会继续，已完成的合并部分仍保持原样，若如果重启太慢，可设置为false。 |
|类型| Boolean |
|默认值| false |
|改后生效方式| 重启服务生效|

* force\_full\_merge

|名字| force\_full\_merge |
|:---:|:---|
|描述| 当设置为 true 时，所有乱序文件合并变为完全合并。|
|类型| Boolean |
|默认值| true |
|改后生效方式| 重启服务生效|

* compaction\_thread\_num

|名字| compaction\_thread\_num |
|:---:|:---|
|描述| 设置多少线程来执行压缩，默认为 10。数值小于等于0时设置为1。|
|类型| Int32 |
|默认值| 10 |
|改后生效方式| 重启服务生效|

* merge\_write\_throughput\_mb\_per\_sec

|名字| merge\_write\_throughput\_mb\_per\_sec |
|:---:|:---|
|描述| 每秒可达到的写入吞吐量合并限制。|
|类型| Int32 |
|默认值| 8 |
|改后生效方式| 重启服务生效|

* query\_timeout\_threshold

|名字| query\_timeout\_threshold |
|:---:|:---|
|描述| 查询的最大执行时间。单位：毫秒。|
|类型| Int32 |
|默认值| 60000 |
|改后生效方式| 重启服务生效|

### 元数据缓存配置

* meta\_data\_cache\_enable

|名字| meta\_data\_cache\_enable |
|:---:|:---|
|描述| 是否缓存元数据Chunk Metadata 和 TimeSeries Metadata）。|
|类型|Boolean|
|默认值| true |
|改后生效方式| 重启服务生效|

* chunk\_timeseriesmeta\_free\_memory\_proportion

|名字| chunk\_timeseriesmeta\_free\_memory\_proportion |
|:---:|:---|
|描述| 读取内存分配比例，ChunkCache、TimeseriesMetadataCache、数据集查询的内存和可用内存的查询。参数形式为a:b:c:d，其中a、b、c、d为整数。 例如“1:1:1:1” ，“1:2:3:4” 。|
|类型|String|
|默认值| 1:2:3:4 |
|改后生效方式| 重启服务生效|

* metadata\_node\_cache\_size

|名字| metadata\_node\_cache\_size |
|:---:|:---|
|描述| Manager的缓存大小。所有路径检查和将具有相应路径的MManager中的TSDataType的缓存，都将被用作提高写入速度。|
|类型|Int32|
|默认值| 300000 |
|改后生效方式|重启服务生效|

## 最新点缓存配置

* enable\_last\_stat

|名字| enable\_last\_cache |
|:---:|:---|
|描述| 是否开启最新点缓存 |
|类型| Boolean |
|默认值| true |
|改后生效方式|重启服务生效|

### 统计监控器配置

* enable\_stat\_monitor

|名字| enable\_stat\_monitor |
|:---:|:---|
|描述| 选择是否启动后台统计功能 |
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

* enable\_monitor\_series\_write

|名字| enable\_monitor\_series\_write |
|:---:|:---|
|描述| 是否开启写入服务监控的时间序列 |
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

### WAL 直接缓冲池配置

* wal\_pool\_trim\_interval\_ms

|名字| wal\_pool\_trim\_interval\_ms |
|:---:|:---|
|描述| 缩短日志池间隔时间，单位：毫秒ms |
|类型| Int32 |
|默认值| 10000 |
|改后生效方式|重启服务生效|
 
* max\_wal\_bytebuffer\_num\_for\_each\_partition

|名字| max\_wal\_bytebuffer\_num\_for\_each\_partition |
|:---:|:---|
|描述| 每个时间分区可以分配的最大日志字节缓冲区数，如果没有乱序数据可以设置为4。应该是偶数 |
|类型| Int32 |
|默认值| 6 |
|改后生效方式|重启服务生效|

### 外部排序配置

* enable\_external\_sort

|名字| enable\_external\_sort |
|:---:|:---|
|描述| 是否开启外部排序功能 |
|类型| Boolean |
|默认值| true |
|改后生效方式|重启服务生效|

* external\_sort\_threshold

|名字| external\_sort\_threshold |
|:---:|:---|
|描述| 单个时间序列的最大同时块读取数。若同时chunk读取的数量大于external_sort_threshold，则使用外部排序。当external_sort_threshold增加时，内存中同时排序的chunk数量可能会增加，这会占用更多的内存；external_sort_threshold 减小时，触发外部排序会增加耗时。|
|类型| Int32 |
|默认值| 1000 |
|改后生效方式|重启服务生效|

### 同步服务器配置

* is\_sync\_enable

|名字| is\_sync\_enable |
|:---:|:---|
|描述| 是否开启同步服务功能 |
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

* sync\_server\_port

|名字| sync\_server\_port |
|:---:|:---|
|描述| 同步服务器端口进行监听 |
|类型| Int32 |
|默认值| 5555 |
|改后生效方式|重启服务生效|

* ip\_white\_list

|名字| ip\_white\_list |
|:---:|:---|
|描述| 同步客户端的白名单。请用网段形式表示IP范围，例如：192.168.0.0/16，若有多个IP段，请用逗号隔开，默认是允许所有IP同步 |
|类型| String |
|默认值| 0.0.0.0/0 |
|改后生效方式|重启服务生效|

### 性能统计配置

* enable\_performance\_stat

|名字| enable\_performance\_stat |
|:---:|:---|
|描述| 是否启用子模块的性能状态 |
|类型| Boolean |
|默认值| true |
|改后生效方式|重启服务生效|

* performance\_stat\_display\_interval

|名字| performance\_stat\_display\_interval |
|:---:|:---|
|描述| 显示统计结果的间隔，单位为毫秒ms。 |
|类型| Int32 |
|默认值| 60000 |
|改后生效方式|重启服务生效|

* performance\_stat\_memory\_in\_kb

|名字| performance\_stat\_memory\_in\_kb |
|:---:|:---|
|描述| 设置性能状态内存（以 kb 为单位）。 |
|类型| Int32 |
|默认值| 20 |
|改后生效方式|重启服务生效|

### 水印模块配置

* watermark\_module\_opened

|名字| watermark\_module\_opened |
|:---:|:---|
|描述| 是否开启水印水印嵌入功能 |
|取值| true or false |
|默认值| false |
|改后生效方式|重启服务生效|

* watermark\_secret\_key

|名字| watermark\_secret\_key |
|:---:|:---|
|描述| 水印嵌入功能秘钥 |
|类型| String |
|默认值| IoTDB * 2019@Beijing |
|改后生效方式|重启服务生效|

* watermark\_bit\_string

|名字| watermark\_bit\_string |
|:---:|:---|
|描述| 水印比特字符串 |
|类型| Int32 |
|默认值| 100101110100 |
|改后生效方式|重启服务生效|

* watermark\_method

|名字| watermark\_method |
|:---:|:---|
|描述| 水印嵌入方法 |
|类型| String |
|默认值| GroupBasedLSBMethod(embed_row_cycle=2,embed_lsb_num=5) |
|改后生效方式|重启服务生效|

### 数据类型自动推断

* enable\_auto\_create\_schema

|名字| enable\_auto\_create\_schema |
|:---:|:---|
|描述| 当写入的序列不存在时，是否自动创建序列 |
|取值| true or false |
|默认值| true |
|改后生效方式|重启服务生效|

* default\_storage\_group\_level

|名字| default\_storage\_group\_level |
|:---:|:---|
|描述| 当写入的数据不存在且自动创建序列时，若需要创建相应的存储组，将序列路径的哪一层当做存储组。例如，如果我们接到一个新序列 root.sg0.d1.s2, 并且 level=1， 那么 root.sg0 被视为存储组（因为 root 是 level 0 层）|
|取值| Int32 |
|默认值| 1 |
|改后生效方式|重启服务生效|

* boolean\_string\_infer\_type

|名字| boolean\_string\_infer\_type |
|:---:|:---|
|描述| "true" 或者 "false" 字符串被推断的数据类型 |
|取值| BOOLEAN 或者 TEXT |
|默认值| BOOLEAN |
|改后生效方式|重启服务生效|

* integer\_string\_infer\_type

|名字| integer\_string\_infer\_type |
|:---:|:---|
|描述| 整型字符串推断的数据类型 |
|取值| INT32, INT64, FLOAT, DOUBLE, TEXT |
|默认值| FLOAT |
|改后生效方式|重启服务生效|

* long\_string\_infer\_type

|名字| long\_string\_infer\_type |
|:---:|:---|
|描述| 大于 2 ^ 24 的整形字符串被推断的数据类型 |
|取值| DOUBLE, FLOAT or TEXT |
|默认值| DOUBLE |
|改后生效方式|重启服务生效|

* floating\_string\_infer\_type

|名字| floating\_string\_infer\_type |
|:---:|:---|
|描述| "6.7"等字符串被推断的数据类型 |
|取值| DOUBLE, FLOAT or TEXT |
|默认值| FLOAT |
|改后生效方式|重启服务生效|

* nan\_string\_infer\_type

|名字| nan\_string\_infer\_type |
|:---:|:---|
|描述| "NaN" 字符串被推断的数据类型 |
|取值| DOUBLE, FLOAT or TEXT |
|默认值| DOUBLE |
|改后生效方式|重启服务生效|

* default\_boolean\_encoding

|名字| default\_boolean\_encoding |
|:---:|:---|
|描述| BOOLEAN 类型编码格式 |
|取值| PLAIN, RLE |
|默认值| RLE |
|改后生效方式|重启服务生效|

* default\_int32\_encoding

|名字| default\_int32\_encoding |
|:---:|:---|
|描述| int32 类型编码格式 |
|取值| PLAIN, RLE, TS_2DIFF, REGULAR, GORILLA |
|默认值| RLE |
|改后生效方式|重启服务生效|

* default\_int64\_encoding

|名字| default\_int64\_encoding |
|:---:|:---|
|描述| int64 类型编码格式 |
|取值| PLAIN, RLE, TS_2DIFF, REGULAR, GORILLA |
|默认值| RLE |
|改后生效方式|重启服务生效|

* default\_float\_encoding

|名字| default\_float\_encoding |
|:---:|:---|
|描述| float 类型编码格式 |
|取值| PLAIN, RLE, TS_2DIFF, GORILLA |
|默认值| GORILLA |
|改后生效方式|重启服务生效|

* default\_double\_encoding

|名字| default\_double\_encoding |
|:---:|:---|
|描述| double 类型编码格式 |
|取值| PLAIN, RLE, TS_2DIFF, GORILLA |
|默认值| GORILLA |
|改后生效方式|重启服务生效|

* default\_text\_encoding

|名字| default\_text\_encoding |
|:---:|:---|
|描述| text 类型编码格式 |
|取值| PLAIN |
|默认值| PLAIN |
|改后生效方式|重启服务生效|

### 文件层配置

* group\_size\_in\_byte

|名字| group\_size\_in\_byte |
|:---:|:---|
|描述| 每次将内存中的数据写入到磁盘时的最大写入字节数 |
|类型| Int32 |
|默认值| 134217728 |
|改后生效方式|触发生效|

* page\_size\_in\_byte

|名字| page\_size\_in\_byte |
|:---:|:---|
|描述| 内存中每个列写出时，写成的单页最大的大小，单位为字节 |
|类型| Int32 |
|默认值| 65536 |
|改后生效方式|触发生效|

* max\_number\_of\_points\_in\_page

|名字| max\_number\_of\_points\_in\_page |
|:---:|:---|
|描述| 一个页中最多包含的数据点（时间戳-值的二元组）数量 |
|类型| Int32 |
|默认值| 1048576 |
|改后生效方式|触发生效|

* time\_series\_data\_type

|名字| time\_series\_data\_type |
|:---:|:---|
|描述| 数据类型配置，输入时间戳数据类型，支持INT32或INT64 |
|类型| String |
|默认值| INT64 |
|改后生效方式|触发生效|

* max\_string\_length

|名字| max\_string\_length |
|:---:|:---|
|描述| 针对字符串类型的数据，单个字符串最大长度，单位为字符|
|类型| Int32 |
|默认值| 128 |
|改后生效方式|触发生效|

* float\_precision

|名字| float\_precision |
|:---:|:---|
|描述| 浮点数精度，为小数点后数字的位数 |
|类型| Int32 |
|默认值| 默认为 2 位。注意：32 位浮点数的十进制精度为 7 位，64 位浮点数的十进制精度为 15 位。如果设置超过机器精度将没有实际意义。 |
|改后生效方式|触发生效|

* time\_encoder

|名字| time\_encoder |
|:---:|:---|
|描述| 时间列编码方式 |
|类型| 枚举 String: “TS_2DIFF”,“PLAIN”,“RLE” |
|默认值| TS_2DIFF |
|改后生效方式|触发生效|

* value\_encoder

|名字| value\_encoder |
|:---:|:---|
|描述| value 列编码方式 |
|类型| 枚举 String: “TS_2DIFF”,“PLAIN”,“RLE” |
|默认值| PLAIN |
|改后生效方式|触发生效|

* compressor

|名字| compressor |
|:---:|:---|
|描述| 数据压缩方法 |
|类型| 枚举 String : “UNCOMPRESSED”, “SNAPPY”, “LZ4” |
|默认值| SNAPPY |
|改后生效方式|触发生效|

* max\_degree\_of\_index\_node

|名字| max\_degree\_of\_index\_node |
|:---:|:---|
|描述| 元数据索引树的最大度（即每个节点的最大子节点个数）。 |
|类型| Int32 |
|默认值| 256 |
|改后生效方式|仅允许在第一次启动服务前修改|

* frequency\_interval\_in\_minute

|名字| frequency\_interval\_in\_minute |
|:---:|:---|
|描述| 计算查询频率的时间间隔（以分钟为单位）。 |
|类型| Int32 |
|默认值| 1 |
|改后生效方式|触发生效|

* slow\_query\_threshold

|名字| slow\_query\_threshold |
|:---:|:---|
|描述| 慢查询的时间成本（毫秒ms）阈值。 |
|类型| Int32 |
|默认值| 5000 |
|改后生效方式|触发生效|

### MQTT代理配置

* enable\_mqtt\_service

|名字| enable\_mqtt\_service。 |
|:---:|:---|
|描述| 是否开启MQTT服务 |
|类型| Boolean |
|默认值| false |
|改后生效方式|触发生效|

* mqtt\_host

|名字| mqtt\_host |
|:---:|:---|
|描述| MQTT服务绑定的host。 |
|类型| String |
|默认值| 0.0.0.0 |
|改后生效方式|触发生效|

* mqtt\_port

|名字| mqtt\_port |
|:---:|:---|
|描述| MQTT服务绑定的port。 |
|类型| Int32 |
|默认值| 1883 |
|改后生效方式|触发生效|

* mqtt\_handler\_pool\_size

|名字| mqtt\_handler\_pool\_size |
|:---:|:---|
|描述| 用于处理MQTT消息的处理程序池大小。 |
|类型| Int32 |
|默认值| 1 |
|改后生效方式|触发生效|

* mqtt\_payload\_formatter

|名字| mqtt\_payload\_formatter |
|:---:|:---|
|描述| MQTT消息有效负载格式化程序。 |
|类型| String |
|默认值| json |
|改后生效方式|触发生效|

* mqtt\_max\_message\_size

|名字| mqtt\_max\_message\_size |
|:---:|:---|
|描述| MQTT消息的最大长度（以字节为单位）。 |
|类型| Int32 |
|默认值| 1048576 |
|改后生效方式|触发生效|

### 授权配置

* authorizer\_provider\_class

|名字| authorizer\_provider\_class |
|:---:|:---|
|描述| 权限服务的类名 |
|类型| String |
|默认值| org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer |
|改后生效方式|重启服务生效|
|其他可选值| org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer |

* openID\_url

|名字| openID\_url |
|:---:|:---|
|描述| openID 服务器地址 （当 OpenIdAuthorizer 被启用时必须设定）|
|类型| String（一个 http 地址）|
|默认值| 无 |
|改后生效方式|重启服务生效|

### UDF查询配置

* udf\_initial\_byte\_array\_length\_for\_memory\_control

|名字| udf\_initial\_byte\_array\_length\_for\_memory\_control |
|:---:|:---|
|描述| 用于评估UDF查询中文本字段的内存使用情况。建议将此值设置为略大于所有文本的平均长度记录。 |
|类型| Int32 |
|默认值| 48 |
|改后生效方式|重启服务生效|

* udf\_memory\_budget\_in\_mb

|名字| udf\_memory\_budget\_in\_mb |
|:---:|:---|
|描述| 在一个UDF查询中使用多少内存（以 MB 为单位）。上限为已分配内存的 20% 用于读取。 |
|类型| Float |
|默认值| 30.0 |
|改后生效方式|重启服务生效|

* udf\_reader\_transformer\_collector\_memory\_proportion

|名字| udf\_reader\_transformer\_collector\_memory\_proportion |
|:---:|:---|
|描述| UDF内存分配比例。参数形式为a:b:c，其中a、b、c为整数。 |
|类型| String |
|默认值| 1:1:1 |
|改后生效方式|重启服务生效|

* udf\_root\_dir

|名字| udf\_root\_dir |
|:---:|:---|
|描述| 取消注释用以下字段来配置udf根目录。 |
|类型| String |
|默认值| ext/udf(Windows:ext\\udf) |
|改后生效方式|重启服务生效|

* index\_root\_dir

|名字| index\_root\_dir |
|:---:|:---|
|描述| 取消注释用以下字段来配置索引根目录。 |
|类型| String |
|默认值| data/index(Windows:data\\index) |
|改后生效方式|重启服务生效|

* enable\_index

|名字| enable\_index |
|:---:|:---|
|描述| 是否启用索引。 |
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

* concurrent\_index\_build\_thread

|名字| concurrent\_index\_build\_thread |
|:---:|:---|
|描述| 有多少线程可以同时构建索引。当<=0时，使用CPU内核数。 |
|类型| Int32 |
|默认值| 0 |
|改后生效方式|重启服务生效|

* default\_index\_window\_range

|名字| default\_index\_window\_range |
|:---:|:---|
|描述| 索引框架中用于子序列匹配的滑动窗口的默认大小。 |
|类型| Int32 |
|默认值| 10 |
|改后生效方式|重启服务生效|

* index\_buffer\_size

|名字| index\_buffer\_size |
|:---:|:---|
|描述| 索引处理器的缓冲区参数。 |
|类型| Int32 |
|默认值| 134217728 |
|改后生效方式|重启服务生效|

* enable\_partition

|名字| enable\_partition |
|:---:|:---|
|描述| 是否开启将数据按时间分区存储的功能，如果关闭，所有数据都属于分区 0|
|类型| Boolean |
|默认值| false |
|改后生效方式|仅允许在第一次启动服务前修改|

* partition\_interval

|名字| partition\_interval |
|:---:|:---|
|描述| 用于存储组分区的时间段长度，用户指定的存储组下会使用该时间段进行分区，单位：秒 |
|类型| Int64 |
|默认值| 604800 |
|改后生效方式|仅允许在第一次启动服务前修改|

* virtual\_storage\_group\_num

|名字| virtual\_storage\_group\_num |
|:---:|:---|
|描述| 每一个用户定义存储组下虚拟存储组的数量, 虚拟存储组是内存中写入的并行单位，每一个虚拟存储组内的写入请求是串行的，推荐值为： [virtual storage group number] = [CPU core number] / [user-defined storage group number]|
|类型| INT32 |
|默认值| 1 |
|改后生效方式|仅允许在第一次启动服务前修改|

* enable\_id\_table

|名字| enable\_id\_table |
|:---:|:---|
|描述| 是否开启ID表，加速写入时元数据访问，注意：ID表与别名(alias)不兼容 |
|类型| bool |
|默认值| false |
|改后生效方式|重启服务生效|

* device\_id\_transformation\_method

|名字| device\_id\_transformation\_method |
|:---:|:---|
|描述| 设备路径id化的方式，可以选择Plain或SHA256 |
|类型| string |
|默认值| Plain |
|改后生效方式|仅允许在第一次启动服务前修改|

* enable\_id\_table\_log\_file

|名字| enable\_id\_table\_log\_file |
|:---:|:---|
|描述| 是否开启ID表设备ID到设备名称记录文件，在使用SHA256映射时，若想保持tsfile的自解析能力，建议开启 |
|类型| bool |
|默认值| false |
|改后生效方式|重启服务生效|

* concurrent\_writing\_time\_partition

|名字| concurrent\_writing\_time\_partition |
|:---:|:---|
|描述| 最大可同时写入的时间分区个数，默认500个分区 |
|类型| Int64 |
|默认值| 500 |
|改后生效方式|重启服务生效|

* admin\_name

|名字| admin\_name |
|:---:|:---|
|描述| 管理员用户名，默认为root |
|类型| String |
|默认值| root |
|改后生效方式|仅允许在第一次启动服务前修改|

* admin\_password

|名字| admin\_password |
|:---:|:---|
|描述| 管理员密码，默认为root |
|类型| String |
|默认值| root |
|改后生效方式|仅允许在第一次启动服务前修改|


## 开启 GC 日志
GC 日志默认是关闭的。为了性能调优，用户可能会需要收集 GC 信息。
若要打开 GC 日志，则需要在启动 IoTDB Server 的时候加上"printgc"参数：

```bash
nohup sbin/start-server.sh printgc >/dev/null 2>&1 &
```
或者

```bash
sbin\start-server.bat printgc
```

GC 日志会被存储在`IOTDB_HOME/logs/gc.log`. 至多会存储 10 个 gc.log 文件，每个文件最多 10MB。
