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

# 配置手册

为方便IoTDB Server的配置与管理，IoTDB Server为用户提供三种配置项，使得用户可以在启动服务器或服务器运行时对其进行配置。

三种配置项的配置文件均位于IoTDB安装目录：`$IOTDB_HOME/conf`文件夹下,其中涉及server配置的共有2个文件，分别为：`iotdb-env.sh`, `iotdb-engine.properties`。用户可以通过更改其中的配置项对系统运行的相关配置项进行配置。

配置文件的说明如下：

* `iotdb-env.sh`：环境配置项的默认配置文件。用户可以在文件中配置JAVA-JVM的相关系统配置项。

* `iotdb-engine.properties`：IoTDB引擎层系统配置项的默认配置文件。用户可以在文件中配置IoTDB引擎运行时的相关参数，如JDBC服务监听端口(`rpc_port`)、overflow数据文件存储目录(`overflow_data_dir`)等。此外，用户可以在文件中配置IoTDB存储时TsFile文件的相关信息，如每次将内存中的数据写入到磁盘时的数据大小(`group_size_in_byte`)，内存中每个列打一次包的大小(`page_size_in_byte`)等。

## 热修改配置项

为方便用户使用，IoTDB Server为用户提供了热修改功能，即在系统运行过程中修改`iotdb-engine.properties`中部分配置参数并即时应用到系统中。下面介绍的参数中，改后
生效方式为`触发生效`的均为支持热修改的配置参数。

触发方式：客户端发送```load configuration```命令至IoTDB Server，客户端的使用方式详见第4章

## 环境配置项

环境配置项主要用于对IoTDB Server运行的Java环境相关参数进行配置，如JVM相关配置。IoTDB Server启动时，此部分配置会被传给JVM。用户可以通过查看 `iotdb-env.sh`(或`iotdb-env.bat`)文件查看环境配置项内容。详细配置项说明如下：

* JMX\_LOCAL

|名字|JMX\_LOCAL|
|:---:|:---|
|描述|JMX监控模式，配置为yes表示仅允许本地监控，设置为no的时候表示允许远程监控|
|类型|枚举String : “yes”, “no”|
|默认值|yes|
|改后生效方式|重启服务器生效|


* JMX\_PORT

|名字|JMX\_PORT|
|:---:|:---|
|描述|JMX监听端口。请确认该端口不是系统保留端口并且未被占用。|
|类型|Short Int: [0,65535]|
|默认值|31999|
|改后生效方式|重启服务器生效|

* MAX\_HEAP\_SIZE

|名字|MAX\_HEAP\_SIZE|
|:---:|:---|
|描述|IoTDB启动时能使用的最大堆内存大小。|
|类型|String|
|默认值|取决于操作系统和机器配置。在Linux或MacOS系统下默认为机器内存的四分之一。在Windows系统下，32位系统的默认值是512M，64位系统默认值是2G。|
|改后生效方式|重启服务器生效|

* HEAP\_NEWSIZE

|名字|HEAP\_NEWSIZE|
|:---:|:---|
|描述|IoTDB启动时能使用的最小堆内存大小。|
|类型|String|
|默认值|取决于操作系统和机器配置。在Linux或MacOS系统下默认值为机器CPU核数乘以100M的值与MAX\_HEAP\_SIZE四分之一这二者的最小值。在Windows系统下，32位系统的默认值是512M，64位系统默认值是2G。。|
|改后生效方式|重启服务器生效|

## 系统配置项

系统配置项是IoTDB Server运行的核心配置，它主要用于设置IoTDB Server文件层和引擎层的参数，便于用户根据自身需求调整Server的相关配置，以达到较好的性能表现。系统配置项可分为两大模块：文件层配置项和引擎层配置项。用户可以通过查看`iotdb-engine.properties`,文件查看和修改两种配置项的内容。在0.7.0版本中字符串类型的配置项大小写敏感。

### 文件层配置

* compressor

|名字|compressor|
|:---:|:---|
|描述|数据压缩方法|
|类型|枚举String : “UNCOMPRESSED”, “SNAPPY”|
|默认值| UNCOMPRESSED |
|改后生效方式|触发生效|

* group\_size\_in\_byte

|名字|group\_size\_in\_byte|
|:---:|:---|
|描述|每次将内存中的数据写入到磁盘时的最大写入字节数|
|类型|Int32|
|默认值| 134217728 |
|改后生效方式|触发生效|

* max\_number\_of\_points\_in\_page

|名字| max\_number\_of\_points\_in\_page |
|:---:|:---|
|描述|一个页中最多包含的数据点（时间戳-值的二元组）数量|
|类型|Int32|
|默认值| 1048576 |
|改后生效方式|触发生效|

* max\_degree\_of\_index\_node

|名字| max\_degree\_of\_index\_node |
|:---:|:---|
|描述|元数据索引树的最大度（即每个节点的最大子节点个数）|
|类型|Int32|
|默认值| 1024 |
|改后生效方式|仅允许在第一次启动服务器前修改|

* max\_string\_length

|名字| max\_string\_length |
|:---:|:---|
|描述|针对字符串类型的数据，单个字符串最大长度，单位为字符|
|类型|Int32|
|默认值| 128 |
|改后生效方式|触发生效|

* page\_size\_in\_byte

|名字| page\_size\_in\_byte |
|:---:|:---|
|描述|内存中每个列写出时，写成的单页最大的大小，单位为字节|
|类型|Int32|
|默认值| 65536 |
|改后生效方式|触发生效|

* time\_series\_data\_type

|名字| time\_series\_data\_type |
|:---:|:---|
|描述|时间戳数据类型|
|类型|枚举String: "INT32", "INT64"|
|默认值| Int64 |
|改后生效方式|触发生效|

* time\_encoder

|名字| time\_encoder |
|:---:|:---|
|描述| 时间列编码方式|
|类型|枚举String: “TS_2DIFF”,“PLAIN”,“RLE”|
|默认值| TS_2DIFF |
|改后生效方式|触发生效|

* float_precision

|名字| float_precision |
|:---:|:---|
|描述| 浮点数精度，为小数点后数字的位数 |
|类型|Int32|
|默认值| 默认为2位。注意：32位浮点数的十进制精度为7位，64位浮点数的十进制精度为15位。如果设置超过机器精度将没有实际意义。|
|改后生效方式|触发生效|


* bloomFilterErrorRate

|名字| bloomFilterErrorRate |
|:---:|:---|
|描述| bloom过滤器的误报率. 在加载元数据之前 Bloom filter 可以检查给定的时间序列是否在 TsFile 中。这可以优化加载元数据的性能，并跳过不包含指定时间序列的 TsFile。如果你想了解更多关于它的细节，你可以参考: [wiki page of bloom filter](https://en.wikipedia.org/wiki/Bloom_filter).|
|类型|浮点数, 范围为(0, 1)|
|默认值| 0.05 |
|改后生效方式|重启生效|




### 引擎层配置

* data\_dirs

|名字| data\_dirs |
|:---:|:---|
|描述| IoTDB数据存储路径，默认存放在和bin目录同级的data目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。|
|类型|String|
|默认值| data |
|改后生效方式|触发生效|

* enable\_wal

|名字| enable\_wal |
|:---:|:---|
|描述| 是否开启写前日志，默认值为true表示开启，配置成false表示关闭 |
|类型|Bool|
|默认值| true |
|改后生效方式|触发生效|

* tag\_attribute\_total\_size

|名字| tag\_attribute\_total\_size |
|:---:|:---|
|描述| 每个时间序列标签和属性的最大持久化字节数|
|类型| Int32 |
|默认值| 700 |
|改后生效方式|仅允许在第一次启动服务器前修改|

* enable\_partial\_insert

|名字| enable\_partial\_insert |
|:---:|:---|
|描述| 在一次insert请求中，如果部分测点写入失败，是否继续写入其他测点|
|类型| Bool |
|默认值| true |
|改后生效方式|重启服务器生效|

* mtree\_snapshot\_interval

|名字| mtree\_snapshot\_interval |
|:---:|:---|
|描述| 创建 MTree snapshot 时至少累积的 mlog 日志行数。单位为日志行数|
|类型| Int32 |
|默认值| 100000 |
|改后生效方式|重启服务器生效|

* fetch\_size

|名字| fetch\_size |
|:---:|:---|
|描述| 批量读取数据的时候，每一次读取数据的数量。单位为数据条数，即不同时间戳的个数。某次会话中，用户可以在使用时自己设定，此时仅在该次会话中生效。|
|类型|Int32|
|默认值| 10000 |
|改后生效方式|重启服务器生效|

* force\_wal\_period\_in\_ms

|名字| force\_wal\_period\_in\_ms |
|:---:|:---|
|描述| 写前日志定期刷新到磁盘的周期，单位毫秒，有可能丢失至多flush\_wal\_period\_in\_ms毫秒的操作。 |
|类型|Int32|
|默认值| 10 |
|改后生效方式|触发生效|

* flush\_wal\_threshold

|名字| flush\_wal\_threshold |
|:---:|:---|
|描述| 写前日志的条数达到该值之后，刷新到磁盘，有可能丢失至多flush\_wal\_threshold个操作 |
|类型|Int32|
|默认值| 10000 |
|改后生效方式|触发生效|

* merge\_concurrent\_threads

|名字| merge\_concurrent\_threads |
|:---:|:---|
|描述| 乱序数据进行合并的时候最多可以用来进行merge的线程数。值越大，对IO和CPU消耗越多。值越小，当乱序数据过多时，磁盘占用量越大，读取会变慢。 |
|类型|Int32|
|默认值| 0 |
|改后生效方式|重启服务器生效|

* enable\_mem\_comtrol

|Name| enable\_mem\_control |
|:---:|:---|
|Description| 开启内存控制，避免爆内存|
|Type|Bool|
|Default| true |
|Effective|重启服务器生效|

* enable\_mem\_comtrol

|Name| enable\_mem\_control |
|:---:|:---|
|Description| 开启内存控制，避免爆内存|
|Type|Bool|
|Default| true |
|Effective|重启服务器生效|

* memtable\_size\_threshold

|Name| memtable\_size\_threshold |
|:---:|:---|
|Description| 内存缓冲区 memtable 阈值|
|Type|Long|
|Default| 1073741824 |
|Effective|enable\_mem\_control为false时生效、重启服务器生效|

* avg\_series\_point\_number\_threshold

|Name| avg\_series\_point\_number\_threshold |
|:---:|:---|
|Description| 内存中平均每个时间序列点数最大值，达到触发flush|
|Type|Int32|
|Default| 10000 |
|Effective|重启服务器生效|

* tsfile\_size\_threshold

|Name| tsfile\_size\_threshold |
|:---:|:---|
|Description| 每个 tsfile 大小|
|Type|Long|
|Default| 536870912 |
|Effective| 重启服务器生效|

* enable\_partition

|Name| enable\_partition |
|:---:|:---|
|Description| 是否开启将数据按时间分区存储的功能，如果关闭，所有数据都属于分区 0|
|Type|Bool|
|Default| false |
|Effective|仅允许在第一次启动服务器前修改|

* partition\_interval

|名字| partition\_interval |
|:---:|:---|
|描述| 用于存储组分区的时间段长度，用户指定的存储组下会使用该时间段进行分区，单位：秒 |
|类型|Int64|
|默认值| 604800 |
|改后生效方式|仅允许在第一次启动服务器前修改|

* memtable\_num\_in\_each\_storage\_group

|名字| memtable\_num\_in\_each\_storage\_group|
|:---:|:---|
|描述| 每个存储组所控制的memtable的最大数量，这决定了来源于多少个不同时间分区的数据可以并发写入<br>举例来说，你的时间分区为按天分区，想要同时并发写入3天的数据，那么这个值应该被设置为6(3个给顺序写入，3个给乱序写入)|
|类型|Int32|
|默认值| 10 |
|改后生效方式|重启服务器生效|

* multi\_dir\_strategy

|名字| multi\_dir\_strategy |
|:---:|:---|
|描述| IoTDB在tsfile\_dir中为TsFile选择目录时采用的策略。可使用简单类名或类名全称。系统提供以下三种策略：<br>1. SequenceStrategy：IoTDB按顺序从tsfile\_dir中选择目录，依次遍历tsfile\_dir中的所有目录，并不断轮循；<br>2. MaxDiskUsableSpaceFirstStrategy：IoTDB优先选择tsfile\_dir中对应磁盘空余空间最大的目录；<br>3. MinFolderOccupiedSpaceFirstStrategy：IoTDB优先选择tsfile\_dir中已使用空间最小的目录；<br>4. UserDfineStrategyPackage（用户自定义策略）<br>您可以通过以下方法完成用户自定义策略：<br>1. 继承cn.edu.tsinghua.iotdb.conf.directories.strategy.DirectoryStrategy类并实现自身的Strategy方法；<br>2. 将实现的类的完整类名（包名加类名，UserDfineStrategyPackage）填写到该配置项；<br>3. 将该类jar包添加到工程中。 |
|类型|String|
|默认值| MaxDiskUsableSpaceFirstStrategy |
|改后生效方式|触发生效|

* rpc_address

|名字| rpc_address |
|:---:|:---|
|描述| |
|类型|String|
|默认值| "0.0.0.0" |
|改后生效方式|重启服务器生效|

* rpc_port

|名字| rpc_port |
|:---:|:---|
|描述|jdbc服务监听端口。请确认该端口不是系统保留端口并且未被占用。|
|类型|Short Int : [0,65535]|
|默认值| 6667 |
|改后生效方式|重启服务器生效||

* time\_zone

|名字| time_zone |
|:---:|:---|
|描述| 服务器所处的时区，默认为北京时间（东八区） |
|类型|Time Zone String|
|默认值| +08:00 |
|改后生效方式|触发生效|

* enable\_stat\_monitor

|名字| enable\_stat\_monitor |
|:---:|:---|
|描述| 选择是否启动后台统计功能|
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务器生效|

* concurrent\_flush\_thread

|名字| concurrent\_flush\_thread |
|:---:|:---|
|描述| 当IoTDB将内存中的数据写入磁盘时，最多启动多少个线程来执行该操作。如果该值小于等于0，那么采用机器所安装的CPU核的数量。默认值为0。|
|类型| Int32 |
|默认值| 0 |
|改后生效方式|重启服务器生效|

* tsfile\_storage\_fs

|名字| tsfile\_storage\_fs |
|:---:|:---|
|描述| Tsfile和相关数据文件的存储文件系统。目前支持LOCAL（本地文件系统）和HDFS两种|
|类型| String |
|默认值|LOCAL |
|改后生效方式|仅允许在第一次启动服务器前修改|

* core\_site\_path

|Name| core\_site\_path |
|:---:|:---|
|描述| 在Tsfile和相关数据文件存储到HDFS的情况下用于配置core-site.xml的绝对路径|
|类型| String |
|默认值|/etc/hadoop/conf/core-site.xml |
|改后生效方式|重启服务器生效|

* hdfs\_site\_path

|Name| hdfs\_site\_path |
|:---:|:---|
|描述| 在Tsfile和相关数据文件存储到HDFS的情况下用于配置hdfs-site.xml的绝对路径|
|类型| String |
|默认值|/etc/hadoop/conf/hdfs-site.xml |
|改后生效方式|重启服务器生效|

* hdfs\_ip

|名字| hdfs\_ip |
|:---:|:---|
|描述| 在Tsfile和相关数据文件存储到HDFS的情况下用于配置HDFS的IP。**如果配置了多于1个hdfs\_ip，则表明启用了Hadoop HA**|
|类型| String |
|默认值|localhost |
|改后生效方式|重启服务器生效|

* hdfs\_port

|名字| hdfs\_port |
|:---:|:---|
|描述| 在Tsfile和相关数据文件存储到HDFS的情况下用于配置HDFS的端口|
|类型| String |
|默认值|9000 |
|改后生效方式|重启服务器生效|

* dfs\_nameservices

|名字| hdfs\_nameservices |
|:---:|:---|
|描述| 在使用Hadoop HA的情况下用于配置HDFS的nameservices|
|类型| String |
|默认值|hdfsnamespace |
|改后生效方式|重启服务器生效|

* dfs\_ha\_namenodes

|名字| hdfs\_ha\_namenodes |
|:---:|:---|
|描述| 在使用Hadoop HA的情况下用于配置HDFS的nameservices下的namenodes|
|类型| String |
|默认值|nn1,nn2 |
|改后生效方式|重启服务器生效|

* dfs\_ha\_automatic\_failover\_enabled

|名字| dfs\_ha\_automatic\_failover\_enabled |
|:---:|:---|
|描述| 在使用Hadoop HA的情况下用于配置是否使用失败自动切换|
|类型| Boolean |
|默认值|true |
|改后生效方式|重启服务器生效|

* dfs\_client\_failover\_proxy\_provider

|名字| dfs\_client\_failover\_proxy\_provider |
|:---:|:---|
|描述| 在使用Hadoop HA且使用失败自动切换的情况下配置失败自动切换的实现方式|
|类型| String |
|默认值|org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider |
|改后生效方式|重启服务器生效|

* hdfs\_use\_kerberos

|名字| hdfs\_use\_kerberos |
|:---:|:---|
|描述| 是否使用kerberos验证访问hdfs|
|类型| String |
|默认值|false |
|改后生效方式|重启服务器生效|

* kerberos\_keytab\_file_path

|名字| kerberos\_keytab\_file_path |
|:---:|:---|
|描述| kerberos keytab file 的完整路径|
|类型| String |
|默认值|/path |
|改后生效方式|重启服务器生效|

* kerberos\_principal

|名字| kerberos\_principal |
|:---:|:---|
|描述| Kerberos 认证原则|
|类型| String |
|默认值|your principal |
|改后生效方式|重启服务器生效|


* authorizer\_provider\_class

|名字| authorizer\_provider\_class |
|:---:|:---|
|描述| 权限服务的类名|
|类型| String |
|默认值|org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer |
|改后生效方式|重启服务器生效|
|其他可选值| org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer |

* openID\_url

|名字| openID\_url |
|:---:|:---|
|描述| openID 服务器地址 （当OpenIdAuthorizer被启用时必须设定）|
|类型| String （一个http地址） |
|默认值| 无 |
|改后生效方式|重启服务器生效|


## 数据类型自动推断


* enable\_auto\_create\_schema

|名字| enable\_auto\_create\_schema |
|:---:|:---|
|描述| 当写入的序列不存在时，是否自动创建序列到Schema|
|取值| true or false |
|默认值|true |
|改后生效方式|重启服务器生效|

* default\_storage\_group\_level

|名字| default\_storage\_group\_level |
|:---:|:---|
|描述| 当写入的数据不存在且自动创建序列时，若需要创建相应的存储组，将序列路径的哪一层当做存储组. 例如, 如果我们接到一个新序列 root.sg0.d1.s2, 并且level=1， 那么root.sg0被视为存储组（因为root是level 0 层）|
|取值| 整数 |
|默认值|1 |
|改后生效方式|重启服务器生效|

* boolean\_string\_infer\_type

|名字| boolean\_string\_infer\_type |
|:---:|:---|
|描述|  "true" 或者 "false" 被视为什么数据|
|取值| BOOLEAN 或者 TEXT |
|默认值|BOOLEAN |
|改后生效方式|重启服务器生效|

* integer\_string\_infer\_type

|名字| integer\_string\_infer\_type |
|:---:|:---|
|描述| 整数型数据被推断成什么 |
|取值| INT32, INT64, FLOAT, DOUBLE, TEXT |
|默认值|FLOAT |
|改后生效方式|重启服务器生效|

* nan\_string\_infer\_type

|名字| nan\_string\_infer\_type |
|:---:|:---|
|描述| NaN 字符串被推断为什么|
|取值| DOUBLE, FLOAT or TEXT |
|默认值|FLOAT |
|改后生效方式|重启服务器生效|

* floating\_string\_infer\_type

|名字| floating\_string\_infer\_type |
|:---:|:---|
|描述| "6.7"等浮点数被推断为什么|
|取值| DOUBLE, FLOAT or TEXT |
|默认值|FLOAT |
|改后生效方式|重启服务器生效|


## 开启GC日志
GC日志默认是关闭的。为了性能调优，用户可能会需要收集GC信息。
若要打开GC日志，则需要在启动IoTDB Server的时候加上"printgc"参数：

```bash
nohup sbin/start-server.sh printgc >/dev/null 2>&1 &
```
或者

```bash
sbin\start-server.bat printgc
```

GC日志会被存储在`IOTDB_HOME/logs/gc.log`. 至多会存储10个gc.log文件，每个文件最多10MB。

