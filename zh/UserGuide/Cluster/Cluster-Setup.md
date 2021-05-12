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

__集群模式目前是测试版！请谨慎在生产环境中使用。__

# 集群设置
安装环境请参考[快速上手/安装环境章节](../QuickStart/QuickStart.md)
## 前提条件
如果您在使用Windows系统，请安装MinGW，WSL或者git bash。
## 集群环境搭建
您可以搭建伪分布式模式或是分布式模式的集群，伪分布式模式和分布式模式的主要区别是配置文件中`seed_nodes`的不同，配置项含义请参考[配置项](#配置项)。
启动其中一个节点的服务，需要执行如下命令：

```bash
# Unix/OS X
> nohup sbin/start-node.sh [printgc] [<conf_path>] >/dev/null 2>&1 &

# Windows
> sbin\start-node.bat [printgc] [<conf_path>] 
```
`printgc`表示在启动的时候，会开启GC日志。
`<conf_path>`使用`conf_path`文件夹里面的配置文件覆盖默认配置文件。

## 被覆盖的单机版选项

iotdb-engines.properties配置文件中的部分内容会不再生效：

* `enable_auto_create_schema` 不再生效，并被视为`false`. 应使用 iotdb-cluster.properties 中的  
`enable_auto_create_schema` 来控制是否自动创建序列。


* `is_sync_enable` 不再生效，并被视为 `false`.


## 配置项

为方便IoTDB Server的配置与管理，IoTDB Server为用户提供三种配置项，使得您可以在启动服务或服务运行时对其进行配置。

三种配置项的配置文件均位于IoTDB安装目录：`$IOTDB_HOME/conf`文件夹下,其中涉及server配置的共有4个文件，分别为：`iotdb-cluster.properties`、`iotdb-engine.properties`、`logback.xml` 和 `iotdb-env.sh`(Unix系统)/`iotdb-env.bat`(Windows系统), 您可以通过更改其中的配置项对系统运行的相关配置项进行配置。

配置文件的说明如下：

* `iotdb-env.sh`/`iotdb-env.bat`：环境配置项的默认配置文件。您可以在文件中配置JAVA-JVM的相关系统配置项。

* `iotdb-engine.properties`：IoTDB引擎层系统配置项的默认配置文件。您可以在文件中配置IoTDB引擎运行时的相关参数。此外，用户可以在文件中配置IoTDB存储时TsFile文件的相关信息，如每次将内存中的数据写入到磁盘前的缓存大小(`group_size_in_byte`)，内存中每个列打一次包的大小(`page_size_in_byte`)等。

* `logback.xml`: 日志配置文件，比如日志级别等。

* `iotdb-cluster.properties`: IoTDB集群所需要的一些配置。

`iotdb-engine.properties`、`iotdb-env.sh`/`iotdb-env.bat` 两个配置文件详细说明请参考[附录/配置手册](../Appendix/Config-Manual.md)，下面描述的配置项是在`iotdb-cluster.properties`文件中的，也可以直接查看[配置文件](https://github.com/apache/iotdb/blob/master/cluster/src/assembly/resources/conf/iotdb-cluster.properties) 中的注释。

* internal\_ip

|名字|internal\_ip|
|:---:|:---|
|描述|IOTDB 集群各个节点之间内部通信的IP地址，比如心跳、snapshot快照、raft log等|
|类型|String|
|默认值|127.0.0.1|
|改后生效方式|重启服务生效，集群建立后不可再修改|

* internal\_meta\_port

|名字|internal\_meta\_port|
|:---:|:---|
|描述|IoTDB meta服务端口，用于元数据组（又称集群管理组）通信，元数据组管理集群配置和存储组信息**IoTDB将为每个meta服务自动创建心跳端口。默认meta服务心跳端口为`internal_meta_port+1`，请确认这两个端口不是系统保留端口并且未被占用**|
|类型|Int32|
|默认值|9003|
|改后生效方式|重启服务生效，集群建立后不可再修改|

* internal\_data\_port

|名字|internal\_data\_port|
|:---:|:---|
|描述|IoTDB data服务端口，用于数据组通信，数据组管理数据模式和数据的存储**IoTDB将为每个data服务自动创建心跳端口。默认的data服务心跳端口为`internal_data_port+1`。请确认这两个端口不是系统保留端口并且未被占用**|
|类型|Int32|
|默认值|40010|
|改后生效方式|重启服务生效，集群建立后不可再修改|

* cluster\_info\_public\_port

|名字|cluster\_info\_public\_port|
|:---:|:---|
|描述|用于查看集群信息（如数据分区）的RPC服务的接口|
|类型|Int32|
|默认值|6567|
|改后生效方式| 重启服务生效|

* open\_server\_rpc\_port

|名字|open\_server\_rpc\_port|
|:---:|:---|
|描述|是否打开单机模块的rpc port，用于调试模式，如果设置为true，则单机模块的rpc port设置为`rpc_port (in iotdb-engines.properties) + 1`|
|类型|Boolean|
|默认值|false|
|改后生效方式|重启服务生效，集群建立后不可再修改|

* seed\_nodes

|名字|seed\_nodes|
|:---:|:---|
|描述|集群中节点的地址，`{IP/DOMAIN}:internal_meta_port`格式，用逗号分割；对于伪分布式模式，可以都填写`localhost`，或是`127.0.0.1` 或是混合填写，但是不能够出现真实的ip地址；对于分布式模式，支持填写real ip 或是hostname，但是不能够出现`localhost`或是`127.0.0.1`。当使用`start-node.sh(.bat)`启动节点时，此配置意味着形成初始群集的节点，每个节点的`seed_nodes`应该一致，否则群集将初始化失败；当使用`add-node.sh(.bat)`添加节点到集群中时，此配置项可以是集群中已经存在的任何节点，不需要是用`start-node.sh(bat)`构建初始集群的节点。|
|类型|String|
|默认值|127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007|
|改后生效方式|重启服务生效|

* rpc\_thrift\_compression\_enable

|名字|rpc\_thrift\_compression\_enable|
|:---:|:---|
|描述|是否开启thrift压缩通信，**注意这个参数要各个节点保持一致，也要与客户端保持一致，同时也要与`iotdb-engine.properties`中`rpc_thrift_compression_enable`参数保持一致**|
|类型| Boolean|
|默认值|false|
|改后生效方式|重启服务生效，需要整个集群同时更改|

* default\_replica\_num

|名字|default\_replica\_num|
|:---:|:---|
|描述|集群副本数|
|类型|Int32|
|默认值|3|
|改后生效方式|重启服务生效，集群建立后不可更改|

* cluster\_name

|名字|cluster\_name|
|:---:|:---|
|描述|集群名称，集群名称用以标识不同的集群，**一个集群中所有节点的cluster_name都应相同**|
|类型|String|
|默认值|default|
|改后生效方式|重启服务生效|

* connection\_timeout\_ms

|名字|connection\_timeout\_ms|
|:---:|:---|
|描述|同一个raft组各个节点之间的心跳超时时间，单位毫秒|
|类型|Int32|
|默认值|20000|
|改后生效方式|重启服务生效|

* read\_operation\_timeout\_ms

|名字|read\_operation\_timeout\_ms|
|:---:|:---|
|描述|读取操作超时时间，仅用于内部通信，不适用于整个操作，单位毫秒|
|类型|Int32|
|默认值|30000|
|改后生效方式|重启服务生效|

* write\_operation\_timeout\_ms

|名字|write\_operation\_timeout\_ms|
|:---:|:---|
|描述|写入操作超时时间，仅用于内部通信，不适用于整个操作，单位毫秒|
|类型|Int32|
|默认值|30000|
|改后生效方式|重启服务生效|

* min\_num\_of\_logs\_in\_mem

|名字|min\_num\_of\_logs\_in\_mem|
|:---:|:---|
|描述|删除日志操作执行后，内存中保留的最多的提交的日志的数量。增大这个值将减少在CatchUp使用快照的机会，但也会增加内存占用量|
|类型|Int32|
|默认值|100|
|改后生效方式|重启服务生效|

* max\_num\_of\_logs\_in\_mem

|名字|max\_num\_of\_logs\_in\_mem|
|:---:|:---|
|描述|当内存中已提交的日志条数达到这个值之后，就会触发删除日志的操作，增大这个值将减少在CatchUp使用快照的机会，但也会增加内存占用量|
|类型|Int32|
|默认值|1000|
|改后生效方式|重启服务生效|

* log\_deletion\_check\_interval\_second

|名字|log\_deletion\_check\_interval\_second|
|:---:|:---|
|描述|检查删除日志任务的时间间隔，每次删除日志任务将会把已提交日志超过min\_num\_of\_logs\_in\_mem条的最老部分删除，单位秒|
|类型|Int32|
|默认值|60|
|改后生效方式|重启服务生效|

* enable\_auto\_create\_schema

|名字|enable\_auto\_create\_schema|
|:---:|:---|
|描述|是否支持自动创建schema，**这个值会覆盖`iotdb-engine.properties`中`enable_auto_create_schema`的配置**|
|类型|BOOLEAN|
|默认值|true|
|改后生效方式|重启服务生效|

* consistency\_level

|名字|consistency\_level|
|:---:|:---|
|描述|读一致性，目前支持3种一致性：strong、mid、weak。strong consistency每次操作都会尝试与Leader同步以获取最新的数据，如果失败（超时），则直接向用户返回错误； mid consistency每次操作将首先尝试与Leader进行同步，但是如果失败（超时），它将使用本地当前数据向用户提供服务； weak consistency不会与Leader进行同步，而只是使用本地数据向用户提供服务|
|类型|strong、mid、weak|
|默认值|mid|
|改后生效方式|重启服务生效|

* is\_enable\_raft\_log\_persistence

|名字|is\_enable\_raft\_log\_persistence|
|:---:|:---|
|描述|是否开启raft log持久化|
|类型|BOOLEAN|
|默认值|true|
|改后生效方式|重启服务生效|

## 开启GC日志
GC日志默认是关闭的。为了性能调优，用户可能会需要收集GC信息。
若要打开GC日志，则需要在启动IoTDB Server的时候加上`printgc`参数：

```bash
nohup sbin/start-node.sh printgc >/dev/null 2>&1 &
```
或者

```bash
sbin\start-node.bat printgc
```

GC日志会被存储在`IOTDB_HOME/logs/`下面。
