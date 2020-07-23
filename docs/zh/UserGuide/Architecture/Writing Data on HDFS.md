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

# 使用HDFS存储数据

## 存储共享架构

当前，TSFile（包括TSFile文件和相关的数据文件）支持存储在本地文件系统和Hadoop分布式文件系统（HDFS）。配置使用HDFS存储TSFile十分容易。

### 系统架构

当你配置使用HDFS存储TSFile之后，你的数据文件将会被分布式存储。系统架构如下：

<img style="width:100%; max-width:700px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/66922722-35180400-f05a-11e9-8ff0-7dd51716e4a8.png">

### Config and usage

如果你希望将TSFile存储在HDFS上，可以遵循以下步骤：

首先下载对应版本的源码发布版或者下载 github 仓库，发布版代码的 tag 为 release/x.x.x

使用maven打包server和Hadoop模块：`mvn clean package -pl server,hadoop -am -Dmaven.test.skip=true`

然后，将Hadoop模块的target jar包`hadoop-tsfile-0.10.0-jar-with-dependencies.jar`复制到server模块的target lib文件夹 `.../server/target/iotdb-server-0.10.0/lib`下。

编辑`iotdb-engine.properties`中的用户配置。相关配置项包括：

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
|改后生效方式|重启服务器生效

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

启动server, Tsfile将会被存储到HDFS上。

如果你想要恢复将TSFile存储到本地文件系统，只需编辑配置项`tsfile_storage_fs`为`LOCAL`。在这种情况下，如果你已经在HDFS上存储了一些数据文件，你需要将它们下载到本地，并移动到你所配置的数据文件文件夹（默认为`../server/target/iotdb-server-0.10.0/data/data`）， 或者重新开始你的整个导入数据过程。

### 常见问题

1. 这个功能支持哪些Hadoop版本?

A: Hadoop 2.x and Hadoop 3.x均可以支持。

2. 当启动服务器或创建时间序列时，我遇到了如下错误：
```
ERROR org.apache.iotdb.tsfile.fileSystem.fsFactory.HDFSFactory:62 - Failed to get Hadoop file system. Please check your dependency of Hadoop module.
```

A: 这表明你没有将Hadoop模块的以来放到IoTDB server中。你可以这样解决：
* 使用Maven打包Hadoop模块: `mvn clean package -pl hadoop -am -Dmaven.test.skip=true`
* 将Hadoop模块的target jar包`hadoop-tsfile-0.10.0-jar-with-dependencies.jar`复制到server模块的target lib文件夹 `.../server/target/iotdb-server-0.10.0/lib`下。
