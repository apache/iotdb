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

## Integration with HDFS

### Shared Storage Architecture

Currently, TsFiles(including both TsFile and related data files) are supported to be stored in local file system and hadoop distributed file system(HDFS). It is very easy to config the storage file system of TSFile.

#### System architecture

When you config to store TSFile on HDFS, your data files will be in distributed storage. The system architecture is as below:

<img style="width:100%; max-width:700px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/66922722-35180400-f05a-11e9-8ff0-7dd51716e4a8.png">

#### Config and usage

To store TSFile and related data files in HDFS, here are the steps:

First, download the source release from website or git clone the repository

Build server and Hadoop module by: `mvn clean package -pl server,hadoop -am -Dmaven.test.skip=true -P get-jar-with-dependencies`

Then, copy the target jar of Hadoop module `hadoop-tsfile-X.X.X-jar-with-dependencies.jar` into server target lib folder `.../server/target/iotdb-server-X.X.X/lib`.

Edit user config in `iotdb-datanode.properties`. Related configurations are:

* tsfile\_storage\_fs

|Name| tsfile\_storage\_fs |
|:---:|:---|
|Description| The storage file system of Tsfile and related data files. Currently LOCAL file system and HDFS are supported.|
|Type| String |
|Default|LOCAL |
|Effective|Only allowed to be modified in first start up|

* core\_site\_path

|Name| core\_site\_path |
|:---:|:---|
|Description| Absolute file path of core-site.xml if Tsfile and related data files are stored in HDFS.|
|Type| String |
|Default|/etc/hadoop/conf/core-site.xml |
|Effective|After restart system|

* hdfs\_site\_path

|Name| hdfs\_site\_path |
|:---:|:---|
|Description| Absolute file path of hdfs-site.xml if Tsfile and related data files are stored in HDFS.|
|Type| String |
|Default|/etc/hadoop/conf/hdfs-site.xml |
|Effective|After restart system|

* hdfs\_ip

|Name| hdfs\_ip |
|:---:|:---|
|Description| IP of HDFS if Tsfile and related data files are stored in HDFS. **If there are more than one hdfs\_ip in configuration, Hadoop HA is used.**|
|Type| String |
|Default|localhost |
|Effective|After restart system|

* hdfs\_port

|Name| hdfs\_port |
|:---:|:---|
|Description| Port of HDFS if Tsfile and related data files are stored in HDFS|
|Type| String |
|Default|9000 |
|Effective|After restart system|

* dfs\_nameservices

|Name| hdfs\_nameservices |
|:---:|:---|
|Description| Nameservices of HDFS HA if using Hadoop HA|
|Type| String |
|Default|hdfsnamespace |
|Effective|After restart system|

* dfs\_ha\_namenodes

|Name| hdfs\_ha\_namenodes |
|:---:|:---|
|Description| Namenodes under DFS nameservices of HDFS HA if using Hadoop HA|
|Type| String |
|Default|nn1,nn2 |
|Effective|After restart system|

* dfs\_ha\_automatic\_failover\_enabled

|Name| dfs\_ha\_automatic\_failover\_enabled |
|:---:|:---|
|Description| Whether using automatic failover if using Hadoop HA|
|Type| Boolean |
|Default|true |
|Effective|After restart system|

* dfs\_client\_failover\_proxy\_provider

|Name| dfs\_client\_failover\_proxy\_provider |
|:---:|:---|
|Description| Proxy provider if using Hadoop HA and enabling automatic failover|
|Type| String |
|Default|org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider |
|Effective|After restart system|

* hdfs\_use\_kerberos

|Name| hdfs\_use\_kerberos |
|:---:|:---|
|Description| Whether use kerberos to authenticate hdfs|
|Type| String |
|Default|false |
|Effective|After restart system|

* kerberos\_keytab\_file_path

|Name| kerberos\_keytab\_file_path |
|:---:|:---|
|Description| Full path of kerberos keytab file|
|Type| String |
|Default|/path |
|Effective|After restart system|

* kerberos\_principal

|Name| kerberos\_principal |
|:---:|:---|
|Description| Kerberos pricipal|
|Type| String |
|Default|your principal |
|Effective|After restart system|

Start server, and Tsfile will be stored on HDFS.

To reset storage file system to local, just edit configuration `tsfile_storage_fs` to `LOCAL`. In this situation, if data files are already on HDFS, you should either download them to local and move them to your config data file folder (`../server/target/iotdb-server-X.X.X/data/data` by default), or restart your process and import data to IoTDB.

#### Frequent questions

1. What Hadoop version does it support?

A: Both Hadoop 2.x and Hadoop 3.x can be supported.

2. When starting the server or trying to create timeseries, I encounter the error below:
```
ERROR org.apache.iotdb.tsfile.fileSystem.fsFactory.HDFSFactory:62 - Failed to get Hadoop file system. Please check your dependency of Hadoop module.
```

A: It indicates that you forget to put Hadoop module dependency in IoTDB server. You can solve it by:
* Build Hadoop module: `mvn clean package -pl hadoop -am -Dmaven.test.skip=true -P get-jar-with-dependencies`
* Copy the target jar of Hadoop module `hadoop-tsfile-X.X.X-jar-with-dependencies.jar` into server target lib folder `.../server/target/iotdb-server-X.X.X/lib`.
