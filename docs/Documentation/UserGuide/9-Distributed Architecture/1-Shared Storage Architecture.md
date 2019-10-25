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

# Chapter 10: Distributed Architecture

## Shared Storage Architecture

Currently, TSFiles(including both TSFile and related data files) are supported to be stored in local file system and hadoop distributed file system(HDFS). It is very easy to config the storage file system of TSFile.

### System architecture

When you config to store TSFile on HDFS, your data files will be in distributed storage. The system architecture is as below:

<img style="width:100%; max-width:700px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/66922722-35180400-f05a-11e9-8ff0-7dd51716e4a8.png">

### Config and usage

If you want to store TSFile and related data files in HDFS, here are the steps:

First, build server and Hadoop module by: `mvn clean package -pl server,hadoop -am -Dmaven.test.skip=true`

Then, copy the target jar of Hadoop module `hadoop-tsfile-0.9.0-SNAPSHOT-jar-with-dependencies.jar` into server target lib folder `.../server/target/iotdb-server-0.9.0-SNAPSHOT/lib`.

Edit user config in `iotdb-engine.properties`. Related configurations are:

* tsfile\_storage\_fs

|Name| tsfile\_storage\_fs |
|:---:|:---|
|Description| The storage file system of Tsfile and related data files. Currently LOCAL file system and HDFS are supported.|
|Type| String |
|Default|LOCAL |
|Effective|After restart system|

* hdfs\_ip

|Name| hdfs\_ip |
|:---:|:---|
|Description| IP of HDFS if Tsfile and related data files are stored in HDFS|
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

Start server, and Tsfile will be stored on HDFS.

If you'd like to reset storage file system to local, just edit configuration `tsfile_storage_fs` to `LOCAL`. In this situation, if you have already had some data files on HDFS, you should either download them to local and move them to your config data file folder (`../server/target/iotdb-server-0.9.0-SNAPSHOT/data/data` by default), or restart your process and import data to IoTDB.

### Frequent questions

1. What Hadoop version does it support?

A: Both Hadoop 2.x and Hadoop 3.x can be supported.

2. When starting the server or trying to create timeseries, I encounter the error below:
```
ERROR org.apache.iotdb.tsfile.fileSystem.fsFactory.HDFSFactory:62 - Failed to get Hadoop file system. Please check your dependency of Hadoop module.
```

A: It indicates that you forget to put Hadoop module dependency in IoTDB server. You can solve it by:
* Build Hadoop module: `mvn clean package -pl hadoop -am -Dmaven.test.skip=true`
* Copy the target jar of Hadoop module `hadoop-tsfile-0.9.0-SNAPSHOT-jar-with-dependencies.jar` into server target lib folder `.../server/target/iotdb-server-0.9.0-SNAPSHOT/lib`.
