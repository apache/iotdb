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

# TsFile的Hive连接器

## 概要

- TsFile的Hive连接器使用手册
  - 什么是TsFile的Hive连接器
  - 系统环境要求
  - 数据类型对应关系
  - 为Hive添加依赖jar包
  - 创建Tsfile-backed的Hive表
  - 从Tsfile-backed的Hive表中查询
    - 选择查询语句示例
    - 聚合查询语句示例
  - 后续工作

## 什么是TsFile的Hive连接器

TsFile的Hive连接器实现了对Hive读取外部Tsfile类型的文件格式的支持，
使用户能够通过Hive操作Tsfile。

有了这个连接器，用户可以
* 将单个Tsfile文件加载进Hive，不论文件是存储在本地文件系统或者是HDFS中
* 将某个特定目录下的所有文件加载进Hive，不论文件是存储在本地文件系统或者是HDFS中
* 使用HQL查询tsfile
* 到现在为止, 写操作在hive-connector中还没有被支持. 所以, HQL中的insert操作是不被允许的

## 系统环境要求

|Hadoop Version |Hive Version | Java Version | TsFile |
|-------------  |------------ | ------------ |------------ |
| `2.7.3` or `3.2.1`       |    `2.3.6` or `3.1.2`  | `1.8`        | `0.10.0`|

> 注意：关于如何下载和使用Tsfile, 请参考以下链接: <https://github.com/apache/iotdb/tree/master/tsfile>。

## 数据类型对应关系

| TsFile 数据类型   | Hive 数据类型 |
| ---------------- | --------------- |
| BOOLEAN          | Boolean         |
| INT32            | INT             |
| INT64       	   | BIGINT          |
| FLOAT       	   | Float           |
| DOUBLE      	   | Double          |
| TEXT      	   | STRING          |


## 为Hive添加依赖jar包

为了在Hive中使用Tsfile的hive连接器，我们需要把hive连接器的jar导入进hive。

从 <https://github.com/apache/iotdb>下载完iotdb后, 你可以使用 `mvn clean package -pl hive-connector -am -Dmaven.test.skip=true`命令得到一个 `hive-connector-X.X.X-SNAPSHOT-jar-with-dependencies.jar`。

然后在hive的命令行中，使用`add jar XXX`命令添加依赖。例如:

```shell
hive> add jar /Users/hive/iotdb/hive-connector/target/hive-connector-0.11.0-SNAPSHOT-jar-with-dependencies.jar;

Added [/Users/hive/iotdb/hive-connector/target/hive-connector-0.11.0-SNAPSHOT-jar-with-dependencies.jar] to class path
Added resources: [/Users/hive/iotdb/hive-connector/target/hive-connector-0.11.0-SNAPSHOT-jar-with-dependencies.jar]
```


## 创建Tsfile-backed的Hive表

为了创建一个Tsfile-backed的表，需要将`serde`指定为`org.apache.iotdb.hive.TsFileSerDe`，
将`inputformat`指定为`org.apache.iotdb.hive.TSFHiveInputFormat`，
将`outputformat`指定为`org.apache.iotdb.hive.TSFHiveOutputFormat`。

同时要提供一个只包含两个字段的Schema，这两个字段分别是`time_stamp`和`sensor_id`。
`time_stamp`代表的是时间序列的时间值，`sensor_id`是你想要从tsfile文件中提取出来分析的传感器名称，比如说`sensor_1`。
表的名字可以是hive所支持的任何表名。

需要提供一个路径供hive-connector从其中拉取最新的数据。

这个路径必须是一个指定的文件夹，这个文件夹可以在你的本地文件系统上，也可以在HDFS上，如果你启动了Hadoop的话。
如果是本地文件系统，要以这样的形式`file:///data/data/sequence/root.baic2.WWS.leftfrontdoor/`

最后需要在`TBLPROPERTIES`里指明`device_id`

例如：

```
CREATE EXTERNAL TABLE IF NOT EXISTS only_sensor_1(
  time_stamp TIMESTAMP,
  sensor_1 BIGINT)
ROW FORMAT SERDE 'org.apache.iotdb.hive.TsFileSerDe'
STORED AS
  INPUTFORMAT 'org.apache.iotdb.hive.TSFHiveInputFormat'
  OUTPUTFORMAT 'org.apache.iotdb.hive.TSFHiveOutputFormat'
LOCATION '/data/data/sequence/root.baic2.WWS.leftfrontdoor/'
TBLPROPERTIES ('device_id'='root.baic2.WWS.leftfrontdoor.plc1');
```

在这个例子里，我们从`/data/data/sequence/root.baic2.WWS.leftfrontdoor/`中拉取`root.baic2.WWS.leftfrontdoor.plc1.sensor_1`的数据。
这个表可能产生如下描述：

```
hive> describe only_sensor_1;
OK
time_stamp          	timestamp              	from deserializer
sensor_1            	bigint              	from deserializer
Time taken: 0.053 seconds, Fetched: 2 row(s)
```

到目前为止, Tsfile-backed的表已经可以像hive中其他表一样被操作了。


## 从Tsfile-backed的Hive表中查询

在做任何查询之前，我们需要通过如下命令，在hive中设置`hive.input.format`：

```
hive> set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
```

现在，我们已经在hive中有了一个名为`only_sensor_1`的外部表。
我们可以使用HQL做任何查询来分析其中的数据。

例如:

### 选择查询语句示例

```
hive> select * from only_sensor_1 limit 10;
OK
1	1000000
2	1000001
3	1000002
4	1000003
5	1000004
6	1000005
7	1000006
8	1000007
9	1000008
10	1000009
Time taken: 1.464 seconds, Fetched: 10 row(s)
```

### 聚合查询语句示例

```
hive> select count(*) from only_sensor_1;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = jackietien_20191016202416_d1e3e233-d367-4453-b39a-2aac9327a3b6
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2019-10-16 20:24:18,305 Stage-1 map = 0%,  reduce = 0%
2019-10-16 20:24:27,443 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local867757288_0002
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 0 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
1000000
Time taken: 11.334 seconds, Fetched: 1 row(s)
```

## 后续工作

我们现在仅支持查询操作，写操作的支持还在开发中...