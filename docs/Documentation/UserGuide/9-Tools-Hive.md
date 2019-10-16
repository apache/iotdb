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
## Outline

- TsFile-Hive-Connector User Guide
	- About TsFile-Hive-Connector
	- System Requirements
	- Data Type Correspondence
	- Add Dependency For Hive
	- Creating Tsfile-backed Hive tables
	- Querying from Tsfile-backed Hive tables
	    - Select Clause Example
	    - Aggregate Clause Example
	- What's next
		
<!-- /TOC -->
# TsFile-Hive-Connector User Guide

## About TsFile-Hive-Connector

TsFile-Hive-Connector implements the support of Hive for external data sources of Tsfile type. This enables users to operate Tsfile by Hive.

With this connector, you can
* Load a single TsFile, from either the local file system or hdfs, into hive
* Load all files in a specific directory, from either the local file system or hdfs, into hive
* Query the tsfile through HQL.
* As of now, the write operation is not supported in hive-connector. So, insert operation in HQL is not allowed while operating tsfile through hive.

## System Requirements

|Hadoop Version |Hive Version | Java Version | TsFile |
|-------------  |------------ | ------------ |------------ |
| `2.7.3`       |    `2.3.6`  | `1.8`        | `0.8.0-SNAPSHOT`|

> Note: For more information about how to download and use TsFile, please see the following link: https://github.com/apache/incubator-iotdb/tree/master/tsfile.

## Data Type Correspondence

| TsFile data type | Hive field type |
| ---------------- | --------------- |
| BOOLEAN          | Boolean         |
| INT32            | INT             |
| INT64       	   | BIGINT          |
| FLOAT       	   | Float           |
| DOUBLE      	   | Double          |
| TEXT      	   | STRING          |


## Add Dependency For Hive

To use hive-connector in hive, we should add the hive-connector jar into hive.

After downloading the code of iotdb from <https://github.com/apache/incubator-iotdb>, you can use the command of `mvn clean package -pl hive-connector -am -Dmaven.test.skip=true` to get a `hive-connector-X.X.X-SNAPSHOT-jar-with-dependencies.jar`.

Then in hive, use the command of `add jar XXX` to add the dependency. For example:

```
hive> add jar /Users/hive/incubator-iotdb/hive-connector/target/hive-connector-0.9.0-SNAPSHOT-jar-with-dependencies.jar;

Added [/Users/hive/incubator-iotdb/hive-connector/target/hive-connector-0.9.0-SNAPSHOT-jar-with-dependencies.jar] to class path
Added resources: [/Users/hive/incubator-iotdb/hive-connector/target/hive-connector-0.9.0-SNAPSHOT-jar-with-dependencies.jar]
```


## Creating Tsfile-backed Hive tables

To create a Tsfile-backed table, specify the `serde` as `org.apache.iotdb.hive.TsFileSerDe`, 
specify the `inputformat` as `org.apache.iotdb.hive.TSFHiveInputFormat`, 
and the `outputformat` as `org.apache.iotdb.hive.TSFHiveOutputFormat`.
 
Also provide a  schema which only contains two fields: `time_stamp` and `sensor_id` for the table. 
`time_stamp` is the time value of the time series 
and `sensor_id` is the name of the sensor you want to extract from the tsfile to hive such as `sensor_1`. 
The name of the table must be the device name that the sensor belongs to.

Also provide a location from which hive-connector will pull the most current data for the table.

The location can be a specific directory or a specific file.

For example:

```
CREATE EXTERNAL TABLE IF NOT EXISTS device_1(
  time_stamp BIGINT,
  sensor_1 BIGINT)
ROW FORMAT SERDE 'org.apache.iotdb.hive.TsFileSerDe'
STORED AS
  INPUTFORMAT 'org.apache.iotdb.hive.TSFHiveInputFormat'
  OUTPUTFORMAT 'org.apache.iotdb.hive.TSFHiveOutputFormat'
LOCATION '/Users/hive/tsfile/data/';
```
In this example we're pulling the data of device_1.sensor_1 from the directory of `/Users/hive/tsfile/data/`. 
This table might result in a description as below:

```
hive> describe device_1;
OK
time_stamp          	bigint              	from deserializer
sensor_1            	bigint              	from deserializer
Time taken: 0.053 seconds, Fetched: 2 row(s)
```
At this point, the Tsfile-backed table can be worked with in Hive like any other table.

## Querying from Tsfile-backed Hive tables

Before we do any queries, we should set the `hive.input.format` in hive by executing the following command.

```
hive> set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
```

Now, we already have an external table named `device_1` in hive. 
We can use any query operations through HQL to analyse it.

For example:

### Select Clause Example

```
hive> select * from device_1 limit 10;
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

### Aggregate Clause Example

```
hive> select count(*) from device_1;
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

## What's next

We're currently only supporting read operation.
Writing tables to Tsfiles are under development.

Also, we're currently only supporting hive 2.x.x and have found it reliable and flexible. 
We'll be working on developing another one to support hive 3.x.x and it is coming soon.

