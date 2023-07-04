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
## Hive-TsFile

### About Hive-TsFile-Connector

Hive-TsFile-Connector implements the support of Hive for external data sources of Tsfile type. This enables users to operate TsFile by Hive.

With this connector, you can

* Load a single TsFile, from either the local file system or hdfs, into hive
* Load all files in a specific directory, from either the local file system or hdfs, into hive
* Query the tsfile through HQL.
* As of now, the write operation is not supported in hive-connector. So, insert operation in HQL is not allowed while operating tsfile through hive.

### System Requirements

|Hadoop Version |Hive Version | Java Version | TsFile |
|-------------  |------------ | ------------ |------------ |
| `2.7.3` or `3.2.1`       |    `2.3.6` or `3.1.2`  | `1.8`        | `1.0.0`|

> Note: For more information about how to download and use TsFile, please see the following link: https://github.com/apache/iotdb/tree/master/tsfile.

### Data Type Correspondence

| TsFile data type | Hive field type |
| ---------------- | --------------- |
| BOOLEAN          | Boolean         |
| INT32            | INT             |
| INT64       	   | BIGINT          |
| FLOAT       	   | Float           |
| DOUBLE      	   | Double          |
| TEXT      	   | STRING          |


### Add Dependency For Hive

To use hive-connector in hive, we should add the hive-connector jar into hive.

After downloading the code of iotdb from <https://github.com/apache/iotdb>, you can use the command of `mvn clean package -pl iotdb-connector/hive-connector -am -Dmaven.test.skip=true -P get-jar-with-dependencies` to get a `hive-connector-X.X.X-jar-with-dependencies.jar`.

Then in hive, use the command of `add jar XXX` to add the dependency. For example:

```
hive> add jar /Users/hive/iotdb/hive-connector/target/hive-connector-1.0.0-jar-with-dependencies.jar;

Added [/Users/hive/iotdb/hive-connector/target/hive-connector-1.0.0-jar-with-dependencies.jar] to class path
Added resources: [/Users/hive/iotdb/hive-connector/target/hive-connector-1.0.0-jar-with-dependencies.jar]
```


### Create Tsfile-backed Hive tables

To create a Tsfile-backed table, specify the `serde` as `org.apache.iotdb.hive.TsFileSerDe`, 
specify the `inputformat` as `org.apache.iotdb.hive.TSFHiveInputFormat`, 
and the `outputformat` as `org.apache.iotdb.hive.TSFHiveOutputFormat`.

Also provide a schema which only contains two fields: `time_stamp` and `sensor_id` for the table. 
`time_stamp` is the time value of the time series 
and `sensor_id` is the sensor name to extract from the tsfile to hive such as `sensor_1`. 
The name of the table can be any valid table names in hive.

Also a location provided for hive-connector to pull the most current data for the table.

The location should be a specific directory on your local file system or HDFS to set up Hadoop.
If it is in your local file system, the location should look like `file:///data/data/sequence/root.baic2.WWS.leftfrontdoor/`

Last, set the `device_id` in `TBLPROPERTIES` to the device name you want to analyze.

For example:

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
In this example, the data of `root.baic2.WWS.leftfrontdoor.plc1.sensor_1` is pulled from the directory of `/data/data/sequence/root.baic2.WWS.leftfrontdoor/`. 
This table results in a description as below:

```
hive> describe only_sensor_1;
OK
time_stamp          	timestamp              	from deserializer
sensor_1            	bigint              	from deserializer
Time taken: 0.053 seconds, Fetched: 2 row(s)
```
At this point, the Tsfile-backed table can be worked with in Hive like any other table.

### Query from TsFile-backed Hive tables

Before we do any queries, we should set the `hive.input.format` in hive by executing the following command.

```
hive> set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
```

Now, we already have an external table named `only_sensor_1` in hive. 
We can use any query operations through HQL to analyse it.

For example:

#### Select Clause Example

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

#### Aggregate Clause Example

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


