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

# Comparison

## Known Time Series Database

As the time series data is more and more important, 
several open sourced time series databases are intorduced in the world.
However, few of them are developed for IoT or IIoT (Industrial IoT) scenario in particular. 


We choose 3 kinds of TSDBs here.

* InfluxDB - Native Time series database

  InfluxDB is one of the most popular TSDBs. 
  
  Interface: InfluxQL and HTTP API

* OpenTSDB and KairosDB - Time series database based on NoSQL

  These two DBs are similar, while the first is based on HBase and the second is based on Cassandra.
  Both of them provides RESTful style API.
  
  Interface: Restful API

* TimesacleDB - Time series database based on Relational Database

  Interface: SQL

Prometheus and Druid are also famous for time series data management. 
However, Prometheus focuses on how to collect data, how to visualize data and how to alert warnings.
Druid focuses on how to analyze data with OLAP workload. We omit them here.
 

## Comparison 
We compare the above time series database from two aspects: the feature comparison and the performance
comparison.


### Feature Comparison

I list the basic features comparison of these databases. 

Legend:
- O: big support greatly
- o: support
- x: not support
- :\-( : support but not very good
- ?: unknown


#### Basic Features

| TSDB                        | IoTDB                       | InfluxDB   | OpenTSDB   | KairosDB   | TimescaleDB |   
|-----------------------------|-----------------------------|------------|------------|------------|-------------|  
| OpenSource                  | **o**                       | o          | o          | **o**      | o           |   
| SQL\-like                   | o                           | o          | x          | x          | **O**       |   
| Schema                      | "Tree\-based, tag\-based\"  | tag\-based | tag\-based | tag\-based | Relational  |   
| Writing out\-of\-order data | o                           | o          | o          | o          | o           |   
| Schema\-less                | o                           | o          | o          | o          | o           |   
| Batch insertion             | o                           | o          | o          | o          | o           |   
| Time range filter           | o                           | o          | o          | o          | o           |   
| Order by time               | **O**                       | o          | x          | x          | o           |   
| Value filter                | o                           | o          | x          | x          | o           |   
| Downsampling                | **O**                       | o          | o          | o          | o           |   
| Fill                        | **O**                       | o          | o          | x          | o           |   
| LIMIT                       | o                           | o          | o          | o          | o           |   
| SLIMIT                      | o                           | o          | x          | x          | ?           |   
| Latest value                | O                           | o          | o          | x          | o           |

**Details**

* OpenSource:  
 
  * IoTDB uses Apache License 2.0 and it is in Apache incubator. 
  * InfluxDB uses MIT license. However, **the cluster version is not open sourced**.
  * OpenTSDB uses LGPL2.1, which **is not compatible with Apache License**.
  * KairosDB uses Apache License 2.0.
  * TimescaleDB uses Timescale License, which is not free for enterprise. 

* SQL like: 

  * IoTDB and InfluxDB supports SQL like language. Besides, The integration of IoTDB and Calcite is alomost done (a PR has been submitted), which means IoTDB will support Standard SQL.
  * OpenTSDB and KairosDB only support Rest API. Besides, IoTDB also supports Rest API (a PR has been submitted).
  * TimescaleDB uses the SQL the same with PG.
  
* Schema:

  * IoTDB: IoTDB proposes a [Tree based schema](http://iotdb.apache.org/UserGuide/Master/Concept/Data%20Model%20and%20Terminology.html). 
   It is quite different with other TSDBs. However, the kind of schema has the following advantages:
    
    * In many industrial scenarios, the management of devices are hierarchical, rather than flat.
    That is why we think a tree based schema is better than tag-value based schema.
    
    * In many real world applications, tag names are constant. For example, a wind turbine manufacturer
    always identify their wind turbines by which country it locates, the farm name it belongs to, and its ID in the farm.
    So, a 4-depth tree ("root.the-country-name.the-farm-name.the-id") is fine. 
    You do not need to repeat to tell IoTDB the 2nd level of the tree is for country name, 
    the 3rd level is for farm id, etc..
    
    * A path based time series ID definition also supports flexible queries, like "root.\*.a.b.\*", wehre \* is wildcard character.
  
  * InfluxDB, KairosDB, OpenTSDB are tag-value based, which is more popular currently.
  
  * TimescaleDB uses relational table.   

* Order by time:
  
  Order by time seems quite trivil for time series database. But... if we consider another feature, called align by time,
  something becomes interesting.  And, that is why we mark OpenTSDB and KairosDB unsupported.
  
  Actually, in each time series, all these TSDBs support order data by timestamps.
  
  However, OpenTSDB and KairosDB do not support order the data from different timeseries in the time order.
  
  Ok, considering a new case: I have two time series, one is for the wind speed in wind farm1, 
  another is for the generated energy of wind turbine1 in farm1. If we want to analyze the relation between the 
  wind speed and the generated energy, we have to know the values of both at the same time.
  That is to say, we have to align the two time series in the time dimension.
  
  So, the result should be:
  
  | timestamp |  wind speed | generated energy |
  |-----------|-------------|------------------|
  |    1      |     5.0     |         13.1     |
  |    2      |     6.0     |         13.3     |
  |    3      |     null    |         13.1     |
  
  or,
  
    | timestamp |     series name   |    value   |
    |-----------|-------------------|------------|
    |    1      |       wind speed  |    5.0     |
    |    1      | generated energy  |    13.1    |
    |    2      |       wind speed  |    6.0     |
    |    2      | generated energy  |    13.3    |
    |    3      | generated energy  |    13.1    |      
  
 Though the second table format does not align data by the time dimension, but it is easy to be implemented in the client-side,
 by just scanning data row by row.
 
 IoTDB supports the first table format (called align by time), InfluxDB supports the second table format.

* Downsampling:

  Downsampling is for changing the granularity of timeseries, e.g., from 10Hz to 1Hz, or 1 point per day.
  
  Different with other systems, IoTDB downsamples data in real time, while others serialized downsampled data on disk.      
  That is to say,
  
  * IoTDB supports **adhoc** downsampling data in **arbitrary time**. 
  e.g., a SQL returns 1 point per 5 minutes and start with 2020-04-27 08:00:00 while another SQL returns 1 point per 5 minutes + 10 seconds and start with 2020-04-27 08:00:01.
  （InfluxDB also supports adhoc downsampling but the performance is ..... hm)
  
  * There is no disk loss for IoTDB.
  

* Fill:

  Sometimes we thought the data is collected in some fixed frequency, e.g., 1Hz (1 point per second). 
  But usually, we may lost some data points, because the network is unstable, the machine is busy, or the machine is down for several minutes.
  
  In this case, filling these holes is important. Data scientists can avoid to many so called dirty work, e.g., data clean.
  
  InfluxDB and OpenTSDB only support using fill in a group by statement, while IoTDB supports to fill data when just given a particular timestamp.
  Besides, IoTDB supports several strategies for filling data.
       
* Slimit:

  Slimit means return limited number of measurements (or, fields in InfluxDB). 
  For example, a wind turbine may have 1000 measurements (speed, voltage, etc..), using slimit and soffset can just return a part of them.    
  

* Latest value:

  As one of the most basic timeseries based applications is monitoring the latest data. 
  Therefore, a query to return the latest value of a time series is very important.
  IoTDB and OpenTSDB support that with a special SQL or API,
  while InfluxDB supports that using an aggregation function.
  (the reason why IoTDB provides a special SQL is IoTDB optimizes the query expressly.)
  
   
  
**Conclusion**:

Well, if we compare the basic features, we can find that OpenTSDB and KairosDB somehow lack some important query features.
TimescaleDB can not be freely used in business.
IoTDB and InfluxDB can meet most requirements of time series data management, while they have some difference.


#### Advanced Features

I listed some interesting features that these systems may differ.

| TSDB                        | IoTDB                           | InfluxDB   | OpenTSDB   | KairosDB   | TimescaleDB |   
|-----------------------------|---------------------------------|------------|------------|------------|-------------|   
| Align by time               | **O**                           | o          | x          | x          | o           |   
| Compression                 | **O**                           | :\-(       | :\-\(      | :\-\(      | :\-\(       |   
| MQTT support                | **O**                           | o          | x          | x          | :\-\(       |   
| Run on Edge-side Device     | **O**                           | o          | x          | :\-\(      | o           |   
| Multi\-instance Sync        | **O**                           | x          | x          | x          | x           |   
| JDBC Driver                 | **o**                           | x          | x          | x          | x           |   
| Standard SQL                | o                               | x          | x          | x          | **O**       |   
| Spark integration           | **O**                           | x          | x          | x          | x           | 
| Hive integration            | **O**                           | x          | x          | x          | x           |
| Writing data to NFS (HDFS)  | **O**                           | x          | o          | x          | x           |
| Flink integration           | **O**                           | x          | x          | x          | x           |


* Align by time: have been introduced. Let's skip it..

* Compression: 
  * IoTDB supports many encoding and compression for time series, like RLE, 2DIFF, Gorilla, etc.. and Snappy compression.
  In IoTDB, you can choose which encoding method you want, according to the data distribution. For more info, see [here](http://iotdb.apache.org/UserGuide/Master/Concept/Encoding.html).
  * InfluxDB also supports encoding and compression, but you can not define which encoding method you want.
  It just depends on the data type. For more info, see [here](https://docs.influxdata.com/influxdb/v1.7/concepts/storage_engine/).
  * OpenTSDB and KairosDB use HBase and Cassandra in backend, and have no special encoding for time series.
  
* MQTT protocol support:
  
  MQTT protocol is an international standard and widely known in industrial users. only IoTDB and InfluxDB support user using MQTT client to write data.  

* Running on Edge-side Device:
  
  Nowdays, edge computing is more and more popular, which means the edge device has more powerful compution resources. 
  Deploying a TSDB on the edge side is useful for managing data on the edge side and serve for edge computing. 
  As OpenTSDB and KairosDB rely another DB, the architecture is a little heavy. Especially, it is hard to run Hadoop on the edge side.

* Multi-instance Sync:
  
  Ok, now we have many TSDB instances on the edge-side. Then, how to upload their data to the data center, to form a ... data lake (or ocean, river,..., whatever).
  One choice is read data from these instances and write the data point by point to the data center instance.
  IoTDB provides another choice, just uploading the data file into the data center incrementally, then the data center can support service on the data. 
  
* JDBC driver:

  Now only IoTDB supports a JDBC driver (though not all interfaces are implemented), and makes it possible to integrate many other JDBC driver based softwares.

* Standard SQL:

  As mentioned, the integration of IoTDB and Calcite is almost done (a PR has been submitted), which means IoTDB will support Standard SQL.
  
* Spark and Hive integration:

  It is very very important that letting big data analysis software to access the data in database for more complex data analysis.
  IoTDB supports Hive-connector and Spark connector for better integration. 

*  Writing data to NFS (HDFS):
  Sharing nothing architecture is good, but sometimes you have to add new servers even your CPU and memory is idle but the disk is full...
  Besides, if we can save the data file directly to HDFS, it will be more easy to use Spark and other softwares to analyze data, without ETL.
  
  * IoTDB supports write data locally or on HDFS directly. IoTDB also allows user extend to store data on other NFS.
  * InfluxDB, KairosDB have to write data locally.
  * OpenTSDB has to write data on HDFS.
    
**Conclusion**:    
  
  We can find that IoTDB has many powerful features that other TSDBs do not support.

### Performance Comparison

Ok... If you say, "well, I just want to use the basic features. If so, IoTDB has little difference with others.".
It is somehow right. But, if you consider the performance, you may change your mind.

#### quick review

Given a workload:

* Write:

10 clients write data concurrently. The number of storage group is 50. There are 1000 devices and each device has 100 measurements (i.e.,, 100K time series totally).
The data type is float and IoTDB uses RLE encoding and Snappy compression. 
IoTDB uses batch insertion API and the batch size is 100 (write 100 data points per write API call).

* Read:

50 clients read data concurrently. Each client just read data from 1 device with 10 measurements in one storage group.

IoTDB is v0.9.0.

**Write performance**:

We write 112GB data totally.

The write throughput (points/second) is:

![Write Throughput (points/second)](https://user-images.githubusercontent.com/1021782/80472896-f1db0e00-8977-11ea-9424-96bf0021588d.png)
<span id = "exp1"> <center>Figure 1. Write throughput (points/second) IoTDB v0.9</center></span>


The disk occupation is:

![Disk Occupation](https://user-images.githubusercontent.com/1021782/80472899-f3a4d180-8977-11ea-8233-268ad4e3713e.png)
<center>Figure 2. Disk occupation(GB) IoTDB v0.9</center>

**Query performance**

![Aggregation query](https://user-images.githubusercontent.com/1021782/80472924-fef7fd00-8977-11ea-9ad4-b4d3c899605e.png)
<center>Figure 3. Aggregation query time cost(ms) IoTDB v0.9</center>

We can see that IoTDB outperforms others. 


#### More details

We provide a benchmarking tool, called IoTDB-benchamrk (https://github.com/thulab/iotdb-benchmark, you may have to use the dev branch to compile it),
it supports IoTDB, InfluxDB, KairosDB, TimescaleDB, OpenTSDB. We have a [article](https://arxiv.org/abs/1901.08304) for comparing these systems using the benchmark tool.
When we publishing the article, IoTDB just entered Apache incubator, so we deleted the performance of IoTDB in that article. But we really did the comparison, and I will 
disclose some results here.

- **IoTDB: 0.8.0**. (notice: **IoTDB v0.9 outperforms than v0.8**, we will update the result once we finish the experiments on v0.9)
- InfluxDB: 1.5.1.
- OpenTSDB: 2.3.1 (HBase 1.2.8)
- KairosDB: 1.2.1 (Cassandra 3.11.3)
- TimescaleDB: 1.0.0 (PostgreSQL 10.5)

All TSDB run on the same server one by one. 

- For InfluxDB, we set the cache-max-memory-size  and max-series-perbase as unlimited (otherwise it will be timeout quickly)

- For OpenTSDB, we modified tsd.http.request.enable_chunked, tsd.http.request.max_chunk and tsd.storage.fix_duplicates for supporting write data in batch
and write out-of-order data.

- For KairosDB, we set Cassandra's read_repair_chance as 0.1 (However it has no effect because we just have one node).

- For TimescaleDB, we use PGTune tool to optimize PostgreSQL.

All TSDBs run on a server with Intel Xeon CPU E5-2697 v4 @2.3GHz, 256GB memory and 10 HDD disks with RAID-5.
The OS is Ubuntu 16.04.2 LTS, 64bits.

Another server run IoTDB benchmark tool.

I omit the detailed workload here, let's see the result:

Legend: 
- I: InfluxDB
- O: OpenTSDB
- T: TimescaleDB
- K: KairosDB
- **D: IoTDB**

![Write experiments](https://user-images.githubusercontent.com/1021782/80476160-95c6b880-897c-11ea-9bb3-9d810cc0c79e.png)
<span id = "exp4"><center>Figure 4. Write experiments IoTDB v0.8.0</center></span>

![Query experiments](https://user-images.githubusercontent.com/1021782/80476181-9c553000-897c-11ea-8170-4768134f5841.png)
<center>Figure 5. Query experiments IoTDB v0.8.0</center>

We can see that IoTDB outperforms others hugely.

In [Figure. 4(c)](#exp4), when the batch size reaches to 10000 points, InfluxDB is better than IoTDB v0.8.
It is because in IoTDB v0.8, batch insert API is not optimized.
 
From IoTDB v0.9 on, using batch insert API can obtain 8 to 10 times write performance improvement. 


For example, using IoTDB v0.8, the write throughput can only reach to 6 million data points per second. 
But using IoTDB v0.9, the write throughput can reach to 40 million data points per second on the same server with the same workload.
(see [Figure. 4(a)](#exp4) vs [Figure. 1](#exp1)).


## Conclusion

If you are considering to find a TSDB for your IIoT application, then Apache IoTDB, a new time series, is your best choice.

We will update this page once we release new version and finish the experiments.
We also welcome more contributors correct this article and contribute IoTDB and reproduce experiments.
