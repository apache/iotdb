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

# Apache IoTDB (incubating) 0.8.0

This is the first official release of Apache IoTDB after joining the Incubator.

## Outline

* New Features
* Incompatible changes
* Miscellaneous changes
* Known Issues
* Bug Fixes

## New Features

* IOTDB-143	Compaction of data file
* IOTDB-205	Support storage-group-level Time To Live (TTL)
* IOTDB-198	Add sync module (Sync TsFiles between IoTDB instances)
* IoTDB 226	Hive-TsFile connector
* IOTDB-188	Delete storage group
* IOTDB-253	support time expression 
* IOTDB-239	Add interface for showing devices
* IOTDB-249	enable lowercase in create_timeseries sql
* IOTDB-203	add "group by device" function for narrow table display
* IOTDB-193	Create schema automatically when inserting
* IOTDB-241	Add query and non query interface in session
* IOTDB-223	Add a TsFile sketch tool
* IOTDB-158	add metrics web service
* IOTDB-187	Enable to choose storage in local file system or HDFS
* IOTDB-199	Add a log visulization tool 
* IoTDB-174	Add interfaces for querying device or timeseries number
* IOTDB-173	add batch write interface in session
* IOTDB-151	support number format in timeseries path
* IOTDB-294	online upgrade from 0.8.0 to 0.9.0
* modify print function in AbstractClient
* Spark-iotdb-connector
* generate cpp, go, and python thrift files under service-rpc
* display cache hit rate through jconsole
* support inserting data that time < 0
* Add interface (Delete timeseries) in session 
* Add a tool to print tsfileResources (each device's start and end time)
* Support watermark feature
* Add micro and nano timestamp precision

## Incompatible changes

* RPC is incompatible, you can not use client-0.8.0 to connect with server-0.9.0 or use client-0.9.0 to connect with server-0.8.0.
* Server is backward compatible, server-0.9.0 could run on data folder of 0.8.0. The data file will be upgraded background.

https://github.com/apache/incubator-iotdb/pull/467

## Miscellaneous changes

* IOTDB-258    Add documents for Query History Visualization Tool and Shared Storage Architecture
* IOTDB-233	keep metadata plan clear
* IOTDB-267	reduce IO operations in deserializing chunk header
* IOTDB-265	Re-adjust the threshold size of memtable
* IOTDB-251	improve TSQueryDataSet structure in RPC
* IOTDB-221	Add a python client example
* IOTDB-180	Get rid of JSON format in "show timeseries"
* IOTDB-161	Add ErrorCode of different response errors
* IOTDB-160	External sort
* IOTDB-153	further limit fetchSize to speed up LIMIT&OFFSET query
* IOTDB-295	Refactor db.exception
* reconstruct antlrv3 grammar to improve performance
* Tooling for release
* Modified Decoder and SequenceReader to support old version of TsFile 
* Remove jdk constrain of jdk8 and 11
* modify print function in AbstractClient

## Known Issues

* IOTDB-20    Need to support UPDATE

## Bug Fixes

* IOTDB-266     NullPoint exception when reading not existed devices using ReadOnlyTsFile
* IOTDB-264	restart failure due to WAL replay error
* IOTDB-240	fix unknown time series in where clause
* IOTDB-244	fix bug when querying with duplicated columns
* IOTDB-174	Fix querying timeseries interface cannot make a query by the specified path prefix
* IOTDB-195	using String.getBytes(utf-9).length to replace string.length() in ChunkGroupMetadata for supporting Chinese
* IOTDB-211	use "%IOTDB_HOME%\lib\*" to refers to all .jar files in the directory in start-server.bat
* fix start-walchecker scripts for leting user define the wal folder