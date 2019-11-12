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

# Apache IoTDB (incubating) 0.9.0

## New Features

* IOTDB-143 Compaction of data file
* IOTDB-151 Support number format in timeseries path
* IOTDB-158 Add metrics web service
* IOTDB-173 Add batch write interface in session
* IoTDB-174 Add interfaces for querying device or timeseries number
* IOTDB-187 Enable to choose storage in local file system or HDFS
* IOTDB-188 Delete storage group
* IOTDB-193 Create schema automatically when inserting
* IOTDB-198 Add sync module (Sync TsFiles between IoTDB instances)
* IOTDB-199 Add a log visulization tool 
* IOTDB-203 Add "group by device" function for narrow table display
* IOTDB-205 Support storage-group-level Time To Live (TTL)
* IOTDB-208 Add Bloom filter in TsFile
* IOTDB-223 Add a TsFile sketch tool
* IoTDB 226 Hive-TsFile connector
* IOTDB-239 Add interface for showing devices
* IOTDB-241 Add query and non query interface in session
* IOTDB-249 Enable lowercase in create_timeseries sql
* IOTDB-253 Support time expression 
* IOTDB-259 Level query of path
* IOTDB-282 Add "show version"
* IOTDB-294 Online upgrade from 0.8.0 to 0.9.0
* Spark-iotdb-connector
* Support quoted measurement name
* Generate cpp, go, and python thrift files under service-rpc
* Display cache hit rate through jconsole
* Support inserting data that time < 0
* Add interface (Delete timeseries) in session 
* Add a tool to print tsfileResources (each device's start and end time)
* Support watermark feature
* Add micro and nano timestamp precision

## Incompatible changes

* RPC is incompatible, you can not use client-0.8.0 to connect with server-0.9.0 or use client-0.9.0 to connect with server-0.8.0.
* Server is backward compatible, server-0.9.0 could run on data folder of 0.8.0. The data file will be upgraded background.
* Change map key in TsDigest from String to enum data type

## Miscellaneous changes

* IOTDB-144 Meta data cache for query
* IOTDB-153 Further limit fetchSize to speed up LIMIT&OFFSET query
* IOTDB-160 External sort
* IOTDB-161 Add ErrorCode of different response errors
* IOTDB-180 Get rid of JSON format in "show timeseries"
* IOTDB-192 Improvement for LRUCache
* IOTDB-210 One else if branch will never be reached in the method optimize of ExpressionOptimizer
* IOTDB-215 Update TsFile sketch tool and TsFile docs for v0.9.0
* IOTDB-221 Add a python client example
* IOTDB-233 keep metadata plan clear
* IOTDB-251 Improve TSQueryDataSet structure in RPC
* IOTDB-257 Makes the client stop fetch when dataSize equals maxPrintRowCount and change client fetchSize less than maxPrintRowCount
* IOTDB-258 Add documents for Query History Visualization Tool and Shared Storage Architecture
* IOTDB-265 Re-adjust the threshold size of memtable
* IOTDB-267 Reduce IO operations in deserializing chunk header
* IOTDB-273 Parallel recovery
* IOTDB-276 Fix inconsistent ways of judging whether a Field is null
* IOTDB-285 Duplicate fields in EngineDataSetWithoutValueFilter.java
* IOTDB-287 Restrict users to only use domain names and IP addresses.
* IOTDB-293 Variable naming convention
* IOTDB-295 Refactor db.exception
* Reconstruct Antlr3 grammar to improve performance
* Tooling for release
* Modified Decoder and SequenceReader to support old version of TsFile 
* Remove jdk constrain of jdk8 and 11
* Modify print function in AbstractClient
* Avoid second execution of parseSQLToPhysicalPlan in executeStatement

## Known Issues

* IOTDB-20 Need to support UPDATE

## Bug Fixes

* IOTDB-168&169 Fix a bug in export-csv tool and fix compatibility of timestamp formats in exportCsv, client display and sql
* IOTDB-174 Fix querying timeseries interface cannot make a query by the specified path prefix
* IOTDB-195 Using String.getBytes(utf-9).length to replace string.length() in ChunkGroupMetadata for supporting Chinese
* IOTDB-211 Use "%IOTDB_HOME%\lib\*" to refers to all .jar files in the directory in start-server.bat
* IOTDB-240 Fix unknown time series in where clause
* IOTDB-244 Fix bug when querying with duplicated columns
* IOTDB-252 Add/fix shell and bat for TsFileSketchTool/TsFileResourcePrinter
* IOTDB-266 NullPoint exception when reading not existed devices using ReadOnlyTsFile
* IOTDB-264 Restart failure due to WAL replay error
* IOTDB-290 Bug about threadlocal field in TSServiceImpl.java
* IOTDB-291 Statement close operation may cause the whole connection's resource to be released
* IOTDB-296 Fix error when skip page data in sequence reader
* Fix Dynamic Config when Creating Existing SG or Time-series
* Fix start-walchecker scripts for leting user define the wal folder


# Apache IoTDB (incubating) 0.8.1

This is a bug-fix version of 0.8.0

* IOTDB-172 Bug in updating startTime and endTime in TsFileResource
* IOTDB-195 Bug about 'serializedSize' in ChunkGroupMetaData.java (for Chinese string)
* IOTDB-202 fix tsfile example data type
* IOTDB-242 fix mvn integration-test failed because the files in the target folder changes
* Abnormal publishing of sequence and unsequence data folders in DirectoryManager
* Fix a bug in TimeRange's intersects function


# Apache IoTDB (incubating) 0.8.0

This is the first official release of Apache IoTDB after joining the Incubator.

## New Features

* IOTDB-1 Add Aggregation query
* IOTDB-4 Asynchronously force sync WAL periodically
* IOTDB-5 Support data deletion
* IOTDB-11 Support start script for jdk 11 on Windows OS
* IOTDB-18 Improve startup script compatible for jdk11
* IOTDB-36 [TsFile] Enable recover data from a incomplete TsFile and continue to write
* IOTDB-37 Add WAL check tool script
* IOTDB-51 Update post-back module to synchronization module
* IOTDB-59 Support GroupBy query
* IOTDB-60 Support Fill function when query
* IOTDB-73 Add REGULAR encoding method for data with fixed frequency
* IOTDB-80 Support custom export file name
* IOTDB-81 Update travis for supporting JDK11 on Windows
* IOTDB-83 Add process bar for import script and show how many rows have been exported
* IOTDB-91 Improve tsfile-spark-connector to support spark-2.4.3
* IOTDB-93 IoTDB Calcite integration
* IOTDB-109 Support appending data at the end of a completed TsFile
* IOTDB-122 Add prepared statement in JDBC
* IOTDB-123 Add documents in Chinese
* IOTDB-130 Dynamic parameters adapter
* IOTDB-134 Add default parameter for client starting script
* Add read-only mode of IoTDB
* New storage engine with asynchronously flush and close data file
* Adding english documents
* Supporting travis + window + jdk8
* Add skipping all UTs: maven integration-test -DskipUTS=true
* Enable users define the location of their thrift compiler
* Add example module
* Add a log appender: put info, warn, error log into one file and disable log_info by default
* Recover when resource file does not exist while tsfile is complete

## Incompatible changes

If you use the previous unofficial version 0.7.0. It is incompatible with 0.8.0.

## Miscellaneous changes

* IOTDB-21 Add ChunkGroup offset information in ChunkGroupMetaData
* IOTDB-25 Add some introduction for JMX MBean Monitor in user guide
* IOTDB-29 Multiple Exceptions when reading empty measurements from TsFileSequenceReader
* IOTDB-39 Add auto repair functionality for RestorableTsFileIOWriter
* IOTDB-45 Update the license in IoTDB
* IOTDB-56 Faster getSortedTimeValuePairList() of Memtable
* IOTDB-62 Change log level from error to debug in SQL parser
* IoTDB-63 Use TsFileInput instead of FileChannel as the input parameter of some functions
* IOTDB-76 Reformat MManager.getMetadataInString() in JSON format
* IOTDB-78 Make unsequence file format more similar with TsFile
* IOTDB-95 Keep stack trace when logging or throwing an exception
* IOTDB-117 Add sync documents
* Modify ASF header for each file and add related maven plugin
* Add IoTDB env script test
* Add sync function for jdbc server to close
* Add cache directories for download jars and sonar plugin of maven in travis
* Add partition start and end offset constraints when loading ChunkGroupMetaData
* Check when creating Storage group
* Try to release memory asap in ReadOnlyMemChunk
* Add more physical plan serializer
* Move all generated tsfiles for test into the target folder
* Make TsFileWriter as AutoClosable
* Print apache-rat violation result on console
* Update multi dir avoid disk is full
* Simplify Path construction
* Separate documents into different chapter folders
* Suppress mvn log in travis
* Add mvn -B in travis

## Known Issues

* IOTDB-20 Need to support UPDATE
* IOTDB-124 Lost timeseries info after restart IoTDB
* IOTDB-125 [potential] a concurrency conflict may occur when a delete command and insertion command appears concurrently
* IOTDB-126 IoTDB will not be closed immediately after run 'stop-server.sh' script
* IOTDB-127 Chinese version documents problems

## Bug Fixes

* IOTDB-2 Maven Test failed before run mvn package -Dmaven.test.skip=true
* IOTDB-7 OpenFileNumUtilTest failed
* IOTDB-15 Fail to install IoTDB on Ubuntu 14.04
* IOTDB-16 Invalid link on https://iotdb.apache.org/#/Documents/Quick Start
* IOTDB-17 Need to update chapter Start of https://iotdb.apache.org/#/Documents/Quick Start
* IOTDB-18 IoTDB startup script does not work on openjdk11
* IOTDB-19 Fail to start start-server.sh script on Ubuntu 14.04/Ubuntu 16.04
* IOTDB-22 BUG in TsFileSequenceReader when reading tsfile
* IOTDB-24 DELETION error after restart a server
* IOTDB-26 Return error when quit client
* IOTDB-27 Delete error message
* IOTDB-30 Flush timeseries cause select to returns "Msg:null"
* IOTDB-31 Cannot set float number precision
* IOTDB-34 Invalid message for show storage group
* IOTDB-44 Error message in server log when select timeseries
* IOTDB-49 Authorizer module outputs too many debug log info
* IOTDB-50 DataSetWithoutTimeGenerator's initHeap behaves wrongly
* IOTDB-52 Cli doesn't support aggregate
* IOTDB-54 Predicates doesn't take effect
* IOTDB-67 ValueDecoder reading new page bug
* IOTDB-70 Disconnect from server when logging in fails
* IOTDB-71 Improve readPositionInfo
* IOTDB-74 THe damaged log will be skipped if it is the only log
* IOTDB-79 Long term test failed because of the version control of deletion function
* IOTDB-81 Fix Windows OS environment for Travis-CI
* IOTDB-82 File not closed in PageHeaderTest and cause UT on Windows fails
* IOTDB-84 Out-of-memory bug
* IOTDB-94 IoTDB failed to start client since the required jars are not in the right folder
* IOTDB-96 The JDBC interface throws an exception when executing the SQL statement "list user"
* IOTDB-99 List privileges User <username> on <path> cannot be used properly
* IOTDB-100 Return error message while executing sum aggregation query
* IOTDB-103 Does not give a hint when encountering unsupported data types
* IOTDB-104 MManager is incorrectly recovered when system reboots
* IOTDB-108 Mistakes in documents
* IOTDB-110 Clis inserts data normally even if there is no space left on the disk
* IOTDB-118 When the disk space is full, the storage group is created successfully
* IOTDB-121 A bug of query on value columns
* IOTDB-128 Probably a bug in iotdb official website
* IOTDB-129 A bug in restoring incomplete tsfile when system restart
* IOTDB-131 IoTDB-Grafana module error when getting the timeseries list from Grafana
* IOTDB-133 Some content is mistaken for links
* System memory check failure in iotdb-env.sh
* Time zone bug in different region
* DateTimeUtilsTest UT bug
* Problem discovered by Sonar
* Openjdk11 + linux11 does not work on travis
* Start JDBC service too slowly
* JDBC cannot be closed
* Close bug in sync thread
* Bug in MManager to get all file names of a path
* Version files of different storage groups are placed into the same place
* Import/export csv script bug
* Log level and stack print in test
* Bug in TsFile-Spark-Connector
* A doc bug of QuickStart.md