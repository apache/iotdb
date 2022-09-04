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

# Apache IoTDB 0.12.0

## New Features
* [IOTDB-68] New shared-nothing cluster
* [IOTDB-507] Add zeppelin-interpreter module
* [IOTDB-825] Aggregation by natural month
* [IOTDB-890] support SDT lossy compression 
* [IOTDB-944] Support UDTF (User-defined Timeseries Generating Function)
* [IOTDB-965] Add timeout parameter for query
* [IOTDB-1077] Add insertOneDeviceRecords API in java session
* [IOTDB-1055] Support data compression type GZIP
* [IOTDB-1024] Support multiple aggregated measurements for group by level statement
* [IOTDB-1276] Add explain sql support and remove debug_state parameter
* [IOTDB-1197] Add iotdb-client-go as a git submodule of IoTDB repo
* [IOTDB-1230] Support spans multi time partitions when loading one TsFile
* [IOTDB-1273] Feature/restrucutre python module as well as supporting pandas dataframe
* [IOTDB-1277] support IoTDB as Flink's data source
* [PR-2605] Add level merge to "merge" command

## Incompatible changes
* [IOTDB-1081] New TsFile Format
* [ISSUE-2730] Add the number of unseq merge times in TsFile name.


## Miscellaneous changes
* [IOTDB-868] Change mlog from txt to bin
* [IOTDB-1069] Restrict the flushing memtable number to avoid OOM when mem_control is disabled
* [IOTDB-1104] Refactor the error handling process of query exceptions
* [IOTDB-1108] Add error log to print file name while error happened
* [IOTDB-1152] Optimize regular data size in traversing
* [IOTDB-1180] Reset the system log file names and maximal disk-space size
* [ISSUE-2515] Set fetchsize through JDBC and Session
* [ISSUE-2598] Throw explicit exception when time series is unknown in where clause
* [PR-2944] Throw exception when device to be queried is not in TsFileMetaData
* [PR-2967] Log memory usage information in SystemInfo for better diagnosis

## Bug Fixes
* [IOTDB-1049] Fix NullpointerException and a delete bug in Last query
* [IOTDB-1050] Fix Count timeserise column name is wrong
* [IOTDB-1068] Fix Time series metadata cache bug
* [IOTDB-1084] Fix temporary memory of flushing may cause OOM
* [IOTDB-1106] Fix delete timeseries bug
* [IOTDB-1126] Fix the unseq tsfile delete due to merge
* [IOTDB-1135] Fix the count timeseries prefix path bug
* [IOTDB-1137] Fix MNode.getLeafCount error when existing sub-device
* [ISSUE-2484] Fix creating timeseries error by using "create" or "insert" statement
* [ISSUE-2545, 2549] Fix unseq merge end time bug
* [ISSUE-2611] An unsequence file that covers too many sequence file causes OOM query
* [ISSUE-2688] LRULinkedHashMap does not work as an LRU Cache
* [ISSUE-2709, 1178] Fix cache not cleared after unseq compaction bug, Fix windows 70,10 ci bug in unseq compaction ci
* [ISSUE-2741] getObject method in JDBC should return an Object
* [ISSUE-2746] Fix data overlapped bug after unseq compaction
* [ISSUE-2758] NullPointerException in QueryTimeManager.checkQueryAlive()
* [ISSUE-2905] Fix Files.deleteIfExists() doesn't work for HDFS file
* [ISSUE-2919] Fix C++ client memory leak bug
* [PR-2613] Fix importCSVTool import directory bug & encode bug
* [PR-2409] Fix import csv which can't import time format str
* [PR-2582] Fix sync bug for tsfiles's directory changed by vitural storage group
* [ISSUE-2911] Fix The write stream is not closed when executing the command 'tracing off'


# Apache IoTDB 0.11.3

## Bug Fixes
* ISSUE-2505 ignore PathNotExistException in recover and change recover error to warn
* IOTDB-1119 Fix C++ SessionDataSet bug when reading value buffer
* Fix SessionPool does not recycle session and can not offer new Session due to RunTimeException
* ISSUE-2588 Fix dead lock between deleting data and querying in parallel
* ISSUE-2546 Fix first chunkmetadata should be consumed first
* IOTDB-1126 Fix unseq tsfile is deleted due to compaction
* IOTDB-1137 MNode.getLeafCount error when existing sub-device
* ISSUE-2624 ISSUE-2625 Avoid OOM if user don't close Statement and Session manually
* ISSUE-2639 Fix possible NPE during end query process
* Alter IT for An error is reported and the system is suspended occasionally
* IOTDB-1149 print error for -e param when set maxPRC<=0
* IOTDB-1247 Fix the insert blocked caused the bugs in mem control module
* ISSUE-2648 Last query not right when having multiple devices
* Delete mods files after compaction
* ISSUE-2687 fix insert NaN bug
* ISSUE-2598 Throw explicit exception when time series is unknown in where clause
* Fix timeseriesMetadata cache is not cleared after the TsFile is deleted by a compaction
* ISSUE-2611 An unsequence file that covers too many sequence file causes OOM query
* IOTDB-1135 Fix count timeseries bug when the paths are nested
* ISSUE-2709 IOTDB-1178 Fix cache is not cleared after compaction
* ISSUE-2746 Fix data overlapped bug after the elimination unseq compaction process
* Fix getObject method in JDBC should return an Object
* IOTDB-1188 Fix IoTDB 0.11 unable to delete data bug
* Fix when covering a tsfile resource with HistoricalVersion = null, it’ll throw a NPE
* fix the elimination unseq compaction may loss data bug after a delete operation is executed
* Fix a bug of checking time partition in DeviceTimeIndex
* Throw exeception when device to be queried is not in tsFileMetaData
* Fix unseq compaction file selector conflicts with time partition bug
* Fix high CPU usage during the compaction process

## Improvements
* IOTDB-1140 optimize regular data encoding
* Add more log for better tracing
* Add backgroup exec for cli -e function
* Add max direct memory size parameter to env.sh
* Change last cache log to debug level

## New Features
* Add explain sql support


# Apache IoTDB 0.11.2

## Bug Fixes
* IOTDB-1049 Fix Nullpointer exception and a delete bug in Last query
* IOTDB-1060 Support full deletion for delete statement without where clause
* IOTDB-1068 Fix Time series metadata cache bug
* IOTDB-1069 restrict the flushing memtable number to avoid OOM when mem_control is disabled
* IOTDB-1077 add insertOneDeviceRecords API in java session
* IOTDB-1087 fix compaction block flush: flush do not return until compaction finished
* IOTDB-1106 Delete timeseries statement will incorrectly delete other timeseries
* Github issue-2137 fix grafana value-time position bug
* Github issue-2169 GetObject returns String for all data types
* Github issue-2240 fix Sync failed: Socket is closed by peer
* Github issue-2387 The deleteData method exists in Session but not in SessionPool.
* add thrift_max_frame_size in iotdb-engine.properties
* Fix incorrect last result after deleting all data
* Fix compaction recover block restart: IoTDB cannot restart until last compaction recover task finished
* Fix compaction ignore modification file: delete does not work after compaction
* print more insert error message in client
* expose enablePartition parameter into iotdb-engines.properpties

# Apache IoTDB 0.11.1

## Bug Fixes
* IOTDB-990 cli parameter maxPRC shouldn't to be set zero
* IOTDB-993 Fix tlog bug
* IOTDB-994 Fix can not get last_value while doing the aggregation query along with first_value
* IOTDB-1000 Fix read redundant data while select with value filter with unseq data
* IOTDB-1007 Fix session pool concurrency and leakage issue when pool.close is called
* IOTDB-1016 overlapped data should be consumed first
* IOTDB-1021 Fix NullPointerException when showing child paths of non-existent path
* IOTDB-1028 add MAX\_POINT\_NUMBER format check
* IOTDB-1034 Fix Show timeseries error in Chinese on Windows
* IOTDB-1035 Fix bug in getDeviceTimeseriesMetadata when querying non-exist device
* IOTDB-1038 Fix flink set storage group bug
* ISSUE-2179 fix insert partial tablet with binary NullPointer bug
* add reject status code
* Update compaction level list delete
* Fix query result is not correct
* Fix import errors in Session.py and SessionExample.py
* Fix modules can not be found when using pypi to pack client-py
* Fix Count timeseries group by level bug
* Fix desc batchdata count bug

# Apache IoTDB 0.11.0

## New Features

* IOTDB-627 Support range deletion for timeseries
* IOTDB-670 Add raw data query interface in Session
* IOTDB-716 add lz4 compression
* IOTDB-736 Add query performance tracing
* IOTDB-776 New memory control strategy
* IOTDB-813 Show storage group under given path prefix
* IOTDB-848 Support order by time asc/desc
* IOTDB-863 add a switch to drop ouf-of-order data
* IOTDB-873 Add count devices DDL
* IOTDB-876 Add count storage group DDL
* IOTDB-926 Support reconnection of Session
* IOTDB-941 Support 'delete storage group <prefixPath>'
* IOTDB-968 Support time predicate in select last, e.g., select last * from root where time >= T
* Show alias if it is used in query
* Add level compaction strategy
* Add partialInsert

## Incompatible changes

* IOTDB-778 Support double/single quotation in Path
* IOTDB-870 change tags and attributes output to two columns with json values

## Miscellaneous changes

* IOTDB-784 Update rpc protocol to V3
* IOTDB-790 change base_dir to system_dir
* IOTDB-829 Accelerate delete multiple timeseries
* IOTDB-839 Make Tablet api more friendly
* IOTDB-902 Optimize max_time aggregation
* IOTDB-916 Add a config entry to make Last cache configurable
* IOTDB-928 Make ENCODING optional in create time series sentence
* IOTDB-938 Re-implement Gorilla encoding algorithm
* IOTDB-942 Optimization of query with long time unsequence page
* IOTDB-943 Fix cpu usage too high
* Add query load log
* Add merge rate limiting

## Bug Fixes

* IOTDB-749 Avoid select * from root OOM
* IOTDB-774 Fix "show timeseries" OOM problem
* IOTDB-832 Fix sessionPool logic when reconnection failed
* IOTDB-833 Fix JMX cannot connect IoTDB in docker
* IOTDB-835 Delete timeseries and change data type then write failed
* IOTDB-836 Statistics classes mismatched when endFile （delete timeseries then recreate）
* IOTDB-837 ArrayIndexOutOfBoundsException if the measurementId size is not consistent with the value size
* IOTDB-847 Fix bug that 'List user privileges' cannot apply to 'root'
* IOTDB-850 a user could list others privilege bug
* IOTDB-851 Enhance failure tolerance when recover WAL (enable partial insertion)
* IOTDB-855 Can not release Session in SessionPool if RuntimeException occurs
* IOTDB-868 Can not redo mlogs with special characters like comma
* IOTDB-872 Enable setting timezone at client
* IOTDB-877 fix prefix bug on show storage group and show devices
* IOTDB-904 fix update last cache NullPointerException
* IOTDB-920 Disable insert row that only contains time/timestamp column
* IOTDB-921 When execute two query simultaneously in one statement, got error
* IOTDB-922 Int and Long can convert to each other in ResultSet
* IOTDB-947 Fix error when counting node with wildcard
* IOTDB-949 Align by device doesn't support 'T*' in path.
* IOTDB-956 Filter will be missed in group by time align by device
* IOTDB-963 Redo deleteStorageGroupPlan failed when recovering
* IOTDB-967 Fix xxx does not have the child node xxx Bug in count timeseries
* IOTDB-970 Restrict log file number and size
* IOTDB-971 More precise error messages of slimit and soffset 
* IOTDB-975 when series does not exist in TsFile, reading wrong ChunkMetadataList

# Apache IoTDB (incubating) 0.10.1

* [IOTDB-797] InsertTablet deserialization from WAL error
* [IOTDB-788] Can not upgrade all storage groups
* [IOTDB-792] deadlock when insert while show latest timeseries
* [IOTDB-794] Rename file or delete file Error in start check in Windows
* [IOTDB-795] BufferUnderflowException in Hive-connector
* [IOTDB-766] Do not release unclosed file reader, a small memory leak
* [IOTDB-796] Concurrent Query throughput is low
* Query result is not correct when some unsequence data exists
* Change the default fetch size to 10000 in session
* [IOTDB-798] fix a set rowLimit and rowOffset bug
* [IOTDB-800] Add a new config type for those parameters which could not be modified any more after the first start 
* [IOTDB-802] Improve "group by" query performance
* [IOTDB-799] remove log visualizer tool from v0.10 
* fix license-binary  
* [IOTDB-805] Fix BufferUnderflowException when querying TsFile stored in HDFS 
* python session client ver-0.10.0
* [IOTDB-808] fix bug in selfCheck() truncate 
* fix doc of MeasurementSchema in Tablet 
* [IOTDB-811] fix upgrading mlog many times when upgrading system.properties crashed
* Improve IoTDB restart process
* remove jol-core dependency which is introduced by hive-serde 2.8.4
* remove org.json dependency because of license compatibility
* [ISSUE-1551] fix set historical version when loading additional tsfile


# Apache IoTDB (incubating) 0.10.0

## New Features

* IOTDB-217 A new GROUPBY syntax, e.g., select avg(s1) from root.sg.d1.s1 GROUP BY ([1, 50), 5ms)
* IOTDB-220 Add hot-load configuration function
* IOTDB-275 allow using user defined JAVA_HOME and allow blank space in the JAVA_HOME
* IOTDB-287 Allow domain in JDBC URL
* IOTDB-292 Add load external tsfile feature
* IOTDB-297 Support "show flush task info"
* IOTDB-298 Support new Last point query. e.g, select last * from root
* IOTDB-305 Add value filter function while executing align by device
* IOTDB-309 add Dockerfiles for 0.8.1, 0.9.0, and 0.9.1
* IOTDB-313 Add RandomOnDiskUsableSpaceStrategy
* IOTDB-323 Support insertRecords in session
* IOTDB-337 Add timestamp precision properties for grafana
* IOTDB-343 Add test method in session
* IOTDB-396 Support new query clause: disable align, e.g., select * from root disable align
* IOTDB-447 Support querying non-existing measurement and constant measurement
* IOTDB-448 Add IN operation, e.g., where time in (1,2,3)
* IOTDB-456 Support GroupByFill Query, e.g., select last_value(s1) from root.sg.d1 GROUP BY ([1, 10), 2ms) FILL(int32[previousUntilLast])
* IOTDB-467 The CLI displays query results in a batch manner
* IOTDB-497 Support Apache Flink Connector with IoTDB
* IOTDB-558 add text support for grafana
* IOTDB-560 Support Apache Flink connecter with TsFile
* IOTDB-565 MQTT Protocol Support, disabled by default, open in iotdb-engine.properties
* IOTDB-574 Specify configuration when start iotdb
* IOTDB-588 Add tags and attributes management
* IOTDB-607 add batch create timeseries in native interface
* IOTDB-612 add limit&offset to show timeseries
* IOTDB-615 Use TsDataType + Binary to replace String in insert plan
* IOTDB-617 Support alter one time series's tag/attribute
* IOTDB-630 Add a jdbc-like way to fetch data in session
* IOTDB-640 Enable system admin sql (flush/merge) in JDBC or Other API
* IOTDB-671 Add clear cache command
* Support open and close time range in group by, e.g, [), (]
* Online upgrade from 0.9.x
* Support special characters in path: -/+&%$#@
* IOTDB-446 Support path start with a digit, e.g., root.sg.12a
* enable rpc compression in session pool
* Make JDBC OSGi usable and added a feature file
* add printing one resource file tool
* Allow count timeseries group by level=x using default path
* IOTDB-700 Add OpenID Connect based JWT Access as alternative to Username / Password
* IOTDB-701 Set heap size by percentage of system total memory when starts
* IOTDB-708 add config for inferring data type from string value
* IOTDB-715 Support previous time range in previousuntillast
* IOTDB-719 add avg_series_point_number_threshold in config
* IOTDB-731 Continue write inside InsertPlan 
* IOTDB-734 Add Support for NaN in Double / Floats in SQL Syntax.
* IOTDB-744 Support upsert alias 


## Incompatible changes

* IOTDB-138 Move All metadata query to usual query
* IOTDB-322 upgrade to thrift 0.12.0-0.13.0
* IOTDB-325 Refactor Statistics in TsFile
* IOTDB-419 Refactor the 'last' and 'first' aggregators to 'last_value' and 'first_value'
* IOTDB-506 upgrade the rpc protocol to v2 to reject clients or servers that version < 0.10
* IOTDB-587 TsFile is upgraded to version 2
* IOTDB-593 add metaOffset in TsFileMetadata
* IOTDB-597 Rename methods in Session: insertBatch to insertTablet, insertInBatch to insertRecords, insert to insertRecord
* RPC is incompatible, you can not use client-v0.9 to connect with server-v0.10
* TsFile format is incompatible, will be upgraded when starting 0.10
* Refine exception code in native api

## Miscellaneous changes

* IOTDB-190 upgrade from antlr3 to antlr4
* IOTDB-418 new query engine
* IOTDB-429 return empty dataset instead of throw exception, e.g., show child paths root.*
* IOTDB-445 Unify the keyword of "timestamp" and "time"
* IOTDB-450 Add design documents
* IOTDB-498 Support date format "2020-02-10"
* IOTDB-503 Add checkTimeseriesExists in java native api
* IOTDB-605 Add more levels of index in TsFileMetadata for handling too many series in one device
* IOTDB-625 Change default level number: root is level 0
* IOTDB-628 rename client to cli
* IOTDB-621 Add Check isNull in Field for querying using session
* IOTDB-632 Performance improve for PreviousFill/LinearFill
* IOTDB-695 Accelerate the count timeseries query 
* IOTDB-707 Optimize TsFileResource memory usage  
* IOTDB-730 continue write in MQTT when some events are failed
* IOTDB-729 shutdown uncessary threadpool 
* IOTDB-733 Enable setting for mqtt max length 
* IOTDB-732 Upgrade fastjson version to 1.2.70
* Allow "count timeseries" without a prefix path
* Add max backup log file number
* add rpc compression api in client and session module
* Continue writing the last unclosed file
* Move the vulnera-checks section into the apache-release profile to accelerate compile
* Add metaquery in python example
* Set inferType of MQTT InsertPlan to true



## Bug Fixes

* IOTDB-125 Potential Concurrency bug while deleting and inserting happen together
* IOTDB-185 fix start-client failed on WinOS if there is blank space in the file path; let start-server.bat suport jdk12,13 etc
* IOTDB-304 Fix bug of incomplete HDFS URI
* IOTDB-341 Fix data type bug in grafana
* IOTDB-346 Fix a bug of renaming tsfile in loading function
* IOTDB-370 fix a concurrent problem in parsing sql
* IOTDB-376 fix metric to show executeQuery
* IOTDB-392 fix export CSV
* IOTDB-393 Fix unclear error message for no privilege users
* IOTDB-401 Correct the calculation of a chunk if there is no data in the chunk, do not flush empty chunk
* IOTDB-412 Paths are not correctly deduplicated
* IOTDB-420 Avoid encoding task dying silently
* IOTDB-425 fix can't change the root password.
* IOTDB-459 Fix calmem tool bug
* IOTDB-470fix IllegalArgumentException when there exists 0 byte TsFile
* IOTDB-529 Relative times and NOW() operator cannot be used in Group By
* IOTDB-531 fix issue when grafana visualize boolean data
* IOTDB-546 Fix show child paths statement doesn't show quotation marks
* IOTDB-643 Concurrent queries cause BufferUnderflowException when storage in HDFS
* IOTDB-663 Fix query cache OOM while executing query
* IOTDB-664 Win -e option
* IOTDB-669 fix getting two columns bug while ”show devices“ in session
* IOTDB-692 merge behaves incorrectly
* IOTDB-712 Meet BufferUnderflowException and can not recover
* IOTDB-718 Fix wrong time precision of NOW()
* IOTDB-735 Fix Concurrent error for MNode when creating time series automatically 
* IOTDB-738 Fix measurements has blank 

* fix concurrent auto create schema conflict bug
* fix meet incompatible file error in restart
* Fix bugs of set core-site.xml and hdfs-site.xml paths in HDFS storage
* fix execute flush command while inserting bug
* Fix sync schema pos bug
* Fix batch execution bug, the following sqls will all fail after one error sql
* Fix recover endTime set bug


# Apache IoTDB (incubating) 0.9.3

## Bug Fixes
- IOTDB-531 Fix that JDBC URL does not support domain issue
- IOTDB-563 Fix pentaho cannot be downloaded because of spring.io address
- IOTDB-608 Skip error Mlog
- IOTDB-634 Fix merge caused errors for TsFile storage in HDFS
- IOTDB-636 Fix Grafana connector does not use correct time unit

## Miscellaneous changes
- IOTDB-528 Modify grafana group by
- IOTDB-635 Add workaround when doing Aggregation over boolean Series
- Remove docs of Load External Tsfile
- Add Grafana IoTDB Bridge Artifact to distribution in tools/grafana folder


# Apache IoTDB (incubating) 0.9.2

## Bug Fixes
- IOTDB-553 Fix Return Empty ResultSet when queried series doesn't exist
- IOTDB-575 add default jmx user and password; fix issues that jmx can't be accessed remotely
- IOTDB-584 Fix InitializerError when recovering files on HDFS
- Fix batch insert once an illegal sql occurs all the sqls after that will not succeed
- Fix concurrent modification exception when iterator TsFileResourceList 
- Fix some HDFS config issues 
- Fix runtime exception not be catched and sync schema pos was nullpointer bug in DataTransferManager
- Fix python rpc grammar mistakes
- Fix upgrade ConcurrentModificationException

## Miscellaneous changes
- IOTDB-332 support Chinese characters in path
- IOTDB-316 add AVG function to 4-SQL Reference.md and modify style 
- improve start-server.bat by using quotes to protect against empty entries
- Add Chinese documents for chapter 4.2
- change download-maven-plugin to 1.3.0
- add session pool 
- add insertInBatch in Session
- add insertInBatch to SessionPool
- modify 0.9 docs to fit website
- remove tsfile-format.properties
- add bloom filter in iotdb-engien.properties
- update server download doc
- typos fix in Rel/0.9 docs
- support 0.12.0 and 0.13.0 thrift

# Apache IoTDB (incubating) 0.9.1

## Bug Fixes

- IOTDB-159 Fix NullPointerException in SeqTsFileRecoverTest and UnseqTsFileRecoverTest
- IOTDB-317 Fix a bug that "flush + wrong aggregation query" causes the following queries to fail
- IOTDB-324 Fix inaccurate statistics when writing in batch
- IOTDB-327 Fix a groupBy-without-value-filter query bug caused by the wrong page skipping logic
- IOTDB-331 Fix a groupBy query bug when axisOrigin-startTimeOfWindow is an integral multiple of interval
- IOTDB-357 Fix NullPointerException in ActiveTimeSeriesCounter
- IOTDB-359 Fix a wrong-data-type bug in TsFileSketchTool
- IOTDB-360 Fix bug of a deadlock in CompressionRatio
- IOTDB-363 Fix link errors in Development-Contributing.md and add Development-Document.md
- IOTDB-392 Fix a bug in CSV export tool
- Fix apache rat header format error in some files

## Miscellaneous changes

- IOTDB-321 Add definitions of time expression and LEVEL in related documents
- Support pypi distribution for Python client

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
* IOTDB-199 Add a log visualization tool 
* IOTDB-203 Add "group by device" function for narrow table display
* IOTDB-205 Support storage-group-level Time To Live (TTL)
* IOTDB-208 Add Bloom filter in TsFile
* IOTDB-223 Add a TsFile sketch tool
* IoTDB-226 Hive-TsFile connector
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
* IOTDB-301 Bug Fix: executing "count nodes root" in client gets "Msg:3"
* Fix Dynamic Config when Creating Existing SG or Time-series
* Fix start-walchecker scripts for letting user define the wal folder
* Fix start script to set JAVA_HOME

# Apache IoTDB (incubating) 0.8.2

 This is a bug-fix version of 0.8.1 

-  IOTDB-264 lack checking datatype before writing WAL 
-  IOTDB-317 Fix "flush + wrong aggregation" causes failed query in v0.8.x 
-  NOTICE and LICENSE file update 

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
* IOTDB-110 Cli inserts data normally even if there is no space left on the disk
* IOTDB-118 When the disk space is full, the storage group is created successfully
* IOTDB-121 A bug of query on value columns
* IOTDB-128 Probably a bug in iotdb official website
* IOTDB-129 A bug in restoring incomplete tsfile when system restart
* IOTDB-131 IoTDB-Grafana-Connector module error when getting the timeseries list from Grafana
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
