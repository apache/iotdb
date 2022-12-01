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

# Apache IoTDB 1.0.0

## New Features

* New architecture that supports standalone and cluster mode with two types of nodes: ConfigNode, DataNode
* Support ConfigNode management: Start/Add, Stop, Remove
* Support DataNode management: Start/Add, Stop, Remove
* Support replication of ConfigNode, Schema and Data
* Support Consensus Protocol: Simple, IoT, Ratis
* Support Cluster management sql: show cluster, show regions
* Support administration in Cluster:  User, Permission, Role management
* Support authorization when login and executing a command
* Support create/show/count/delete database
* Support show/count devices
* Support create/show/count timeseries
* Support schema template management
* Support MPP(massively parallel process) framework in cluster
* Support insertion, deletion and all query types in Cluster
* Support CSV import/export tools
* Support TsFile import/export tools
* Support Sync TsFile from an IoTDB with one replica to another with any replica number
* Support UDF framework in Cluster
* Support new UDF 'change_points
* Support stateful, stateless Trigger in Cluster
* Support Select into in Cluster
* Support Continuous Query in Cluster
* Support flush on local/cluster
* Support clear cache on local/cluster
* Support metric for DataNode and ConfigNode with output to IoTDB, Prometheus, and JMX
* Support DBAPI in python client
* Support RestApi, MQTT for cluster
* Support having clause and between expression in query
* Support order by timeseries in last query
* Support hot configuration of data_dirs

# Apache IoTDB 0.13.3

## Improvements

* [IOTDB-4525] Accelerate restart process
* [IOTDB-3164] Add compaction memory control
* [IOTDB-4364] Reduce read amplification in compaction
* [IOTDB-4424] Specify error message when time value of insert sql can not be correctly parsed
* [IOTDB-4492] Control total file size of cross space compaction task
* [IOTDB-4542] Optimize schema validate error message
* Optimize pattern matching in Regexp

## Bug Fixes

* [IOTDB-3988] Fix reload problem of metric module and refactor metric module
* [IOTDB-4239] fix NPE to insert a null value into a TEXT timeseries
* [IOTDB-4318] Fix RESTAPI data type conversion failed
* [IOTDB-4320] Fix insert row with null cause recover throw NPE
* [IOTDB-4343] Fix NPE when using MQTT protocol
* [IOTDB-4357] fix start in windows, IOTDB_LOG_DIR_IS_UNDEFINED folder appears
* [IOTDB-4585] Incorrect query result after deleting data for aligned timeseries
* [IOTDB-4615] TTL adapts timestamp precision
* [IOTDB-4636] IndexOutOfBoundsException when compacting aligned series
* Fix PathAlreadyExistException during concurrent auto creating aligned timeseries

# Apache IoTDB 0.13.2

## Improvements

[IOTDB-2669] Improve C++ client insertTablet performance
[IOTDB-3087] enlarge default value of avg_series_point_number_threshold
[IOTDB-3861] Enable insert null values in Session
[IOTDB-3996] REST API nonQuery support Continuous Query
[IOTDB-4120] Optimize memory allocation of Expression
[IOTDB-4190] update documents about nifi-iotdb-bundle
REST support "select into" in nonQuery API
Import-CSV supports specify data type and no need quotation for text value

## Bug Fixes

[IOTDB-2736] DELETE_STORAGE_GROUP can not be granted to user (reporting 401)
[IOTDB-2760] Ordinary users do not grant any permissions, but can still show operations
[IOTDB-2769] Fix auth mapping of GRANT_ROLE_PRIVILEGE and GRANT_USER_ROLE
[IOTDB-3302] Without any authorization, ordinary users still have the right to query other user information
[IOTDB-4023] The C++ interface query result returns records error
[IOTDB-4047] Query NPE after change device alignment
[IOTDB-4096] Fix the names of metric pushed to Prometheus are inconsistency in micrometer and dropwizard
[IOTDB-4194] IOException happened in Compaction
[IOTDB-4215] Fix incorrect aggregate query results due to wrong unseq file traversal order
[IOTDB-4216] Fix execute create aligned timeseries but a non-aligned timeseries created
[IOTDB-4222] DeadLock when concurrently deleting and creating storage groups
[ISSUE-6774] Connection error when using DataGrip with JDBC driver
[ISSUE-6937] After restart, the aligned series turns to non-aligned
[ISSUE-6987] Fix select error when selecting a single quotation mark

# Apache IoTDB 0.13.1

## New Features

* [IOTDB-2602] "Without null" supports filtering based on partial columns
* [IOTDB-3873] Aligned timeseries support single point fill query
* [ISSUE-6171] Support createTimeseriesOfTemplate in Session
* [IOTDB-3742] Support COUNT NODES by root.**

## Improvements

* [IOTDB-2820] Update UserGuide SQL about Trigger
* [IOTDB-2837] Add check and sort for NumpyTablet to make sure timestamps are ordered
* [IOTDB-2838] Check and auto correct endian type for NumpyTablet
* [IOTDB-2873] provide json template for grafana
* [IOTDB-2888] Unary expression can followed by a constant
* [IOTDB-3747] Default Paging of Schema Query with limit 10000
* [IOTDB-3797] Print detailed info in session when connection fails
* [IOTDB-3851] C++ client method of tablet sorting optimization
* [IOTDB-3879] Modify document about the Programming-Cpp-Native-API
* [IOTDB-3901] C++ client method of insertRecordsOfOneDevice sorting optimization

## Bug Fixes

* [IOTDB-2753] Insert a time series with a null value and report 500
* [IOTDB-2759] Result of "Show paths set schema template" or "using template" is not complete
* [IOTDB-2775] Fix throwing exception when query non-exist device in TsFileSequenceReader
* [IOTDB-2787] Fix aligned mem chunk concurrent problem
* [IOTDB-2826] Fix can not drop schema template
* [IOTDB-2828] Update system_version in system.properties after upgrading
* [IOTDB-2835] Fix empty page in selfcheck method of TsFileSequenceReader
* [IOTDB-2837] Add check and sort for NumpyTablet to make sure timestamps are ordered
* [IOTDB-2852] The import-csv tool can not import the data to nonaligned device
* [IOTDB-2859] Fix python tablet with None value is incorrect
* [IOTDB-2862] Fix SQL injection risks of grafana-connector
* [IOTDB-2864] Fix Read-only occurred when insert Text value to aligned timeseries
* [IOTDB-2882] Fix unary expression display bug
* [IOTDB-2902] Handling user privileges for aligned timeseries related features
* [IOTDB-2903] Fix show latest timeseries result does not sorted by time
* [IOTDB-2910] Fix count result not right after deleting storage group
* [IOTDB-2915] MLogTxtWriter error while parsing CreateAlignedTimeseriesPlan
* [IOTDB-2922] Fix NPE when compacting with files that contains zero device
* [IOTDB-2924] UDF Framework: index overflow while iterating sliding windows
* [IOTDB-2983] Serialization error in Partial insert
* [IOTDB-2999] Remove useless config and fix default value error.
* [IOTDB-3018] Fix compaction bugs on handling deleted target file
* [IOTDB-3029] The prefix path generated by the select into target sequence contains * and ** currently unchecked
* [IOTDB-3045] The query result contains some timeseries that have been deleted
* [IOTDB-3120] Print the tsfile name when meet IOException
* [IOTDB-3158] Fix NPE exception when use iotdb-reporter
* [IOTDB-3160] TsFile will be corrupted when flushing memtable appears OOM
* [IOTDB-3168] Forbid the path with * could be inserted
* [IOTDB-3171] Fix NPE when getting modification file
* [IOTDB-3219] Fix stop-server on windows
* [IOTDB-3247] Recover aligned sensors after deleting timeseries, query lost data
* [IOTDB-3301] Tag recover bug after IoTDB server restart
* [IOTDB-3364] Fix Query stucked with null valued aligned timeseries bug
* [IOTDB-3420] Fix show paths set schema template t1 error
* [IOTDB-3494] Fix TypeError in py-session
* [IOTDB-3523] Fix the count and COUNT not equal bug when querying with group by level
* [IOTDB-3645] Fix use statistics bug in aggregation query
* [IOTDB-3651] Stop compaction schedule when all compaction is disable
* [IOTDB-3709] Fix a loop occurred in TsFileResourceList, causing the query to fail and oom occurs
* [IOTDB-3730] Fix ArrayIndexOutOfBounds when flushing a memtable
* [IOTDB-3795] Remove setting read-only when handling compaction exception
* [IOTDB-3803] failed to insert data of TEXT by session
* [IOTDB-3816] Fix /zero bug in recover
* [IOTDB-3822] Fix cross compaction overlap bug
* [IOTDB-3826] Fix duplicate success when concurrent creating same timeseries
* [IOTDB-3858] IndexOutOfBoundsException: bitIndex < 0
* [ISSUE-5482] Fix Release zip files include incorrect version of Guava
* [ISSUE-5773] Aggregation result is not complete
* [ISSUE-5964] Fix bug of aligned timeseries time duplicated aggregation


# Apache IoTDB 0.13.0

## New Features

* [IOTDB-924] Support insert multi rows in SQL
* [IOTDB-959] Add Create Storage Group Grammar 
* [IOTDB-1037] set rpc\_compression as a parameter in JDBC URL
* [IOTDB-1059] Support sql statement insert without timestamp
* [IOTDB-1143] Support Continuous query
* [IOTDB-1199] Support aligned timeseries and schema template 
* [IOTDB-1319] Support Trigger
* [IOTDB-1391] Add a new Aggregation function extreme (max absolute value)
* [IOTDB-1399] Add a session interface to connect multiple nodes
* [IOTDB-1400] Support arithmetic operations in SELECT clauses
* [IOTDB-1403] Dictionary encoding for TEXT
* [IOTDB-1490] Add built-in UDTFs: sinh, conh, tanh
* [IOTDB-1514] Support null in insertTablet
* [IOTDB-1524] Support SELECT ... INTO ... clause 
* [IOTDB-1647] Nested Expressions in SELECT clauses
* [IOTDB-1673] CLI upgrade to JLine3
* [IOTDB-1739] Constant timeseries generating functions (const, pi and e) 
* [IOTDB-1760] Support avg, count, extreme, first\_value, last\_value, max\_time, max\_value, min\_time, min\_value, sum aggregations in group by fill
* [IOTDB-1761] Add metric framework for IoTDB
* [IOTDB-1775] Add CAST function to convert data type
* [IOTDB-1823] Support group by multi level
* [IOTDB-1844] Query support timeseries prefix and suffix matching: root.\*sg\*
* [IOTDB-1859] Support REST API
* [IOTDB-1860] New Grafana plugin
* [IOTDB-1886] Support Constant Expressions in Select Clauses
* [IOTDB-1973] Supports aggregate queries, constants, and arithmetic nested expressions in SELECT clauses
* [IOTDB-1986] Support select UDF as alisa clauses 
* [IOTDB-1989] Spark-IoTDB-connector support inserting data
* [IOTDB-2131] Support previous, linear, constant value fill funtion New fill
* [ISSUE-3811] Provide a data type column for the last query dataset
* add rabbitmq example

## Improvements

* [IOTDB-1280] Rewrite the Antlr grammar file
* [IOTDB-1372] Enhance management of TsFileResource 
* [IOTDB-1428] Ask query threads to quit if query is timeout 
* [IOTDB-1450] Deletion should only delete related time partitions
* [IOTDB-1463] Implement builder pattern for Session and SessionPool 
* [IOTDB-1477] Optimize code logic of generateAlignByDevicePlan() 
* [IOTDB-1559] Refactor the IT framework
* [IOTDB-1564] Make hearbeat and election timeout parameters be configurable
* [IOTDB-1581] Consider deletions when recovering tsFileResource of incomplete tsfile 
* [IOTDB-1607] Optimize Tracing
* [IOTDB-1613] Recover mods file if  write modification failed
* [IOTDB-1639] Refactoring the cluster class structure to make it consistent with the server module 
* [IOTDB-1730] client-cpp, enhance session::insertTablet() etc.'s performance 
* [IOTDB-1852] Accelerate queryies by using statistics
* [IOTDB-1857] Remove useless handle logic for CountPlan in executeNonQuery in cluster module
* [IOTDB-1884] Distinguish between zero and null values in sum aggregation
* [IOTDB-1924] Remove the operation of clearing the cache after the compaction is over
* [IOTDB-1950] Add Bloom Filter cache for query 
* [IOTDB-2001] Remove redundant StorageGroupNotReadyException
* [IOTDB-2011] Update last cache while doing show latest timeseries query
* [IOTDB-2022] SessionDataSet implements AutoCloseable
* [IOTDB-2075] Accelerate the process of insertTablets by using thread pool
* [IOTDB-2119] IoTDB CSV export tools add timestamp accuracy setting function 
* [IOTDB-2162] Simplify the recovery merge process
* [IOTDB-2176] Limit target chunk size when performing inner space compaction
* [IOTDB-2193] Reduce unnecessary lock operations of RaftLogManager to improve writing performance 
* [IOTDB-2195] Control the concurrent query execution thread
* [IOTDB-2632] Set compaction_write_throughput_mb_per_sec to 16 by default
* [ISSUE-3445] New compaction strategy and compaction scheduling strategy
* [ISSUE-3856] refine exception handling logic in commitTo in RaftLogManager
* [Cluster] No need to shake hands with itself when one node restart 


## Incompatible changes

* [IOTDB-1026] Support wildcard \*\* in Path And Replace PrefixPath usage with PathPattern in IOTDB-SQL
* [IOTDB-1620] Support backtick (\`) character and double quotes (") to quote identifiers 
* [IOTDB-1650] Rename the sql command `move` to `unload` 

## Miscellaneous changes

* [IOTDB-1342] modify error message about LIMIT and OFFSET used in conjunction with the FILL clause
* [IOTDB-1372] delete devices field in FileTimeIndex 
* [IOTDB-1531] Check tsfile creation time when recovering
* [IOTDB-1541] Change sequence of wal and memtable in insertion
* [IOTDB-1923] Separate the request unpacking and execution processing logic of TSServiceImpl
* [IOTDB-1969] Remove statistic resources of old metric framework from iotdb-server
* [IOTDB-2014] MicrometerPrometheusReporter#getReporterType return null 
* [IOTDB-2043] refactor: remove haveSorted param from Session
* [IOTDB-2154] add TsFileUtils.isTsFileComplete
* [IOTDB-2206] Rename StorageGroupProcessor to VirtualStorageGroupProcessor
* [IOTDB-2208] Reconstruct the process of generating resultset header of query
* [ISSUE-4047] Generic type in Statistics extend Serializable 
* Add compaction version in cache key
* Add a constructor of IoTDBDescriptorHolder to prohibit instantiation

## Bug Fixes

* [IOTDB-1266] Fix SHOW TIMESERIES will only display 2000 timeseries
* [IOTDB-1478] The whole IoTDB can not read/write if any one sg is not ready
* [IOTDB-1562] Fix incorrect exception processing in insertXXX() API
* [IOTDB-1583] Raft log failed to be committed in cluster version 
* [IOTDB-1592] BugFix: SLimit Not effective in align by device 
* [IOTDB-1736] Fix error code is not printed in TSServiceImpl's log 
* [IOTDB-1749] sync-tool's lockInstance() dose not take effect
* [IOTDB-1758] sync-tool, empty uuid file cause tool can not auto-recovery
* [IOTDB-1848] Failed to initialize pool: Does not support setReadOnly in grafana-connector
* [IOTDB-1853] Fix bug about more than one TimeseriesMetadata in TsFile
* [IOTDB-2010] fix incomplete show timeseries result
* [IOTDB-2021] Fix Bloom Filter Cache doesn't take effect bug 
* [IOTDB-2060] Fix NPE when using fill without a filter
* [IOTDB-2074] Fix NullPointerException when recovering compaction in MacOS
* [IOTDB-2077] Unexpected exception when executing raw data query without VF with limit clause
* [IOTDB-2088] Fix IndexOutOfBound exception in AggregationPlan.groupAggResultByLevel
* [IOTDB-2116] Fix all client connections are stuck bug caused by logback bug 
* [IOTDB-2129] Wrong result returned when querying by nested expressions with aliases
* [IOTDB-2143] fix wrong precision in cluster mode
* [IOTDB-2153] [IOTDB-2157] fix incorrect path search space 
* [IOTDB-2159] Fix wrong status code when inserting data to a read-only cluster
* [IOTDB-2163] Fix unexpected amount of columns in a cluster slimit query
* [IOTDB-2174] Fix Regexp filter serializing and deserializing error 
* [IOTDB-2180] Fix show latest timeseries in cluster
* [IOTDB-2183] Fix the config problems in cluster mode
* [IOTDB-2185] Fix get an exception when parsing the header of CSV
* [IOTDB-2209] Fix logback CVE-2021-42550 issue
* [IOTDB-2251] [IOTDB-2252] Fix Query deadlock 
* [IOTDB-2267] UDF: Error code 500 caused by user logic
* [IOTDB-2282] fix tag recovery bug after tag upsert
* [IOTDB-2290] Fix Incorrect query result in C++ client 
* Fix CPP client could not be successfully built on windows
* Fix dead lock in setDataTTL method


# Apache IoTDB 0.12.5

## New Features
* [IOTDB-2078] Split large TsFile tool
* [IOTDB-2192] Support extreme function

## Improvements
* [IOTDB-1297] Refactor the memory control when enabling time partitions
* [IOTDB-2195] Control the concurrent query execution thread
* [IOTDB-2475] Remove sg not ready log in batch process
* [IOTDB-2502] Add query sql in error log if encountering exception
* [IOTDB-2506] Refine the lock granularity of the aggregation query
* [IOTDB-2534] add character support while using double quote
* [IOTDB-2562] Change default value of sync mlog period parameter
* Avoid too many warning logs being printed, when opening too many file handlers


## Bug Fixes
* [IOTDB-1960] Fix count timeseries bug in cluster mode
* [IOTDB-2174] Fix Regexp filter serializing and deserializing error
* [IoTDB-2185] fix get an exception bug when parsing the header of CSV
* [IOTDB-2194] Fix SHOW TIMESERIES will only display 2000 timeseries in cluster mode
* [IOTDB-2197] Fix datatype conversion exception in Spark Connector
* [IOTDB-2209] Fix logback CVE-2021-42550 issue
* [IOTDB-2219] Fix query in-memory data is incorrect in cluster mode
* [IOTDB-2222] Fix OOM and data was written in incorrectly bugs of Spark Connector
* [IOTDB-2251] [IOTDB-2252] Fix Query deadlock
* [IOTDB-2282] fix tag recover bug after tag update
* [IOTDB-2320] MemoryLeak cause by wal Scheduled trim task thread
* [IOTDB-2381] Fix deadlock caused by incorrect buffer pool size counter
* [IOTDB-2400] Fix series reader bug
* [IOTDB-2426] WAL deadlock caused by too many open files
* [IOTDB-2445] Fix overlapped data should be consumed first bug
* [IOTDB-2499] Fix division by zero error when recovering merge
* [IOTDB-2507] Fix NPE when merge recover
* [IOTDB-2528] Fix MLog corruption and recovery bug after killing system
* [IOTDB-2532] Fix query with align by device can't get value after clear cache
* [IOTDB-2533] Fix change max_deduplicated_path_num does not take effect
* [IOTDB-2544] Fix tag info sync error during metadata sync
* [IOTDB-2550] Avoid show timeseries error after alter tag on sync sender
* [IOTDB-2567]  Fix thread and ByteBuffer leak after service stopped
* [IOTDB-2568] "show query processlist" is blocked
* [IOTDB-2580] Fix DirectByteBuffer and thread leak when deleting storage group
* [IOTDB-2584] Fix cross space compaction selector
* [IOTDB-2603] Fix compaction recover
* [IOTDB-2604] Fix batch size is invalid in import-csv tool
* [IOTDB-2620] Unrecognizable operator type (SHOW) for AuthorityChecker
* [IOTDB-2624] Fix "overlapped data should be consumed first" occurs when executing query
* [IOTDB-2640] Fix cross compaction recover bug
* [IOTDB-2641] Fix time range of TsFile resource overlaps after unseq compaction
* [IOTDB-2642] Fix the new file has a higher compact priority than the old file in unseq compaction
* Fix a logical bug in processPlanLocally in cluster mode
* Throw Exception while using last query with align by device
* Add a judgement to determine raft log size can fit into buffer before log appending in cluster mode
* Fix grafana can't be used bug

# Apache IoTDB 0.12.4

## New Features

* [IOTDB-1823] group by multi level

## Improvements

* [IOTDB-2027] Rollback invalid entry after WAL writing failure
* [IOTDB-2061] Add max concurrent sub query parameter, read data in batches to limit max IO and add max cached buffer size configuration
* [IOTDB-2065] release TsFileSequenceReader soon when it is no longer used
* [IOTDB-2072] Remove TVListAllocator to reduce the TVList mem cost
* [IOTDB-2101] Reduce the memory footprint of QueryDataSource
* [IOTDB-2102] Push limit operator down to each reader
* [IOTDB-2123] Accelerate recovery process
* update user guide for cpp-cpi and disable compiling nodejs in cpp-cli
* Ignore too many WAL BufferOverflow log


## Bug Fixes
* [IOTDB-1408] Statement with 'as' executes incorrectly in mutil-path scenes
* [IOTDB-2023] Fix serializing and deserializing bugs of Filters
* [IOTDB-2025] Fix count nodes and devices incorrectly in cluster
* [IOTDB-2031] Fix incorrect result of descending query with value filter in cluster
* [IOTDB-2032] Fix incorrect result of descending query with multiple time partitions
* [IOTDB-2039] Fix data redundant after too many open files exception occurs during compaction
* [IOTDB-2047] Fix NPE when memControl is disabled and insert TEXT value to a non-TEXT series
* [IOTDB-2058] Fix Query is blocked without sub-query-threads exist bug
* [IOTDB-2063] Fix MinTimeDescAggregationResult implementation bug
* [IOTDB-2064] Fix the NPE caused by map serde
* [IOTDB-2068] Fix GZIP compressor meets ArrayIndexOutOfBoundsException
* [IOTDB-2124] the filtering condition does not take efffect for last query in cluster
* [IOTDB-2138] Fix data loss after IoTDB recover
* [IOTDB-2140] Fix merge throw NullPointerException
* [IOTDB-2152] PyClient: Override `__eq__()` of TSDataType, TSEncoding and Compressor to avoid unexpected comparation behaviour
* [IOTDB-2160] Fix cluster groupby query cross-node reference leaks
* [ISSUE-3335] Fix the bug of start-cli.sh -e mode can't work with wild card \*
* fix memory leak: replace RandomDeleteCache with Caffine CacheLoader
* Fix connection refused using session when users forget to set client ip


# Apache IoTDB 0.12.3

## Improvements

* [IOTDB-842] Better Export/Import-CSV Tool
* [IOTDB-1738] Cache paths list in batched insert plan
* [IOTDB-1792] remove tomcat-embed dependency and make all transitive dependencies versions consistent
* [ISSUE-4072] Parallel insert records in Session
* Print the file path while meeting error in case of reading chunk

## Bug Fixes

* [IOTDB-1275] Fix backgroup exec for cli -e function causes an infinite loop
* [IOTDB-1287] Fix C++ class Session has 2 useless sort()
* [IOTDB-1289] fix CPP mem-leak in SessionExample.cpp insertRecords()
* [IOTDB-1484] fix auto create schema in cluster
* [IOTDB-1578] Set unsequnce when loading TsFile with the same establish time
* [IOTDB-1619] Fix an error msg when restart iotdb-cluster
* [IOTDB-1629] fix the NPE when using value fill in cluster mode
* [IOTDB-1632] Fix Value fill function fills even when the data exists
* [IOTDB-1651] add reconnect to solve out of sequence in sync module
* [IOTDB-1659] Fix Windows CLI cannot set maxPRC less than or equal to 0
* [IOTDB-1670] Fix cli -e mode didn't fetch timestamp_precision from server
* [IOTDB-1674] Fix command interpret error causing somaxconn warning failed
* [IOTDB-1677] Fix not generate file apache-iotdb-0.x.x-client-cpp-linux-x86_64-bin.zip.sha512
* [IOTDB-1678] Fix client-cpp session bug: can cause connection leak.
* [IOTDB-1679] client-cpp: Session descontruction need release server resource
* [IOTDB-1690] Fix align by device type cast error
* [IOTDB-1693] fix IoTDB restart does not truncate broken ChunkGroup bug
* [IOTDB-1703] Fix MManager slow recover with tag
* [IOTDB-1714] fix Could not find or load main class when start with jmx on win 
* [IOTDB-1723] Fix concurrency issue in compaction selection
* [IOTDB-1726] Wrong hashCode() and equals() method in ChunkMetadata
* [IOTDB-1727] Fix Slow creation of timeseries with tag
* [IOTDB-1731] Fix sync error between different os
* [IOTDB-1733] Fix dropping built-in function
* [IOTDB-1741] Avoid double close in level compaction execution
* [IOTDB-1785] Fix Illegal String ending with . being parsed to PartialPath
* [IOTDB-1836] Fix Query Exception Bug after deleting all sgs
* [IOTDB-1837] Fix tagIndex rebuild failure after upgrade mlog from mlog.txt to mlog.bin
* [IOTDB-1838] The compacting status in SGP is always false
* [IOTDB-1846] Fix the error when count the total number of devices in cluster mode
* [IoTDB-1847] Not throw excpetion when pulling non--existent time series
* [IOTDB-1850] Fix deserialize page merge rate limiter
* [IoTDB-1865] Compaction is blocking when removing old files in Cluster
* [IOTDB-1868] Use RwLock to reduce the lock time for nodeRing
* [IOTDB-1872] Fix data increases abnormally after IoTDB restarts
* [IOTDB-1877] Fix Sync recovery and reconnection bugs in both sender and receiver
* [IOTDB-1879] Fix some Unsequence files never be merged to higher level or Sequence folder
* [IOTDB-1887] Fix importing csv data containing null throws exception
* [IOTDB-1893] Fix Can not release file lock in sync verify singleton 
* [IOTDB-1895] Cache leader optimization for batch write interfaces on multiple devices
* [IOTDB-1903] Fix IndexOutOfRangeException when starting IoTDB
* [IoTDB-1913] Fix When exporting a amount of data from csv, it will report network error or OOM
* [IOTDB-1925] Fix the modification of max_select_unseq_file_num_in_each_compaction parameter does not take effect
* [IOTDB-1958] Add storage group not ready exception
* [IOTDB-1961] Cluster query memory leak
* [IOTDB-1975] OOM caused by that MaxQueryDeduplicatedPathNum doesn't take effect
* [IOTDB-1983] Fix DescReadWriteBatchData serializing bug
* [IOTDB-1990] Fix unchecked null result by calling IReaderByTimestamp.getValuesInTimestamps()
* [ISSUE-3945] Fix Fuzzy query not support multiDevices and alignByDevice Dataset
* [ISSUE-4288] Fix CI issue caused by the invalid pentaho download url
* [ISSUE-4293] SessionPool: InterruptedException is not properly handled in synchronized wait()
* [ISSUE-4308] READ_TIMESERIES privilege granted to users and roles can not take effect when quering by UDFs
* fix merge ClassCastException: MeasurementMNode
* change sync version check to major version
* init dummyIndex after restart cluster

# Apache IoTDB 0.12.2

## New Features

* [IOTDB-959] Add create storage group Grammar
* [IOTDB-1399] Add a session interface to connect multiple nodes
* [IOTDB-1466] Support device template
* [IOTDB-1491] UDTF query supported in cluster
* [IOTDB-1496] Timed flush memtable
* [IOTDB-1536] Support fuzzy query REGEXP
* [IOTDB-1561] Support fill by specific value
* [IOTDB-1565] Add sql: set system to readonly/writable
* [IOTDB-1569] Timed close TsFileProcessor
* [IOTDB-1586] Support mysql-style Like clause
* [ISSUE-3811] Provide a data type column for the last query dataset
* TTL can be set to the prefix path of storage group
* add JMX monitor to all ThreadPools in the server module

## Improvements

* [IOTDB-1566] Do not restrict concurrent write partitions
* [IOTDB-1585] ModificationFile‘s write interface blocking
* [IOTDB-1587] SessionPool optimization: a more aggressive Session creation strategy
* Use StringCachedPool in TsFileResource to reduce the memory size
* write performance optimization when replicaNum == 1
* Optimize Primitive Array Manager
* Function Improvement: add overlapped page rate in Tracing

## Bug Fixes

* [IOTDB-1282] fix C++ class SessionDataSet mem-leak
* [IOTDB-1407] fix Filtering time series based on tags query fails Occasionally
* [IOTDB-1437] Fix the TsFileSketchTool NPE
* [IOTDB-1442] Time filter & TTL do not take effect in cluster
* [IOTDB-1452] remove compaction log/ change logger to daily
* [IOTDB-1447] ClientPool is blocking other nodes when one node fails
* [IOTDB-1456] Fix Error occurred while executing delete timeseries statement
* [IOTDB-1461] Fix compaction conflicts with ttl
* [IOTDB-1462] Fix cross space compaction recover null pointer bug
* [IOTDB-1464] fix take byte array null pointer
* [IOTDB-1469] fix cross space compaction lost data bug
* [IOTDB-1471] Fix path not right in "sg may not ready" log
* [IOTDB-1475] MeasurementId check while create timeseries or template/ disable time or timestamp in timeseries path
* [IOTDB-1488] Fix metaMember's forwarding clientPool timeout in cluster module
* [IOTDB-1494] fix compaction block flush bug
* [IoTDB-1499] Remove series registration using IoTDBSink
* [IoTDB-1501] Fix compaction recover delete tsfile bug
* [IOTDB-1529] Fix mlog recover idx bug and synchronize setStorageGroup
* [IOTDB-1537] fix insertTablet permission
* [IOTDB-1539] Fix delete operation with value filter is abnormal
* [IOTDB-1540] Bug Fix: 500 when using IN operator
* [IOTDB-1541] Fix query result not right due to non-precise time index of resource
* [IOTDB-1542] Cpp client segment fault: char[] buffer overflow caused by long exception message
* [IOTDB-1545] Query dataset memory leak on server caused by cpp client
* [IOTDB-1546] Optimize the Upgrade Tool rewrite logic to reduce the temp memory cost
* [IOTDB-1552] Only allow equivalent filter for TEXT data type
* [IOTDB-1556] Abort auto create device when meet exception in setStorageGroup
* [IOTDB-1574] Deleted file handler leak
* [IOTDB-1580] Error result of order by time desc when enable time partition
* [IOTDB-1584] Doesn't support order by time desc in cluster mode
* [IOTDB-1588] Bug fix: MAX_TIME is incorrect in cluster mode
* [IOTDB-1594] Fix show timeseries returns incorrect tag value
* [IOTDB-1600] Fix InsertRowsOfOneDevicePlan being not supported in cluster mode
* [IOTDB-1610] Fix TsFileRewriteTool writing incorrect data file
* [ISSUE-3116] Bug when using natural month unit in time interval in group by query
* [ISSUE-3316] Query result with the same time range is inconsistent in group by query
* [ISSUE-3436] Fix query result not right after deleting multiple time interval of one timeseries
* [ISSUE-3458] fix load configuration does not take effect
* [ISSUE-3545] Fix Time interval value is disorder in group by month
* [ISSUE-3653] fix Max_time and last return inconsistent result
* [ISSUE-3690] Memory leaks on the server when cpp client invokes checkTimeseriesExists
* [ISSUE-3805] OOM caused by Chunk cache
* [ISSUE-3865] Meaningless connection reset issues caused by low default value for SOMAXCONN
* Fix DataMigrationExample OOM if migrate too many timeseries
* Handle false positive cases which may cause NPE of tsfile bloom filter
* Fix Windows shell error on JDK11 & fix iotdb-env.bat not working
* Fix cluster auto create schema bug when retry locally
* Fix thrift out of sequence in cluster module
* Skip non exist measurement in where clause in align by device
* fix blocking query when selecting TsFile in compaction
* Fix redundant data in compaction recover
* Fix load tsfile with time partition enable

## Incompatible changes

* [IOTDB-1485] Replace tsfile_size_threshold by unseq_tsfile_size/seq_tsfile_size

## Miscellaneous changes

* [IOTDB-1499] Remove unused exception throwing notation in IoTDBSink
* [IOTDB-1500] Remove current dynamic query memory control
* [ISSUE-3674] Disable thrift code generation for Javascript
* enable cacheLeader by default
* add audit log when execute delete and set sg for tracing
* modify nodeTool user to root

# Apache IoTDB 0.12.1

## Bug Fixes

* [GITHUB-3373] Remove the broken cached leader connection & optimize the insertRecords method in session
* [IOTDB-1433] Fix bug in getMetadataAndEndOffset when querying non-exist device
* [IOTDB-1432] fix level compaction loss data
* [IOTDB-1427] Fix compaction lock with query
* [IOTDB-1420] Fix compaction ttl bug
* [IOTDB-1419] Remove redundant clearCompactionStatus, fix continuous compaction doesn't take effect when
  enablePartition
* [IOTDB-1415] Fix OOM caused by ChunkCache
* [IOTDB-1414] NPE occurred when call getStorageGroupNodeByPath() method using not exist path
* [IOTDB-1412] Unclear exception message thrown when executing empty InsertTabletPlan
* [IOTDB-1411] Fix thriftMaxFrameSize and thriftDefaultBufferSize does not in effect
* [IOTDB-1398] Do not select unseq files when there are uncompacted old unseq files
* [IOTDB-1390] Fix unseq compaction loss data bug
* [IOTDB-1384] Fix group by bug
* [ISSUE-3378] Fix NPE when clear upgrade folder; Fix some upgraded pageHeader missing statistics
* [GITHUB-3339] Try to fix sg dead lock
* [GITHUB-3329] Fix upgrade NPE and DeadLock
* [GITHUB-3319] Fix upgrade tool cannot close file reader
* [IOTDB-1212] Fix The given error message is not right when executing select sin(non_existence) from root.sg1.d1
* [IOTDB-1219] Fix a potential NPE issue in UDF module
* [IOTDB-1286] Fix 4 C++ mem-leak points
* [IOTDB-1294] Fix delete operation become invalid after compaction
* [IOTDB-1313] Fix lossing time precision when import csv with unsupported timestamp format
* [IOTDB-1316] The importCsv tool should continue inserting if a part of insertion failed
* [IOTDB-1317] Fix log CatchUp always failed due to not check the follower's match index
* [IOTDB-1323] Fix return a success message when encounter RuntimeException during the insertion process
* [IOTDB-1325] Fix StackOverflow Exception in group by natural month query
* [IOTDB-1330] Fix the load tsfile bug when the cross multi partition's tsfile only have one page
* [IOTDB-1348] Fix Last plan not work in cluster mode
* [IOTDB-1376] Fix BatchProcessException was not correctly handled in BaseApplier
* [ISSUE-3277] Fix TotalSeriesNumber in MManager counted twice when recovering
* [ISSUE-3116] Fix bug when using natural month unit in time interval in group by query
* [ISSUE-3309] Fix InsertRecordsOfOneDevice runs too slow
* Fix the plan index is always zero when using insertRecords interface to run the cluster
* Add authority check for users create timeseries using executeBatch interface without the privilege
* Fix versionInfo NPE when query upgrading 0.11 tsfile
* Fix upgrade tool cannot load old tsfile if time partition enabled in 0.11
* Fix import csv throw ArrayOutOfIndexError when the last value in a line is null
* Fix upgrade tool cannot close file reader

## Improvements

* [GITHUB-3399] Change the default primitive array size to 32
* [IOTDB-1387] Support Without Null ALL in align by device clause, Filter RowRecord automatically if any column in it is
  null or all columns are null
* [IOTDB-1385] Extract the super user to the configuration
* [IOTDB-1315] ExportCsvTool should support timestamp `yyyy-MM-dd'T'HH:mm:ss.SSSZ`
* [IOTDB-1339] optimize TimeoutChangeableTSnappyFramedTransport
* [IOTDB-1356] Separate unseq_file_num_in_each_level from selecting candidate file in unseq compaction
* [IOTDB-1357] Compaction use append chunk merge strategy when chunk is already large enough
* [IOTDB-1380] Automatically close the dataset while there is no more data
* Optimize sync leader for meta

## New Features

* [GITHUB-3389] TTL can be set to any path
* [GITHUB-3387] Add parameter compaction_interval=10000ms
* [IOTDB-1190] Fully support HTTP URL char set in timeseries path
* [IOTDB-1321][IOTDB-1322] Filter RowRecord automatically if any column in it is null or all columns are null
* [IOTDB-1357] Compaction use append chunk merge strategy when chunk is already large
* [ISSUE-3089] Make it possible for storage groups to have name with hyphen

## Miscellaneous changes

* [GITHUB-3346] upgrade netty and claim exclusion for enforcer check
* [IOTDB-1259] upgrade libthrift from 0.12.0/0.13.0 to 0.14.1
* Uncomment the less used configurations
* Enable the configration `concurrent_writing_time_partition`


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

# Apache IoTDB 0.11.4

## Bug Fixes

* IOTDB-1303 Disable group by without aggregation function in select clause
* IOTDB-1306 Fix insertion blocked caused the deadlock in memory control module
* IOTDB-1308 Fix users with READ_TIMESERIES permission cannot execute group by fill queries
* IOTDB-1344 Fix cannot create timeseries caused by the timeseries count doesn't reset when deleting storage group
* IOTDB-1384 Some value will disappear while using group by query
* IOTDB-1398 Do not select unseq files when there are uncompacted old unseq files
* ISSUE-3316 Fix query result with the same time range is inconsistent in group by query
* Fix TotalSeriesNumber in MManager counted twice when recovering
* Fix unseq compaction throws a wrong exception if some paths are not in the file
* Fix overlapped data should be consumed first exception when query

## Improvements

* IOTDB-1356 Separate unseq_file_num_in_each_level from selecting candidate file in unseq compaction
* IOTDB-1412 Unclear exception message thrown when executing empty InsertTabletPlan
* continuous compaction in level compaction strategy when no tsfile is to be closed

## New Features

* support brackets with number in timeseries path

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
