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

# Time partition

## Features

Time partition divides data according to time, and a time partition is used to save all data within a certain time range. The time partition number is represented by a natural number. Number 0 means January 1, 1970, it will increase by one every partition_interval milliseconds. Time partition number's calculation formula is timestamp / partition_interval. The main configuration items are as follows:

* time\_partition\_interval

|Name| time\_partition\_interval                                                              |
 |:---:|:-------------------------------------------------------------------------------------------------------|
|Description| Time range for dividing database, time series data will be divided into groups by this time range |
|Type| Int64                                                                                                  |
|Default| 604800000                                                                                               |
|Effective| Only allowed to be modified in first start up                                                          |

## Configuration example

Enable time partition and set partition_interval to 86400000 (one day), then the data distribution is shown as the following figure:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Data-Concept/Time-Partition/time_partition_example.png?raw=true" alt="time partition example">

* Insert one datapoint with timestamp 0, calculate 0/86400000 = 0, then this datapoint will be stored in TsFile under folder 0

* Insert one datapoint with timestamp 1609459200010, calculate 1609459200010/86400000 = 18628, then this datapoint will be stored in TsFile under folder 18628

## Suggestions

When enabling time partition, it is better to enable timed flush memtable, configuration params are detailed in [Config manual for timed flush](../Reference/DataNode-Config-Manual.md).

* enable_timed_flush_unseq_memtable: Whether to enable timed flush unsequence memtable, enabled by default.

* enable_timed_flush_seq_memtable: Whether to enable timed flush sequence memtable, disabled by default. It should be enabled when time partition is enabled, so inactive time partition's memtable can be flushed regularly.