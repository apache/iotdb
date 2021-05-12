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

# Data partition

Time series data is partitioned on two levels of storage groups and time ranges.

## Storage group

The storage group is specified by the user display. Use the statement "SET STORAGE GROUP TO" to specify the storage group. Each storage group has a corresponding StorageGroupProcessor.

The main fields it has are:

* Read-write lock: insertLock

* Unclosed sequential file processors for each time partition: workSequenceTsFileProcessors

* Unclosed out-of-order file processor corresponding to each time partition: workUnsequenceTsFileProcessors

* Full sequential file list for this storage group (sorted by time): sequenceFileTreeSet

* List of all out-of-order files for this storage group (unordered): unSequenceFileList

* A map that records the last write time of each device. When sequential data is flashed, the time recorded by this map is used: latestTimeForEachDevice

* A map that records the last flash time of each device to distinguish between sequential and out-of-order data: latestFlushedTimeForEachDevice

* A version generator map corresponding to each time partition, which is convenient for determining the priority of different chunks when querying: timePartitionIdVersionControllerMap


### Related code

* src/main/java/org/apache/iotdb/db/engine/StorageEngine.java


## Time range

The data in the same storage group is partitioned according to the time range specified by the user. The related parameter is partition_interval and the default is week. That is, data of different weeks will be placed in different partitions.

### Implementation logic

StorageGroupProcessor performs partition calculation on the inserted data to find the specified TsFileProcessor, and the TsFile corresponding to each TsFileProcessor will be placed in a different partition folder.

### File structure

The file structure after partitioning is as follows:

data

-- sequence

---- [Storage group name1]

------ [Time division ID1]

-------- xxxx.tsfile

-------- xxxx.resource

------ [Time division ID2]

---- [Storage group name 2]

-- unsequence

### Related code

* getOrCreateTsFileProcessorIntern  method in src/main/java/org/apache/iotdb/db/engine/storagegroup.StoragetGroupProcessor.java
