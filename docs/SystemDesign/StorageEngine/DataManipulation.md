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

# Data addition, deletion and modification

The following describes four common data manipulation operations, which are insert, update, delete, and TTL settings.

## Data insertion

### Write a single line of data (one device, one timestamp, multiple values)

* Corresponding interface
  * JDBC's execute and executeBatch interfaces
  * Session's insertRecord and insertRecords
* Main entrance: ```public void insert(InsertRowPlan insertRowPlan)```   StorageEngine.java
  * Find the corresponding StorageGroupProcessor
  * Find the corresponding TsFileProcessor according to the time of writing the data and the last time stamp of the current device order
  * Pre-write log
  * Typo in mestable corresponding to TsFileProcessor
      * If the file is out of order, update the endTimeMap in tsfileResource
      * If there is no information about the device in tsfile, then update the startTimeMap in tsfileResource
  * Determine whether to trigger asynchronous persistent memtable operation based on memtable size
      * If it is a sequential file and the flashing action is performed, the endTimeMap in tsfileResource is updated
  * Determine whether to trigger a file close operation based on the size of the current disk TsFile

### Batch data (multiple timestamp multiple values for one device) write

* Corresponding interface
	* Session‘s insertTablet

* Main entrance: ```public void insertTablet(InsertTabletPlan insertTabletPlan)```  StorageEngine.java
    * Find the corresponding StorageGroupProcessor
	* According to the time of this batch of data and the last timestamp of the current device order, this batch of data is divided into small batches, which correspond to a TsFileProcessor
	* Pre-write log
	* Write each small batch to the corresponding memtable of TsFileProcessor
	    * If the file is out of order, update the endTimeMap in tsfileResource
	    * If there is no information about the device in tsfile, then update the startTimeMap in tsfileResource
	* Determine whether to trigger asynchronous persistent memtable operation based on memtable size
	    * If it is a sequential file and the flashing action is performed, the endTimeMap in tsfileResource is updated
	* Determine whether to trigger a file close operation based on the size of the current disk TsFile


## Data Update

Currently does not support data in-place update operations, that is, update statements, but users can directly insert new data, the same time series at the same time point is based on the latest inserted data.
Old data is automatically deleted by merging, see:

* [File merge mechanism](../StorageEngine/MergeManager.md)

## Data deletion

* Corresponding interface
  * JDBC's execute interface, using delete SQL statements

Each StorageGroupProcessor maintains a ascending version for each partition, which is managed by SimpleFileVersionController.
Each memtable will apply a version when submitted to flush. After flushing to TsFile, a current position-version will added to TsFileMetadata. 
This information will be used to set version to ChunkMetadata when query.

Main entrance in StorageEngine.java: 
 
```public void delete(String deviceId, String measurementId, long startTime, long endTime)```

  * Find the corresponding StorageGroupProcessor
  * Find all impacted working TsFileProcessors to write WAL
  * Find all impacted TsFileResources to record a Modification in its mods file, the Modification format is: path，version, startTime, endTime
  * If the TsFile is not closed，get its TsFileProcessor
    * If there exists the working memtable, delete data in it
    * If there exists flushing memtable，record the deleted start time and end time in it for query.（Notice that the Modification is recorded in mods for these memtables）

The Mods file stores records of delete information.
For the following mods file, data of d1.s1 falls in range [100, 200], [180, 300], and data of d1.s2 falls in range [500, 1000] are deleted.
![](https://user-images.githubusercontent.com/59866276/88248546-20952600-ccd4-11ea-88e9-84af8dde4304.jpg)
## Data TTL setting

* Corresponding interface
	* JDBC's execute interface, using the SET TTL statement

* Main entrance: ```public void setTTL(String storageGroup, long dataTTL) ```StorageEngine.java
    * Find the corresponding StorageGroupProcessor
    * Set new data ttl in StorageGroupProcessor
    * TTL check on all TsfileResource
    * If a file expires under the current TTL, delete the file

At the same time, we started a thread to periodically check the file TTL in StorageEngine.

- start method in src/main/java/org/apache/iotdb/db/engine/StorageEngine.java