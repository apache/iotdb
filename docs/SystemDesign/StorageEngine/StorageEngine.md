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

# Storage engine

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625255-03fe2680-467f-11ea-91ae-64407ef1125c.png">

## Design ideas

The storage engine is based on the LSM design. The data is first written to the memory buffer memtable and then flushed to disk. For each device, the maximum timestamp being flushed (including those that have been flushed and are being flushed) is maintained in memory. The data is divided into sequential data and out-of-order data according to this timestamp. Different types of data are separated into different memtables and flushed into different TsFiles.

Each data file TsFile corresponds to a file index information TsFileResource in memory for query use.

In addition, the storage engine includes asynchronous persistence and file merge mechanisms.

## Write process

### Related code

* org.apache.iotdb.db.engine.StorageEngine

  Responsible for writing and accessing an IoTDB instance and managing all StorageGroupProsessor.

* org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor

  Responsible for writing and accessing data within a time partition of a storage group. 

  Manages all partitions‘ TsFileProcessor .

* org.apache.iotdb.db.engine.storagegroup.TsFileProcessor

  Responsible for data writing and accessing a TsFile file.

## Data write
See details:
* [Data write](../StorageEngine/DataManipulation.md)

## Data access

* Main entrance（StorageEngine）: public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context, QueryFileManager filePathsManager)
  ​    
	* Find all ordered and out-of-order TsFileResources containing this time series and return them for use by the query engine

## Related documents

* [Write Ahead Log (WAL)](../StorageEngine/WAL.md)

* [memtable Endurance](../StorageEngine/FlushManager.md)

* [File merge mechanism](../StorageEngine/MergeManager.md)
