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

# Files

In IoTDB, there are many kinds of data needed to be stored. This section introduces IoTDB's data storage strategy to provide you an explicit understanding of IoTDB's data management.

The data in IoTDB is divided into three categories, namely data files, system files, and pre-write log files.

## Data Files
> under directory basedir/data/

Data files store all the data that the user wrote to IoTDB, which contains TsFile and other files. TsFile storage directory can be configured with the `data_dirs` configuration item (see [file layer](../Reference/DataNode-Config-Manual.md) for details). Other files can be configured through [data_dirs](../Reference/DataNode-Config-Manual.md) configuration item (see [Engine Layer](../Reference/DataNode-Config-Manual.md) for details).

In order to support users' storage requirements such as disk space expansion better, IoTDB supports multiple file directories storage methods for TsFile storage configuration. Users can set multiple storage paths as data storage locations( see [data_dirs](../Reference/DataNode-Config-Manual.md) configuration item), and you can specify or customize the directory selection strategy (see [multi_dir_strategy](../Reference/DataNode-Config-Manual.md) configuration item for details).

### TsFile
> under directory data/sequence or unsequence/{DatabaseName}/{DataRegionId}/{TimePartitionId}/

1. {time}-{version}-{inner_compaction_count}-{cross_compaction_count}.tsfile
    + normal data file
2. {TsFileName}.tsfile.mod
    + modification file
    + record delete operation

### TsFileResource
1. {TsFileName}.tsfile.resource
    + descriptor and statistic file of a TsFile
  
### Compaction Related Files
> under directory basedir/data/sequence or unsequence/{DatabaseName}/

1. file suffixe with `.cross ` or `.inner`
    + temporary files of metadata generated in a compaction task
2. file suffixe with `.inner-compaction.log` or `.cross-compaction.log`
    + record the progress of a compaction task
3. file suffixe with `.compaction.mods`
    + modification file generated during a compaction task
4. file suffixe with `.meta`
    + temporary files of metadata generated during a merge

## System files

System files include schema files, which store metadata information of data in IoTDB. It can be configured through the `base_dir` configuration item (see [System Layer](../Reference/DataNode-Config-Manual.md) for details).

### MetaData Related Files
> under directory basedir/system/schema

#### Meta
1. mlog.bin
    + record the meta operation
2. mtree-1.snapshot
    + snapshot of metadata
3. mtree-1.snapshot.tmp
    + temp file, to avoid damaging the snapshot when updating it

#### Tags&Attributes
1. tlog.txt
    + store tags and attributes of each TimeSeries
    + about 700 bytes for each TimeSeries

### Other System Files
#### Version
> under directory basedir/system/database/{DatabaseName}/{TimePartitionId} or upgrade 

1. Version-{version}
    + version file, record the max version in fileName of a storage group

#### Upgrade
> under directory basedir/system/upgrade

1. upgrade.txt
    + record which files have been upgraded

#### Authority
> under directory basedir/system/users/
> under directory basedir/system/roles/

#### CompressRatio
> under directory basedir/system/compression_ration
1. Ration-{compressionRatioSum}-{calTimes}
    + record compression ratio of each tsfile
## Pre-write Log Files

Pre-write log files store WAL files. It can be configured through the `wal_dir` configuration item (see [System Layer](../Reference/DataNode-Config-Manual.md) for details).

> under directory basedir/wal

1. {DatabaseName}-{TsFileName}/wal1
    + every storage group has several wal files, and every memtable has one associated wal file before it is flushed into a TsFile 
## Example of Setting Data storage Directory

For a clearer understanding of configuring the data storage directory, we will give an example in this section.

The data directory path included in storage directory setting are: base_dir, data_dirs, multi_dir_strategy, and wal_dir, which refer to system files, data folders, storage strategy, and pre-write log files.

An example of the configuration items are as follows:

```
dn_system_dir = $IOTDB_HOME/data/datanode/system
dn_data_dirs = /data1/datanode/data, /data2/datanode/data, /data3/datanode/data 
dn_multi_dir_strategy=MaxDiskUsableSpaceFirstStrategy
dn_wal_dirs= $IOTDB_HOME/data/datanode/wal
```
After setting the configuration, the system will:

* Save all system files in $IOTDB_HOME/data/datanode/system
* Save TsFile in /data1/datanode/data, /data2/datanode/data, /data3/datanode/data. And the choosing strategy is `MaxDiskUsableSpaceFirstStrategy`, when data writes to the disk, the system will automatically select a directory with the largest remaining disk space to write data.
* Save WAL data in $IOTDB_HOME/data/datanode/wal
* 
