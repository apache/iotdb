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

> Here are all files generated or used by IoTDB
>
> Continuously Updating...

# Stand-alone

## Configuration Files
> under conf directory
1. iotdb-engine.properties
2. logback.xml
3. iotdb-env.sh
4. jmx.access
5. jmx.password
6. iotdb-sync-client.properties
    + only sync tool use it

> under directory basedir/system/schema
1. system.properties
    + record all immutable properties, will be checked when starting IoTDB to avoid system errors

## State Related Files

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

### Data Related Files
> under directory basedir/data/

#### WAL
> under directory basedir/wal

1. {StorageGroupName}-{TsFileName}/wal1
    + every storage group has several wal files, and every memtable has one associated wal file before it is flushed into a TsFile 

#### TsFile
> under directory data/sequence or unsequence/{StorageGroupName}/{TimePartitionId}/

1. {time}-{version}-{mergeCnt}.tsfile
    + normal data file
2. {TsFileName}.tsfile.mod
    + modification file
    + record delete operation

#### TsFileResource
1. {TsFileName}.tsfile.resource
    + descriptor and statistic file of a TsFile
2. {TsFileName}.tsfile.resource.temp
    + temp file
    + avoid damaging the tsfile.resource when updating it
3. {TsFileName}.tsfile.resource.closing
    + close flag file, to mark a tsfile closing so during restarts we can continue to close it or reopen it

#### Version
> under directory basedir/system/storage_groups/{StorageGroupName}/{TimePartitionId} or upgrade

1. Version-{version}
    + version file, record the max version in fileName of a storage group

#### Upgrade
> under directory basedir/system/upgrade

1. upgrade.txt
    + record which files have been upgraded

#### Merge
> under directory basedir/system/storage_groups/{StorageGroup}/

1. merge.mods
    + modification file generated during a merge
2. merge.log
    + record the progress of a merge
3. tsfile.merge
    + temporary merge result file, an involved sequence tsfile may have one during a merge

#### Authority
> under directory basedir/system/users/
> under directory basedir/system/roles/

#### CompressRatio
> under directory basedir/system/compression_ration
1. Ration-{compressionRatioSum}-{calTimes}
    + record compression ratio of each tsfile

---

# Cluster-Mode
> Attention: the following files are newly added

## Configuration Files
1. iotdb-cluster.properties

## State Related Files
> under directory basedir/

1. node_identifier
    + the identifier of the local node in a cluster
2. partitions
    + partition table file, records the distribution of data
3. {time}_{random}.task
    + pullSnapshotTask file, record the slots and owners. When a node joins a cluster,
    it will create pullSnapshotTask file to track which data to be pulled
    + under directory basedir/raft/{nodeIdentifier}/snapshot_task/

## Raft Related Files
> under directory basedir/system/raftLog/{nodeIdentifier}/

### Raft Log
1. .data-{version}
    + raft committed logs, only save the latest 1000(configurable) committed logs

### Raft Meta
1. logMeta
    + raft meta, like hardState and Meta
        + hardState: voteFor, term
        + Meta: commitLogTerm, commitLogIndex, lastLogTerm, lastLogIndex
        + ...
2. logMeta.tmp
    + temp file, to avoid damaging the logMeta when updating it

### Raft Catch Up
> under directory basedir/remote/{nodeIdentifier}/{storageGroupName}/{partitionNum}/

1. {fileName}.tsfile
    + remote TsFile, will be loaded during snapshot installation
2. {fileName}.tsfile.mod
    + remote TsFile modification file, will be loaded during snapshot installation