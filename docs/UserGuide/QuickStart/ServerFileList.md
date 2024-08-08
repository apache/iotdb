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
1. iotdb-datanode.properties
2. logback.xml
3. datanode-env.sh
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
    + every database has several wal files, and every memtable has one associated wal file before it is flushed into a TsFile 

#### TsFile
> under directory data/sequence or unsequence/{DatabaseName}/{DataRegionId}/{TimePartitionId}/

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
> under directory basedir/system/databases/{DatabaseName}/{DataRegionId}/{TimePartitionId} or upgrade

1. Version-{version}
    + version file, record the max version in fileName of a database

#### Upgrade
> under directory basedir/system/upgrade

1. upgrade.txt
    + record which files have been upgraded

#### Merge
> under directory basedir/system/databases/{StorageGroup}/

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

