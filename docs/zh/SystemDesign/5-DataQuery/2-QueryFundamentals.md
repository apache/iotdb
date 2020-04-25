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

# 查询基础介绍

## 顺序和乱序tsFile文件

在对某一个时间序列插入数据的过程中，由于插入数据的时间戳的特点会产生顺序和乱序的tsFile文件。如果我们按照时间戳递增的顺序插入数据，那么只会产生顺序文件。顺序文件被写入到磁盘后，一旦新写入的数据时间戳在顺序文件的最大时间戳之前则会产生乱序文件。

IoTDB会将顺序和乱序文件分开存储在data/sequence和data/unsequence文件目录下。在查询过程中也会对顺序和乱序文件中的数据分别进行处理，我们总会使用`QueryResourceManager.java`中的`getQueryDataSource()`方法通过时间序列的全路径得到存储该时间序列的顺序和乱序文件。


## 读取TsFile的一般流程

TsFile 各级结构在前面的[1-TsFile](/#/SystemDesign/progress/chap1/sec1)文档中已有介绍，读取一个时间序列的过程需要按照层级各级展开TsFileResource -> TimeseriesMetadata -> ChunkMetadata -> IPageReader -> BatchData。

文件读取的功能方法在
`org.apache.iotdb.db.utils.FileLoaderUtils`

* `loadTimeSeriesMetadata()`用来读取一个TsFileResource对应于某一个时间序列的timeseriesMetadata，该方法同时接受一个时间戳的Filter条件来保证该方法返回满足条件的timeseriesMetadata，若没有满足条件的timeseriesMetadata则返回null。
* `loadChunkMetadataList()`得到这个timeseries所包含的所有chunkMetadata列表。
* `loadPageReaderList()`可以用来读取一个chunkMetadata所包含的所有page列表，用pageReader来进行访问。

以上在对于时间序列数据的各种读取方法中总会涉及到读取内存和磁盘数据两种情况。

读取内存数据是指读取存在于memtable中但尚未被写入磁盘的数据，例如`loadTimeSeriesMetadata()`中使用`TsFileResource.getTimeSeriesMetadata()`得到一个未被封口的timeseriesMetadata。一旦这个timeseriesMetadata被flush到磁盘中之后,我们将只能通过访问磁盘读取到其中的数据。磁盘和内存读取metadata的相关类为DiskChunkMetadataLoader和MemChunkMetadataLoader。

`loadPageReaderList()`读取page数据也是一样，分别通过两个辅助类MemChunkLoader和DiskChunkLoader进行处理。



## 顺序和乱序文件的数据特点

对于顺序和乱序文件的数据，其数据在文件中的分部特征有所不同。
顺序文件的TimeseriesMetadata中所包含的ChunkMetadata也是有序的，也就是说如果按照chunkMetadata1, chunkMetadata2的顺序存储，那么将会保证chunkMetadata1.endtime <= chunkMetadata2.startTime。

乱序文件的TimeseriesMetadata中所包含的ChunkMetadata是无序的，乱序文件中多个Chunk所覆盖的数据可能存在重叠，同时也可能与顺序文件中的chunk数据存在重叠。

每个Chunk结构内部所包含的Page数据总是有序的，不管是从属于顺序文件还是乱序文件。也就是说前一个Page的最大时间戳不小于后一个的最小时间戳。因此在查询过程中可以充分利用这种有序性，通过统计信息对Page数据进行提前筛选。



## 查询中的数据修改处理

IoTDB的数据删除操作对磁盘数据只记录了 mods 文件，并未真正执行删除逻辑，因此查询时需要考虑数据删除的逻辑。

如果一个文件中有数据被删除了，将删除操作记录到 mods 文件中。记录三列：删除的时间序列，删除范围的最大时间点，删除操作对应的版本。


对于任意的timeseriesMetadata,chunkMetadata和pageHeader都有相应的modified标记，表示当前的数据块是否存在更改。由于数据删除都是从一个时间节点删除该时间前面的数据，因此如果存在数据删除会导致数据块统计信息中的startTime失效。因此在使用统计信息中的startTime之前必须检查数据块是否包含modification。对于timeseriesMetadata，如果删除时间点等于endTime也会导致统计信息中的endTime失效。

