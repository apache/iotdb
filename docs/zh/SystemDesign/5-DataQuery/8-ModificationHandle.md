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

# 查询中的数据修改处理

数据删除操作对磁盘数据只记录了 mods 文件，并未真正执行删除逻辑，因此查询时需要考虑数据删除的逻辑。

查询时每个时间序列会单独处理。针对一个时间序列，由大到小有 5 个层次：TsFileResource -> TimeseriesMetadata -> ChunkMetadata -> IPageReader -> BatchData

查询资源：TsFileResource 以及可能存在的 mods 文件，如果一个文件中有数据被删除了，将删除操作记录到 mods 文件中。记录三列：删除的时间序列，删除范围的最大时间点，删除操作对应的版本。

![](https://user-images.githubusercontent.com/7240743/78339324-deca5d80-75c6-11ea-8fa8-dbd94232b756.png)

* TsFileResource -> TimeseriesMetadata

```
// 只要这个时间序列有对应的 modification，就标记 TimeseriesMetadata 中的统计信息不可用
FileLoaderUtils.loadTimeseriesMetadata()
```

* TimeseriesMetadata -> List\<ChunkMetadata\>

```
// 对于每个 ChunkMetadata，找到比其 version 大的所有 modification 中最大时间戳, 设置到  ChunkMetadata 的 deleteAt 中，并标记 统计信息不可用
FileLoaderUtils.loadChunkMetadataList()
```

对于以上示例，读取到的 ChunkMetadataList 为

![](https://user-images.githubusercontent.com/7240743/78339335-e427a800-75c6-11ea-815f-16dc5b6ebfa3.png)

* ChunkMetadata -> List\<IPageReader\>

```
// 跳过被完全删除的 Page，将 deleteAt 设置到 PageReader 里，将数据被部分删除的 page 标记统计信息不可用
FileLoaderUtils.loadPageReaderList()
```

* IPageReader -> BatchData

```
// 对于磁盘数据，跳过被删除的和过滤掉的，对于内存数据，跳过被过滤掉的
IPageReader.getAllSatisfiedPageData()
```

