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

# Modification handling in query

Data deletion only record a mods file for disk data, the data is not really deleted. Therefore, we need to consider the modifications in query.

Each timeseries is treated independently in query process. For each timeseries, there are 5 levels: TsFileResource -> TimeseriesMetadata -> ChunkMetadata -> IPageReader -> BatchData

Query resource: TsFileResource and possibly exist mods file. If a TsFile is influenced by deletion, a modification log will be recorded in its mods file. The log contains 3 parts: path, deleted time, version

![](https://user-images.githubusercontent.com/7240743/78339324-deca5d80-75c6-11ea-8fa8-dbd94232b756.png)

* TsFileResource -> TimeseriesMetadata

```
// Set the statistics in TimeseriesMetadata unusable if the timeseries contains modifications
FileLoaderUtils.loadTimeseriesMetadata()
```

* TimeseriesMetadata -> List\<ChunkMetadata\>

```
// For each ChunkMetadata, find the largest timestamp in all modifications whose version is larger than it. Set deleted time to ChunkMetadata. 
// set the statistics in ChunkMetadata is unusable if it is affected by deletion
FileLoaderUtils.loadChunkMetadataList()
```

E.g., the got ChunkMetadatas are:

![](https://user-images.githubusercontent.com/7240743/78339335-e427a800-75c6-11ea-815f-16dc5b6ebfa3.png)

* ChunkMetadata -> List\<IPageReader\>

```
// Skip the fully deleted page, set deleteAt into PageReader，Set the page statistics unusalbe if it is affected by deletion
FileLoaderUtils.loadPageReaderList()
```

* IPageReader -> BatchData

```
// For disk page, skip the data points that be deleted and filterd out. For memory data, skip data points be filtered out.
IPageReader.getAllSatisfiedPageData()
```