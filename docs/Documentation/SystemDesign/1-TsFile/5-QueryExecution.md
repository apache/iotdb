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

# Query Execution

This chapter introduces how to answer queries using filters and query expressions.

- [1 Design](#1-Design)
- [2 Three Components](#2-Three-Components)
    - [2.1 FileSeriesReader](#21-FileSeriesReader)
    - [2.2 FileSeriesReaderByTimestamp](#22-FileSeriesReaderByTimestamp)
    - [2.3 TsFileTimeGenerator](#23-TsFileTimeGenerator)
- [3 Merge Query](#3-Merge-Query)
- [4 Join Query](#4-Join-Query)
- [5 Query of TsFile](#5-Query-of-TsFile)
- [6 Related Concepts](#6-Related-Concepts)

## 1 Design

The interface of TsFile on file level is intended only for queries on original data. According to the existence of value filter, the query can be divided into two groups: those without filters or only with a time filter, and those containing value filters.

To execute the two kinds of queries, we design two query execution methods:

* merge query

    It generates multiple readers, aligns the result in time, and returns the result set.

* join query

    According to the query conditions, it generates satisfying timestamp, which is used for generating result set.

## 2 Three Components 

### 2.1 FileSeriesReader
org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader

**Functions**: FileSeriesReader queries the data points of a time series in a file, which satisfies the filter conditions. It outputs the data points of the given time series in the given file in timestamp ascending order. There can be no filters at all.

**Implementation**: FileSeriesReader retrieves the Chunk information of the given time series according to the Path, it then traverses each Chunk in timestamp ascending order, and outputs the satisfying data points. 

### 2.2 FileSeriesReaderByTimestamp

org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp

**Functions**: FileSeriesReaderByTimestamp queries the data points of a time series in a file, whose timestamp satisfies the timestamp constraints.

**Implementation**: This component provides an interface, getValueInTimestamp(long timestamp), which receives increasing timestamp value,  and outputs the data points on the time series whose timestamp is identical. If there's no such data point, the result is null.

### 2.3 TsFileTimeGenerator
org.apache.iotdb.tsfile.read.query.timegenerator.TsFileTimeGenerator

**Functions**: According to the filter condition, TimeGeneratorImpl generates the satisfying timestamp. It first transforms the filter conditions to be a binary tree, and recursively generate the satisfying timestamp. This component is used in executing join query.

An executable expression contains one or nultiple SingleSeriesExpressions. The relation between two SingleSeriesExpressions is either AND or OR. Therefore, the filter in an executable expression can be transformed to a binary tree, where the leaf nodes are FileSeriesReader, and the non-leaf nodes are AndNode or OrNode. Particularly, when the expression only contains a single SingleSeriesExpression, the binary tree only has one node. The satisfying timestamp can be generated using the binary tree. 

This component provides two basic functions: 

1. check if there exists a next satisfying timestamp

2. get the next satisfying timestamp

## 3 Merge Query
org.apache.iotdb.tsfile.read.query.dataset.DataSetWithoutTimeGenerator

Suppose there are n time series. For each one of them, a FileSeriesReader will be generated. If there's a GlobalTimeExpression, the filter in it will be input to the FileSeriesReaders.

A DataSetWithoutTimeGenerator will be generated using these FileSeriesReaders. Since each FileSeriesReader outputs data points in time stamp ascending order, we can use the idea of k-way merge to align the result data points. 

The steps include: 

(1) Create a min-heap, which stores timestamps. The min-heap is organized according to the value of the timestamp.

(2) Initialize the min-heap. Access each FileSeriesReader once. If there's a next data point in the FileSeriesReader, add its timestamp to the min-heap. Till now, the heap contains at most one timestamp of each time series, which is the minimum in its corresponding time series.

(3) If the size of the heap is greater than 0, take the timestamp on the top of the heap, denote it as t, and remove it from the heap, then go to step (4). If the number of the heap is 0, go to step (5).

(4) Create a new RowRecord. In turns access each time series. When dealing with a time series, first check if there's a next data point, if it fails, set the result data point to be null. Otherwise, check if minimum timestamp in the time series is identical to t. If it is not, set the result data point to be null. Otherwise, get the data point and set it to be the result data point. If there exists a next data point after that. If it has, then set timestamp of the next data point in the time series to be its minimum timestamp. After accessing all time series, combine the result data points to form a RowRecord. Finally, go to step (3).

(5) Terminate the process.

## 4 Join Query

org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator

Join query execution generates timestamp according to the filter conditions, and retrieves the data point on the projected time series to form a RowRecord. Main steps include;

(1) According to QueryExpression, initialize the timestamp generation module, TimeGeneratorImpl

(2) Generate FileSeriesReaderByTimestamp for each projected time series.

(3) If there exists a next timestamp in the "timestamp generation module", denote the timestamp t, go to step (4).Otherwise, terminate the query execution.

(4) According to t, utilize FileSeriesReaderByTimestamp on each time series to retrieve the data point whose corresponding timestamp is t. If there's no such data point, use null to represent it.

(5) Combine the data points in step (4) to form a RowRecord, then go to step (3) to generate another row of records.

## 5 Query of TsFile

 org.apache.iotdb.tsfile.read.query.executor.TsFileExecutor

TsFileExecutor receives a QueryExpression, execute the query and outputs the QueryDataSet。The work flow includes the following steps: 

(1) Receive a QueryExpression.

(2) If the QueryExpression contains no filter conditions, execute merge query. If it contains any Filters, use ExpressionOptimizer to optimize the IExpression in QueryExpression. If the optimized IExpression is a GlobalTimeExpression, execute merge query. If it contains value filters, it sends a message to ExecutorWithTimeGenerator to execute join query.

(3) Generate the QueryDataSet. It iteratively generates RowRecord and returns the results.


## 6 Related Concepts

* Chunk: Chunk is the storage structure of a chunk of time series. IChunkReader is for reading the content。

* ChunkMetaData: ChunkMetaData records the offset, data type and encoding info of the Chunk in the File. 
  
* IMetadataQuerier: IMetadataQuerier is a querier for TsFile metadata. It queries the metadata of a file, and the ChunkMetaData of a time series.

* IChunkLoader:  IChunkLoader is the loader for Chunk, whose main function is getting the corresponding Chunk of the given the ChunkMetaData.

* IChunkReader: IChunkReader reads the data in a Chunk. It receives a Chunk and parse it according to the info in the ChunkHeader. It provides two interface: 

    * hasNextSatisfiedPage & nextPageData: iteratively returns a Page
    * getPageReaderList: return all PageReader

* IPageReader: IPageReader reads the data in a Page. It provides two interface:

    * getAllSatisfiedPageData(): return all satisfying values
    * getStatistics(): return the statistic information of the Page

* QueryExpression

    QueryExpression is the query expression, which contains the time series to project, and filter constraints.

* QueryDataSet

    The result of a query. It contains one or multiple RowRecord, which combines data points having identical time stamp together. QueryDataSet provides two interface: 

    * check if there's a next RowRecord.
    * return the next RowRecord.

