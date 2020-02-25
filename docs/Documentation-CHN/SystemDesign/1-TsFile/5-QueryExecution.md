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

# TsFile 查询执行过程

本章节介绍 TsFile 的查询执行过程，即如何利用查询表达式得到结果集。


* 1 设计原理
* 2 三大查询组件
    * 2.1 FileSeriesReader 组件
    * 2.2 FileSeriesReaderByTimestamp 组件
    * 2.3 TimeGeneratorImpl 组件
* 3 归并查询
* 4 连接查询
* 5 查询入口
* 6 相关代码介绍

## 1 设计原理

TsFile 文件层查询接口只包含原始数据查询，根据是否包含值过滤条件，可以将查询分为两类“无过滤条件或仅包含时间过滤条件查询”和“包含值过滤条件的查询”

为了执行以上两类查询，有两套查询流程

* 归并查询

	生成多个 reader，按照 time 对齐，返回结果集。

* 连接查询

	根据查询条件生成满足过滤条件的时间戳，通过满足条件的时间戳查询投影列的值，返回结果集。

## 2 三大查询组件

### 2.1 FileSeriesReader 组件
org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader

**功能**：该组件用于查询一个文件中单个时间序列满足过滤条件的数据点。根据给定的查询路径和被查询的文件，按照时间戳递增的顺序查询出该时间序列在文件中的所有数据点。其中过滤条件可为空。

**实现**：该组件首先获取给定的路径查询出所有 Chunk 的信息，然后按照起始时间戳从小到大的顺序遍历每个 Chunk，并从中读取出满足条件的数据点。

### 2.2 FileSeriesReaderByTimestamp 组件

org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp

**功能**：该组件主要用于查询一个文件中单个时间序列在指定时间戳上的数据点。

**实现**：该组件提供一个接口，getValueInTimestamp(long timestamp)，通过接口依次传入递增的时间戳，返回时间序列上与该时间戳相对应的数据点。如果满足该时间戳的数据点不存在，则返回 null。

### 2.3 TsFileTimeGenerator 组件
org.apache.iotdb.tsfile.read.query.timegenerator.TsFileTimeGenerator

**功能**：根据“选择条件”，计算出满足该“选择条件”的时间戳，先将“选择条件”转化为一棵二叉树，然后递归地计算满足“选择条件”的时间戳。主要用于连接查询。

一个可执行的过滤条件由一个或多个 SingleSeriesExpression 构成，且 SingleSeriesExpression 之间具有相应的与或关系。所以，一个可执行的过滤条件可以转为一棵表示“查询条件”的二叉树，二叉树的叶子节点（ LeafNode ）为 FileSeriesReader，中间节点为 AndNode 或 OrNode。特殊地，当可执行的过滤条件仅由一个 SingleSeriesExpression 构成时，该二叉树仅包含一个节点。得到由“选择条件”转化后的二叉树后，便可以计算“满足该选择条件”的时间戳。
该组件提供两个基本的功能：

1. 判断是否还有下一个满足“选择条件”的时间戳

2. 返回下一个满足“选择条件”的时间戳


## 3 归并查询
org.apache.iotdb.tsfile.read.query.dataset.DataSetWithoutTimeGenerator

设当查询 n 个时间序列，为每个时间序列构建一个 FileSeriesReader，如果有 GlobalTimeExpression，则将其中的 Filter 传入 FileSeriesReader。

根据所有的 FileSeriesReader 生成一个 DataSetWithoutTimeGenerator，由于每个 FileSeriesReader 会按照时间戳从小到大的顺序迭代地返回数据点，所以可以采用“多路归并”对所有 FileSeriesReader 的结果进行按时间戳对齐。

数据合并的步骤为：

（1） 创建一个最小堆，堆里面存储“时间戳”，该堆将按照每个时间戳的大小进行组织。

（2） 初始化堆，依次访问每一个 FileSeriesReader，如果该 FileSeriesReader 中还有数据点，则获取数据点的时间戳并放入堆中。此时每个时间序列最多有1个时间戳被放入到堆中，即该序列最小的时间戳。

（3） 如果堆的 size > 0，获取堆顶的时间戳，记为t，并将其在堆中删除，进入步骤（4）；如果堆的 size 等于0，则跳到步骤（5），结束数据合并过程。

（4） 创建新的 RowRecord。依次遍历每一条时间序列。在处理其中一条时间序列时，如果该序列没有更多的数据点，则将该列标记为 null 并添加在 RowRecord 中；否则，判断最小的时间戳是否与 t 相同，若不相同，则将该列标记为 null 并添加在 RowRecord 中。若相同，将该数据点添加在 RowRecord 中，同时判断该时间序列是否有新的数据点，若存在，则将下一个时间戳 t' 添加在堆中，并将 t' 设为当前时间序列的最小时间戳。最后，返回步骤（3）。

（5） 结束数据合并过程。

## 4 连接查询

org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator

连接查询生成满足“选择条件”的时间戳、查询被投影列在对应时间戳下的数据点、合成 RowRecord。主要流程如下：

（1）	根据 QueryExpression，初始化时间戳计算模块 TimeGeneratorImpl

（2）	为每个被投影的时间序列创建 FileSeriesReaderByTimestamp

（3）	如果“时间戳计算模块”中还有下一个时间戳，则计算出下一个时间戳 t ，进入步骤（4）；否则，结束查询。

（4）	根据 t，在每个时间序列上使用 FileSeriesReaderByTimestamp 组件获取在时间戳 t 下的数据点；如果在该时间戳下没有对应的数据点，则用 null 表示。

（5）	将步骤（4）中得到的所有数据点合并成一个 RowRecord，此时得到一条查询结果，返回步骤（3）计算下一个查询结果。


## 5 查询入口

 org.apache.iotdb.tsfile.read.query.executor.TsFileExecutor

TsFileExecutor 接收一个 QueryExpression ，执行该查询并返回相应的 QueryDataSet。基本工作流程如下：

（1）接收一个 QueryExpression

（2）如果无过滤条件，执行归并查询。如果该 QueryExpression 包含 Filter（过滤条件），则通过 ExpressionOptimizer 对该 QueryExpression 的 Filter 进行优化。如果是 GlobalTimeExpression，执行归并查询。如果包含值过滤，交给 ExecutorWithTimeGenerator 执行连接查询。

（3） 生成对应的 QueryDataSet，迭代地生成 RowRecord，将查询结果返回。



## 6 相关代码介绍

* Chunk：一段时间序列的内存结构，可供 IChunkReader 进行读取。

* ChunkMetaData：记录对应 Chunk 在文件中的偏移量及数据类型和编码方式，便于对 Chunk 进行读取。

* IMetadataQuerier：一个 TsFile 的元数据加载器。可以加载整个文件的元数据和一个序列的所有 ChunkMetaData。

* IChunkLoader： IChunkLoader 为 Chunk 的加载器，主要功能为，给定一个 ChunkMetaData，返回对应的 Chunk。

* IChunkReader：对一个 Chunk 中的数据进行读取，其接收一个 Chunk，根据其中 ChunkHeader 中的相关信息，对该 Chunk 进行解析。其提供两套接口：

	* hasNextSatisfiedPage & nextPageData：迭代式的返回一个一个的 Page
	* getPageReaderList：一次返回所有 PageReader

* IPageReader：对一个 Page 中的数据进行读取，提供两个基本的接口：

	* getAllSatisfiedPageData()：一次返回所有满足条件的值
	* getStatistics()：返回 Page 的统计信息

* QueryExpression

	QueryExpression 为查询表达式，包含投影的时间序列和过滤条件。

* QueryDataSet

	一次查询所返回的结果，具有相同时间戳的数据点合并为一个 RowRecord。 QueryDataSet 提供两个基本的功能：

	* 判断是否还有下一个 RowRecord
	* 返回下一个 RowRecord




