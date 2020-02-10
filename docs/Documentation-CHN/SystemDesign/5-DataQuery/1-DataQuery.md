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

# 数据查询

* 查询种类
	* 原始数据查询
	* 聚合查询
	* 降采样查询
	* 单点补空值查询
	* 最新数据查询等
	
## 查询计划

主要包括两种查询框架：

* 不带值过滤条件的（如，select f1, f2, f3 from root.d1）
	
	可以按照多路归并的方式进行查询，分别查询三列的结果，最后按照时间进行对齐。
	
* 带值过滤条件的（如，select f1, f2, f3 from root.d1 where f1 = 1 and f2 = 2）

	先根据过滤条件生成结果集的时间戳，再根据时间戳查询其他列的值。

## 查询组件

为了实现任意过滤条件下的多种查询，有以下查询组件

* SeriesReaderWithoutValueFilter

	一个时间序列的 reader，不带任何值过滤条件，但是可能带时间过滤条件。

* SeriesReaderWithValueFilter

	一个时间序列的 reader，带值过滤条件（也可能包含时间过滤条件）。

* SeriesReaderByTimestamp

	一个时间序列的 reader，每给一个时间戳，返回此时间戳对应的值，若没有则返回空。

* TimeGenerator
	* 功能：根据过滤条件构造表达式的树形结构，生成结果集的时间戳，如 f1 = 1 and f2 = 2，构造一棵树 AND（leaf(f1=1), leaf(f2=2)）。
	* 叶子节点：对应一个 SeriesReaderWithValueFilter，返回满足条件的结果的时间戳。
	* 中间节点：按照此节点的谓词逻辑对时间戳做计算。
	* 根节点：返回最终结果集的时间戳。

## AbstractSeriesReader

单个时间序列的基础 Reader 类。输入为一个 QueryDataSource，包括顺序和乱序文件列表。