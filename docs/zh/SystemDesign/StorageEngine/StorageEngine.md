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

# 存储引擎

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625255-03fe2680-467f-11ea-91ae-64407ef1125c.png">

## 设计思想

存储引擎基于 LSM 设计。数据首先写入内存缓冲区 memtable 中，再刷到磁盘。内存中为每个设备维护当前持久化的（包括已经落盘的和正在持久化的）最大时间戳，根据这个时间戳将数据区分为顺序数据和乱序数据，不同种类的数据通过不同的 memtable 和 TsFile 管理。

每个数据文件 TsFile 在内存中对应一个文件索引信息 TsFileResource，供查询使用。

此外，存储引擎还包括异步持久化和文件合并机制。

## 写入流程

### 相关代码

* org.apache.iotdb.db.engine.StorageEngine

	负责一个 IoTDB 实例的写入和访问，管理所有的 StorageGroupProsessor。
	
* org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor

	负责一个存储组一个时间分区内的数据写入和访问。管理所有分区的TsFileProcessor。

* org.apache.iotdb.db.engine.storagegroup.TsFileProcessor

  负责一个 TsFile 文件的数据写入和访问。

## 数据写入
详见：
* [数据写入](../StorageEngine/DataManipulation.md)

## 数据访问

* 总入口（StorageEngine）: public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context,
  ​    QueryFileManager filePathsManager)
  ​    
	* 找到所有包含这个时间序列的顺序和乱序的 TsFileResource 进行返回，供查询引擎使用。

## 相关文档

* [写前日志 (WAL)](../StorageEngine/WAL.md)

* [memtable 持久化](../StorageEngine/FlushManager.md)

* [文件合并机制](../StorageEngine/MergeManager.md)