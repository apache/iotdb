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

# 数据增删改

下面介绍四种常用数据操控操作，分别是插入，更新，删除和TTL设置

## 数据插入

### 单行数据（一个设备一个时间戳多个值）写入

* 对应的接口
	* JDBC 的 execute 和 executeBatch 接口
	* Session 的 insertRecord 和 insertRecords

* 总入口: public void insert(InsertRowPlan insertRowPlan)   StorageEngine.java
	* 找到对应的 StorageGroupProcessor
	* 根据写入数据的时间以及当前设备落盘的最后时间戳，找到对应的 TsFileProcessor
	* 记录写前日志
	* 写入 TsFileProcessor 对应的 memtable 中
	    * 如果是乱序文件，则更新tsfileResource中的endTimeMap
	    * 如果tsfile中没有该设备的信息，则更新tsfileResource中的startTimeMap
	* 根据 memtable 大小，来判断是否触发异步持久化 memtable 操作
	    * 如果是顺序文件且执行了刷盘动作，则更新tsfileResource中的endTimeMap
	* 根据当前磁盘 TsFile 的大小，判断是否触发文件关闭操作

### 批量数据（一个设备多个时间戳多个值）写入

* 对应的接口
	* Session 的 insertTablet

* 总入口: public void insertTablet(InsertTabletPlan insertTabletPlan)  StorageEngine.java
    * 找到对应的 StorageGroupProcessor
	* 根据这批数据的时间以及当前设备落盘的最后时间戳，将这批数据分成小批，分别对应到一个 TsFileProcessor 中
	* 记录写前日志
	* 分别将每小批写入 TsFileProcessor 对应的 memtable 中
	    * 如果是乱序文件，则更新tsfileResource中的endTimeMap
	    * 如果tsfile中没有该设备的信息，则更新tsfileResource中的startTimeMap
	* 根据 memtable 大小，来判断是否触发异步持久化 memtable 操作
	    * 如果是顺序文件且执行了刷盘动作，则更新tsfileResource中的endTimeMap
	* 根据当前磁盘 TsFile 的大小，判断是否触发文件关闭操作


## 数据更新

目前不支持数据的原地更新操作，即update语句，但用户可以直接插入新的数据，在同一个时间点上的同一个时间序列以最新插入的数据为准
旧数据会通过合并来自动删除，参见：

* [文件合并机制](../StorageEngine/MergeManager.md)

## 数据删除

* 对应的接口
	* JDBC 的 execute 接口，使用delete SQL语句
	

每个 StorageGroupProsessor 中针对每个分区会维护一个自增的版本号，由 SimpleFileVersionController 管理。
每个内存缓冲区 memtable 在持久化的时候会申请一个版本号。持久化到 TsFile 后，会在 TsFileMetadata 中记录此 memtable 对应的 多个 ChunkGroup 的终止位置和版本号。
查询时会根据此信息对 ChunkMetadata 赋 version。

StorageEngine.java中的delete入口: 

```public void delete(String deviceId, String measurementId, long timestamp)```
  * 找到对应的 StorageGroupProcessor
  * 找到受影响的所有 working TsFileProcessor 记录写前日志
  * 找到受影响的所有 TsFileResource，在其对应的 mods 文件中记录一条记录：path，version，startTime，endTime
    * 如果存在 working memtable：则删除内存中的数据
    * 如果存在 正在 flush 的 memtable，记录一条记录，查询时跳过删掉的数据（注意此时文件中已经为这些 memtable 记录了 mods）

Mods文件用来存储所有的删除记录。下图的mods文件中，d1.s1落在 [100, 200], [180, 300]范围的数据，以及d1.s2落在[500, 1000]范围中的数据将会被删除。

![](https://user-images.githubusercontent.com/59866276/88248546-20952600-ccd4-11ea-88e9-84af8dde4304.jpg)

## 数据TTL设置

* 对应的接口
	* JDBC 的 execute 接口，使用SET TTL语句

* 总入口: public void setTTL(String storageGroup, long dataTTL) StorageEngine.java
    * 找到对应的 StorageGroupProcessor
    * 在 StorageGroupProcessor 中设置新的data ttl
    * 对所有TsfileResource进行TTL检查
    * 如果某个文件在当前TTL下失效，则删除文件

同时，我们在 StorageEngine 中启动了一个定时检查文件TTL的线程，详见

* src/main/java/org/apache/iotdb/db/engine/StorageEngine.java 中的 start 方法