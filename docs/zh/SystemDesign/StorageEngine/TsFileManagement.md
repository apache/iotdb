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

# TsFileManagement -热合并

## 整体思路

现在 iotdb 会因为自动参数优化将很小的数据块写入文件，使得数据文件变得零散化，这导致了用户进行即席查询时需要的读盘次数过多，而后期的合并过程又不是即时进行的，这导致了系统对热数据的查询效率变慢，所以我们借鉴了虚拟内存的思想，在写入流程中增加了热合并过程，通过提高一部分写放大保证了写入到正式文件的数据块不小于给定阈值，提升系统即席查询的效率。将这部分逻辑写到 StorageGroupProcessor 里去，针对封口的 tsfile 文件进行合并。

- iotdb-engine.properties 加一个参数 tsfile_manage_strategy：表示 tsfile 文件管理策略
- tsfile_manage_strategy 内置两个策略：LevelStrategy 和 NormalStrategy
- LevelStrategy 即开启热合并并使用层级合并方法，NormalStrategy 即关闭热合并
- iotdb-engine.properties 中加 merge_chunk_point_number：最大的chunk大小限制，LevelStrategy 时，当达到该参数就合并到最后一层
- iotdb-engine.properties 加一个参数 max_level_num：LevelStrategy 时最大层数
- 新建一个 TsFileManagement 类，专门管理StorageGroupProcessor中的seqFileList和unSeqFileList，在这里写热合并的主体逻辑，对外抽象对seqFileList和unSeqFileList的一系列所需接口
- 对于普通 merge 会调用 TsFileManagement 的 getStableTsFileList() 来获取准备进行文件合并的文件列表，这里对于 LevelStrategy 来说就是返回第 {max_level_num} 层的文件，对于 NormalStrategy 来说就是返回所有文件列表

## TsFileManagement 对外提供的接口和类

- TsFileManagement 也管理未封口文件
- TsFileManagement 同时管理 Seq 和 UnSeq 文件列表，在 StorageGroupProcessor 中只创建一个 TsFileManagement
- List\<TsFileResource\> getStableTsFileList() 对外提供已经完成热合并的稳定的 TsFileResource 列表
- List\<TsFileResource\> getTsFileList(boolean sequence) 对外提供顺序的 TsFileResource 列表
- Iterator\<TsFileResource\> getIterator(boolean sequence) 对外提供顺序的 TsFileResource 迭代器
- void remove(TsFileResource tsFileResource, boolean sequence) 删除对应的 TsFileResource 文件
- void removeAll(List\<TsFileResource\> tsfileReourceList, boolean sequence) 删除对应的 TsFileResource 列表
- void addAll(List\<TsFileResource\> tsfileReourceList, boolean sequence) 批量加入 TsFileResource 列表
- boolean isEmpty(boolean sequence) 是否为空
- int size(boolean sequence) 对应的文件列表长度
- void forkCurrentFileList(long timePartition) 保存当前文件的某时间分区列表
- void recover() 调用对应的恢复流程
- HotCompactionMergeTask 调用对应的异步合并流程，传入 closeUnsealedTsFileProcessorCallBack 以通知外部类合并结束

## LevelStrategy

* 外部调用 forkCurrentFileList(long timePartition) 保存当前文件的某时间分区列表
* 外部异步创建并提交 merge 线程
* 判断是否要进行全局热合并
	* 生成目标文件 {first_file_name}-{max_level_num - 1}.tsfile
	* 生成合并日志 .hot_compaction.log
	* 记录是全局合并
	* 记录目标文件
	* 进行合并（记录日志 device - offset）
	* 记录完成合并
* 循环判断每一层的文件是否要合并到下一层
	* 生成目标文件 {first_file_name}-{level + 1}.tsfile
	* 生成合并日志 .hot_compaction.log
	* 记录是全局合并
	* 记录目标文件
	* 进行合并（记录日志 device - offset）
	* 记录完成合并
* 加写锁
* 从磁盘删掉待合并的文件，并从正式文件列表中移除
* 删除合并日志 .hot_compaction.log
* 释放写锁

## LevelStrategy recover 流程

* 如果日志文件存在
	* 如果是全局合并（把所有小文件合并到最后一层）
		* 如果合并没结束
			* 截断文件
			* 继续全局合并
	* 如果是层级合并
		* 如果合并没结束
			* 截断文件
			* 继续层级合并
* 如果日志文件不存在
	* 无需恢复
