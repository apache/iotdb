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

# 重启恢复流程

重启恢复是以存储组为粒度进行的，恢复的入口是 StorageGroupProcessor 的 recover()

## 存储组恢复流程

* org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor.recover()

* 首先获得该存储组下所有以 .tsfile 结尾的数据文件，返回 TsFileResource，共有如下几个文件列表

* 顺序文件
	* 0.10 版本的文件（封口/未封口）
	* 0.9 版本的文件（封口）
* 乱序文件
	* 0.10 版本的文件（封口/未封口）
	* 0.9 版本的文件（封口） 

* 若该存储组下有 0.9 版本的 TsFile 文件，则将旧版本的顺序和乱序文件分别加入`upgradeSeqFileList`和`upgradeSeqFileList`中，供升级和查询使用。

* 将顺序、乱序文件按照分区分组 Map<Long, List<TsFileResource>>

* 恢复每个分区的顺序文件，将上一步获得的每个分区的顺序 TsFile 文件作为参数，调用`recoverTsFiles`进行恢复，该方法会将恢复后的顺序 TsFile 以 TsFileResource 的形式放入`sequenceFileTreeSet`中，若该 TsFile 是此分区的最后一个，且未封口，则还要为其构造`TsFileProcessor`对象，并加入`workSequenceTsFileProcessors`中，该方法的具体细节会在下一小节阐述。

* 恢复每个分区的乱序文件，将上一步获得的每个分区的乱序 TsFile 文件作为参数，调用`recoverTsFiles`进行恢复，该方法会将恢复后的乱序 TsFile 以 TsFileResource 的形式放入`unSequenceFileList`中，若该 TsFile 是此分区的最后一个，且未封口，则还要为其构造`TsFileProcessor`对象，并加入`workUnsequenceTsFileProcessors`中，该方法的具体细节会在下一小节阐述。

* 分别遍历上两步得到的`sequenceFileTreeSet`和`unSequenceFileList`，更新分区对应的版本号

* 检查有没有 merge 时候的 Modification 文件，并调用`RecoverMergeTask.recoverMerge`方法对 merge 进行恢复

* 调用`updateLastestFlushedTime()`方法，用 0.9 版本的顺序 tsfile 文件，更新`latestTimeForEachDevice`, `partitionLatestFlushedTimeForEachDevice`以及`globalLatestFlushedTimeForEachDevice`

	* `latestTimeForEachDevice` 记录了所有 device 已经插入的各个分区下的最新的时间戳（包括未 flush 的和已 flush 的）
	* `partitionLatestFlushedTimeForEachDevice` 记录了所有 device 已经 flush 的各个分区下的最新的时间戳，它用来判断一个新插入的点是不是乱序点
	* `globalLatestFlushedTimeForEachDevice` 记录了所有 device 已经 flush 的最新时间戳（是各个分区的最新时间戳的汇总）

* 最后遍历`sequenceFileTreeSet`，用恢复出来的顺序文件，再次更新`latestTimeForEachDevice`, `partitionLatestFlushedTimeForEachDevice`以及`globalLatestFlushedTimeForEachDevice`

## 恢复一个分区的（顺序/乱序） TsFile

* org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor.recoverTsFiles

该方法主要负责遍历传进来的所有 TsFile，挨个进行恢复。

* 构造出`TsFileRecoverPerformer`对象，对 TsFile 文件进行恢复，恢复的逻辑封装在`TsFileRecoverPerformer`的`recover()`方法中（具体细节在下一小节展开阐述），该方法会返回一个恢复后的`RestorableTsFileIOWriter`对象。
	* 若恢复过程失败，则记录 log，并跳过该 tsfile

* 若该 TsFile 文件不是最后一个文件，或者该 TsFile 文件是最后一个文件，但已经被关闭或标记被关闭，只需将该 TsFile 文件在内存中对应的`TsFileResource`对象的`closed`属性置成`true`即可。 

* 若该 TsFile 文件可以继续写入，则表示这是此分区的最后一个 TsFile，且未封口，则继续保持其未封口的状态，需要为它构造一个`TsFileProcessor`对象，并将其放到`workSequenceTsFileProcessors`或`workUnsequenceTsFileProcessors`中。

* 最后将恢复出来的 TsFile 文件在内存中对应的`TsFileResource`对象放入`sequenceFileTreeSet`或`unSequenceFileList`中

### 恢复一个 TsFile 文件

* org.apache.iotdb.db.writelog.recover.TsFileRecoverPerformer.recover

该方法主要负责每个具体的 TsFile 文件的恢复。

* 首先用 tsfile 文件构造出一个`RestorableTsFileIOWriter`对象，在`RestorableTsFileIOWriter`的构造方法中，会对该 tsfile 的文件内容进行检查，必要时进行截断
	* 如果这个文件中没有任何内容，则为其写入`MAGIC_STRING`和`VERSION_NUMBER`后，直接返回，此时的`crashed`为`false`，`canWrite`为`true`；
	* 如果文件中有内容，构造`TsFileSequenceReader`对象对内容进行解析，调用`selfCheck`方法进行自检，并将不完整的内容截断，初始化`truncatedSize`为`HeaderLength`
		* 若文件内容完整（有完整的头部的`MAGIC_STRING`和`VERSION_NUMBER`，以及尾部的`MAGIC_STRING`），则返回`TsFileCheckStatus.COMPLETE_FILE`
		* 若文件长度小于`HeaderLength(len(MAGIC_STRING) + len(VERSION_NUMBER))`，或者文件头部内容不是`MAGIC_STRING`，则返回`INCOMPATIBLE_FILE`
		* 若文件长度刚好等于`HeaderLength`，且文件内容就是`MAGIC_STRING + VERSION_NUMBER`，则返回`HeaderLength`
		* 若文件长度大于`HeaderLength`，且文件头合法，但文件尾部没有`MAGIC_STRING`，表示该文件不完整，需要进行截断。从`VERSION_NUMBER`往后读，读出 chunk 中的数据，并根据 chunk 中的数据恢复出 ChunkMetadata，若遇到`CHUNK_GROUP_FOOTER`，则表示整个 ChunkGroup 是完整的，更新`truncatedSize`至当前位置
		* 返回`truncatedSize`
	* 根据返回的`truncatedSize`，对文件进行截断
		* 若`truncatedSize`等于`TsFileCheckStatus.COMPLETE_FILE`，则将`crashed`和`canWrite`置为`false`，并关闭文件的输出流
		* 若`truncatedSize`等于`TsFileCheckStatus.INCOMPATIBLE_FILE`，则关闭文件的输出流，并抛异常
		* 否则，将`crashed`和`canWrite`置为`true`，并将文件截断至`truncatedSize`

		
* 通过返回的 RestorableTsFileIOWriter 判断文件是否完整
	
	* 若该 TsFile 文件是完整的
		* 若 TsFile 文件对应的 resource 文件存在，则将 resource 文件反序列化（包括每个设备在该 tsfile 文件中的最小和最大时间戳），并恢复文件版本号
		* 若 TsFile 文件对应的 resource 文件不存在，则重新生成 resource 文件
		* 返回生成的 `RestorableTsFileIOWriter`

	* 若 TsFile 不完整
		* 调用`recoverResourceFromWriter`，通过`RestorableTsFileIOWriter`中的 ChunkMetadata 信息，恢复出 resource 信息
		* 调用`redoLogs`方法将这个文件对应的一个或多个写前日志文件中的数据都写到一个临时 Memtable 中，并持久化到这个不完整的 TsFile 中
			* 对于顺序文件，跳过时间戳小于等于当前 resource 的 WAL
			* 对于乱序文件，将 WAL 全部重做，有可能重复写入多个 device 的 ChunkGroup
		* 如果该 TsFile 不是当前分区的最后一个 TsFile，或者该 TsFile 有`.closing`文件存在，则调用`RestorableTsFileIOWriter`的`endFile()`方法，将文件封口，并删除`.closing`文件，并为其生成 resource 文件