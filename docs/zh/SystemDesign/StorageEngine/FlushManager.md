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

# Flush Memtable

## 设计思想

内存缓冲区 memtable 达到一定阈值后，会交给 FlushManager 进行异步的持久化，不阻塞正常写入。持久化的过程采用流水线的方式。

## 相关代码

* org.apache.iotdb.db.engine.flush.FlushManager

	Memtable 的 Flush 任务管理器。
	
* org.apache.iotdb.db.engine.flush.MemtableFlushTask

	负责持久化一个 Memtable。

## FlushManager: 持久化管理器

FlushManager 可以接受 memtable 的持久化任务，提交者有两个，第一个是 TsFileProcessor，第二个是持久化子线程 FlushThread。

每个 TsFileProcessor 同一时刻只会有一个 flush 任务执行，一个 TsFileProcessor 可能对应多个需要持久化的 memtable

## MemTableFlushTask: 持久化任务

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625254-03fe2680-467f-11ea-8197-115f3a749cbd.png">

背景：每个 memtable 可包含多个 device，每个 device 可包含多个 measurement。

### 三个线程

一个 memtable 的持久化的过程有三个线程，只有当所有任务都完成后，主线程工作才结束。

* MemTableFlushTask 所在线程
	
	持久化主线程兼排序线程，负责给每个 measurement 对应的 chunk 排序。

* encodingTask 线程

	编码线程，负责给每个 Chunk 进行编码，编码成字节数组。
	
* ioTask 线程

	IO 线程，负责将编码好的 Chunk 持久化到磁盘的 TsFile 文件上。

### 两个任务队列

三个线程之间通过两个任务队列交互

* encodingTaskQueue: 排序线程->编码线程，包括三种任务
	
	* StartFlushGroupIOTask：开始持久化一个 device (ChunkGroup)， encoding 不处理这个命令，直接发给 IO 线程。
	
	* Pair\<TVList, MeasurementSchema\>：编码一个 Chunk
	
	* EndChunkGroupIoTask：结束一个 device (ChunkGroup) 的持久化，encoding 不处理这个命令，直接发给 IO 线程。

* ioTaskQueue: 编码线程->IO 线程，包括三种任务
	
	* StartFlushGroupIOTask：开始持久化一个 device (ChunkGroup)。
	
	* IChunkWriter：持久化一个 Chunk 到磁盘上
	
	* EndChunkGroupIoTask：结束一个 device (ChunkGroup) 的持久化。