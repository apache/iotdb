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

## Design ideas

After the memory buffer memtable reaches a certain threshold, it will be handed over to the FlushManager for asynchronous persistence without blocking normal writes. The persistence process is pipelined.

## Related idea

* org.apache.iotdb.db.engine.flush.FlushManager

	Memtable's Flush task manager.
	
* org.apache.iotdb.db.engine.flush.MemtableFlushTask

	flush a Memtable。

## FlushManager: Persistence manager

FlushManager can accept memtable persistent tasks. There are two submitters. The first is TsFileProcessor and the second is the persistent child thread FlushThread.

Each TsFileProcessor will only have one flush task executed at a time. A TsFileProcessor may correspond to multiple memtables that need to be persisted.

## MemTableFlushTask: Persistent task

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625254-03fe2680-467f-11ea-8197-115f3a749cbd.png">

Background: Each memtable can contain multiple devices, and each device can contain multiple measurements.

### Three threads

A memtable's persistence process has three threads, and the main thread's work does not end until all tasks are completed.

* MemTableFlushTask  Thread

  The sorting thread (the main thread), responsible for sorting the chunks corresponding to each measurement and submit tasks to the encoding task.

* encodingTask Thread

  The encoding thread is responsible for encoding each Chunk and encoding it into a byte array.

* ioTask Thread

  The IO thread is responsible for persisting the encoded Chunk to the TsFile on the disk.

### Two task queues

Three threads interact through two task queues

* encodingTaskQueue: Sorting thread-> encoding thread, including three tasks
	
	* StartFlushGroupIOTask：Began to persist a device (ChunkGroup), encoding does not process this command, and sends it directly to the IO thread.
	
	* Pair\<TVList, MeasurementSchema\>：Encoding a Chunk
	
	* EndChunkGroupIoTask：End the persistence of a device (ChunkGroup). The encoding thread does not process this command and  send directly to the IO thread.

* ioTaskQueue: Encoding thread-> IO thread, including three tasks
	
	* StartFlushGroupIOTask：Starting to persist a device (ChunkGroup).
	
	* IChunkWriter：Persisting a Chunk to Disk
	
	* EndChunkGroupIoTask：Ends the persistence of a device (ChunkGroup).
