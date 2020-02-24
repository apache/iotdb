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

# TsFile 写流程

* org.apache.iotdb.tsfile.write.*

TsFile 的写入流程如下图所示：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625238-efba2980-467e-11ea-927e-a7021f8153af.png">

其中，每个设备对应一个 ChunkGroupWriter，每个传感器对应一个 ChunkWriter。

TsFile 文件层的写入接口有两种

* TsFileWriter.write(TSRecord record)

	写入一个设备一个时间戳多个测点。

* TsFileWriter.write(RowBatch rowBatch)

	写入一个设备多个时间戳多个测点。

文件的写入主要分为三种操作，在图上用 1、2、3 标注

* 1、写内存缓冲区
* 2、持久化 ChunkGroup
* 3、关闭文件

## 1、写内存缓冲区

* 当调用 TsFileWriter.write 接口时，这个设备的数据会交给对应的 ChunkGroupWriter，其中的每个测点(DataPoint)会交给对应的 ChunkWriter 进行写入。
* 当 ChunkWriter 写入一个传感器的数据点时，会调用 PageWriter.write 方法将数据写入 PageWriter 中的 time 和 value 各自的缓冲区，并更新统计值。 
* 当前 PageWriter 缓冲区到达一定内存时，PageWriter 将内部数据打包成一个完整的 page(包括 header 和 data) 放到 pageBuffer 中，并且更新 chunk 中的统计值。 

## 2、持久化 ChunkGroup

* TsFileWriter.flushAllChunkGroups()

当内存中的数据达到一定阈值，会触发持久化操作。每次持久化会把当前内存中所有设备的数据全部持久化到磁盘的 TsFile 文件中。每个设备对应一个 ChunkGroup，每个测点对应一个 Chunk。

持久化完成后会在内存中缓存对应的元数据信息，以供查询和生成文件尾部 metadata。

## 3、关闭文件

* TsFileWriter.close()

根据内存中缓存的元数据，生成 TsFileMetadata 追加到文件尾部，最后关闭文件。
