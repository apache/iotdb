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

# 运维工具
## 集群管理工具
以本地环境为例，演示 IoTDB 集群的启动、扩容与缩容。

**注意：本文档为使用本地不同端口，进行伪分布式环境部署的教程，仅用于练习。在真实环境部署时，一般不需要修改节点端口，仅需配置节点 IPV4 地址或域名即可。**

### 1. 准备启动环境

解压 apache-iotdb-1.0.0-all-bin.zip 至 cluster0 目录。

### 2. 启动最小集群

在 Linux 环境中，部署 1 个 ConfigNode 和 1 个 DataNode（1C1D）集群版，默认 1 副本：
```
./cluster0/sbin/start-confignode.sh
./cluster0/sbin/start-datanode.sh
```

### 3. 验证最小集群

+ 最小集群启动成功，启动 Cli 进行验证：
```
./cluster0/sbin/start-cli.sh
```

+ 在 Cli 执行 [show cluster details](https://iotdb.apache.org/zh/UserGuide/Master/Maintenance-Tools/Maintenance-Command.html#%E6%9F%A5%E7%9C%8B%E5%85%A8%E9%83%A8%E8%8A%82%E7%82%B9%E4%BF%A1%E6%81%AF)
  指令，结果如下所示：
```
IoTDB> show cluster details
+------+----------+-------+---------------+------------+-------------------+----------+-------+--------+-------------------+-----------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|RpcAddress|RpcPort|MppPort |SchemaConsensusPort|DataConsensusPort|
+------+----------+-------+---------------+------------+-------------------+----------+-------+--------+-------------------+-----------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|              10720|          |       |        |                   |                 |
|     1|  DataNode|Running|      127.0.0.1|       10730|                   | 127.0.0.1|   6667|   10740|              10750|            10760|
+------+----------+-------+---------------+------------+-------------------+----------+-------+--------+-------------------+-----------------+
Total line number = 2
It costs 0.242s
```

### 4. 准备扩容环境

解压 apache-iotdb-1.0.0-all-bin.zip 至 cluster1 目录和 cluster2 目录

### 5. 修改节点配置文件

对于 cluster1 目录：

+ 修改 ConfigNode 配置：

| **配置项**                        | **值**           |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 10711           |
| cn\_consensus\_port            | 10721           |
| cn\_target\_config\_node\_list | 127.0.0.1:10710 |

+ 修改 DataNode 配置：

| **配置项**                                | **值**           |
|----------------------------------------|-----------------|
| dn\_rpc\_address                       | 127.0.0.1       |
| dn\_rpc\_port                          | 6668            |
| dn\_internal\_address                  | 127.0.0.1       |
| dn\_internal\_port                     | 10731           |
| dn\_mpp\_data\_exchange\_port          | 10741           |
| dn\_schema\_region\_consensus\_port    | 10751           |
| dn\_data\_region\_consensus\_port      | 10761           |
| dn\_target\_config\_node\_list         | 127.0.0.1:10710 |

对于 cluster2 目录：

+ 修改 ConfigNode 配置：

| **配置项**                        | **值**           |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 10712           |
| cn\_consensus\_port            | 10722           |
| cn\_target\_config\_node\_list | 127.0.0.1:10710 |

+ 修改 DataNode 配置：

| **配置项**                              | **值**           |
|--------------------------------------|-----------------|
| dn\_rpc\_address                     | 127.0.0.1       |
| dn\_rpc\_port                        | 6669            |
| dn\_internal\_address                | 127.0.0.1       |
| dn\_internal\_port                   | 10732           |
| dn\_mpp\_data\_exchange\_port        | 10742           |
| dn\_schema\_region\_consensus\_port  | 10752           |
| dn\_data\_region\_consensus\_port    | 10762           |
| dn\_target\_config\_node\_list       | 127.0.0.1:10710 |

### 6. 集群扩容

将集群扩容至 3 个 ConfigNode 和 3 个 DataNode（3C3D）集群版，
指令执行顺序为先启动 ConfigNode，再启动 DataNode：
```
./cluster1/sbin/start-confignode.sh
./cluster2/sbin/start-confignode.sh
./cluster1/sbin/start-datanode.sh
./cluster2/sbin/start-datanode.sh
```

### 7. 验证扩容结果

在 Cli 执行 `show cluster details`，结果如下：
```
IoTDB> show cluster details
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|RpcAddress|RpcPort|MppPort|SchemaConsensusPort|DataConsensusPort|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|              10720|          |       |       |                   |                 |
|     2|ConfigNode|Running|      127.0.0.1|       10711|              10721|          |       |       |                   |                 |
|     3|ConfigNode|Running|      127.0.0.1|       10712|              10722|          |       |       |                   |                 |
|     1|  DataNode|Running|      127.0.0.1|       10730|                   | 127.0.0.1|   6667|  10740|              10750|            10760|
|     4|  DataNode|Running|      127.0.0.1|       10731|                   | 127.0.0.1|   6668|  10741|              10751|            10761|
|     5|  DataNode|Running|      127.0.0.1|       10732|                   | 127.0.0.1|   6669|  10742|              10752|            10762|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
Total line number = 6
It costs 0.012s
```

### 8. 集群缩容

+ 缩容一个 ConfigNode：
```
# 使用 ip:port 移除
./cluster0/sbin/remove-confignode.sh 127.0.0.1:10711

# 使用节点编号移除
./cluster0/sbin/remove-confignode.sh 2
```

+ 缩容一个 DataNode：
```
# 使用 ip:port 移除
./cluster0/sbin/remove-datanode.sh 127.0.0.1:6668

# 使用节点编号移除
./cluster0/sbin/remove-confignode.sh 4
```

### 9. 验证缩容结果

在 Cli 执行 `show cluster details`，结果如下：
```
IoTDB> show cluster details
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|RpcAddress|RpcPort|MppPort|SchemaConsensusPort|DataConsensusPort|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|              10720|          |       |       |                   |                 |
|     3|ConfigNode|Running|      127.0.0.1|       10712|              10722|          |       |       |                   |                 |
|     1|  DataNode|Running|      127.0.0.1|       10730|                   | 127.0.0.1|   6667|  10740|              10750|            10760|
|     5|  DataNode|Running|      127.0.0.1|       10732|                   | 127.0.0.1|   6669|  10742|              10752|            10762|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
Total line number = 4
It costs 0.005s
```


## 数据文件夹概览工具

IoTDB数据文件夹概览工具用于打印出数据文件夹的结构概览信息，工具位置为 tools/tsfile/print-iotdb-data-dir。

### 用法

-   Windows:

```bash
.\print-iotdb-data-dir.bat <IoTDB数据文件夹路径，如果是多个文件夹用逗号分隔> (<输出结果的存储路径>) 
```

-   Linux or MacOs:

```shell
./print-iotdb-data-dir.sh <IoTDB数据文件夹路径，如果是多个文件夹用逗号分隔> (<输出结果的存储路径>)
```

注意：如果没有设置输出结果的存储路径, 将使用相对路径"IoTDB_data_dir_overview.txt"作为默认值。

### 示例

以Windows系统为例：

`````````````````````````bash
.\print-iotdb-data-dir.bat D:\github\master\iotdb\data\datanode\data
````````````````````````
Starting Printing the IoTDB Data Directory Overview
````````````````````````
output save path:IoTDB_data_dir_overview.txt
data dir num:1
143  [main] WARN  o.a.i.t.c.conf.TSFileDescriptor - not found iotdb-common.properties, use the default configs.
|==============================================================
|D:\github\master\iotdb\data\datanode\data
|--sequence
|  |--root.redirect0
|  |  |--1
|  |  |  |--0
|  |--root.redirect1
|  |  |--2
|  |  |  |--0
|  |--root.redirect2
|  |  |--3
|  |  |  |--0
|  |--root.redirect3
|  |  |--4
|  |  |  |--0
|  |--root.redirect4
|  |  |--5
|  |  |  |--0
|  |--root.redirect5
|  |  |--6
|  |  |  |--0
|  |--root.sg1
|  |  |--0
|  |  |  |--0
|  |  |  |--2760
|--unsequence
|==============================================================
`````````````````````````

## TsFile概览工具

TsFile概览工具用于以概要模式打印出一个TsFile的内容，工具位置为 tools/tsfile/print-tsfile。

### 用法

-   Windows:

```bash
.\print-tsfile-sketch.bat <TsFile文件路径> (<输出结果的存储路径>) 
```

-   Linux or MacOs:

```shell
./print-tsfile-sketch.sh <TsFile文件路径> (<输出结果的存储路径>) 
```

注意：如果没有设置输出结果的存储路径, 将使用相对路径"TsFile_sketch_view.txt"作为默认值。

### 示例

以Windows系统为例：

`````````````````````````bash
.\print-tsfile.bat D:\github\master\1669359533965-1-0-0.tsfile D:\github\master\sketch.txt
````````````````````````
Starting Printing the TsFile Sketch
````````````````````````
TsFile path:D:\github\master\1669359533965-1-0-0.tsfile
Sketch save path:D:\github\master\sketch.txt
148  [main] WARN  o.a.i.t.c.conf.TSFileDescriptor - not found iotdb-common.properties, use the default configs.
-------------------------------- TsFile Sketch --------------------------------
file path: D:\github\master\1669359533965-1-0-0.tsfile
file length: 2974

            POSITION|   CONTENT
            --------    -------
                   0|   [magic head] TsFile
                   6|   [version number] 3
||||||||||||||||||||| [Chunk Group] of root.sg1.d1, num of Chunks:3
                   7|   [Chunk Group Header]
                    |           [marker] 0
                    |           [deviceID] root.sg1.d1
                  20|   [Chunk] of root.sg1.d1.s1, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-9032452783138882770,maxValue:9117677033041335123,firstValue:7068645577795875906,lastValue:-5833792328174747265,sumValue:5.795959009889246E19]
                    |           [chunk header] marker=5, measurementID=s1, dataSize=864, dataType=INT64, compressionType=SNAPPY, encodingType=RLE
                    |           [page]  UncompressedSize:862, CompressedSize:860
                 893|   [Chunk] of root.sg1.d1.s2, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-8806861312244965718,maxValue:9192550740609853234,firstValue:1150295375739457693,lastValue:-2839553973758938646,sumValue:8.2822564314572677E18]
                    |           [chunk header] marker=5, measurementID=s2, dataSize=864, dataType=INT64, compressionType=SNAPPY, encodingType=RLE
                    |           [page]  UncompressedSize:862, CompressedSize:860
                1766|   [Chunk] of root.sg1.d1.s3, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-9076669333460323191,maxValue:9175278522960949594,firstValue:2537897870994797700,lastValue:7194625271253769397,sumValue:-2.126008424849926E19]
                    |           [chunk header] marker=5, measurementID=s3, dataSize=864, dataType=INT64, compressionType=SNAPPY, encodingType=RLE
                    |           [page]  UncompressedSize:862, CompressedSize:860
||||||||||||||||||||| [Chunk Group] of root.sg1.d1 ends
                2656|   [marker] 2
                2657|   [TimeseriesIndex] of root.sg1.d1.s1, tsDataType:INT64, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-9032452783138882770,maxValue:9117677033041335123,firstValue:7068645577795875906,lastValue:-5833792328174747265,sumValue:5.795959009889246E19]
                    |           [ChunkIndex] offset=20
                2728|   [TimeseriesIndex] of root.sg1.d1.s2, tsDataType:INT64, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-8806861312244965718,maxValue:9192550740609853234,firstValue:1150295375739457693,lastValue:-2839553973758938646,sumValue:8.2822564314572677E18]
                    |           [ChunkIndex] offset=893
                2799|   [TimeseriesIndex] of root.sg1.d1.s3, tsDataType:INT64, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-9076669333460323191,maxValue:9175278522960949594,firstValue:2537897870994797700,lastValue:7194625271253769397,sumValue:-2.126008424849926E19]
                    |           [ChunkIndex] offset=1766
                2870|   [IndexOfTimerseriesIndex Node] type=LEAF_MEASUREMENT
                    |           <s1, 2657>
                    |           <endOffset, 2870>
||||||||||||||||||||| [TsFileMetadata] begins
                2891|   [IndexOfTimerseriesIndex Node] type=LEAF_DEVICE
                    |           <root.sg1.d1, 2870>
                    |           <endOffset, 2891>
                    |   [meta offset] 2656
                    |   [bloom filter] bit vector byte array length=31, filterSize=256, hashFunctionSize=5
||||||||||||||||||||| [TsFileMetadata] ends
                2964|   [TsFileMetadataSize] 73
                2968|   [magic tail] TsFile
                2974|   END of TsFile
---------------------------- IndexOfTimerseriesIndex Tree -----------------------------
        [MetadataIndex:LEAF_DEVICE]
        └──────[root.sg1.d1,2870]
                        [MetadataIndex:LEAF_MEASUREMENT]
                        └──────[s1,2657]
---------------------------------- TsFile Sketch End ----------------------------------
`````````````````````````

解释：

-   以"|"为分隔，左边是在TsFile文件中的实际位置，右边是梗概内容。
-   "|||||||||||||||||||||"是为增强可读性而添加的导引信息，不是TsFile中实际存储的数据。
-   最后打印的"IndexOfTimerseriesIndex Tree"是对TsFile文件末尾的元数据索引树的重新整理打印，便于直观理解，不是TsFile中存储的实际数据。

## TsFile Resource概览工具

TsFile resource概览工具用于打印出TsFile resource文件的内容，工具位置为 tools/tsfile/print-tsfile-resource-files。

### 用法

-   Windows:

```bash
.\print-tsfile-resource-files.bat <TsFile resource文件所在的文件夹路径，或者单个TsFile resource文件路径>
```

-   Linux or MacOs:

```
./print-tsfile-resource-files.sh <TsFile resource文件所在的文件夹路径，或者单个TsFile resource文件路径> 
```

### 示例

以Windows系统为例：

`````````````````````````bash
.\print-tsfile-resource-files.bat D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0
````````````````````````
Starting Printing the TsFileResources
````````````````````````
147  [main] WARN  o.a.i.t.c.conf.TSFileDescriptor - not found iotdb-common.properties, use the default configs.
230  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-common.properties, use default configuration
231  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Couldn't load the configuration iotdb-common.properties from any of the known sources.
233  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-datanode.properties, use default configuration
237  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Couldn't load the configuration iotdb-datanode.properties from any of the known sources.
Analyzing D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0\1669359533489-1-0-0.tsfile ...

Resource plan index range [9223372036854775807, -9223372036854775808]
device root.sg1.d1, start time 0 (1970-01-01T08:00+08:00[GMT+08:00]), end time 99 (1970-01-01T08:00:00.099+08:00[GMT+08:00])

Analyzing the resource file folder D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0 finished.
`````````````````````````

`````````````````````````bash
.\print-tsfile-resource-files.bat D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0\1669359533489-1-0-0.tsfile.resource
````````````````````````
Starting Printing the TsFileResources
````````````````````````
178  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-common.properties, use default configuration
186  [main] WARN  o.a.i.t.c.conf.TSFileDescriptor - not found iotdb-common.properties, use the default configs.
187  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Couldn't load the configuration iotdb-common.properties from any of the known sources.
188  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-datanode.properties, use default configuration
192  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Couldn't load the configuration iotdb-datanode.properties from any of the known sources.
Analyzing D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0\1669359533489-1-0-0.tsfile ...

Resource plan index range [9223372036854775807, -9223372036854775808]
device root.sg1.d1, start time 0 (1970-01-01T08:00+08:00[GMT+08:00]), end time 99 (1970-01-01T08:00:00.099+08:00[GMT+08:00])

Analyzing the resource file D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0\1669359533489-1-0-0.tsfile.resource finished.
`````````````````````````
