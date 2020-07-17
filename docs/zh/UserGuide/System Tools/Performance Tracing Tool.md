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
# 性能追踪工具

IoTDB 支持使用 `TRACING` 语句来启用/禁用对查询语句的性能追踪，默认处于禁用状态。用户可以使用性能追踪工具来分析某些查询中存在的潜在性能问题。性能分析的日志文件默认存储在 `./data/tracing` 目录下。

启用 Tracing：

`IoTDB> TRACING ON`

禁用 Tracing：

`IoTDB> TRACING OFF`

由于一个 IoTDB 查询时间主要取决于查询的时间序列数、涉及访问的 Tsfile 文件数、需要扫描的 chunk 总数以及平均每个 chunk 的大小（指该 chunk 中包含的数据点的个数）。因此，目前性能分析包括的内容如下：

- Start time
- Query statement
- Number of series paths
- Number of tsfiles
- Number of sequence files
- Number of unsequence files
- Number of chunks
- Average size of chunks
- End time

## Example

例如执行 `select * from root`，则 tracing 日志文件的内容会包括以下内容：

```
Query Id: 2 - Start time: 2020-06-28 10:53:54.727
Query Id: 2 - Query Statement: select * from root
Query Id: 2 - Number of series paths: 3
Query Id: 2 - Number of tsfiles: 2
Query Id: 2 - Number of sequence files: 2
Query Id: 2 - Number of unsequence files: 0
Query Id: 2 - Number of chunks: 3
Query Id: 2 - Average size of chunks: 4113
Query Id: 2 - End time: 2020-06-28 10:54:44.059
```

为了避免多个查询同时执行导致输出信息乱序，在每条输出信息前均增加了该次查询的 Query Id，用户可以使用 `grep "Query Id: 2" tracing.txt` 来提取某次查询的所有追踪信息。
