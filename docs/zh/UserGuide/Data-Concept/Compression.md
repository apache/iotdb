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

## 压缩方式

当时间序列写入并按照指定的类型编码为二进制数据后，IoTDB 会使用压缩技术对该数据进行压缩，进一步提升空间存储效率。虽然编码和压缩都旨在提升存储效率，但编码技术通常只适合特定的数据类型（如二阶差分编码只适合与 INT32 或者 INT64 编码，存储浮点数需要先将他们乘以 10m 以转换为整数），然后将它们转换为二进制流。压缩方式（SNAPPY）针对二进制流进行压缩，因此压缩方式的使用不再受数据类型的限制。

### 基本压缩方式

IoTDB 允许在创建一个时间序列的时候指定该列的压缩方式。现阶段 IoTDB 支持以下几种压缩方式：

* UNCOMPRESSED（不压缩）
* SNAPPY 压缩
* LZ4 压缩
* GZIP 压缩

压缩方式的指定语法详见本文 [SQL 参考文档](../Reference/SQL-Reference.md)。

### 压缩比统计信息

压缩比统计信息文件：data/system/storage_groups/compression_ratio/Ratio-{ratio_sum}-{memtable_flush_time}

* ratio_sum: memtable压缩比的总和
* memtable_flush_time: memtable刷盘的总次数

通过 `ratio_sum / memtable_flush_time` 可以计算出平均压缩比