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

## 编码方式

### 基本编码方式

为了提高数据的存储效率，需要在数据写入的过程中对数据进行编码，从而减少磁盘空间的使用量。在写数据以及读数据的过程中都能够减少 I/O 操作的数据量从而提高性能。IoTDB 支持多种针对不同类型的数据的编码方法：

* PLAIN 编码（PLAIN）

PLAIN 编码，默认的编码方式，即不编码，支持多种数据类型，压缩和解压缩的时间效率较高，但空间存储效率较低。

* 二阶差分编码（TS_2DIFF）

二阶差分编码，比较适合编码单调递增或者递减的序列数据，不适合编码波动较大的数据。

* 游程编码（RLE）

游程编码，比较适合存储某些数值连续出现的序列，不适合编码大部分情况下前后值不一样的序列数据。

游程编码也可用于对浮点数进行编码，但在创建时间序列的时候需指定保留小数位数（MAX_POINT_NUMBER，具体指定方式参见本文 [SQL 参考文档](../Reference/SQL-Reference.md)）。比较适合存储某些浮点数值连续出现的序列数据，不适合存储对小数点后精度要求较高以及前后波动较大的序列数据。

> 游程编码（RLE）和二阶差分编码（TS_2DIFF）对 float 和 double 的编码是有精度限制的，默认保留 2 位小数。推荐使用 GORILLA。

* GORILLA 编码（GORILLA）

GORILLA 编码是一种无损编码，它比较适合编码前后值比较接近的数值序列，不适合编码前后波动较大的数据。

当前系统中存在两个版本的 GORILLA 编码实现，推荐使用`GORILLA`，不推荐使用`GORILLA_V1`（已过时）。

使用限制：使用 Gorilla 编码 INT32 数据时，需要保证序列中不存在值为`Integer.MIN_VALUE`的数据点；使用 Gorilla 编码 INT64 数据时，需要保证序列中不存在值为`Long.MIN_VALUE`的数据点。

* 字典编码 （DICTIONARY）

字典编码是一种无损编码。它适合编码基数小的数据（即数据去重后唯一值数量小）。不推荐用于基数大的数据。

* 频域编码 （FREQ）

频域编码是一种有损编码，它基于变换编码的思想，将时序数据变换为频域，仅保留部分高能量的频域分量，以少许的精度损失为代价大幅提高空间效率。该编码适合于频域能量分布较为集中的数据（特别是具有明显周期性的数据），不适合能量分布均匀的数据（如白噪声）。

> 频域编码在配置文件中包括两个参数：`freq_snr`指定了编码的信噪比（与标准均方根误差的关系为$NRMSE=10^{-SNR/20}$），该参数增大会同时降低压缩比和精度损失，请根据实际应用的需要进行设置；`freq_block_size`指定了编码进行时频域变换的分组大小，推荐不对默认值进行修改。参数影响的实验结果和分析详见设计文档。

* ZIGZAG 编码

ZigZag编码将有符号整型映射到无符号整型，适合比较小的整数。

* CHIMP

CHIMP 是一种无损编码。它是一种新的流式浮点数据压缩算法，可以节省存储空间。这个编码适用于前后值比较接近的数值序列，对波动小和随机噪声少的序列数据更加友好。

使用限制：如果对 INT32 类型数据使用 CHIMP 编码，需要确保数据点中没有 `Integer.MIN_VALUE`。 如果对 INT64 类型数据使用 CHIMP 编码，需要确保数据点中没有 `Long.MIN_VALUE`。

### 数据类型与编码的对应关系

前文介绍的五种编码适用于不同的数据类型，若对应关系错误，则无法正确创建时间序列。数据类型与支持其编码的编码方式对应关系总结如下表所示。

| 数据类型 | 支持的编码                                  |
| :----- | :----- |
| BOOLEAN  | PLAIN, RLE                                  |
| INT32    | PLAIN, RLE, TS_2DIFF, GORILLA, FREQ, ZIGZAG |
| INT64    | PLAIN, RLE, TS_2DIFF, GORILLA, FREQ, ZIGZAG |
| FLOAT    | PLAIN, RLE, TS_2DIFF, GORILLA, FREQ         |
| DOUBLE   | PLAIN, RLE, TS_2DIFF, GORILLA, FREQ         |
| TEXT     | PLAIN, DICTIONARY                           |

当用户输入的数据类型与编码方式不对应时，系统会提示错误。如下所示，二阶差分编码不支持布尔类型：

```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
Msg: 507: encoding TS_2DIFF does not support BOOLEAN
```
