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

# 编码方式

为了提高数据的存储效率，需要在数据写入的过程中对数据进行编码，从而减少磁盘空间的使用量。在写数据以及读数据的过程中都能够减少I/O操作的数据量从而提高性能。IoTDB支持四种针对不同类型的数据的编码方法：

* PLAIN编码（PLAIN）

PLAIN编码，默认的编码方式，即不编码，支持多种数据类型，压缩和解压缩的时间效率较高，但空间存储效率较低。

* 二阶差分编码（TS_2DIFF）

二阶差分编码，比较适合编码单调递增或者递减的序列数据，不适合编码波动较大的数据。

* 游程编码（RLE）

游程编码，比较适合存储某些整数值连续出现的序列，不适合编码大部分情况下前后值不一样的序列数据。

游程编码也可用于对浮点数进行编码，但在创建时间序列的时候需指定保留小数位数（MAX_POINT_NUMBER，具体指定方式参见本文本文[第5.4节](../Operation%20Manual/SQL%20Reference.md)）。比较适合存储某些浮点数值连续出现的序列数据，不适合存储对小数点后精度要求较高以及前后波动较大的序列数据。

> 游程编码（RLE）和二阶差分编码（TS_2DIFF）对 float 和 double 的编码是有精度限制的，默认保留2位小数。推荐使用 GORILLA。

* GORILLA编码（GORILLA）

GORILLA编码，比较适合编码前后值比较接近的浮点数序列，不适合编码前后波动较大的数据。

* 定频数据编码 (REGULAR)

定频数据编码，仅适用于整形（INT32）和长整型（INT64）的定频数据，且允许数据中有一些点缺失，使用此方法编码定频数据优于二阶差分编码（TS_2DIFF）。

定频数据编码无法用于非定频数据，建议使用二阶差分编码（TS_2DIFF）进行处理。

* 数据类型与编码的对应关系

前文介绍的四种编码适用于不同的数据类型，若对应关系错误，则无法正确创建时间序列。数据类型与支持其编码的编码方式对应关系总结如表格2-3。

<center> **表格2-3 数据类型与支持其编码的对应关系**

|数据类型	|支持的编码|
|:---:|:---:|
|BOOLEAN|	PLAIN, RLE|
|INT32	|PLAIN, RLE, TS_2DIFF, REGULAR|
|INT64	|PLAIN, RLE, TS_2DIFF, REGULAR|
|FLOAT	|PLAIN, RLE, TS_2DIFF, GORILLA|
|DOUBLE	|PLAIN, RLE, TS_2DIFF, GORILLA|
|TEXT	|PLAIN|

</center>
