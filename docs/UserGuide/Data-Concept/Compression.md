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

# Compression

When the time series is written and encoded as binary data according to the specified type, IoTDB compresses the data using compression technology to further improve space storage efficiency. Although both encoding and compression are designed to improve storage efficiency, encoding techniques are usually available only for specific data types (e.g., second-order differential encoding is only suitable for INT32 or INT64 data type, and storing floating-point numbers requires multiplying them by 10m to convert to integers), after which the data is converted to a binary stream. The compression method (SNAPPY) compresses the binary stream, so the use of the compression method is no longer limited by the data type.

## Basic Compression Methods

IoTDB allows you to specify the compression method of the column when creating a time series, and supports the following compression methods: 

* UNCOMPRESSED

* SNAPPY

* LZ4

* GZIP

* ZSTD

* LZMA2

The specified syntax for compression is detailed in [Create Timeseries Statement](../Reference/SQL-Reference.md).

## Compression Ratio Statistics

Compression ratio statistics file: data/system/compression_ratio/Ratio-{ratio_sum}-{memtable_flush_time}

* ratio_sum: sum of memtable compression ratios
* memtable_flush_time: memtable flush times

The average compression ratio can be calculated by `ratio_sum / memtable_flush_time`