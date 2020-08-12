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

# Encoding 

In order to improve the efficiency of data storage, it is necessary to encode data during data writing, thereby reducing the amount of disk space used. In the process of writing and reading data, the amount of data involved in the I/O operations can be reduced to improve performance. IoTDB supports four encoding methods for different types of data:

* PLAIN

PLAIN encoding, the default encoding mode, i.e, no encoding, supports multiple data types. It has high compression and decompression efficiency while suffering from low space storage efficiency.

* TS_2DIFF

Second-order differential encoding is more suitable for encoding monotonically increasing or decreasing sequence data, and is not recommended for sequence data with large fluctuations.

* RLE

Run-length encoding is more suitable for storing sequence with continuous integer values, and is not recommended for sequence data with most of the time different values.

Run-length encoding can also be used to encode floating-point numbers, but it is necessary to specify reserved decimal digits (MAX\_POINT\_NUMBER, see [this page](../Operation%20Manual/SQL%20Reference.md) for more information on how to specify) when creating time series. It is more suitable for storing sequence data where floating-point values appear continuously, monotonously increasing or decreasing, and it is not suitable for storing sequence data with high precision requirements after the decimal point or with large fluctuations.

> TS_2DIFF and RLE have precision limit for data type of float and double. By default, two decimal places are reserved. GORILLA is recommended. 

* GORILLA

GORILLA encoding is more suitable for floating-point sequence with similar values and is not recommended for sequence data with large fluctuations.

* REGULAR

Regular data encoding is more suitable for encoding regular sequence increasing data (e.g. the timeseries with the same time elapsed between each data point), in which case it's better than TS_2DIFF.

Regular data encoding method is not suitable for the data with fluctuations (irregular data), and TS_2DIFF is recommended to deal with it.

* Correspondence between data type and encoding

The four encodings described in the previous sections are applicable to different data types. If the correspondence is wrong, the time series cannot be created correctly. The correspondence between the data type and its supported encodings is summarized in Table 2-3.

<center> **Table 2-3 The correspondence between the data type and its supported encodings**

|Data Type	|Supported Encoding|
|:---:|:---:|
|BOOLEAN|	PLAIN, RLE|
|INT32	|PLAIN, RLE, TS_2DIFF, REGULAR|
|INT64	|PLAIN, RLE, TS_2DIFF, REGULAR|
|FLOAT	|PLAIN, RLE, TS_2DIFF, GORILLA|
|DOUBLE	|PLAIN, RLE, TS_2DIFF, GORILLA|
|TEXT	|PLAIN|

</center>
