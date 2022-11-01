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


## Encoding Methods

To improve the efficiency of data storage, it is necessary to encode data during data writing, thereby reducing the amount of disk space used. In the process of writing and reading data, the amount of data involved in the I/O operations can be reduced to improve performance. IoTDB supports the following encoding methods for different data types:

* PLAIN

PLAIN encoding, the default encoding mode, i.e, no encoding, supports multiple data types. It has high compression and decompression efficiency while suffering from low space storage efficiency.

* TS_2DIFF

Second-order differential encoding is more suitable for encoding monotonically increasing or decreasing sequence data, and is not recommended for sequence data with large fluctuations.

* RLE

Run-length encoding is suitable for storing sequence with continuous values, and is not recommended for sequence data with most of the time different values.

Run-length encoding can also be used to encode floating-point numbers, while it is necessary to specify reserved decimal digits (MAX\_POINT\_NUMBER) when creating time series. It is more suitable to store sequence data where floating-point values appear continuously, monotonously increasing or decreasing, and it is not suitable for storing sequence data with high precision requirements after the decimal point or with large fluctuations.

> TS_2DIFF and RLE have precision limit for data type of float and double. By default, two decimal places are reserved. GORILLA is recommended. 

* GORILLA

GORILLA encoding is lossless. It is more suitable for numerical sequence with similar values and is not recommended for sequence data with large fluctuations.

Currently, there are two versions of GORILLA encoding implementation, it is recommended to use `GORILLA` instead of `GORILLA_V1` (deprecated).

Usage restrictions: When using GORILLA to encode INT32 data, you need to ensure that there is no data point with the value `Integer.MIN_VALUE` in the sequence. When using GORILLA to encode INT64 data, you need to ensure that there is no data point with the value `Long.MIN_VALUE` in the sequence.

* RAKE

The RAKE encoding is based only on bits counting operations. It is more suitable for the ‘1’s of binary numbers to be more sparsely.

* RLBE

The RLBE encoding proposes to combine delta, run-length and Fibonacci based encoding ideas. It has five steps: differential coding, binary encoding, run-length, Fibonacci coding and concatenation.
It is more suitable for the differential value of time series is positive and small.

* SPRINTZ

The SPRINTZ encoding combines encodings in four steps: predicting, bit-packing, run-length encoding and entropy encoding. SPRINTZ algorithm is suitable for predictable time series. For delta function, the vast repeats or linearly increasing time series is the best target.

* DICTIONARY

DICTIONARY encoding is lossless. It is suitable for TEXT data with low cardinality (i.e. low number of distinct values). It is not recommended to use it for high-cardinality data. 

* TEXTRLE

TEXT Run-Length Encoding (TEXTRLE) performs especially for data with strings of repeated characters (the length of the string is called a run).

* HUFFMAN

It is more suitable for data with many high frequency values in skewed data distribution and many repeated characters.


## Correspondence between data type and encoding

The five encodings described in the previous sections are applicable to different data types. If the correspondence is wrong, the time series cannot be created correctly. The correspondence between the data type and its supported encodings is summarized in the Table below.

<div style="text-align: center;"> 

**The correspondence between the data type and its supported encodings**

|Data Type	|                 Supported Encoding                 |
|:---:|:--------------------------------------------------:|
|BOOLEAN|                    	PLAIN, RLE                     |
|INT32	|           PLAIN, RLE, TS_2DIFF, GORILLA, RAKE, RLBE, SPRINTZ            |
|INT64	|           PLAIN, RLE, TS_2DIFF, GORILLA, RAKE, RLBE, SPRINTZ            |
|FLOAT	|           PLAIN, RLE, TS_2DIFF, GORILLA, RAKE, RLBE, SPRINTZ            |
|DOUBLE	| PLAIN, RLE, TS_2DIFF, GORILLA, RAKE, RLBE, SPRINTZ |
|TEXT	|        PLAIN, DICTIONARY, TEXTRLE, HUFFMAN         |

</div>
