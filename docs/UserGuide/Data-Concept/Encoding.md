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

* DICTIONARY

DICTIONARY encoding is lossless. It is suitable for TEXT data with low cardinality (i.e. low number of distinct values). It is not recommended to use it for high-cardinality data. 

* FREQ

FREQ encoding is lossy. Based on the idea of transform coding, it transforms the time sequence to the frequency domain and only reserve part of the frequency components with high energy. Thus, it greatly improves the space efficiency with little accuracy loss. It is suitable for data with high energy concentration (especially those with obvious periodicity), not suitable for data with uniformly distributed energy (such as white noise).

> There are two parameters of FREQ encoding in the configuration file: `freq_snr` defines the signal-noise-ratio (SNR). There is a mathematical relationship between SNR and NRMSE as $NRMSE = 10^{-SNR/20}$. Both the compression ratio and accuracy loss decrease when it increases. `freq_block_size` defines the data size in a time-frequency transformation. It is not recommended to modify the default value. The detailed experimental results and analysis of the influences of parameters are in the design document. 

* ZIGZAG 
  
ZIGZAG encoding maps signed integers to unsigned integers so that numbers with a small absolute value (for instance, -1) have a small variant encoded value too. It does this in a way that "zig-zags" back and forth through the positive and negative integers.

* CHIMP

CHIMP encoding is lossless. It is the state-of-the-art compression algorithm for streaming floating point data, providing impressive savings compared to earlier approaches. It is suitable for any numerical sequence with similar values and works best for sequence data without large fluctuations and/or random noise.

Usage restrictions: When using CHIMP to encode INT32 data, you need to ensure that there is no data point with the value `Integer.MIN_VALUE` in the sequence. When using CHIMP to encode INT64 data, you need to ensure that there is no data point with the value `Long.MIN_VALUE` in the sequence.

## Correspondence between data type and encoding

The five encodings described in the previous sections are applicable to different data types. If the correspondence is wrong, the time series cannot be created correctly. 

The correspondence between the data type and its supported encodings is summarized in the Table below.

| Data Type | Supported Encoding                          |
|:---------:|:-------------------------------------------:|
| BOOLEAN   | PLAIN, RLE                                  |
| INT32     | PLAIN, RLE, TS_2DIFF, GORILLA, FREQ, ZIGZAG |
| INT64     | PLAIN, RLE, TS_2DIFF, GORILLA, FREQ, ZIGZAG |
| FLOAT     | PLAIN, RLE, TS_2DIFF, GORILLA, FREQ         |
| DOUBLE    | PLAIN, RLE, TS_2DIFF, GORILLA, FREQ         |
| TEXT      | PLAIN, DICTIONARY                           |

When the data type specified by the user does not correspond to the encoding method, the system will prompt an error. 

As shown below, the second-order difference encoding does not support the Boolean type:

```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
Msg: 507: encoding TS_2DIFF does not support BOOLEAN
```
