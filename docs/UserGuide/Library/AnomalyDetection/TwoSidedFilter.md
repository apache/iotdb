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
# TwoSidedFilter
## Usage

The function is used to filter anomalies of a numeric time series based on two-sided window detection.

**Name:** TWOSIDEDFILTER  

**Input Series:** Only support a single input series. The data type is  INT32 / INT64 / FLOAT / DOUBLE  

**Output Series:** Output a single series. The type is the same as the input. It is the input without anomalies.

**Parameter:**

- `len`: The size of the window, which is a positive integer. By default, it's 5. When `len`=3, the algorithm detects forward window and backward window with length 3 and calculates the outlierness of the current point.

- `threshold`: The threshold of outlierness, which is a floating number in (0,1). By default, it's 0.3. The strict standard of detecting anomalies is in proportion to the threshold.

## Examples
Input series:
```
+-----------------------------+------------+
|                         Time|root.test.s0|
+-----------------------------+------------+
|1970-01-01T08:00:00.000+08:00|      2002.0|
|1970-01-01T08:00:01.000+08:00|      1946.0|
|1970-01-01T08:00:02.000+08:00|      1958.0|
|1970-01-01T08:00:03.000+08:00|      2012.0|
|1970-01-01T08:00:04.000+08:00|      2051.0|
|1970-01-01T08:00:05.000+08:00|      1898.0|
|1970-01-01T08:00:06.000+08:00|      2014.0|
|1970-01-01T08:00:07.000+08:00|      2052.0|
|1970-01-01T08:00:08.000+08:00|      1935.0|
|1970-01-01T08:00:09.000+08:00|      1901.0|
|1970-01-01T08:00:10.000+08:00|      1972.0|
|1970-01-01T08:00:11.000+08:00|      1969.0|
|1970-01-01T08:00:12.000+08:00|      1984.0|
|1970-01-01T08:00:13.000+08:00|      2018.0|
|1970-01-01T08:00:37.000+08:00|      1484.0|
|1970-01-01T08:00:38.000+08:00|      1055.0|
|1970-01-01T08:00:39.000+08:00|      1050.0|
|1970-01-01T08:01:05.000+08:00|      1023.0|
|1970-01-01T08:01:06.000+08:00|      1056.0|
|1970-01-01T08:01:07.000+08:00|       978.0|
|1970-01-01T08:01:08.000+08:00|      1050.0|
|1970-01-01T08:01:09.000+08:00|      1123.0|
|1970-01-01T08:01:10.000+08:00|      1150.0|
|1970-01-01T08:01:11.000+08:00|      1034.0|
|1970-01-01T08:01:12.000+08:00|       950.0|
|1970-01-01T08:01:13.000+08:00|      1059.0|
+-----------------------------+------------+
```
SQL for query:
```sql
select TwoSidedFilter(s0, 'len'='5', 'threshold'='0.3') from root.test
```
Output series:
```
+-----------------------------+------------+
|                         Time|root.test.s0|
+-----------------------------+------------+
|1970-01-01T08:00:00.000+08:00|      2002.0|
|1970-01-01T08:00:01.000+08:00|      1946.0|
|1970-01-01T08:00:02.000+08:00|      1958.0|
|1970-01-01T08:00:03.000+08:00|      2012.0|
|1970-01-01T08:00:04.000+08:00|      2051.0|
|1970-01-01T08:00:05.000+08:00|      1898.0|
|1970-01-01T08:00:06.000+08:00|      2014.0|
|1970-01-01T08:00:07.000+08:00|      2052.0|
|1970-01-01T08:00:08.000+08:00|      1935.0|
|1970-01-01T08:00:09.000+08:00|      1901.0|
|1970-01-01T08:00:10.000+08:00|      1972.0|
|1970-01-01T08:00:11.000+08:00|      1969.0|
|1970-01-01T08:00:12.000+08:00|      1984.0|
|1970-01-01T08:00:13.000+08:00|      2018.0|
|1970-01-01T08:01:05.000+08:00|      1023.0|
|1970-01-01T08:01:06.000+08:00|      1056.0|
|1970-01-01T08:01:07.000+08:00|       978.0|
|1970-01-01T08:01:08.000+08:00|      1050.0|
|1970-01-01T08:01:09.000+08:00|      1123.0|
|1970-01-01T08:01:10.000+08:00|      1150.0|
|1970-01-01T08:01:11.000+08:00|      1034.0|
|1970-01-01T08:01:12.000+08:00|       950.0|
|1970-01-01T08:01:13.000+08:00|      1059.0|
+-----------------------------+------------+
```