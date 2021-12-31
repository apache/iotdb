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
# Segment

## Usage

This function is used to segment a time series into subsequences according to linear trend, and returns linear fitted values of first values in each subsequence or every data point.

**Name:** SEGMENT

**Input Series:** Only support a single input series. The type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:** 

+ `output` :"all" to output all fitted points; "first" to output first fitted points in each subsequence.

+ `error`: error allowed at linear regression. It is defined as mean absolute error of a subsequence. 

**Output Series:** Output a single series. The type is DOUBLE. 

**Note:** This function treat input series as equal-interval sampled. All data are loaded, so downsample input series first if there are too many data points.

## Examples

Input series:

```
+-----------------------------+------------+
|                         Time|root.test.s1|
+-----------------------------+------------+
|1970-01-01T08:00:00.000+08:00|         5.0|
|1970-01-01T08:00:00.100+08:00|         0.0|
|1970-01-01T08:00:00.200+08:00|         1.0|
|1970-01-01T08:00:00.300+08:00|         2.0|
|1970-01-01T08:00:00.400+08:00|         3.0|
|1970-01-01T08:00:00.500+08:00|         4.0|
|1970-01-01T08:00:00.600+08:00|         5.0|
|1970-01-01T08:00:00.700+08:00|         6.0|
|1970-01-01T08:00:00.800+08:00|         7.0|
|1970-01-01T08:00:00.900+08:00|         8.0|
|1970-01-01T08:00:01.000+08:00|         9.0|
|1970-01-01T08:00:01.100+08:00|         9.1|
|1970-01-01T08:00:01.200+08:00|         9.2|
|1970-01-01T08:00:01.300+08:00|         9.3|
|1970-01-01T08:00:01.400+08:00|         9.4|
|1970-01-01T08:00:01.500+08:00|         9.5|
|1970-01-01T08:00:01.600+08:00|         9.6|
|1970-01-01T08:00:01.700+08:00|         9.7|
|1970-01-01T08:00:01.800+08:00|         9.8|
|1970-01-01T08:00:01.900+08:00|         9.9|
|1970-01-01T08:00:02.000+08:00|        10.0|
|1970-01-01T08:00:02.100+08:00|         8.0|
|1970-01-01T08:00:02.200+08:00|         6.0|
|1970-01-01T08:00:02.300+08:00|         4.0|
|1970-01-01T08:00:02.400+08:00|         2.0|
|1970-01-01T08:00:02.500+08:00|         0.0|
|1970-01-01T08:00:02.600+08:00|        -2.0|
|1970-01-01T08:00:02.700+08:00|        -4.0|
|1970-01-01T08:00:02.800+08:00|        -6.0|
|1970-01-01T08:00:02.900+08:00|        -8.0|
|1970-01-01T08:00:03.000+08:00|       -10.0|
|1970-01-01T08:00:03.100+08:00|        10.0|
|1970-01-01T08:00:03.200+08:00|        10.0|
|1970-01-01T08:00:03.300+08:00|        10.0|
|1970-01-01T08:00:03.400+08:00|        10.0|
|1970-01-01T08:00:03.500+08:00|        10.0|
|1970-01-01T08:00:03.600+08:00|        10.0|
|1970-01-01T08:00:03.700+08:00|        10.0|
|1970-01-01T08:00:03.800+08:00|        10.0|
|1970-01-01T08:00:03.900+08:00|        10.0|
+-----------------------------+------------+
```

SQL for query:

```sql
select segment(s1, "error"="0.1") from root.test
```

Output series:

```
+-----------------------------+------------------------------------+
|                         Time|segment(root.test.s1, "error"="0.1")|
+-----------------------------+------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                 5.0|
|1970-01-01T08:00:00.200+08:00|                                 1.0|
|1970-01-01T08:00:01.000+08:00|                                 9.0|
|1970-01-01T08:00:02.000+08:00|                                10.0|
|1970-01-01T08:00:03.000+08:00|                               -10.0|
|1970-01-01T08:00:03.200+08:00|                                10.0|
+-----------------------------+------------------------------------+
```

