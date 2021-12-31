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
# DTW

## Usage

This function is used to calculate the DTW distance between two input series.

**Name:** DTW

**Input Series:** Only support two input series. The types are both INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is DOUBLE. There is only one data point in the series, whose timestamp is 0 and value is the DTW distance.

**Note:** 

+ If a row contains missing points, null points or `NaN`, it will be ignored;
+ If all rows are ignored, `0` will be output.


## Examples

Input series:

```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d2.s1|root.test.d2.s2|
+-----------------------------+---------------+---------------+
|1970-01-01T08:00:00.001+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.002+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.003+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.004+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.005+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.006+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.007+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.008+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.009+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.010+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.011+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.012+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.013+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.014+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.015+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.016+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.017+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.018+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.019+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.020+08:00|            1.0|            2.0|
+-----------------------------+---------------+---------------+
```

SQL for query:

```sql
select dtw(s1,s2) from root.test.d2
```

Output series:

```
+-----------------------------+-------------------------------------+
|                         Time|dtw(root.test.d2.s1, root.test.d2.s2)|
+-----------------------------+-------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                 20.0|
+-----------------------------+-------------------------------------+
```