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
# Histogram

## Usage

This function is used to calculate the distribution histogram of a single column of numerical data.

**Name:** HISTOGRAM

**Input Series:** Only supports a single input sequence, the type is INT32 / INT64 / FLOAT / DOUBLE

**Parameters:**

+ `min`: The lower limit of the requested data range, the default value is -Double.MAX_VALUE.
+ `max`: The upper limit of the requested data range, the default value is Double.MAX_VALUE, and the value of start must be less than or equal to end.
+ `count`: The number of buckets of the histogram, the default value is 1. It must be a positive integer.

**Output Series:** The value of the bucket of the histogram, where the lower bound represented by the i-th bucket (index starts from 1) is $min+ (i-1)\cdot\frac{max-min}{count}$ and the upper bound is $min + i \cdot \frac{max-min}{count}$.

**Note:** 

+ If the value is lower than `min`, it will be put into the 1st bucket. If the value is larger than `max`, it will be put into the last bucket.
+ Missing points, null points and `NaN` in the input series will be ignored.

## Examples

Input series: 

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:00.000+08:00|            1.0|
|2020-01-01T00:00:01.000+08:00|            2.0|
|2020-01-01T00:00:02.000+08:00|            3.0|
|2020-01-01T00:00:03.000+08:00|            4.0|
|2020-01-01T00:00:04.000+08:00|            5.0|
|2020-01-01T00:00:05.000+08:00|            6.0|
|2020-01-01T00:00:06.000+08:00|            7.0|
|2020-01-01T00:00:07.000+08:00|            8.0|
|2020-01-01T00:00:08.000+08:00|            9.0|
|2020-01-01T00:00:09.000+08:00|           10.0|
|2020-01-01T00:00:10.000+08:00|           11.0|
|2020-01-01T00:00:11.000+08:00|           12.0|
|2020-01-01T00:00:12.000+08:00|           13.0|
|2020-01-01T00:00:13.000+08:00|           14.0|
|2020-01-01T00:00:14.000+08:00|           15.0|
|2020-01-01T00:00:15.000+08:00|           16.0|
|2020-01-01T00:00:16.000+08:00|           17.0|
|2020-01-01T00:00:17.000+08:00|           18.0|
|2020-01-01T00:00:18.000+08:00|           19.0|
|2020-01-01T00:00:19.000+08:00|           20.0|
+-----------------------------+---------------+
```

SQL for query: 

```sql
select histogram(s1,"min"="1","max"="20","count"="10") from root.test.d1
```

Output series:

```
+-----------------------------+---------------------------------------------------------------+
|                         Time|histogram(root.test.d1.s1, "min"="1", "max"="20", "count"="10")|
+-----------------------------+---------------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                                              2|
|1970-01-01T08:00:00.001+08:00|                                                              2|
|1970-01-01T08:00:00.002+08:00|                                                              2|
|1970-01-01T08:00:00.003+08:00|                                                              2|
|1970-01-01T08:00:00.004+08:00|                                                              2|
|1970-01-01T08:00:00.005+08:00|                                                              2|
|1970-01-01T08:00:00.006+08:00|                                                              2|
|1970-01-01T08:00:00.007+08:00|                                                              2|
|1970-01-01T08:00:00.008+08:00|                                                              2|
|1970-01-01T08:00:00.009+08:00|                                                              2|
+-----------------------------+---------------------------------------------------------------+
```
