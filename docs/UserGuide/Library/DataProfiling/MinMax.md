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
# MinMax

## Usage

This function is used to standardize the input series with min-max. Minimum value is transformed to 0; maximum value is transformed to 1.

**Name:** MINMAX

**Input Series:** Only support a single input series. The type is INT32 / INT64 / FLOAT / DOUBLE.

+ `compute`: When set to "batch", anomaly test is conducted after importing all data points; when set to "stream", it is required to provide minimum and maximum values. The default method is "batch".
+ `min`: The maximum value when method is set to "stream".
+ `max`: The minimum value when method is set to "stream".

**Output Series:** Output a single series. The type is DOUBLE.

## Examples

### Batch computing

Input series:

```
+-----------------------------+------------+
|                         Time|root.test.s1|
+-----------------------------+------------+
|1970-01-01T08:00:00.100+08:00|         0.0|
|1970-01-01T08:00:00.200+08:00|         0.0|
|1970-01-01T08:00:00.300+08:00|         1.0|
|1970-01-01T08:00:00.400+08:00|        -1.0|
|1970-01-01T08:00:00.500+08:00|         0.0|
|1970-01-01T08:00:00.600+08:00|         0.0|
|1970-01-01T08:00:00.700+08:00|        -2.0|
|1970-01-01T08:00:00.800+08:00|         2.0|
|1970-01-01T08:00:00.900+08:00|         0.0|
|1970-01-01T08:00:01.000+08:00|         0.0|
|1970-01-01T08:00:01.100+08:00|         1.0|
|1970-01-01T08:00:01.200+08:00|        -1.0|
|1970-01-01T08:00:01.300+08:00|        -1.0|
|1970-01-01T08:00:01.400+08:00|         1.0|
|1970-01-01T08:00:01.500+08:00|         0.0|
|1970-01-01T08:00:01.600+08:00|         0.0|
|1970-01-01T08:00:01.700+08:00|        10.0|
|1970-01-01T08:00:01.800+08:00|         2.0|
|1970-01-01T08:00:01.900+08:00|        -2.0|
|1970-01-01T08:00:02.000+08:00|         0.0|
+-----------------------------+------------+
```

SQL for query: 

```sql
select minmax(s1) from root.test
```

Output series:

```
+-----------------------------+--------------------+
|                         Time|minmax(root.test.s1)|
+-----------------------------+--------------------+
|1970-01-01T08:00:00.100+08:00| 0.16666666666666666|
|1970-01-01T08:00:00.200+08:00| 0.16666666666666666|
|1970-01-01T08:00:00.300+08:00|                0.25|
|1970-01-01T08:00:00.400+08:00| 0.08333333333333333|
|1970-01-01T08:00:00.500+08:00| 0.16666666666666666|
|1970-01-01T08:00:00.600+08:00| 0.16666666666666666|
|1970-01-01T08:00:00.700+08:00|                 0.0|
|1970-01-01T08:00:00.800+08:00|  0.3333333333333333|
|1970-01-01T08:00:00.900+08:00| 0.16666666666666666|
|1970-01-01T08:00:01.000+08:00| 0.16666666666666666|
|1970-01-01T08:00:01.100+08:00|                0.25|
|1970-01-01T08:00:01.200+08:00| 0.08333333333333333|
|1970-01-01T08:00:01.300+08:00| 0.08333333333333333|
|1970-01-01T08:00:01.400+08:00|                0.25|
|1970-01-01T08:00:01.500+08:00| 0.16666666666666666|
|1970-01-01T08:00:01.600+08:00| 0.16666666666666666|
|1970-01-01T08:00:01.700+08:00|                 1.0|
|1970-01-01T08:00:01.800+08:00|  0.3333333333333333|
|1970-01-01T08:00:01.900+08:00|                 0.0|
|1970-01-01T08:00:02.000+08:00| 0.16666666666666666|
+-----------------------------+--------------------+
```

