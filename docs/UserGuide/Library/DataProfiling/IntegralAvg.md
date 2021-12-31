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
# IntegralAvg

## Usage

This function is used to calculate the function average of time series. 
The output equals to the area divided by the time interval using the same time `unit`. 
For more information of the area under the curve, please refer to `Integral` function.

**Name:** INTEGRALAVG

**Input Series:** Only support a single input numeric series. The type is INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is DOUBLE. There is only one data point in the series, whose timestamp is 0 and value is the time-weighted average.

**Note:**

+ The time-weighted value equals to the integral value with any `unit` divided by the time interval of input series. 
The result is irrelevant to the time unit used in integral, and it's consistent with the timestamp precision of IoTDB by default.
  
+ `NaN` values in the input series will be ignored. The curve or trapezoids will skip these points and use the next valid point.

+ If the input series is empty, the output value will be 0.0, but if there is only one data point, the value will equal to the input value.

## Examples

Input series:
```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:01.000+08:00|              1|
|2020-01-01T00:00:02.000+08:00|              2|
|2020-01-01T00:00:03.000+08:00|              5|
|2020-01-01T00:00:04.000+08:00|              6|
|2020-01-01T00:00:05.000+08:00|              7|
|2020-01-01T00:00:08.000+08:00|              8|
|2020-01-01T00:00:09.000+08:00|            NaN|
|2020-01-01T00:00:10.000+08:00|             10|
+-----------------------------+---------------+
```

SQL for query:

```sql
select integralavg(s1) from root.test.d1 where time <= 2020-01-01 00:00:10
```

Output series:
```
+-----------------------------+----------------------------+
|                         Time|integralavg(root.test.d1.s1)|
+-----------------------------+----------------------------+
|1970-01-01T08:00:00.000+08:00|                        5.75|
+-----------------------------+----------------------------+
```

Calculation expression: 
$$\frac{1}{2}[(1+2) \times 1 + (2+5) \times 1 + (5+6) \times 1 + (6+7) \times 1 + (7+8) \times 3 + (8+10) \times 2] / 10 = 5.75$$
