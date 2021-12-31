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
# Integral

## Usage

This function is used to calculate the integration of time series, 
which equals to the area under the curve with time as X-axis and values as Y-axis.

**Name:** INTEGRAL

**Input Series:** Only support a single input numeric series. The type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `unit`: The unit of time used when computing the integral. 
  The value should be chosen from "1S", "1s", "1m", "1H", "1d"(case-sensitive), 
  and each represents taking one millisecond / second / minute / hour / day as 1.0 while calculating the area and integral.

**Output Series:** Output a single series. The type is DOUBLE. There is only one data point in the series, whose timestamp is 0 and value is the integration.

**Note:**

+ The integral value equals to the sum of the areas of right-angled trapezoids consisting of each two adjacent points and the time-axis. 
Choosing different `unit` implies different scaling of time axis, thus making it apparent to convert the value among those results with constant coefficient.
  
+ `NaN` values in the input series will be ignored. The curve or trapezoids will skip these points and use the next valid point.

## Examples

### Default Parameters

With default parameters, this function will take one second as 1.0. 

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
select integral(s1) from root.test.d1 where time <= 2020-01-01 00:00:10
```

Output series:
```
+-----------------------------+-------------------------+
|                         Time|integral(root.test.d1.s1)|
+-----------------------------+-------------------------+
|1970-01-01T08:00:00.000+08:00|                     57.5|
+-----------------------------+-------------------------+
```

Calculation expression: 
$$\frac{1}{2}[(1+2) \times 1 + (2+5) \times 1 + (5+6) \times 1 + (6+7) \times 1 + (7+8) \times 3 + (8+10) \times 2] = 57.5$$

### Specific time unit

With time unit specified as "1m", this function will take one minute as 1.0. 

Input series is the same as above, the SQL for query is shown below:

```sql
select integral(s1, "unit"="1m") from root.test.d1 where time <= 2020-01-01 00:00:10
```

Output series:
```
+-----------------------------+-------------------------+
|                         Time|integral(root.test.d1.s1)|
+-----------------------------+-------------------------+
|1970-01-01T08:00:00.000+08:00|                    0.958|
+-----------------------------+-------------------------+
```

Calculation expression: 
$$\frac{1}{2\times 60}[(1+2) \times 1 + (2+5) \times 1 + (5+6) \times 1 + (6+7) \times 1 + (7+8) \times 3 + (8+10) \times 2] = 0.958$$
