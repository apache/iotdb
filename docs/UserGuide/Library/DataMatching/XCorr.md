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
# XCorr

## Usage

This function is used to calculate the cross correlation function of given two time series. 
For discrete time series, cross correlation is given by
$$CR(n) = \frac{1}{N} \sum_{m=1}^N S_1[m]S_2[m+n]$$
which represent the similarities between two series with different index shifts.

**Name:** XCORR

**Input Series:** Only support two input numeric series. The type is INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series with DOUBLE as datatype. 
There are $2N-1$ data points in the series, the center of which represents the cross correlation 
calculated with pre-aligned series(that is $CR(0)$ in the formula above), 
and the previous(or post) values represent those with shifting the latter series forward(or backward otherwise) 
until the two series are no longer overlapped(not included).
In short, the values of output series are given by(index starts from 1) 
$$OS[i] = CR(-N+i) = \frac{1}{N} \sum_{m=1}^{i} S_1[m]S_2[N-i+m],\ if\ i <= N$$
$$OS[i] = CR(i-N) = \frac{1}{N} \sum_{m=1}^{2N-i} S_1[i-N+m]S_2[m],\ if\ i > N$$

**Note:**

+ `null` and `NaN` values in the input series will be ignored and treated as 0.

## Examples

Input series:
```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d1.s1|root.test.d1.s2|
+-----------------------------+---------------+---------------+
|2020-01-01T00:00:01.000+08:00|           null|              6|
|2020-01-01T00:00:02.000+08:00|              2|              7|
|2020-01-01T00:00:03.000+08:00|              3|            NaN|
|2020-01-01T00:00:04.000+08:00|              4|              9|
|2020-01-01T00:00:05.000+08:00|              5|             10|
+-----------------------------+---------------+---------------+
```

SQL for query:

```sql
select xcorr(s1, s2) from root.test.d1 where time <= 2020-01-01 00:00:05
```

Output series:
```
+-----------------------------+---------------------------------------+
|                         Time|xcorr(root.test.d1.s1, root.test.d1.s2)|
+-----------------------------+---------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                    0.0|
|1970-01-01T08:00:00.002+08:00|                                    4.0|
|1970-01-01T08:00:00.003+08:00|                                    9.6|
|1970-01-01T08:00:00.004+08:00|                                   13.4|
|1970-01-01T08:00:00.005+08:00|                                   20.0|
|1970-01-01T08:00:00.006+08:00|                                   15.6|
|1970-01-01T08:00:00.007+08:00|                                    9.2|
|1970-01-01T08:00:00.008+08:00|                                   11.8|
|1970-01-01T08:00:00.009+08:00|                                    6.0|
+-----------------------------+---------------------------------------+
```
