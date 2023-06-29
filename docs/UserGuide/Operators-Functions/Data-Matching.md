<!--

​    Licensed to the Apache Software Foundation (ASF) under one
​    or more contributor license agreements.  See the NOTICE file
​    distributed with this work for additional information
​    regarding copyright ownership.  The ASF licenses this file
​    to you under the Apache License, Version 2.0 (the
​    "License"); you may not use this file except in compliance
​    with the License.  You may obtain a copy of the License at
​
​        http://www.apache.org/licenses/LICENSE-2.0
​
​    Unless required by applicable law or agreed to in writing,
​    software distributed under the License is distributed on an
​    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
​    KIND, either express or implied.  See the License for the
​    specific language governing permissions and limitations
​    under the License.

-->

# Data Matching

## Cov

### Usage

This function is used to calculate the population covariance.

**Name:** COV

**Input Series:** Only support two input series. The types are both INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is DOUBLE. There is only one data point in the series, whose timestamp is 0 and value is the population covariance.

**Note:**

+ If a row contains missing points, null points or `NaN`, it will be ignored;
+ If all rows are ignored, `NaN` will be output.


### Examples

Input series:

```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d2.s1|root.test.d2.s2|
+-----------------------------+---------------+---------------+
|2020-01-01T00:00:02.000+08:00|          100.0|          101.0|
|2020-01-01T00:00:03.000+08:00|          101.0|           null|
|2020-01-01T00:00:04.000+08:00|          102.0|          101.0|
|2020-01-01T00:00:06.000+08:00|          104.0|          102.0|
|2020-01-01T00:00:08.000+08:00|          126.0|          102.0|
|2020-01-01T00:00:10.000+08:00|          108.0|          103.0|
|2020-01-01T00:00:12.000+08:00|           null|          103.0|
|2020-01-01T00:00:14.000+08:00|          112.0|          104.0|
|2020-01-01T00:00:15.000+08:00|          113.0|           null|
|2020-01-01T00:00:16.000+08:00|          114.0|          104.0|
|2020-01-01T00:00:18.000+08:00|          116.0|          105.0|
|2020-01-01T00:00:20.000+08:00|          118.0|          105.0|
|2020-01-01T00:00:22.000+08:00|          100.0|          106.0|
|2020-01-01T00:00:26.000+08:00|          124.0|          108.0|
|2020-01-01T00:00:28.000+08:00|          126.0|          108.0|
|2020-01-01T00:00:30.000+08:00|            NaN|          108.0|
+-----------------------------+---------------+---------------+
```

SQL for query:

```sql
select cov(s1,s2) from root.test.d2
```

Output series:

```
+-----------------------------+-------------------------------------+
|                         Time|cov(root.test.d2.s1, root.test.d2.s2)|
+-----------------------------+-------------------------------------+
|1970-01-01T08:00:00.000+08:00|                   12.291666666666666|
+-----------------------------+-------------------------------------+
```

## DTW

### Usage

This function is used to calculate the DTW distance between two input series.

**Name:** DTW

**Input Series:** Only support two input series. The types are both INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is DOUBLE. There is only one data point in the series, whose timestamp is 0 and value is the DTW distance.

**Note:**

+ If a row contains missing points, null points or `NaN`, it will be ignored;
+ If all rows are ignored, `0` will be output.


### Examples

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

## Pearson

### Usage

This function is used to calculate the Pearson Correlation Coefficient.

**Name:** PEARSON

**Input Series:** Only support two input series. The types are both INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is DOUBLE. There is only one data point in the series, whose timestamp is 0 and value is the Pearson Correlation Coefficient.

**Note:**

+ If a row contains missing points, null points or `NaN`, it will be ignored;
+ If all rows are ignored, `NaN` will be output.


### Examples

Input series:

```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d2.s1|root.test.d2.s2|
+-----------------------------+---------------+---------------+
|2020-01-01T00:00:02.000+08:00|          100.0|          101.0|
|2020-01-01T00:00:03.000+08:00|          101.0|           null|
|2020-01-01T00:00:04.000+08:00|          102.0|          101.0|
|2020-01-01T00:00:06.000+08:00|          104.0|          102.0|
|2020-01-01T00:00:08.000+08:00|          126.0|          102.0|
|2020-01-01T00:00:10.000+08:00|          108.0|          103.0|
|2020-01-01T00:00:12.000+08:00|           null|          103.0|
|2020-01-01T00:00:14.000+08:00|          112.0|          104.0|
|2020-01-01T00:00:15.000+08:00|          113.0|           null|
|2020-01-01T00:00:16.000+08:00|          114.0|          104.0|
|2020-01-01T00:00:18.000+08:00|          116.0|          105.0|
|2020-01-01T00:00:20.000+08:00|          118.0|          105.0|
|2020-01-01T00:00:22.000+08:00|          100.0|          106.0|
|2020-01-01T00:00:26.000+08:00|          124.0|          108.0|
|2020-01-01T00:00:28.000+08:00|          126.0|          108.0|
|2020-01-01T00:00:30.000+08:00|            NaN|          108.0|
+-----------------------------+---------------+---------------+
```

SQL for query:

```sql
select pearson(s1,s2) from root.test.d2
```

Output series:

```
+-----------------------------+-----------------------------------------+
|                         Time|pearson(root.test.d2.s1, root.test.d2.s2)|
+-----------------------------+-----------------------------------------+
|1970-01-01T08:00:00.000+08:00|                       0.5630881927754872|
+-----------------------------+-----------------------------------------+
```

## PtnSym

### Usage

This function is used to find all symmetric subseries in the input whose degree of symmetry is less than the threshold.
The degree of symmetry is calculated by DTW.
The smaller the degree, the more symmetrical the series is.

**Name:** PATTERNSYMMETRIC

**Input Series:** Only support a single input series. The type is INT32 / INT64 / FLOAT / DOUBLE

**Parameter:**

+ `window`: The length of the symmetric subseries. It's a positive integer and the default value is 10.
+ `threshold`: The threshold of the degree of symmetry. It's non-negative. Only the subseries whose degree of symmetry is below it will be output. By default, all subseries will be output.


**Output Series:** Output a single series. The type is DOUBLE. Each data point in the output series corresponds to a symmetric subseries. The output timestamp is the starting timestamp of the subseries and the output value is the degree of symmetry.

### Example

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s4|
+-----------------------------+---------------+
|2021-01-01T12:00:00.000+08:00|            1.0|
|2021-01-01T12:00:01.000+08:00|            2.0|
|2021-01-01T12:00:02.000+08:00|            3.0|
|2021-01-01T12:00:03.000+08:00|            2.0|
|2021-01-01T12:00:04.000+08:00|            1.0|
|2021-01-01T12:00:05.000+08:00|            1.0|
|2021-01-01T12:00:06.000+08:00|            1.0|
|2021-01-01T12:00:07.000+08:00|            1.0|
|2021-01-01T12:00:08.000+08:00|            2.0|
|2021-01-01T12:00:09.000+08:00|            3.0|
|2021-01-01T12:00:10.000+08:00|            2.0|
|2021-01-01T12:00:11.000+08:00|            1.0|
+-----------------------------+---------------+
```

SQL for query:

```sql
select ptnsym(s4, 'window'='5', 'threshold'='0') from root.test.d1
```

Output series:

```
+-----------------------------+------------------------------------------------------+
|                         Time|ptnsym(root.test.d1.s4, "window"="5", "threshold"="0")|
+-----------------------------+------------------------------------------------------+
|2021-01-01T12:00:00.000+08:00|                                                   0.0|
|2021-01-01T12:00:07.000+08:00|                                                   0.0|
+-----------------------------+------------------------------------------------------+
```

## XCorr

### Usage

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

### Examples

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

### Top-K DTW

#### Usage

This function calculates the first K substring with the closest DTW distance between 
the target timeseries and the pattern timeseries by using the sliding window matching algorithm.

**Name:** TOP_K_DTW_SLIDING_WINDOW

**Input Parameters:**
+ First timeseries: target timeseries
+ Second timeseries: pattern timeseries
+ Parameter k: the number of substrings with the closest DTW distance to be calculated
+ (Optional) Parameter batchSize: The number of rows entered by the algorithm per batch. The default value is 65535

**Output Series:** Output a single timeseries containing a total of K data points, each containing the following information:
+ startTime：The start timestamp of the substring in the target timeseries
+ endTime：The end timestamp of the substring in the target timeseries
+ distance：The DTW distance between the substring and the pattern timeseries

#### Examples

First ensure that the IoTDB has written the target timeseries 
and the pattern timeseries, and then use the following SQL statement to query:

```sql
select top_k_dtw_sliding_window(s, p, 'k'='20') from root.database.device;
```

Output series:

```
+-----------------------------+----------------------------------------------------------------------------------+
|                         Time|top_k_dtw_sliding_window(root.database.device.s, root.database.device.p, "k"="20")|
+-----------------------------+----------------------------------------------------------------------------------+
|2000-03-31T14:46:40.000+08:00|               DTWPath{startTime=954485200000, endTime=954485224000, distance=0.0}|
|2000-03-31T17:33:45.000+08:00|               DTWPath{startTime=954495225000, endTime=954495250000, distance=0.0}|
|2000-03-31T20:20:51.000+08:00|               DTWPath{startTime=954505251000, endTime=954505278000, distance=0.0}|
|2000-03-31T23:07:59.000+08:00|               DTWPath{startTime=954515279000, endTime=954515301000, distance=0.0}|
|2000-04-01T01:55:02.000+08:00|               DTWPath{startTime=954525302000, endTime=954525325000, distance=0.0}|
|2000-04-01T04:42:06.000+08:00|               DTWPath{startTime=954535326000, endTime=954535350000, distance=0.0}|
|2000-04-01T07:29:11.000+08:00|               DTWPath{startTime=954545351000, endTime=954545376000, distance=0.0}|
|2000-04-01T10:16:17.000+08:00|               DTWPath{startTime=954555377000, endTime=954555403000, distance=0.0}|
|2000-04-01T13:03:24.000+08:00|               DTWPath{startTime=954565404000, endTime=954565432000, distance=0.0}|
|2000-04-01T15:50:33.000+08:00|               DTWPath{startTime=954575433000, endTime=954575461000, distance=0.0}|
|2000-04-01T18:37:42.000+08:00|               DTWPath{startTime=954585462000, endTime=954585488000, distance=0.0}|
|2000-04-01T21:24:49.000+08:00|               DTWPath{startTime=954595489000, endTime=954595516000, distance=0.0}|
|2000-04-02T00:11:57.000+08:00|               DTWPath{startTime=954605517000, endTime=954605541000, distance=0.0}|
|2000-04-02T02:59:02.000+08:00|               DTWPath{startTime=954615542000, endTime=954615570000, distance=0.0}|
|2000-04-02T05:46:11.000+08:00|               DTWPath{startTime=954625571000, endTime=954625597000, distance=0.0}|
|2000-04-02T08:33:18.000+08:00|               DTWPath{startTime=954635598000, endTime=954635623000, distance=0.0}|
|2000-04-02T11:20:24.000+08:00|               DTWPath{startTime=954645624000, endTime=954645657000, distance=0.0}|
|2000-04-02T14:07:38.000+08:00|               DTWPath{startTime=954655658000, endTime=954655682000, distance=0.0}|
|2000-04-02T16:54:43.000+08:00|               DTWPath{startTime=954665683000, endTime=954665708000, distance=0.0}|
|2000-04-02T19:41:49.000+08:00|               DTWPath{startTime=954675709000, endTime=954675736000, distance=0.0}|
+-----------------------------+----------------------------------------------------------------------------------+
```