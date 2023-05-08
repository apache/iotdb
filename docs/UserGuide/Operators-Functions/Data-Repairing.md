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

# Data Repairing

## TimestampRepair

This function is used for timestamp repair.
According to the given standard time interval,
the method of minimizing the repair cost is adopted.
By fine-tuning the timestamps,
the original data with unstable timestamp interval is repaired to strictly equispaced data.
If no standard time interval is given,
this function will use the **median**, **mode** or **cluster** of the time interval to estimate the standard time interval.

**Name:** TIMESTAMPREPAIR

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `interval`: The standard time interval whose unit is millisecond. It is a positive integer. By default, it will be estimated according to the given method.
+ `method`: The method to estimate the standard time interval, which is 'median', 'mode' or 'cluster'. This parameter is only valid when `interval` is not given. By default, median will be used.

**Output Series:** Output a single series. The type is the same as the input. This series is the input after repairing.

### Examples

#### Manually Specify the Standard Time Interval

When `interval` is given, this function repairs according to the given standard time interval.

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d2.s1|
+-----------------------------+---------------+
|2021-07-01T12:00:00.000+08:00|            1.0|
|2021-07-01T12:00:10.000+08:00|            2.0|
|2021-07-01T12:00:19.000+08:00|            3.0|
|2021-07-01T12:00:30.000+08:00|            4.0|
|2021-07-01T12:00:40.000+08:00|            5.0|
|2021-07-01T12:00:50.000+08:00|            6.0|
|2021-07-01T12:01:01.000+08:00|            7.0|
|2021-07-01T12:01:11.000+08:00|            8.0|
|2021-07-01T12:01:21.000+08:00|            9.0|
|2021-07-01T12:01:31.000+08:00|           10.0|
+-----------------------------+---------------+
```

SQL for query:

```sql
select timestamprepair(s1,'interval'='10000') from root.test.d2
```

Output series:


```
+-----------------------------+----------------------------------------------------+
|                         Time|timestamprepair(root.test.d2.s1, "interval"="10000")|
+-----------------------------+----------------------------------------------------+
|2021-07-01T12:00:00.000+08:00|                                                 1.0|
|2021-07-01T12:00:10.000+08:00|                                                 2.0|
|2021-07-01T12:00:20.000+08:00|                                                 3.0|
|2021-07-01T12:00:30.000+08:00|                                                 4.0|
|2021-07-01T12:00:40.000+08:00|                                                 5.0|
|2021-07-01T12:00:50.000+08:00|                                                 6.0|
|2021-07-01T12:01:00.000+08:00|                                                 7.0|
|2021-07-01T12:01:10.000+08:00|                                                 8.0|
|2021-07-01T12:01:20.000+08:00|                                                 9.0|
|2021-07-01T12:01:30.000+08:00|                                                10.0|
+-----------------------------+----------------------------------------------------+
```

#### Automatically Estimate the Standard Time Interval

When `interval` is default, this function estimates the standard time interval.

Input series is the same as above, the SQL for query is shown below:

```sql
select timestamprepair(s1) from root.test.d2
```

Output series:

```
+-----------------------------+--------------------------------+
|                         Time|timestamprepair(root.test.d2.s1)|
+-----------------------------+--------------------------------+
|2021-07-01T12:00:00.000+08:00|                             1.0|
|2021-07-01T12:00:10.000+08:00|                             2.0|
|2021-07-01T12:00:20.000+08:00|                             3.0|
|2021-07-01T12:00:30.000+08:00|                             4.0|
|2021-07-01T12:00:40.000+08:00|                             5.0|
|2021-07-01T12:00:50.000+08:00|                             6.0|
|2021-07-01T12:01:00.000+08:00|                             7.0|
|2021-07-01T12:01:10.000+08:00|                             8.0|
|2021-07-01T12:01:20.000+08:00|                             9.0|
|2021-07-01T12:01:30.000+08:00|                            10.0|
+-----------------------------+--------------------------------+
```

## ValueFill

### Usage

This function is used to impute time series. Several methods are supported.

**Name**: ValueFill
**Input Series:** Only support a single input series. The type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `method`: {"mean", "previous", "linear", "likelihood", "AR", "MA", "SCREEN"}, default "linear".
  Method to use for imputation in series. "mean": use global mean value to fill holes; "previous": propagate last valid observation forward to next valid. "linear": simplest interpolation method; "likelihood":Maximum likelihood estimation based on the normal distribution of speed; "AR": auto regression; "MA": moving average; "SCREEN": speed constraint.

**Output Series:** Output a single series. The type is the same as the input. This series is the input after repairing.

**Note:** AR method use AR(1) model. Input value should be auto-correlated, or the function would output a single point (0, 0.0).

### Examples

#### Fill with linear

When `method` is "linear" or the default, Screen method is used to impute.

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d2.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:02.000+08:00|            NaN|
|2020-01-01T00:00:03.000+08:00|          101.0|
|2020-01-01T00:00:04.000+08:00|          102.0|
|2020-01-01T00:00:06.000+08:00|          104.0|
|2020-01-01T00:00:08.000+08:00|          126.0|
|2020-01-01T00:00:10.000+08:00|          108.0|
|2020-01-01T00:00:14.000+08:00|            NaN|
|2020-01-01T00:00:15.000+08:00|          113.0|
|2020-01-01T00:00:16.000+08:00|          114.0|
|2020-01-01T00:00:18.000+08:00|          116.0|
|2020-01-01T00:00:20.000+08:00|            NaN|
|2020-01-01T00:00:22.000+08:00|            NaN|
|2020-01-01T00:00:26.000+08:00|          124.0|
|2020-01-01T00:00:28.000+08:00|          126.0|
|2020-01-01T00:00:30.000+08:00|          128.0|
+-----------------------------+---------------+
```

SQL for query:

```sql
select valuefill(s1) from root.test.d2
```

Output series:

```
+-----------------------------+-----------------------+
|                         Time|valuefill(root.test.d2)|
+-----------------------------+-----------------------+
|2020-01-01T00:00:02.000+08:00|                    NaN|
|2020-01-01T00:00:03.000+08:00|                  101.0|
|2020-01-01T00:00:04.000+08:00|                  102.0|
|2020-01-01T00:00:06.000+08:00|                  104.0|
|2020-01-01T00:00:08.000+08:00|                  126.0|
|2020-01-01T00:00:10.000+08:00|                  108.0|
|2020-01-01T00:00:14.000+08:00|                  108.0|
|2020-01-01T00:00:15.000+08:00|                  113.0|
|2020-01-01T00:00:16.000+08:00|                  114.0|
|2020-01-01T00:00:18.000+08:00|                  116.0|
|2020-01-01T00:00:20.000+08:00|                  118.7|
|2020-01-01T00:00:22.000+08:00|                  121.3|
|2020-01-01T00:00:26.000+08:00|                  124.0|
|2020-01-01T00:00:28.000+08:00|                  126.0|
|2020-01-01T00:00:30.000+08:00|                  128.0|
+-----------------------------+-----------------------+
```

#### Previous Fill

When `method` is "previous", previous method is used.

Input series is the same as above, the SQL for query is shown below:

```sql
select valuefill(s1,"method"="previous") from root.test.d2
```

Output series:

```
+-----------------------------+-------------------------------------------+
|                         Time|valuefill(root.test.d2,"method"="previous")|
+-----------------------------+-------------------------------------------+
|2020-01-01T00:00:02.000+08:00|                                        NaN|
|2020-01-01T00:00:03.000+08:00|                                      101.0|
|2020-01-01T00:00:04.000+08:00|                                      102.0|
|2020-01-01T00:00:06.000+08:00|                                      104.0|
|2020-01-01T00:00:08.000+08:00|                                      126.0|
|2020-01-01T00:00:10.000+08:00|                                      108.0|
|2020-01-01T00:00:14.000+08:00|                                      110.5|
|2020-01-01T00:00:15.000+08:00|                                      113.0|
|2020-01-01T00:00:16.000+08:00|                                      114.0|
|2020-01-01T00:00:18.000+08:00|                                      116.0|
|2020-01-01T00:00:20.000+08:00|                                      116.0|
|2020-01-01T00:00:22.000+08:00|                                      116.0|
|2020-01-01T00:00:26.000+08:00|                                      124.0|
|2020-01-01T00:00:28.000+08:00|                                      126.0|
|2020-01-01T00:00:30.000+08:00|                                      128.0|
+-----------------------------+-------------------------------------------+
```

## ValueRepair

### Usage

This function is used to repair the value of the time series.
Currently, two methods are supported:
**Screen** is a method based on speed threshold, which makes all speeds meet the threshold requirements under the premise of minimum changes;
**LsGreedy** is a method based on speed change likelihood, which models speed changes as Gaussian distribution, and uses a greedy algorithm to maximize the likelihood.


**Name:** VALUEREPAIR

**Input Series:** Only support a single input series. The type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `method`: The method used to repair, which is 'Screen' or 'LsGreedy'. By default, Screen is used.
+ `minSpeed`: This parameter is only valid with Screen. It is the speed threshold. Speeds below it will be regarded as outliers. By default, it is the median minus 3 times of median absolute deviation.
+ `maxSpeed`: This parameter is only valid with Screen. It is the speed threshold. Speeds above it will be regarded as outliers. By default, it is the median plus 3 times of median absolute deviation.
+ `center`: This parameter is only valid with LsGreedy. It is the center of the Gaussian distribution of speed changes. By default, it is 0.
+ `sigma`: This parameter is only valid with LsGreedy. It is the standard deviation of the Gaussian distribution of speed changes. By default, it is the median absolute deviation.

**Output Series:** Output a single series. The type is the same as the input. This series is the input after repairing.

**Note:** `NaN` will be filled with linear interpolation before repairing.

### Examples

#### Repair with Screen

When `method` is 'Screen' or the default, Screen method is used.

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d2.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:02.000+08:00|          100.0|
|2020-01-01T00:00:03.000+08:00|          101.0|
|2020-01-01T00:00:04.000+08:00|          102.0|
|2020-01-01T00:00:06.000+08:00|          104.0|
|2020-01-01T00:00:08.000+08:00|          126.0|
|2020-01-01T00:00:10.000+08:00|          108.0|
|2020-01-01T00:00:14.000+08:00|          112.0|
|2020-01-01T00:00:15.000+08:00|          113.0|
|2020-01-01T00:00:16.000+08:00|          114.0|
|2020-01-01T00:00:18.000+08:00|          116.0|
|2020-01-01T00:00:20.000+08:00|          118.0|
|2020-01-01T00:00:22.000+08:00|          100.0|
|2020-01-01T00:00:26.000+08:00|          124.0|
|2020-01-01T00:00:28.000+08:00|          126.0|
|2020-01-01T00:00:30.000+08:00|            NaN|
+-----------------------------+---------------+
```

SQL for query:

```sql
select valuerepair(s1) from root.test.d2
```

Output series:

```
+-----------------------------+----------------------------+
|                         Time|valuerepair(root.test.d2.s1)|
+-----------------------------+----------------------------+
|2020-01-01T00:00:02.000+08:00|                       100.0|
|2020-01-01T00:00:03.000+08:00|                       101.0|
|2020-01-01T00:00:04.000+08:00|                       102.0|
|2020-01-01T00:00:06.000+08:00|                       104.0|
|2020-01-01T00:00:08.000+08:00|                       106.0|
|2020-01-01T00:00:10.000+08:00|                       108.0|
|2020-01-01T00:00:14.000+08:00|                       112.0|
|2020-01-01T00:00:15.000+08:00|                       113.0|
|2020-01-01T00:00:16.000+08:00|                       114.0|
|2020-01-01T00:00:18.000+08:00|                       116.0|
|2020-01-01T00:00:20.000+08:00|                       118.0|
|2020-01-01T00:00:22.000+08:00|                       120.0|
|2020-01-01T00:00:26.000+08:00|                       124.0|
|2020-01-01T00:00:28.000+08:00|                       126.0|
|2020-01-01T00:00:30.000+08:00|                       128.0|
+-----------------------------+----------------------------+
```

#### Repair with LsGreedy

When `method` is 'LsGreedy', LsGreedy method is used.

Input series is the same as above, the SQL for query is shown below:

```sql
select valuerepair(s1,'method'='LsGreedy') from root.test.d2
```

Output series:

```
+-----------------------------+-------------------------------------------------+
|                         Time|valuerepair(root.test.d2.s1, "method"="LsGreedy")|
+-----------------------------+-------------------------------------------------+
|2020-01-01T00:00:02.000+08:00|                                            100.0|
|2020-01-01T00:00:03.000+08:00|                                            101.0|
|2020-01-01T00:00:04.000+08:00|                                            102.0|
|2020-01-01T00:00:06.000+08:00|                                            104.0|
|2020-01-01T00:00:08.000+08:00|                                            106.0|
|2020-01-01T00:00:10.000+08:00|                                            108.0|
|2020-01-01T00:00:14.000+08:00|                                            112.0|
|2020-01-01T00:00:15.000+08:00|                                            113.0|
|2020-01-01T00:00:16.000+08:00|                                            114.0|
|2020-01-01T00:00:18.000+08:00|                                            116.0|
|2020-01-01T00:00:20.000+08:00|                                            118.0|
|2020-01-01T00:00:22.000+08:00|                                            120.0|
|2020-01-01T00:00:26.000+08:00|                                            124.0|
|2020-01-01T00:00:28.000+08:00|                                            126.0|
|2020-01-01T00:00:30.000+08:00|                                            128.0|
+-----------------------------+-------------------------------------------------+
```

## MasterRepair

### Usage

This function is used to clean time series with master data.

**Name**: MasterRepair
**Input Series:** Support multiple input series. The types are are in INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `omega`: The window size. It is a non-negative integer whose unit is millisecond. By default, it will be estimated according to the distances of two tuples with various time differences.
+ `eta`: The distance threshold. It is a positive number. By default, it will be estimated according to the distance distribution of tuples in windows.
+ `k`: The number of neighbors in master data. It is a positive integer. By default, it will be estimated according to the tuple dis- tance of the k-th nearest neighbor in the master data.
+ `output_column`: The repaired column to output, defaults to 1 which means output the repair result of the first column.

**Output Series:** Output a single series. The type is the same as the input. This series is the input after repairing.

### Examples

Input series:

```
+-----------------------------+------------+------------+------------+------------+------------+------------+
|                         Time|root.test.t1|root.test.t2|root.test.t3|root.test.m1|root.test.m2|root.test.m3|
+-----------------------------+------------+------------+------------+------------+------------+------------+
|2021-07-01T12:00:01.000+08:00|        1704|     1154.55|       0.195|        1704|     1154.55|       0.195|
|2021-07-01T12:00:02.000+08:00|        1702|     1152.30|       0.193|        1702|     1152.30|       0.193|
|2021-07-01T12:00:03.000+08:00|        1702|     1148.65|       0.192|        1702|     1148.65|       0.192|
|2021-07-01T12:00:04.000+08:00|        1701|     1145.20|       0.194|        1701|     1145.20|       0.194|
|2021-07-01T12:00:07.000+08:00|        1703|     1150.55|       0.195|        1703|     1150.55|       0.195|
|2021-07-01T12:00:08.000+08:00|        1694|     1151.55|       0.193|        1704|     1151.55|       0.193|
|2021-07-01T12:01:09.000+08:00|        1705|     1153.55|       0.194|        1705|     1153.55|       0.194|
|2021-07-01T12:01:10.000+08:00|        1706|     1152.30|       0.190|        1706|     1152.30|       0.190|
+-----------------------------+------------+------------+------------+------------+------------+------------+
```

SQL for query:

```sql
select MasterRepair(t1,t2,t3,m1,m2,m3) from root.test
```

Output series:


```
+-----------------------------+-------------------------------------------------------------------------------------------+
|                         Time|MasterRepair(root.test.t1,root.test.t2,root.test.t3,root.test.m1,root.test.m2,root.test.m3)|
+-----------------------------+-------------------------------------------------------------------------------------------+
|2021-07-01T12:00:01.000+08:00|                                                                                       1704|
|2021-07-01T12:00:02.000+08:00|                                                                                       1702|
|2021-07-01T12:00:03.000+08:00|                                                                                       1702|
|2021-07-01T12:00:04.000+08:00|                                                                                       1701|
|2021-07-01T12:00:07.000+08:00|                                                                                       1703|
|2021-07-01T12:00:08.000+08:00|                                                                                       1704|
|2021-07-01T12:01:09.000+08:00|                                                                                       1705|
|2021-07-01T12:01:10.000+08:00|                                                                                       1706|
+-----------------------------+-------------------------------------------------------------------------------------------+
```

## SeasonalRepair

### Usage
This function is used to repair the value of the seasonal time series via decomposition. Currently, two methods are supported: **Classical** - detect irregular fluctuations through residual component decomposed by classical decomposition, and repair them through moving average;  **Improved** - detect irregular fluctuations through residual component decomposed by improved decomposition, and repair them through moving median.

**Name:** SEASONALREPAIR

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `method`: The decomposition method used to repair, which is 'Classical' or 'Improved'. By default, classical decomposition is used.
+ `period`: It is the period of  the time series.
+ `k`: It is the range threshold of residual term, which limits the degree to which the residual term is off-center. By default, it is 9.
+ `max_iter`: It is the maximum number of iterations for the algorithm. By default, it is 10.

**Output Series:** Output a single series. The type is the same as the input. This series is the input after repairing.

**Note:** `NaN` will be filled with linear interpolation before repairing.

### Examples

#### Repair with Classical

When `method` is 'Classical' or default value, classical decomposition method is used. 

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d2.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:02.000+08:00|          100.0|
|2020-01-01T00:00:04.000+08:00|          120.0|
|2020-01-01T00:00:06.000+08:00|           80.0|
|2020-01-01T00:00:08.000+08:00|          100.5|
|2020-01-01T00:00:10.000+08:00|          119.5|
|2020-01-01T00:00:12.000+08:00|          101.0|
|2020-01-01T00:00:14.000+08:00|           99.5|
|2020-01-01T00:00:16.000+08:00|          119.0|
|2020-01-01T00:00:18.000+08:00|           80.5|
|2020-01-01T00:00:20.000+08:00|           99.0|
|2020-01-01T00:00:22.000+08:00|          121.0|
|2020-01-01T00:00:24.000+08:00|           79.5|
+-----------------------------+---------------+
```

SQL for query:

```sql
select seasonalrepair(s1,'period'=3,'k'=2) from root.test.d2
```

Output series:

```
+-----------------------------+--------------------------------------------------+
|                         Time|seasonalrepair(root.test.d2.s1, 'period'=4, 'k'=2)|
+-----------------------------+--------------------------------------------------+
|2020-01-01T00:00:02.000+08:00|                                             100.0|
|2020-01-01T00:00:04.000+08:00|                                             120.0|
|2020-01-01T00:00:06.000+08:00|                                              80.0|
|2020-01-01T00:00:08.000+08:00|                                             100.5|
|2020-01-01T00:00:10.000+08:00|                                             119.5|
|2020-01-01T00:00:12.000+08:00|                                              87.0|
|2020-01-01T00:00:14.000+08:00|                                              99.5|
|2020-01-01T00:00:16.000+08:00|                                             119.0|
|2020-01-01T00:00:18.000+08:00|                                              80.5|
|2020-01-01T00:00:20.000+08:00|                                              99.0|
|2020-01-01T00:00:22.000+08:00|                                             121.0|
|2020-01-01T00:00:24.000+08:00|                                              79.5|
+-----------------------------+--------------------------------------------------+
```

#### Repair with Improved
When `method` is 'Improved', improved decomposition method is used.

Input series is the same as above, the SQL for query is shown below:

```sql
select seasonalrepair(s1,'method'='improved','period'=3) from root.test.d2
```

Output series:

```
+-----------------------------+-------------------------------------------------------------+
|                         Time|valuerepair(root.test.d2.s1, 'method'='improved', 'period'=3)|
+-----------------------------+-------------------------------------------------------------+
|2020-01-01T00:00:02.000+08:00|                                                        100.0|
|2020-01-01T00:00:04.000+08:00|                                                        120.0|
|2020-01-01T00:00:06.000+08:00|                                                         80.0|
|2020-01-01T00:00:08.000+08:00|                                                        100.5|
|2020-01-01T00:00:10.000+08:00|                                                        119.5|
|2020-01-01T00:00:12.000+08:00|                                                         81.5|
|2020-01-01T00:00:14.000+08:00|                                                         99.5|
|2020-01-01T00:00:16.000+08:00|                                                        119.0|
|2020-01-01T00:00:18.000+08:00|                                                         80.5|
|2020-01-01T00:00:20.000+08:00|                                                         99.0|
|2020-01-01T00:00:22.000+08:00|                                                        121.0|
|2020-01-01T00:00:24.000+08:00|                                                         79.5|
+-----------------------------+-------------------------------------------------------------+
```