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

# Fill Null Value

When performing segment aggregation on a time series, there may be no data for a certain period of time, and the aggregated result of this segment of data is null, but this kind of null value is not conducive to data visualization and analysis, and the null value needs to be filled.

Fill null value allows the user to fill the query result with null values according to a specific method, such as taking the previous value that is not null, or linear interpolation. The query result after filling the null value can better reflect the data distribution, which is beneficial for users to perform data analysis.

In IoTDB, users can use the FILL clause to specify the fill mode when data is missing at a point in time or a time window. If the queried point's value is not null, the fill function will not work.

## Fill Methods

IoTDB supports previous, linear, and value fill methods. Following table lists the data types and supported fill methods.

| Data Type | Supported Fill Methods  |
| :-------- | :---------------------- |
| boolean   | previous, value         |
| int32     | previous, linear, value |
| int64     | previous, linear, value |
| float     | previous, linear, value |
| double    | previous, linear, value |
| text      | previous                |

> Note: Only one Fill method can be specified in a Fill statement. Null value fill is compatible with version 0.12 and previous syntax (fill((<data_type>[<fill_method>(, <before_range>, <after_range>)?])+)), but the old syntax could not specify multiple fill methods at the same time

## Single Point Fill

When data in a particular timestamp is null, the null values can be filled using single fill, as described below:

### Previous Fill

When the value in the queried timestamp is null, the value of the previous timestamp is used to fill the blank. The formalized previous method is as follows:

```sql
select <path> from <prefixPath> where time = <T> fill(previous(, <before_range>)?)
```

Detailed descriptions of all parameters are given in following table:


| Parameter name (case insensitive) | Interpretation                                               |
| :-------------------------------- | :----------------------------------------------------------- |
| path, prefixPath                  | query path; mandatory field                                  |
| T                                 | query timestamp (only one can be specified); mandatory field |
| before\_range                     | represents the valid time range of the previous method. The previous method works when there are values in the [T-before\_range, T] range. When before\_range is not specified, before\_range takes the default value default\_fill\_interval; -1 represents infinit; optional field |

Here we give an example of filling null values using the previous method. The SQL statement is as follows:

```sql
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(previous, 1s) 
```
which means:

Because the timeseries root.sgcc.wf03.wt01.temperature is null at 2017-11-01T16:37:50.000, the system uses the previous timestamp 2017-11-01T16:37:00.000 (and the timestamp is in the [2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000] time range) for fill and display.

On the [sample data](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt), the execution result of this statement is shown below:

```
+-----------------------------+-------------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|
+-----------------------------+-------------------------------+
|2017-11-01T16:37:50.000+08:00|                          21.93|
+-----------------------------+-------------------------------+
Total line number = 1
It costs 0.016s
```

It is worth noting that if there is no value in the specified valid time range, the system will not fill the null value, as shown below:

```
IoTDB> select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1s]) 
+-----------------------------+-------------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|
+-----------------------------+-------------------------------+
|2017-11-01T16:37:50.000+08:00|                           null|
+-----------------------------+-------------------------------+
Total line number = 1
It costs 0.004s
```

### Linear Fill

When the value in the queried timestamp is null, the value of the previous and the next timestamp is used to fill the blank. The formalized linear method is as follows:

```sql
select <path> from <prefixPath> where time = <T> fill(linear(, <before_range>, <after_range>)?)
```
Detailed descriptions of all parameters are given in following table:

| Parameter name (case insensitive) | Interpretation                                               |
| :-------------------------------- | :----------------------------------------------------------- |
| path, prefixPath                  | query path; mandatory field                                  |
| T                                 | query timestamp (only one can be specified); mandatory field |
| before\_range, after\_range       | represents the valid time range of the linear method. The previous method works when there are values in the [T-before\_range, T+after\_range] range. When before\_range and after\_range are not explicitly specified, default\_fill\_interval is used. -1 represents infinity; optional field |

**Note** if the timeseries has a valid value at query timestamp T, this value will be used as the linear fill value.
Otherwise, if there is no valid fill value in either range [T - before_range, T] or [T, T + after_range], linear fill method will return null.

Here we give an example of filling null values using the linear method. The SQL statement is as follows:

```sql
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(linear, 1m, 1m)
```
which means:

Because the timeseries root.sgcc.wf03.wt01.temperature is null at 2017-11-01T16:37:50.000, the system uses the previous timestamp 2017-11-01T16:37:00.000 (and the timestamp is in the [2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000] time range) and its value 21.927326, the next timestamp 2017-11-01T16:38:00.000 (and the timestamp is in the [2017-11-01T16:37:50.000, 2017-11-01T16:38:50.000] time range) and its value 25.311783 to perform linear fitting calculation: 21.927326 + (25.311783-21.927326)/60s * 50s = 24.747707

On the [sample data](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt), the execution result of this statement is shown below:

```
+-----------------------------+-------------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|
+-----------------------------+-------------------------------+
|2017-11-01T16:37:50.000+08:00|                      24.746666|
+-----------------------------+-------------------------------+
Total line number = 1
It costs 0.017s
```

### Value Fill

When the value in the queried timestamp is null, given fill value is used to fill the blank. The formalized value method is as follows:

```sql
select <path> from <prefixPath> where time = <T> fill(constant)
```
Detailed descriptions of all parameters are given in following table:

| Parameter name (case insensitive) | Interpretation                                               |
| :-------------------------------- | :----------------------------------------------------------- |
| path, prefixPath                  | query path; mandatory field                                  |
| T                                 | query timestamp (only one can be specified); mandatory field |
| constant                          | represents given fill value                                  |

**Note** if the timeseries has a valid value at query timestamp T, this value will be used as the specific fill value.

Here we give an example of filling null values using the value method. The SQL statement is as follows:

```sql
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(2.0)
```
which means:

Because the timeseries root.sgcc.wf03.wt01.temperature is null at 2017-11-01T16:37:50.000, the system uses given specific value 2.0 to fill

On the [sample data](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt), the execution result of this statement is shown below:

```
+-----------------------------+-------------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|
+-----------------------------+-------------------------------+
|2017-11-01T16:37:50.000+08:00|                            2.0|
+-----------------------------+-------------------------------+
Total line number = 1
It costs 0.007s
```

When using the ValueFill, note that IoTDB will not fill the query result if the data type is different from the input constant

example:

```sql
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill('test')
```

result:

```
+-----------------------------+-------------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|
+-----------------------------+-------------------------------+
|2017-11-01T16:37:50.000+08:00|                          null |
+-----------------------------+-------------------------------+
Total line number = 1
It costs 0.007s
```

## Downsampling with Fill
IoTDB supports null value filling of original down-frequency aggregate results. Previous, Linear, and Value fill methods can be used in any aggregation operator in a query statement, but only one fill method can be used in a query statement. In addition, the following two points should be paid attention to when using:
- GroupByFill will not fill the aggregate result of count in any case, because in IoTDB, if there is no data in a time range, the aggregate result of count is 0.
- GroupByFill will classify sum aggregation results. In IoTDB, if a query interval does not contain any data, sum aggregation result is null, and GroupByFill will fill sum. If the sum of a time range happens to be 0, GroupByFill will not fill the value

The syntax of downsampling aggregate query is similar to that of single fill query. Simple examples and usage details are listed below:

### Difference Between PREVIOUSUNTILLAST And PREVIOUS:

* PREVIOUS will fill any null value as long as there exist value is not null before it.
* PREVIOUSUNTILLAST won't fill the result whose time is after the last time of that time series.

first, we check value root.ln.wf01.wt01.temperature when time after 2017-11-07T23:49:00.

```
IoTDB> SELECT temperature FROM root.ln.wf01.wt01 where time >= 2017-11-07T23:49:00
+-----------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+
|2017-11-07T23:49:00.000+08:00|                         23.7|
|2017-11-07T23:51:00.000+08:00|                        22.24|
|2017-11-07T23:53:00.000+08:00|                        24.58|
|2017-11-07T23:54:00.000+08:00|                        22.52|
|2017-11-07T23:57:00.000+08:00|                        24.39|
|2017-11-08T00:00:00.000+08:00|                        21.07|
+-----------------------------+-----------------------------+
Total line number = 6
It costs 0.010s
```

we will find that in root.ln.wf01.wt01.temperature the first time and value are 2017-11-07T23:49:00 and 23.7 and the last time and value are 2017-11-08T00:00:00 and 21.07 respectively.

Then execute SQL statements:

```sql
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (PREVIOUSUNTILLAST);
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (PREVIOUS);
```

result:

```
IoTDB> SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (PREVIOUSUNTILLAST);
+-----------------------------+-----------------------------------------+
|                         Time|last_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-----------------------------------------+
|2017-11-07T23:50:00.000+08:00|                                     null|
|2017-11-07T23:51:00.000+08:00|                                    22.24|
|2017-11-07T23:52:00.000+08:00|                                    22.24|
|2017-11-07T23:53:00.000+08:00|                                    24.58|
|2017-11-07T23:54:00.000+08:00|                                    22.52|
|2017-11-07T23:55:00.000+08:00|                                    22.52|
|2017-11-07T23:56:00.000+08:00|                                    22.52|
|2017-11-07T23:57:00.000+08:00|                                    24.39|
|2017-11-07T23:58:00.000+08:00|                                     null|
+-----------------------------+-----------------------------------------+
Total line number = 9
It costs 0.007s

IoTDB> SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (PREVIOUS);
+-----------------------------+-----------------------------------------+
|                         Time|last_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-----------------------------------------+
|2017-11-07T23:50:00.000+08:00|                                     null|
|2017-11-07T23:51:00.000+08:00|                                    22.24|
|2017-11-07T23:52:00.000+08:00|                                    22.24|
|2017-11-07T23:53:00.000+08:00|                                    24.58|
|2017-11-07T23:54:00.000+08:00|                                    22.52|
|2017-11-07T23:55:00.000+08:00|                                    22.52|
|2017-11-07T23:56:00.000+08:00|                                    22.52|
|2017-11-07T23:57:00.000+08:00|                                    24.39|
|2017-11-07T23:58:00.000+08:00|                                    24.39|
+-----------------------------+-----------------------------------------+
Total line number = 9
It costs 0.006s
```

which means:

using PREVIOUSUNTILLAST won't fill time after 2017-11-07T23:57.

### Fill The First And Last Null Value

The fill methods of IoTDB can be divided into three categories: PreviousFill, LinearFill and ValueFill. Where PreviousFill needs to know the first not-null value before the null value, LinearFill needs to know the first not-null value before and after the null value to fill. If the first or last value in the result returned by a query statement is null, a sequence of null values may exist at the beginning or the end of the result set, which does not meet GroupByFill's expectations.

In the above example, there is no data in the first time interval [2017-11-07T23:50:00, 2017-11-07T23:51:00). The previous time interval with data is [2017-11-01T23:49:00, 2017-11-07T23:50:00). The first interval can be filled by setting PREVIOUS to fill the forward query parameter before_range as shown in the following example:

```sql
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (PREVIOUS, 1m);
```

result:
```
IoTDB> SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (PREVIOUS, 1m);
+-----------------------------+-----------------------------------------+
|                         Time|last_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-----------------------------------------+
|2017-11-07T23:50:00.000+08:00|                                     23.7|
|2017-11-07T23:51:00.000+08:00|                                    22.24|
|2017-11-07T23:52:00.000+08:00|                                    22.24|
|2017-11-07T23:53:00.000+08:00|                                    24.58|
|2017-11-07T23:54:00.000+08:00|                                    22.52|
|2017-11-07T23:55:00.000+08:00|                                    22.52|
|2017-11-07T23:56:00.000+08:00|                                     null|
|2017-11-07T23:57:00.000+08:00|                                    24.39|
|2017-11-07T23:58:00.000+08:00|                                    24.39|
+-----------------------------+-----------------------------------------+
Total line number = 9
It costs 0.005s
```

explain:

In order not to conflict with the original semantics, when before_range and after_range parameters are not set, the null value of GroupByFill is filled with the first/next not-null value of the null value. When setting before_range, after_range parameters, and the timestamp of the null record is set to T. GroupByFill takes the previous/last not-null value in [t-before_range, t+after_range) to complete the fill.

Because there is no data in the time interval [2017-11-07T23:55:00, 2017-11-07T23:57:00), Therefore, although this example fills the data of [2017-11-07T23:50:00, 2017-11-07T23:51:00) by setting before_range, due to the small before_range, [2017-11-07T23:56:00, 2017-11-07T23:57:00) data cannot be filled.

Before_range and after_range parameters can also be filled with LINEAR, as shown in the following example:

```sql
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (LINEAR, 5m, 5m);
```

result：

```
IoTDB> SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (LINEAR, 5m, 5m);
+-----------------------------+-----------------------------------------+
|                         Time|last_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-----------------------------------------+
|2017-11-07T23:50:00.000+08:00|                                22.970001|
|2017-11-07T23:51:00.000+08:00|                                    22.24|
|2017-11-07T23:52:00.000+08:00|                                    23.41|
|2017-11-07T23:53:00.000+08:00|                                    24.58|
|2017-11-07T23:54:00.000+08:00|                                    22.52|
|2017-11-07T23:55:00.000+08:00|                                23.143333|
|2017-11-07T23:56:00.000+08:00|                                23.766666|
|2017-11-07T23:57:00.000+08:00|                                    24.39|
|2017-11-07T23:58:00.000+08:00|                                23.283333|
+-----------------------------+-----------------------------------------+
Total line number = 9
It costs 0.008s
```

> Note: Set the initial down-frequency query interval to [start_time, end_time). The query result will keep the same as not setting before_range and after_range parameters. However, the query interval is changed to [start_time - before_range, end_time + after_range). Therefore, the efficiency will be affected when these two parameters are set too large. Please pay attention to them when using.

### Value Fill
The ValueFill method parses the input constant value into a string. During fill, the string constant is converted to the corresponding type of data. If the conversion succeeds, the null record will be filled; otherwise, it is not filled. Examples are as follows:

```sql
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (20.0)
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL ('temperature')
```

result:
```
IoTDB> SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL (20.0);
+-----------------------------+-----------------------------------------+
|                         Time|last_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-----------------------------------------+
|2017-11-07T23:50:00.000+08:00|                                     20.0|
|2017-11-07T23:51:00.000+08:00|                                    22.24|
|2017-11-07T23:52:00.000+08:00|                                     20.0|
|2017-11-07T23:53:00.000+08:00|                                    24.58|
|2017-11-07T23:54:00.000+08:00|                                    22.52|
|2017-11-07T23:55:00.000+08:00|                                     20.0|
|2017-11-07T23:56:00.000+08:00|                                     20.0|
|2017-11-07T23:57:00.000+08:00|                                    24.39|
|2017-11-07T23:58:00.000+08:00|                                     20.0|
+-----------------------------+-----------------------------------------+
Total line number = 9
It costs 0.007s

IoTDB> SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00),1m) FILL ('temperature');
+-----------------------------+-----------------------------------------+
|                         Time|last_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-----------------------------------------+
|2017-11-07T23:50:00.000+08:00|                                     null|
|2017-11-07T23:51:00.000+08:00|                                    22.24|
|2017-11-07T23:52:00.000+08:00|                                     null|
|2017-11-07T23:53:00.000+08:00|                                    24.58|
|2017-11-07T23:54:00.000+08:00|                                    22.52|
|2017-11-07T23:55:00.000+08:00|                                     null|
|2017-11-07T23:56:00.000+08:00|                                     null|
|2017-11-07T23:57:00.000+08:00|                                    24.39|
|2017-11-07T23:58:00.000+08:00|                                     null|
+-----------------------------+-----------------------------------------+
Total line number = 9
It costs 0.005s
```