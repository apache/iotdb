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

# DML (Data Manipulation Language)

## INSERT
### Insert Real-time Data

IoTDB provides users with a variety of ways to insert real-time data, such as directly inputting [INSERT SQL statement](../Operation%20Manual/SQL%20Reference.md) in [Client/Shell tools](../Client/Command%20Line%20Interface.md), or using [Java JDBC](../Client/Programming%20-%20JDBC.md) to perform single or batch execution of [INSERT SQL statement](../Operation%20Manual/SQL%20Reference.md).

This section mainly introduces the use of [INSERT SQL statement](../Operation%20Manual/SQL%20Reference.md) for real-time data import in the scenario.

#### Use of INSERT Statements
The [INSERT SQL statement](../Operation%20Manual/SQL%20Reference.md) statement is used to insert data into one or more specified timeseries created. For each point of data inserted, it consists of a [timestamp](../Concept/Data%20Model%20and%20Terminology.md) and a sensor acquisition value (see [Data Type](../Concept/Data%20Type.md)).

In the scenario of this section, take two timeseries `root.ln.wf02.wt02.status` and `root.ln.wf02.wt02.hardware` as an example, and their data types are BOOLEAN and TEXT, respectively.

The sample code for single column data insertion is as follows:
```
IoTDB > insert into root.ln.wf02.wt02(timestamp,status) values(1,true)
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1, "v1")
```

The above example code inserts the long integer timestamp and the value "true" into the timeseries `root.ln.wf02.wt02.status` and inserts the long integer timestamp and the value "v1" into the timeseries `root.ln.wf02.wt02.hardware`. When the execution is successful, cost time is shown to indicate that the data insertion has been completed.

> Note: In IoTDB, TEXT type data can be represented by single and double quotation marks. The insertion statement above uses double quotation marks for TEXT type data. The following example will use single quotation marks for TEXT type data.

The INSERT statement can also support the insertion of multi-column data at the same time point.  The sample code of  inserting the values of the two timeseries at the same time point '2' is as follows:

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, status, hardware) VALUES (2, false, 'v2')
```

After inserting the data, we can simply query the inserted data using the SELECT statement:

```
IoTDB > select * from root.ln.wf02 where time < 3
```

The result is shown below. The query result shows that the insertion statements of single column and multi column data are performed correctly.

```
+-----------------------------+--------------------------+------------------------+
|                         Time|root.ln.wf02.wt02.hardware|root.ln.wf02.wt02.status|
+-----------------------------+--------------------------+------------------------+
|1970-01-01T08:00:00.001+08:00|                        v1|                    true|
|1970-01-01T08:00:00.002+08:00|                        v2|                   false|
+-----------------------------+--------------------------+------------------------+
Total line number = 2
It costs 0.170s
```

## SELECT

### Time Slice Query

This chapter mainly introduces the relevant examples of time slice query using IoTDB SELECT statements. Detailed SQL syntax and usage specifications can be found in [SQL Documentation](../Operation%20Manual/SQL%20Reference.md). You can also use the [Java JDBC](../Client/Programming%20-%20JDBC.md) standard interface to execute related queries.

#### Select a Column of Data Based on a Time Interval

The SQL statement is:

```
select temperature from root.ln.wf01.wt01 where time < 2017-11-01T00:08:00.000
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the temperature sensor (temperature). The SQL statement requires that all temperature sensor values before the time point of "2017-11-01T00:08:00.000" be selected.

The execution result of this SQL statement is as follows:

```
+-----------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+
|2017-11-01T00:00:00.000+08:00|                        25.96|
|2017-11-01T00:01:00.000+08:00|                        24.36|
|2017-11-01T00:02:00.000+08:00|                        20.09|
|2017-11-01T00:03:00.000+08:00|                        20.18|
|2017-11-01T00:04:00.000+08:00|                        21.13|
|2017-11-01T00:05:00.000+08:00|                        22.72|
|2017-11-01T00:06:00.000+08:00|                        20.71|
|2017-11-01T00:07:00.000+08:00|                        21.45|
+-----------------------------+-----------------------------+
Total line number = 8
It costs 0.026s
```

#### Select Multiple Columns of Data Based on a Time Interval

The SQL statement is:

```
select status, temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000;
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires that the status and temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The execution result of this SQL statement is as follows:

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
|2017-11-01T00:08:00.000+08:00|                   false|                        22.58|
|2017-11-01T00:09:00.000+08:00|                   false|                        20.98|
|2017-11-01T00:10:00.000+08:00|                    true|                        25.52|
|2017-11-01T00:11:00.000+08:00|                   false|                        22.91|
+-----------------------------+------------------------+-----------------------------+
Total line number = 6
It costs 0.018s
```

#### Select Multiple Columns of Data for the Same Device According to Multiple Time Intervals

IoTDB supports specifying multiple time interval conditions in a query. Users can combine time interval conditions at will according to their needs. For example, the SQL statement is:

```
select status,temperature from root.ln.wf01.wt01 where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature"; the statement specifies two different time intervals, namely "2017-11-01T00:05:00.000 to 2017-11-01T00:12:00.000" and "2017-11-01T16:35:00.000 to 2017-11-01T16:37:00.000". The SQL statement requires that the values of selected timeseries satisfying any time interval be selected.

The execution result of this SQL statement is as follows:
```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
|2017-11-01T00:08:00.000+08:00|                   false|                        22.58|
|2017-11-01T00:09:00.000+08:00|                   false|                        20.98|
|2017-11-01T00:10:00.000+08:00|                    true|                        25.52|
|2017-11-01T00:11:00.000+08:00|                   false|                        22.91|
|2017-11-01T16:35:00.000+08:00|                    true|                        23.44|
|2017-11-01T16:36:00.000+08:00|                   false|                        21.98|
|2017-11-01T16:37:00.000+08:00|                   false|                        21.93|
+-----------------------------+------------------------+-----------------------------+
Total line number = 9
It costs 0.018s
```


#### Choose Multiple Columns of Data for Different Devices According to Multiple Time Intervals

The system supports the selection of data in any column in a query, i.e., the selected columns can come from different devices. For example, the SQL statement is:

```
select wf01.wt01.status,wf02.wt02.hardware from root.ln where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```
which means:

The selected timeseries are "the power supply status of ln group wf01 plant wt01 device" and "the hardware version of ln group wf02 plant wt02 device"; the statement specifies two different time intervals, namely "2017-11-01T00:05:00.000 to 2017-11-01T00:12:00.000" and "2017-11-01T16:35:00.000 to 2017-11-01T16:37:00.000". The SQL statement requires that the values of selected timeseries satisfying any time interval be selected.

The execution result of this SQL statement is as follows:

```
+-----------------------------+------------------------+--------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf02.wt02.hardware|
+-----------------------------+------------------------+--------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|                        v1|
|2017-11-01T00:07:00.000+08:00|                   false|                        v1|
|2017-11-01T00:08:00.000+08:00|                   false|                        v1|
|2017-11-01T00:09:00.000+08:00|                   false|                        v1|
|2017-11-01T00:10:00.000+08:00|                    true|                        v2|
|2017-11-01T00:11:00.000+08:00|                   false|                        v1|
|2017-11-01T16:35:00.000+08:00|                    true|                        v2|
|2017-11-01T16:36:00.000+08:00|                   false|                        v1|
|2017-11-01T16:37:00.000+08:00|                   false|                        v1|
+-----------------------------+------------------------+--------------------------+
Total line number = 9
It costs 0.014s
```

#### Order By Time Query
IoTDB supports the 'order by time' statement since 0.11, it's used to display results in descending order by time.
For example, the SQL statement is:

```sql
select * from root.ln where time > 1 order by time desc limit 10;
```
The execution result of this SQL statement is as follows:

```
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf02.wt02.hardware|root.ln.wf02.wt02.status|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
|2017-11-07T23:59:00.000+08:00|                        v1|                   false|                        21.07|                   false|
|2017-11-07T23:58:00.000+08:00|                        v1|                   false|                        22.93|                   false|
|2017-11-07T23:57:00.000+08:00|                        v2|                    true|                        24.39|                    true|
|2017-11-07T23:56:00.000+08:00|                        v2|                    true|                        24.44|                    true|
|2017-11-07T23:55:00.000+08:00|                        v2|                    true|                         25.9|                    true|
|2017-11-07T23:54:00.000+08:00|                        v1|                   false|                        22.52|                   false|
|2017-11-07T23:53:00.000+08:00|                        v2|                    true|                        24.58|                    true|
|2017-11-07T23:52:00.000+08:00|                        v1|                   false|                        20.18|                   false|
|2017-11-07T23:51:00.000+08:00|                        v1|                   false|                        22.24|                   false|
|2017-11-07T23:50:00.000+08:00|                        v2|                    true|                         23.7|                    true|
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
Total line number = 10
It costs 0.016s
```

### Time Series Generating Functions

The time series generating function takes several time series as input and outputs one time series. Unlike the aggregation function, the result set of the time series generating function has a timestamp column.

All time series generating functions can accept * as input.

IoTDB supports hybrid queries of time series generating function queries and raw data queries.

#### Mathematical Functions

Currently IoTDB supports the following mathematical functions. The behavior of these mathematical functions is consistent with the behavior of these functions in the Java Math standard library.

| Function Name | Allowed Input Series Data Types | Output Series Data Type       | Corresponding Implementation in the Java Standard Library    |
| ------------- | ------------------------------- | ----------------------------- | ------------------------------------------------------------ |
| SIN           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#sin(double)                                             |
| COS           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#cos(double)                                             |
| TAN           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#tan(double)                                             |
| ASIN          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#asin(double)                                            |
| ACOS          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#acos(double)                                            |
| ATAN          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#atan(double)                                            |
| DEGREES       | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#toDegrees(double)                                       |
| RADIANS       | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#toRadians(double)                                       |
| ABS           | INT32 / INT64 / FLOAT / DOUBLE  | Same type as the input series | Math#abs(int) / Math#abs(long) /Math#abs(float) /Math#abs(double) |
| SIGN          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#signum(double)                                          |
| CEIL          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#ceil(double)                                            |
| FLOOR         | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#floor(double)                                           |
| ROUND         | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#rint(double)                                            |
| EXP           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#exp(double)                                             |
| LN            | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#log(double)                                             |
| LOG10         | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#log10(double)                                           |
| SQRT          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#sqrt(double)                                            |

Example:

```   sql
select s1, sin(s1), cos(s1), tan(s1) from root.sg1.d1 limit 5 offset 1000;
```

Result:

```
+-----------------------------+-------------------+-------------------+--------------------+-------------------+
|                         Time|     root.sg1.d1.s1|sin(root.sg1.d1.s1)| cos(root.sg1.d1.s1)|tan(root.sg1.d1.s1)|
+-----------------------------+-------------------+-------------------+--------------------+-------------------+
|2020-12-10T17:11:49.037+08:00|7360723084922759782| 0.8133527237573284|  0.5817708713544664| 1.3980636773094157|
|2020-12-10T17:11:49.038+08:00|4377791063319964531|-0.8938962705202537|  0.4482738644511651| -1.994085181866842|
|2020-12-10T17:11:49.039+08:00|7972485567734642915| 0.9627757585308978|-0.27030138509681073|-3.5618602479083545|
|2020-12-10T17:11:49.040+08:00|2508858212791964081|-0.6073417341629443| -0.7944406950452296| 0.7644897069734913|
|2020-12-10T17:11:49.041+08:00|2817297431185141819|-0.8419358900502509| -0.5395775727782725| 1.5603611649667768|
+-----------------------------+-------------------+-------------------+--------------------+-------------------+
Total line number = 5
It costs 0.008s
```

#### String Processing Functions

Currently IoTDB supports the following string processing functions:

| Function Name   | Allowed Input Series Data Types | Required Attributes                                          | Output Series Data Type | Description                                            |
| --------------- | ------------------------------- | ------------------------------------------------------------ | ----------------------- | ------------------------------------------------------ |
| STRING_CONTAINS | TEXT                            | `s`: the sequence to search for                              | BOOLEAN                 | Determine whether `s` is in the string                 |
| STRING_MATCHES  | TEXT                            | `regex`: the regular expression to which the string is to be matched | BOOLEAN                 | Determine whether the string can be matched by `regex` |

Example：

```   sql
select s1, string_contains(s1, "s"="warn"), string_matches(s1, "regex"="[^\\s]+37229") from root.sg1.d4;
```

Result：

``` 
+-----------------------------+--------------+-------------------------------------------+------------------------------------------------------+
|                         Time|root.sg1.d4.s1|string_contains(root.sg1.d4.s1, "s"="warn")|string_matches(root.sg1.d4.s1, "regex"="[^\\s]+37229")|
+-----------------------------+--------------+-------------------------------------------+------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|    warn:-8721|                                       true|                                                 false|
|1970-01-01T08:00:00.002+08:00|  error:-37229|                                      false|                                                  true|
|1970-01-01T08:00:00.003+08:00|     warn:1731|                                       true|                                                 false|
+-----------------------------+--------------+-------------------------------------------+------------------------------------------------------+
Total line number = 3
It costs 0.007s
```

#### Selector Functions

Currently IoTDB supports the following selector functions:

| Function Name | Allowed Input Series Data Types       | Required Attributes                                          | Output Series Data Type       | Description                                                  |
| ------------- | ------------------------------------- | ------------------------------------------------------------ | ----------------------------- | ------------------------------------------------------------ |
| TOP_K         | INT32 / INT64 / FLOAT / DOUBLE / TEXT | `k`: the maximum number of selected data points, must be greater than 0 and less than or equal to 1000 | Same type as the input series | Returns `k` data points with the largest values in a time series. |
| BOTTOM_K      | INT32 / INT64 / FLOAT / DOUBLE / TEXT | `k`: the maximum number of selected data points, must be greater than 0 and less than or equal to 1000 | Same type as the input series | Returns `k` data points with the smallest values in a time series. |

Example：

```   sql
select s1, top_k(s1, "k"="2"), bottom_k(s1, "k"="2") from root.sg1.d2 where time > 2020-12-10T20:36:15.530+08:00;
```

Result：

``` 
+-----------------------------+--------------------+------------------------------+---------------------------------+
|                         Time|      root.sg1.d2.s1|top_k(root.sg1.d2.s1, "k"="2")|bottom_k(root.sg1.d2.s1, "k"="2")|
+-----------------------------+--------------------+------------------------------+---------------------------------+
|2020-12-10T20:36:15.531+08:00| 1531604122307244742|           1531604122307244742|                             null|
|2020-12-10T20:36:15.532+08:00|-7426070874923281101|                          null|                             null|
|2020-12-10T20:36:15.533+08:00|-7162825364312197604|          -7162825364312197604|                             null|
|2020-12-10T20:36:15.534+08:00|-8581625725655917595|                          null|             -8581625725655917595|
|2020-12-10T20:36:15.535+08:00|-7667364751255535391|                          null|             -7667364751255535391|
+-----------------------------+--------------------+------------------------------+---------------------------------+
Total line number = 5
It costs 0.006s
```

#### Variation Trend Calculation Functions

Currently IoTDB supports the following variation trend calculation functions:

| Function Name           | Allowed Input Series Data Types                 | Output Series Data Type       | Description                                                  |
| ----------------------- | ----------------------------------------------- | ----------------------------- | ------------------------------------------------------------ |
| TIME_DIFFERENCE         | INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | INT64                         | Calculates the difference between the time stamp of a data point and the time stamp of the previous data point. There is no corresponding output for the first data point. |
| DIFFERENCE              | INT32 / INT64 / FLOAT / DOUBLE                  | Same type as the input series | Calculates the difference between the value of a data point and the value of the previous data point. There is no corresponding output for the first data point. |
| NON_NEGATIVE_DIFFERENCE | INT32 / INT64 / FLOAT / DOUBLE                  | Same type as the input series | Calculates the absolute value of the difference between the value of a data point and the value of the previous data point. There is no corresponding output for the first data point. |
| DERIVATIVE              | INT32 / INT64 / FLOAT / DOUBLE                  | DOUBLE                        | Calculates the rate of change of a data point compared to the previous data point, the result is equals to DIFFERENCE / TIME_DIFFERENCE. There is no corresponding output for the first data point. |
| NON_NEGATIVE_DERIVATIVE | INT32 / INT64 / FLOAT / DOUBLE                  | DOUBLE                        | Calculates the absolute value of the rate of change of a data point compared to the previous data point, the result is equals to NON_NEGATIVE_DIFFERENCE / TIME_DIFFERENCE. There is no corresponding output for the first data point. |

Example:

```   sql
select s1, time_difference(s1), difference(s1), non_negative_difference(s1), derivative(s1), non_negative_derivative(s1) from root.sg1.d1 limit 5 offset 1000; 
```

Result:

``` 
+-----------------------------+-------------------+-------------------------------+--------------------------+---------------------------------------+--------------------------+---------------------------------------+
|                         Time|     root.sg1.d1.s1|time_difference(root.sg1.d1.s1)|difference(root.sg1.d1.s1)|non_negative_difference(root.sg1.d1.s1)|derivative(root.sg1.d1.s1)|non_negative_derivative(root.sg1.d1.s1)|
+-----------------------------+-------------------+-------------------------------+--------------------------+---------------------------------------+--------------------------+---------------------------------------+
|2020-12-10T17:11:49.037+08:00|7360723084922759782|                              1|      -8431715764844238876|                    8431715764844238876|    -8.4317157648442388E18|                  8.4317157648442388E18|
|2020-12-10T17:11:49.038+08:00|4377791063319964531|                              1|      -2982932021602795251|                    2982932021602795251|     -2.982932021602795E18|                   2.982932021602795E18|
|2020-12-10T17:11:49.039+08:00|7972485567734642915|                              1|       3594694504414678384|                    3594694504414678384|     3.5946945044146785E18|                  3.5946945044146785E18|
|2020-12-10T17:11:49.040+08:00|2508858212791964081|                              1|      -5463627354942678834|                    5463627354942678834|     -5.463627354942679E18|                   5.463627354942679E18|
|2020-12-10T17:11:49.041+08:00|2817297431185141819|                              1|        308439218393177738|                     308439218393177738|     3.0843921839317773E17|                  3.0843921839317773E17|
+-----------------------------+-------------------+-------------------------------+--------------------------+---------------------------------------+--------------------------+---------------------------------------+
Total line number = 5
It costs 0.014s
```

#### User Defined Timeseries Generating Functions

Please refer to [UDF (User Defined Function)](../Operation%20Manual/UDF%20User%20Defined%20Function.md).

### Aggregate Query

This section mainly introduces the related examples of aggregate query.

#### Count Points

```
select count(status) from root.ln.wf01.wt01;
```
Result:

```
+-------------------------------+
|count(root.ln.wf01.wt01.status)|
+-------------------------------+
|                          10080|
+-------------------------------+
Total line number = 1
It costs 0.016s
```

##### Aggregation By Level

**Aggregation by level statement** is used for aggregating upon specific hierarchical level of timeseries path.
For all timeseries paths, by convention, "level=0" represents *root* level. 
That is, to tally the points of any measurements under "root.ln", the level should be set to 1.

For example, there are multiple series under "root.ln.wf01", such as "root.ln.wf01.wt01.status","root.ln.wf01.wt02.status","root.ln.wf01.wt03.status".
To count the number of "status" points of all these series, use query:
```
select count(status) from root.ln.wf01.* group by level=2
```
Result:

```
+----------------------------+
|COUNT(root.ln.wf01.*.status)|
+----------------------------+
|                       10080|
+----------------------------+
Total line number = 1
It costs 0.003s
```


Suppose we add another two timeseries, "root.ln.wf01.wt01.temperature" and "root.ln.wf02.wt01.temperature".
To query the count and the sum of "temperature" under path "root.ln.*.*", 
aggregating on level=2, use following statement:

```
select count(temperature), sum(temperature) from root.ln.*.* group by level=2
```
Result：

```
+---------------------------------+---------------------------------+-------------------------------+-------------------------------+
|count(root.ln.wf02.*.temperature)|count(root.ln.wf01.*.temperature)|sum(root.ln.wf02.*.temperature)|sum(root.ln.wf01.*.temperature)|
+---------------------------------+---------------------------------+-------------------------------+-------------------------------+
|                                8|                                4|                          228.0|              91.83000183105469|
+---------------------------------+---------------------------------+-------------------------------+-------------------------------+
Total line number = 1
It costs 0.013s
```

To query the count and the sum of path "root.ln.\*.\*.temperature" aggregating on "root.ln" level,
simply set level=1

```
select count(temperature), sum(temperature) from root.ln.*.* group by level=1
```
Result：

```
+------------------------------+----------------------------+
|count(root.ln.*.*.temperature)|sum(root.ln.*.*.temperature)|
+------------------------------+----------------------------+
|                            12|           319.8300018310547|
+------------------------------+----------------------------+
Total line number = 1
It costs 0.013s
```

All supported aggregation functions are: count, sum, avg, last_value, first_value, min_time, max_time, min_value, max_value.
When using four aggregations: sum, avg, min_value and max_value, please make sure all the aggregated series have exactly the same data type.
Otherwise, it will generate a syntax error.

### Down-Frequency Aggregate Query

This section mainly introduces the related examples of down-frequency aggregation query, 
using the [GROUP BY clause](../Operation%20Manual/SQL%20Reference.md), 
which is used to partition the result set according to the user's given partitioning conditions and aggregate the partitioned result set. 
IoTDB supports partitioning result sets according to time interval and customized sliding step which should not be smaller than the time interval and defaults to equal the time interval if not set. And by default results are sorted by time in ascending order. 
You can also use the [Java JDBC](../Client/Programming%20-%20Native%20API.md) standard interface to execute related queries.

The GROUP BY statement provides users with three types of specified parameters:

* Parameter 1: The display window on the time axis
* Parameter 2: Time interval for dividing the time axis(should be positive)
* Parameter 3: Time sliding step (optional and should not be smaller than the time interval and defaults to equal the time interval if not set)

The actual meanings of the three types of parameters are shown in Figure 5.2 below. 
Among them, the parameter 3 is optional. 
There are three typical examples of frequency reduction aggregation: 
parameter 3 not specified, 
parameter 3 specified, 
and value filtering conditions specified.

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69109512-f808bc80-0ab2-11ea-9e4d-b2b2f58fb474.png">
    </center>

**Figure 5.2 The actual meanings of the three types of parameters**

#### Down-Frequency Aggregate Query without Specifying the Sliding Step Length

The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d);
```
which means:

Since the sliding step length is not specified, the GROUP BY statement by default set the sliding step the same as the time interval which is `1d`.

The fist parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-11-01T00:00:00, 2017-11-07T23:00:00).

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1d) as time interval and startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [0,1d), [1d, 2d), [2d, 3d), etc.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-11-01T00:00:00, 2017-11-07 T23:00:00]), and map these data to the previously segmented time axis (in this case there are mapped data in every 1-day period from 2017-11-01T00:00:00 to 2017-11-07T23:00:00:00).

Since there is data for each time period in the result range to be displayed, the execution result of the SQL statement is shown below:

```
+-----------------------------+-------------------------------+----------------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|max_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-------------------------------+----------------------------------------+
|2017-11-01T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-02T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-03T00:00:00.000+08:00|                           1440|                                   25.99|
|2017-11-04T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-05T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-06T00:00:00.000+08:00|                           1440|                                   25.99|
|2017-11-07T00:00:00.000+08:00|                           1380|                                    26.0|
+-----------------------------+-------------------------------+----------------------------------------+
Total line number = 7
It costs 0.024s
```

#### Down-Frequency Aggregate Query Specifying the Sliding Step Length

The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d);
```

which means:

Since the user specifies the sliding step parameter as 1d, the GROUP BY statement will move the time interval `1 day` long instead of `3 hours` as default.

That means we want to fetch all the data of 00:00:00 to 02:59:59 every day from 2017-11-01 to 2017-11-07.

The first parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-11-01T00:00:00, 2017-11-07T23:00:00).

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (3h) as time interval and the startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [2017-11-01T00:00:00, 2017-11-01T03:00:00), [2017-11-02T00:00:00, 2017-11-02T03:00:00), [2017-11-03T00:00:00, 2017-11-03T03:00:00), etc.

The third parameter of the GROUP BY statement above is the sliding step for each time interval moving.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-11-01T00:00:00, 2017-11-07T23:00:00]), and map these data to the previously segmented time axis (in this case there are mapped data in every 3-hour period for each day from 2017-11-01T00:00:00 to 2017-11-07T23:00:00:00).

Since there is data for each time period in the result range to be displayed, the execution result of the SQL statement is shown below:

```
+-----------------------------+-------------------------------+----------------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|max_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-------------------------------+----------------------------------------+
|2017-11-01T00:00:00.000+08:00|                            180|                                   25.98|
|2017-11-02T00:00:00.000+08:00|                            180|                                   25.98|
|2017-11-03T00:00:00.000+08:00|                            180|                                   25.96|
|2017-11-04T00:00:00.000+08:00|                            180|                                   25.96|
|2017-11-05T00:00:00.000+08:00|                            180|                                    26.0|
|2017-11-06T00:00:00.000+08:00|                            180|                                   25.85|
|2017-11-07T00:00:00.000+08:00|                            180|                                   25.99|
+-----------------------------+-------------------------------+----------------------------------------+
Total line number = 7
It costs 0.006s
```

#### Down-Frequency Aggregate Query by Natural Month

The SQL statement is:

```
select count(status) from root.ln.wf01.wt01 group by([2017-11-01T00:00:00, 2019-11-07T23:00:00), 1mo, 2mo);
```

which means:

Since the user specifies the sliding step parameter as `2mo`, the GROUP BY statement will move the time interval `2 months` long instead of `1 month` as default.

The first parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-11-01T00:00:00, 2019-11-07T23:00:00).

The start time is 2017-11-01T00:00:00. The sliding step will increment monthly based on the start date, and the 1st day of the month will be used as the time interval's start time.

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1mo) as time interval and the startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [2017-11-01T00:00:00, 2017-12-01T00:00:00), [2018-02-01T00:00:00, 2018-03-01T00:00:00), [2018-05-03T00:00:00, 2018-06-01T00:00:00)), etc.

The third parameter of the GROUP BY statement above is the sliding step for each time interval moving.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of (2017-11-01T00:00:00, 2019-11-07T23:00:00], and map these data to the previously segmented time axis (in this case there are mapped data of the first month in every two month period from 2017-11-01T00:00:00 to 2019-11-07T23:00:00).

The SQL execution result is:

```
+-----------------------------+-------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|
+-----------------------------+-------------------------------+
|2017-11-01T00:00:00.000+08:00|                            259|
|2018-01-01T00:00:00.000+08:00|                            250|
|2018-03-01T00:00:00.000+08:00|                            259|
|2018-05-01T00:00:00.000+08:00|                            251|
|2018-07-01T00:00:00.000+08:00|                            242|
|2018-09-01T00:00:00.000+08:00|                            225|
|2018-11-01T00:00:00.000+08:00|                            216|
|2019-01-01T00:00:00.000+08:00|                            207|
|2019-03-01T00:00:00.000+08:00|                            216|
|2019-05-01T00:00:00.000+08:00|                            207|
|2019-07-01T00:00:00.000+08:00|                            199|
|2019-09-01T00:00:00.000+08:00|                            181|
|2019-11-01T00:00:00.000+08:00|                             60|
+-----------------------------+-------------------------------+
```

The SQL statement is:

```
select count(status) from root.ln.wf01.wt01 group by([2017-10-31T00:00:00, 2019-11-07T23:00:00), 1mo, 2mo);
```

which means:

Since the user specifies the sliding step parameter as `2mo`, the GROUP BY statement will move the time interval `2 months` long instead of `1 month` as default.

The first parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-10-31T00:00:00, 2019-11-07T23:00:00).

Different from the previous example, the start time is set to 2017-10-31T00:00:00.  The sliding step will increment monthly based on the start date, and the 31st day of the month meaning the last day of the month will be used as the time interval's start time. If the start time is set to the 30th date, the sliding step will use the 30th or the last day of the month.

The start time is 2017-10-31T00:00:00. The sliding step will increment monthly based on the start time, and the 1st day of the month will be used as the time interval's start time.

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1mo) as time interval and the startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [2017-10-31T00:00:00, 2017-11-31T00:00:00), [2018-02-31T00:00:00, 2018-03-31T00:00:00), [2018-05-31T00:00:00, 2018-06-31T00:00:00), etc.

The third parameter of the GROUP BY statement above is the sliding step for each time interval moving.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-10-31T00:00:00, 2019-11-07T23:00:00) and map these data to the previously segmented time axis (in this case there are mapped data of the first month in every two month period from 2017-10-31T00:00:00 to 2019-11-07T23:00:00).

The SQL execution result is:

```
+-----------------------------+-------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|
+-----------------------------+-------------------------------+
|2017-10-31T00:00:00.000+08:00|                            251|
|2017-12-31T00:00:00.000+08:00|                            250|
|2018-02-28T00:00:00.000+08:00|                            259|
|2018-04-30T00:00:00.000+08:00|                            250|
|2018-06-30T00:00:00.000+08:00|                            242|
|2018-08-31T00:00:00.000+08:00|                            225|
|2018-10-31T00:00:00.000+08:00|                            216|
|2018-12-31T00:00:00.000+08:00|                            208|
|2019-02-28T00:00:00.000+08:00|                            216|
|2019-04-30T00:00:00.000+08:00|                            208|
|2019-06-30T00:00:00.000+08:00|                            199|
|2019-08-31T00:00:00.000+08:00|                            181|
|2019-10-31T00:00:00.000+08:00|                             69|
+-----------------------------+-------------------------------+
```

#### Left Open And Right Close Range

The SQL statement is:

```
select count(status) from root.ln.wf01.wt01 group by ((2017-11-01T00:00:00, 2017-11-07T23:00:00],1d);
```

In this sql, the time interval is left open and right close, so we won't include the value of timestamp 2017-11-01T00:00:00 and instead we will include the value of timestamp 2017-11-07T23:00:00.

We will get the result like following:

```
+-----------------------------+-------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|
+-----------------------------+-------------------------------+
|2017-11-02T00:00:00.000+08:00|                           1440|
|2017-11-03T00:00:00.000+08:00|                           1440|
|2017-11-04T00:00:00.000+08:00|                           1440|
|2017-11-05T00:00:00.000+08:00|                           1440|
|2017-11-06T00:00:00.000+08:00|                           1440|
|2017-11-07T00:00:00.000+08:00|                           1440|
|2017-11-07T23:00:00.000+08:00|                           1380|
+-----------------------------+-------------------------------+
Total line number = 7
It costs 0.004s
```


#### Down-Frequency Aggregate Query with Level Clause

Level could be defined to show count the number of points of each node at the given level in current Metadata Tree.

This could be used to query the number of points under each device.

The SQL statement is:

Get down-frequency aggregate query by level.

```
select count(status) from root.ln.wf01.wt01 group by ((2017-11-01T00:00:00, 2017-11-07T23:00:00],1d), level=1;
```
Result:

```
+-----------------------------+-------------------------+
|                         Time|COUNT(root.ln.*.*.status)|
+-----------------------------+-------------------------+
|2017-11-02T00:00:00.000+08:00|                     1440|
|2017-11-03T00:00:00.000+08:00|                     1440|
|2017-11-04T00:00:00.000+08:00|                     1440|
|2017-11-05T00:00:00.000+08:00|                     1440|
|2017-11-06T00:00:00.000+08:00|                     1440|
|2017-11-07T00:00:00.000+08:00|                     1440|
|2017-11-07T23:00:00.000+08:00|                     1380|
+-----------------------------+-------------------------+
Total line number = 7
It costs 0.006s
```

Down-frequency aggregate query with sliding step and by level.

```
select count(status) from root.ln.wf01.wt01 group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d), level=1;
```

Result:

```
+-----------------------------+-------------------------+
|                         Time|COUNT(root.ln.*.*.status)|
+-----------------------------+-------------------------+
|2017-11-01T00:00:00.000+08:00|                      180|
|2017-11-02T00:00:00.000+08:00|                      180|
|2017-11-03T00:00:00.000+08:00|                      180|
|2017-11-04T00:00:00.000+08:00|                      180|
|2017-11-05T00:00:00.000+08:00|                      180|
|2017-11-06T00:00:00.000+08:00|                      180|
|2017-11-07T00:00:00.000+08:00|                      180|
+-----------------------------+-------------------------+
Total line number = 7
It costs 0.004s
```

#### Down-Frequency Aggregate Query with Fill Clause

In group by fill, sliding step is not supported in group by clause

Now, only last_value aggregation function is supported in group by fill.

Linear fill is not supported in group by fill.


##### Difference Between PREVIOUSUNTILLAST And PREVIOUS

* PREVIOUS will fill any null value as long as there exist value is not null before it.
* PREVIOUSUNTILLAST won't fill the result whose time is after the last time of that time series.

first, we check value root.ln.wf01.wt01.temperature when time after 2017-11-07T23:50:00.

```
IoTDB> SELECT temperature FROM root.ln.wf01.wt01 where time >= 2017-11-07T23:50:00
+-----------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+
|2017-11-07T23:50:00.000+08:00|                         23.7|
|2017-11-07T23:51:00.000+08:00|                        22.24|
|2017-11-07T23:52:00.000+08:00|                        20.18|
|2017-11-07T23:53:00.000+08:00|                        24.58|
|2017-11-07T23:54:00.000+08:00|                        22.52|
|2017-11-07T23:55:00.000+08:00|                         25.9|
|2017-11-07T23:56:00.000+08:00|                        24.44|
|2017-11-07T23:57:00.000+08:00|                        24.39|
|2017-11-07T23:58:00.000+08:00|                        22.93|
|2017-11-07T23:59:00.000+08:00|                        21.07|
+-----------------------------+-----------------------------+
Total line number = 10
It costs 0.002s
```

we will find the last time and value of root.ln.wf01.wt01.temperature are 2017-11-07T23:59:00 and 21.07 respectively. 

Then execute SQL statements:

```
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-08T00:01:00),1m) FILL (float[PREVIOUSUNTILLAST]);
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([2017-11-07T23:50:00, 2017-11-08T00:01:00),1m) FILL (float[PREVIOUS]);
```

result:
```
+-----------------------------+-----------------------------------------+
|                         Time|last_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-----------------------------------------+
|2017-11-07T23:50:00.000+08:00|                                     23.7|
|2017-11-07T23:51:00.000+08:00|                                    22.24|
|2017-11-07T23:52:00.000+08:00|                                    20.18|
|2017-11-07T23:53:00.000+08:00|                                    24.58|
|2017-11-07T23:54:00.000+08:00|                                    22.52|
|2017-11-07T23:55:00.000+08:00|                                     25.9|
|2017-11-07T23:56:00.000+08:00|                                    24.44|
|2017-11-07T23:57:00.000+08:00|                                    24.39|
|2017-11-07T23:58:00.000+08:00|                                    22.93|
|2017-11-07T23:59:00.000+08:00|                                    21.07|
|2017-11-08T00:00:00.000+08:00|                                     null|
+-----------------------------+-----------------------------------------+
Total line number = 11
It costs 0.005s

+-----------------------------+-----------------------------------------+
|                         Time|last_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-----------------------------------------+
|2017-11-07T23:50:00.000+08:00|                                     23.7|
|2017-11-07T23:51:00.000+08:00|                                    22.24|
|2017-11-07T23:52:00.000+08:00|                                    20.18|
|2017-11-07T23:53:00.000+08:00|                                    24.58|
|2017-11-07T23:54:00.000+08:00|                                    22.52|
|2017-11-07T23:55:00.000+08:00|                                     25.9|
|2017-11-07T23:56:00.000+08:00|                                    24.44|
|2017-11-07T23:57:00.000+08:00|                                    24.39|
|2017-11-07T23:58:00.000+08:00|                                    22.93|
|2017-11-07T23:59:00.000+08:00|                                    21.07|
|2017-11-08T00:00:00.000+08:00|                                    21.07|
+-----------------------------+-----------------------------------------+
Total line number = 11
It costs 0.006s
```

which means:

using PREVIOUSUNTILLAST won't fill time after 2017-11-07T23:59.

### Last point Query

In scenarios when IoT devices updates data in a fast manner, users are more interested in the most recent point of IoT devices.

The Last point query is to return the most recent data point of the given timeseries in a three column format.

The SQL statement is defined as:

```
select last <Path> [COMMA <Path>]* from < PrefixPath > [COMMA < PrefixPath >]* <WhereClause>
```

which means: Query and return the last data points of timeseries prefixPath.path.

Only time filter with '>' or '>=' is supported in \<WhereClause\>. Any other filters given in the \<WhereClause\> will give an exception.

The result will be returned in a three column table format.

```
| Time | Path | Value |
```

Example 1: get the last point of root.ln.wf01.wt01.status:

```
IoTDB> select last status from root.ln.wf01.wt01
+-----------------------------+------------------------+-----+
|                         Time|              timeseries|value|
+-----------------------------+------------------------+-----+
|2017-11-07T23:59:00.000+08:00|root.ln.wf01.wt01.status|false|
+-----------------------------+------------------------+-----+
Total line number = 1
It costs 0.000s
```

Example 2: get the last status and temperature points of root.ln.wf01.wt01,
whose timestamp larger or equal to 2017-11-07T23:50:00。

```
IoTDB> select last status, temperature from root.ln.wf01.wt01 where time >= 2017-11-07T23:50:00
+-----------------------------+-----------------------------+---------+
|                         Time|                   timeseries|    value|
+-----------------------------+-----------------------------+---------+
|2017-11-07T23:59:00.000+08:00|     root.ln.wf01.wt01.status|    false|
|2017-11-07T23:59:00.000+08:00|root.ln.wf01.wt01.temperature|21.067368|
+-----------------------------+-----------------------------+---------+
Total line number = 2
It costs 0.002s
```


### Automated Fill

In the actual use of IoTDB, when doing the query operation of timeseries, situations where the value is null at some time points may appear, which will obstruct the further analysis by users. In order to better reflect the degree of data change, users expect missing values to be automatically filled. Therefore, the IoTDB system introduces the function of Automated Fill.

Automated fill function refers to filling empty values according to the user's specified method and effective time range when performing timeseries queries for single or multiple columns. If the queried point's value is not null, the fill function will not work.

> Note: In the current version, IoTDB provides users with two methods: Previous and Linear. The previous method fills blanks with previous value. The linear method fills blanks through linear fitting. And the fill function can only be used when performing point-in-time queries.

#### Fill Function

* Previous Function

When the value of the queried timestamp is null, the value of the previous timestamp is used to fill the blank. The formalized previous method is as follows (see Section 7.1.3.6 for detailed syntax):

```
select <path> from <prefixPath> where time = <T> fill(<data_type>[previous, <before_range>], …)
```

Detailed descriptions of all parameters are given in Table 3-4.

<center>**Table 3-4 Previous fill paramter list**

|Parameter name (case insensitive)|Interpretation|
|:---|:---|
|path, prefixPath|query path; mandatory field|
|T|query timestamp (only one can be specified); mandatory field|
|data\_type|the type of data used by the fill method. Optional values are int32, int64, float, double, boolean, text; optional field|
|before\_range|represents the valid time range of the previous method. The previous method works when there are values in the [T-before\_range, T] range. When before\_range is not specified, before\_range takes the default value default\_fill\_interval; -1 represents infinit; optional field|
</center>

Here we give an example of filling null values using the previous method. The SQL statement is as follows:

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1s]) 
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

* Linear Method

When the value of the queried timestamp is null, the value of the previous and the next timestamp is used to fill the blank. The formalized linear method is as follows:

```
select <path> from <prefixPath> where time = <T> fill(<data_type>[linear, <before_range>, <after_range>]…)
```
Detailed descriptions of all parameters are given in Table 3-5.

<center>**Table 3-5 Linear fill paramter list**

|Parameter name (case insensitive)|Interpretation|
|:---|:---|
|path, prefixPath|query path; mandatory field|
|T|query timestamp (only one can be specified); mandatory field|
|data_type|the type of data used by the fill method. Optional values are int32, int64, float, double, boolean, text; optional field|
|before\_range, after\_range|represents the valid time range of the linear method. The previous method works when there are values in the [T-before\_range, T+after\_range] range. When before\_range and after\_range are not explicitly specified, default\_fill\_interval is used. -1 represents infinity; optional field|
</center>

**Note** if the timeseries has a valid value at query timestamp T, this value will be used as the linear fill value.
Otherwise, if there is no valid fill value in either range [T-before_range，T] or [T, T + after_range], linear fill method will return null.

Here we give an example of filling null values using the linear method. The SQL statement is as follows:

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float [linear, 1m, 1m])
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

#### Correspondence between Data Type and Fill Method

Data types and the supported fill methods are shown in Table 3-6.

<center>**Table 3-6 Data types and the supported fill methods**

|Data Type|Supported Fill Methods|
|:---|:---|
|boolean|previous|
|int32|previous, linear|
|int64|previous, linear|
|float|previous, linear|
|double|previous, linear|
|text|previous|
</center>

When the fill method is not specified, each data type bears its own default fill methods and parameters. The corresponding relationship is shown in Table 3-7.

<center>**Table 3-7 Default fill methods and parameters for various data types**

|Data Type|Default Fill Methods and Parameters|
|:---|:---|
|boolean|previous, 600000|
|int32|previous, 600000|
|int64|previous, 600000|
|float|previous, 600000|
|double|previous, 600000|
|text|previous, 600000|
</center>

> Note: In version 0.7.0, at least one fill method should be specified in the Fill statement.

### Row and Column Control over Query Results

IoTDB provides [LIMIT/SLIMIT](../Operation%20Manual/SQL%20Reference.md) clause and [OFFSET/SOFFSET](../Operation%20Manual/SQL%20Reference.md) 
clause in order to make users have more control over query results. 
The use of LIMIT and SLIMIT clauses allows users to control the number of rows and columns of query results, 
and the use of OFFSET and SOFSET clauses allows users to set the starting position of the results for display.

Note that the LIMIT and OFFSET are not supported in group by query.

This chapter mainly introduces related examples of row and column control of query results. You can also use the [Java JDBC](../Client/Programming%20-%20JDBC.md) standard interface to execute queries.

#### Row Control over Query Results

By using LIMIT and OFFSET clauses, users control the query results in a row-related manner. We demonstrate how to use LIMIT and OFFSET clauses through the following examples.

* Example 1: basic LIMIT clause

The SQL statement is:

```
select status, temperature from root.ln.wf01.wt01 limit 10
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires the first 10 rows of the query result.

The result is shown below:

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:00:00.000+08:00|                    true|                        25.96|
|2017-11-01T00:01:00.000+08:00|                    true|                        24.36|
|2017-11-01T00:02:00.000+08:00|                   false|                        20.09|
|2017-11-01T00:03:00.000+08:00|                   false|                        20.18|
|2017-11-01T00:04:00.000+08:00|                   false|                        21.13|
|2017-11-01T00:05:00.000+08:00|                   false|                        22.72|
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
|2017-11-01T00:08:00.000+08:00|                   false|                        22.58|
|2017-11-01T00:09:00.000+08:00|                   false|                        20.98|
+-----------------------------+------------------------+-----------------------------+
Total line number = 10
It costs 0.000s
```

* Example 2: LIMIT clause with OFFSET

The SQL statement is:

```
select status, temperature from root.ln.wf01.wt01 limit 5 offset 3
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires rows 3 to 7 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:03:00.000+08:00|                   false|                        20.18|
|2017-11-01T00:04:00.000+08:00|                   false|                        21.13|
|2017-11-01T00:05:00.000+08:00|                   false|                        22.72|
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
+-----------------------------+------------------------+-----------------------------+
Total line number = 5
It costs 0.342s
```

* Example 3: LIMIT clause combined with WHERE clause

The SQL statement is:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time< 2017-11-01T00:12:00.000 limit 2 offset 3
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires rows 3 to 4 of  the status and temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" (with the first row numbered as row 0).

The result is shown below:

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:03:00.000+08:00|                   false|                        20.18|
|2017-11-01T00:04:00.000+08:00|                   false|                        21.13|
|2017-11-01T00:05:00.000+08:00|                   false|                        22.72|
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
+-----------------------------+------------------------+-----------------------------+
Total line number = 5
It costs 0.000s
```

* Example 4: LIMIT clause combined with GROUP BY clause

The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d) limit 5 offset 3
```
which means:

The SQL statement clause requires rows 3 to 7 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

```
+-----------------------------+-------------------------------+----------------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|max_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-------------------------------+----------------------------------------+
|2017-11-04T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-05T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-06T00:00:00.000+08:00|                           1440|                                   25.99|
|2017-11-07T00:00:00.000+08:00|                           1380|                                    26.0|
+-----------------------------+-------------------------------+----------------------------------------+
Total line number = 4
It costs 0.016s
```

It is worth noting that because the current FILL clause can only fill in the missing value of timeseries at a certain time point, that is to say, the execution result of FILL clause is exactly one line, so LIMIT and OFFSET are not expected to be used in combination with FILL clause, otherwise errors will be prompted. For example, executing the following SQL statement:

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) limit 10
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 401: line 1:107 mismatched input 'limit' expecting {<EOF>, SLIMIT, SOFFSET, GROUP, DISABLE, ALIGN}
```

#### Column Control over Query Results

By using SLIMIT and SOFFSET clauses, users can control the query results in a column-related manner. We will demonstrate how to use SLIMIT and SOFFSET clauses through the following examples.

* Example 1: basic SLIMIT clause

The SQL statement is:

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the first column under this device, i.e., the power supply status. The SQL statement requires the status sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The result is shown below:

```
+-----------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+
|2017-11-01T00:06:00.000+08:00|                        20.71|
|2017-11-01T00:07:00.000+08:00|                        21.45|
|2017-11-01T00:08:00.000+08:00|                        22.58|
|2017-11-01T00:09:00.000+08:00|                        20.98|
|2017-11-01T00:10:00.000+08:00|                        25.52|
|2017-11-01T00:11:00.000+08:00|                        22.91|
+-----------------------------+-----------------------------+
Total line number = 6
It costs 0.000s
```

* Example 2: SLIMIT clause with SOFFSET

The SQL statement is:

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the second column under this device, i.e., the temperature. The SQL statement requires the temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The result is shown below:

```
+-----------------------------+------------------------+
|                         Time|root.ln.wf01.wt01.status|
+-----------------------------+------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|
|2017-11-01T00:07:00.000+08:00|                   false|
|2017-11-01T00:08:00.000+08:00|                   false|
|2017-11-01T00:09:00.000+08:00|                   false|
|2017-11-01T00:10:00.000+08:00|                    true|
|2017-11-01T00:11:00.000+08:00|                   false|
+-----------------------------+------------------------+
Total line number = 6
It costs 0.003s
```

* Example 3: SLIMIT clause combined with GROUP BY clause

The SQL statement is:

```
select max_value(*) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d) slimit 1 soffset 1
```

The result is shown below:

```
+-----------------------------+-----------------------------------+
|                         Time|max_value(root.ln.wf01.wt01.status)|
+-----------------------------+-----------------------------------+
|2017-11-01T00:00:00.000+08:00|                               true|
|2017-11-02T00:00:00.000+08:00|                               true|
|2017-11-03T00:00:00.000+08:00|                               true|
|2017-11-04T00:00:00.000+08:00|                               true|
|2017-11-05T00:00:00.000+08:00|                               true|
|2017-11-06T00:00:00.000+08:00|                               true|
|2017-11-07T00:00:00.000+08:00|                               true|
+-----------------------------+-----------------------------------+
Total line number = 7
It costs 0.000s
```

* Example 4: SLIMIT clause combined with FILL clause

The SQL statement is:

```
select * from root.sgcc.wf03.wt01 where time = 2017-11-01T16:35:00 fill(float[previous, 1m]) slimit 1 soffset 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the second column under this device, i.e., the temperature.

The result is shown below:

```
+-----------------------------+--------------------------+
|                         Time|root.sgcc.wf03.wt01.status|
+-----------------------------+--------------------------+
|2017-11-01T16:35:00.000+08:00|                      true|
+-----------------------------+--------------------------+
Total line number = 1
It costs 0.007s
```

#### Row and Column Control over Query Results

In addition to row or column control over query results, IoTDB allows users to control both rows and columns of query results. Here is a complete example with both LIMIT clauses and SLIMIT clauses.

The SQL statement is:

```
select * from root.ln.wf01.wt01 limit 10 offset 100 slimit 2 soffset 0
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is columns 0 to 1 under this device (with the first column numbered as column 0). The SQL statement clause requires rows 100 to 109 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

```
+-----------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+-----------------------------+-----------------------------+------------------------+
|2017-11-01T01:40:00.000+08:00|                        21.19|                   false|
|2017-11-01T01:41:00.000+08:00|                        22.79|                   false|
|2017-11-01T01:42:00.000+08:00|                        22.98|                   false|
|2017-11-01T01:43:00.000+08:00|                        21.52|                   false|
|2017-11-01T01:44:00.000+08:00|                        23.45|                    true|
|2017-11-01T01:45:00.000+08:00|                        24.06|                    true|
|2017-11-01T01:46:00.000+08:00|                         22.6|                   false|
|2017-11-01T01:47:00.000+08:00|                        23.78|                    true|
|2017-11-01T01:48:00.000+08:00|                        24.72|                    true|
|2017-11-01T01:49:00.000+08:00|                        24.68|                    true|
+-----------------------------+-----------------------------+------------------------+
Total line number = 10
It costs 0.009s
```

### Use Alias

Since the unique data model of IoTDB, lots of additional information like device will be carried before each sensor. Sometimes, we want to query just one specific device, then these prefix information show frequently will be redundant in this situation, influencing the analysis of result set. At this time, we can use `AS` function provided by IoTDB, assign an alias to time series selected in query.  

For example：

```
select s1 as temperature, s2 as speed from root.ln.wf01.wt01;
```

The result set is：

| Time | temperature | speed |
| ---- | ----------- | ----- |
| ...  | ...         | ...   |

#### Other ResultSet Format

In addition, IoTDB supports two other results set format: 'align by device' and 'disable align'.

The 'align by device' indicates that the deviceId is considered as a column. Therefore, there are totally limited columns in the dataset. 

The SQL statement is:

```
select * from root.ln.* where time <= 2017-11-01T00:01:00 align by device
```

The result shows below:

```
+-----------------------------+-----------------+-----------+------+--------+
|                         Time|           Device|temperature|status|hardware|
+-----------------------------+-----------------+-----------+------+--------+
|2017-11-01T00:00:00.000+08:00|root.ln.wf01.wt01|      25.96|  true|    null|
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|      24.36|  true|    null|
|1970-01-01T08:00:00.001+08:00|root.ln.wf02.wt02|       null|  true|      v1|
|1970-01-01T08:00:00.002+08:00|root.ln.wf02.wt02|       null| false|      v2|
|2017-11-01T00:00:00.000+08:00|root.ln.wf02.wt02|       null|  true|      v2|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|       null|  true|      v2|
+-----------------------------+-----------------+-----------+------+--------+
Total line number = 6
It costs 0.012s
```


For more syntax description, please read [SQL Reference](../Operation%20Manual/SQL%20Reference.md).

The 'disable align' indicates that there are 2 columns for each time series in the result set. Disable Align Clause can only be used at the end of a query statement. Disable Align Clause cannot be used with Aggregation, Fill Statements, Group By or Group By Device Statements, but can with Limit Statements. The display principle of the result table is that only when the column (or row) has existing data will the column (or row) be shown, with nonexistent cells being empty.

The SQL statement is:

```
select * from root.ln.* where time <= 2017-11-01T00:01:00 disable align
```

The result shows below:

```
+-----------------------------+--------------------------+-----------------------------+------------------------+-----------------------------+-----------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf02.wt02.hardware|                         Time|root.ln.wf02.wt02.status|                         Time|root.ln.wf01.wt01.temperature|                         Time|root.ln.wf01.wt01.status|
+-----------------------------+--------------------------+-----------------------------+------------------------+-----------------------------+-----------------------------+-----------------------------+------------------------+
|1970-01-01T08:00:00.001+08:00|                        v1|1970-01-01T08:00:00.001+08:00|                    true|2017-11-01T00:00:00.000+08:00|                        25.96|2017-11-01T00:00:00.000+08:00|                    true|
|1970-01-01T08:00:00.002+08:00|                        v2|1970-01-01T08:00:00.002+08:00|                   false|2017-11-01T00:01:00.000+08:00|                        24.36|2017-11-01T00:01:00.000+08:00|                    true|
|2017-11-01T00:00:00.000+08:00|                        v2|2017-11-01T00:00:00.000+08:00|                    true|                         null|                         null|                         null|                    null|
|2017-11-01T00:01:00.000+08:00|                        v2|2017-11-01T00:01:00.000+08:00|                    true|                         null|                         null|                         null|                    null|
+-----------------------------+--------------------------+-----------------------------+------------------------+-----------------------------+-----------------------------+-----------------------------+------------------------+
Total line number = 4
It costs 0.018s
```


For more syntax description, please read [SQL Reference](../Operation%20Manual/SQL%20Reference.md).

####  Error Handling

If the parameter N/SN of LIMIT/SLIMIT exceeds the size of the result set, IoTDB returns all the results as expected. For example, the query result of the original SQL statement consists of six rows, and we select the first 100 rows through the LIMIT clause:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 100
```
The result is shown below:

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
|2017-11-01T00:08:00.000+08:00|                   false|                        22.58|
|2017-11-01T00:09:00.000+08:00|                   false|                        20.98|
|2017-11-01T00:10:00.000+08:00|                    true|                        25.52|
|2017-11-01T00:11:00.000+08:00|                   false|                        22.91|
+-----------------------------+------------------------+-----------------------------+
Total line number = 6
It costs 0.005s
```

If the parameter N/SN of LIMIT/SLIMIT clause exceeds the allowable maximum value (N/SN is of type int32), the system prompts errors. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 1234567890123456789
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 303: check metadata error: Out of range. LIMIT <N>: N should be Int32.
```

If the parameter N/SN of LIMIT/SLIMIT clause is not a positive intege, the system prompts errors. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 13.1
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 401: line 1:129 mismatched input '.' expecting {<EOF>, SLIMIT, OFFSET, SOFFSET, GROUP, DISABLE, ALIGN}
```

If the parameter OFFSET of LIMIT clause exceeds the size of the result set, IoTDB will return an empty result set. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 2 offset 6
```
The result is shown below:
```
+----+------------------------+-----------------------------+
|Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+----+------------------------+-----------------------------+
+----+------------------------+-----------------------------+
Empty set.
It costs 0.005s
```

If the parameter SOFFSET of SLIMIT clause is not smaller than the number of available timeseries, the system prompts errors. For example, executing the following SQL statement:

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 2
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 411: Meet error in query process: The value of SOFFSET (2) is equal to or exceeds the number of sequences (2) that can actually be returned.
```


## DELETE

Users can delete data that meet the deletion condition in the specified timeseries by using the [DELETE statement](../Operation%20Manual/SQL%20Reference.md). When deleting data, users can select one or more timeseries paths, prefix paths, or paths with star  to delete data within a certain time interval.

In a JAVA programming environment, you can use the [Java JDBC](../Client/Programming%20-%20JDBC.md) to execute single or batch UPDATE statements.

### Delete Single Timeseries
Taking ln Group as an example, there exists such a usage scenario:

The wf02 plant's wt02 device has many segments of errors in its power supply status before 2017-11-01 16:26:00, and the data cannot be analyzed correctly. The erroneous data affected the correlation analysis with other devices. At this point, the data before this time point needs to be deleted. The SQL statement for this operation is

```
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

In case we hope to merely delete the data before 2017-11-01 16:26:00 in the year of 2017, The SQL statement is:
```
delete from root.ln.wf02.wt02.status where time>=2017-01-01T00:00:00 and time<=2017-11-01T16:26:00;
```

IoTDB supports to delete a range of timeseries points. Users can write SQL expressions as follows to specify the delete interval:

```
delete from root.ln.wf02.wt02.status where time < 10
delete from root.ln.wf02.wt02.status where time <= 10
delete from root.ln.wf02.wt02.status where time < 20 and time > 10
delete from root.ln.wf02.wt02.status where time <= 20 and time >= 10
delete from root.ln.wf02.wt02.status where time > 20
delete from root.ln.wf02.wt02.status where time >= 20
delete from root.ln.wf02.wt02.status where time = 20
```

Please pay attention that multiple intervals connected by "OR" expression are not supported in delete statement:

```
delete from root.ln.wf02.wt02.status where time > 4 or time < 0
Msg: 303: Check metadata error: For delete statement, where clause can only contain atomic
expressions like : time > XXX, time <= XXX, or two atomic expressions connected by 'AND'
```

If no "where" clause specified in a delete statement, all the data in a timeseries will be deleted.

```
delete from root.ln.wf02.status
```


### Delete Multiple Timeseries
If both the power supply status and hardware version of the ln group wf02 plant wt02 device before 2017-11-01 16:26:00 need to be deleted, [the prefix path with broader meaning or the path with star](../Concept/Data%20Model%20and%20Terminology.md) can be used to delete the data. The SQL statement for this operation is:

```
delete from root.ln.wf02.wt02 where time <= 2017-11-01T16:26:00;
```
or

```
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```
It should be noted that when the deleted path does not exist, IoTDB will give the corresponding error prompt as shown below:

```
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
Msg: TimeSeries does not exist and its data cannot be deleted
```

## Delete Time Partition (experimental)
You may delete all data in a time partition of a storage group using the following grammar:

```
DELETE PARTITION root.ln 0,1,2
```

The `0,1,2` above is the id of the partition that is to be deleted, you can find it from the IoTDB
data folders or convert a timestamp manually to an id using `timestamp / partitionInterval
` (flooring), and the `partitionInterval` should be in your config (if time-partitioning is
supported in your version).

Please notice that this function is experimental and mainly for development, please use it with care.
