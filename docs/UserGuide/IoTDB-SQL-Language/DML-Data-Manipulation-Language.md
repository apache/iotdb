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

IoTDB provides users with a variety of ways to insert real-time data, such as directly inputting [INSERT SQL statement](../Appendix/SQL-Reference.md) in [Client/Shell tools](../CLI/Command-Line-Interface.md), or using [Java JDBC](../API/Programming-JDBC.md) to perform single or batch execution of [INSERT SQL statement](../Appendix/SQL-Reference.md).

This section mainly introduces the use of [INSERT SQL statement](../Appendix/SQL-Reference.md) for real-time data import in the scenario.

#### Use of INSERT Statements
The [INSERT SQL statement](../Appendix/SQL-Reference.md) statement is used to insert data into one or more specified timeseries created. For each point of data inserted, it consists of a [timestamp](../Data-Concept/Data-Model-and-Terminology.md) and a sensor acquisition value (see [Data Type](../Data-Concept/Data-Type.md)).

In the scenario of this section, take two timeseries `root.ln.wf02.wt02.status` and `root.ln.wf02.wt02.hardware` as an example, and their data types are BOOLEAN and TEXT, respectively.

The sample code for single column data insertion is as follows:
```
IoTDB > insert into root.ln.wf02.wt02(timestamp,status) values(1,true)
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1, 'v1')
```

The above example code inserts the long integer timestamp and the value "true" into the timeseries `root.ln.wf02.wt02.status` and inserts the long integer timestamp and the value "v1" into the timeseries `root.ln.wf02.wt02.hardware`. When the execution is successful, cost time is shown to indicate that the data insertion has been completed.

> Note: In IoTDB, TEXT type data can be represented by single and double quotation marks. The insertion statement above uses double quotation marks for TEXT type data. The following example will use single quotation marks for TEXT type data.

The INSERT statement can also support the insertion of multi-column data at the same time point.  The sample code of  inserting the values of the two timeseries at the same time point '2' is as follows:

```sql
IoTDB > insert into root.ln.wf02.wt02(timestamp, status, hardware) VALUES (2, false, 'v2')
```

In addition, The INSERT statement support insert multi-rows at once. The sample code of inserting two rows as follows:

```sql
IoTDB > insert into root.ln.wf02.wt02(timestamp, status, hardware) VALUES (3, false, 'v3'),(4, true, 'v4')
```

After inserting the data, we can simply query the inserted data using the SELECT statement:

```sql
IoTDB > select * from root.ln.wf02 where time < 5
```

The result is shown below. The query result shows that the insertion statements of single column and multi column data are performed correctly.

```
+-----------------------------+--------------------------+------------------------+
|                         Time|root.ln.wf02.wt02.hardware|root.ln.wf02.wt02.status|
+-----------------------------+--------------------------+------------------------+
|1970-01-01T08:00:00.001+08:00|                        v1|                    true|
|1970-01-01T08:00:00.002+08:00|                        v2|                   false|
|1970-01-01T08:00:00.003+08:00|                        v3|                   false|
|1970-01-01T08:00:00.004+08:00|                        v4|                    true|
+-----------------------------+--------------------------+------------------------+
Total line number = 4
It costs 0.170s
```

## SELECT

### Time Slice Query

This chapter mainly introduces the relevant examples of time slice query using IoTDB SELECT statements. Detailed SQL syntax and usage specifications can be found in [SQL Documentation](../Appendix/SQL-Reference.md). You can also use the [Java JDBC](../API/Programming-JDBC.md) standard interface to execute related queries.

#### Select a Column of Data Based on a Time Interval

The SQL statement is:

```sql
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

```sql
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

```sql
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

```sql
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
select * from root.ln.** where time > 1 order by time desc limit 10;
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

### Arithmetic Query

#### Unary Arithmetic Operators

Supported operators: `+`, `-`

Supported input data types: `INT32`, `INT64`, `FLOAT` and `DOUBLE`

Output data type: consistent with the input data type

#### Binary Arithmetic Operators

Supported operators: `+`, `-`, `*`, `/`, `%`

Supported input data types: `INT32`, `INT64`, `FLOAT` and `DOUBLE`

Output data type: `DOUBLE`

Note: Only when the left operand and the right operand under a certain timestamp are not  `null`, the binary arithmetic operation will have an output value.

#### Example

```sql
select s1, - s1, s2, + s2, s1 + s2, s1 - s2, s1 * s2, s1 / s2, s1 % s2 from root.sg.d1
```

Result:

```
+-----------------------------+-------------+--------------+-------------+-------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|                         Time|root.sg.d1.s1|-root.sg.d1.s1|root.sg.d1.s2|root.sg.d1.s2|root.sg.d1.s1 + root.sg.d1.s2|root.sg.d1.s1 - root.sg.d1.s2|root.sg.d1.s1 * root.sg.d1.s2|root.sg.d1.s1 / root.sg.d1.s2|root.sg.d1.s1 % root.sg.d1.s2|
+-----------------------------+-------------+--------------+-------------+-------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|1970-01-01T08:00:00.001+08:00|          1.0|          -1.0|          1.0|          1.0|                          2.0|                          0.0|                          1.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.002+08:00|          2.0|          -2.0|          2.0|          2.0|                          4.0|                          0.0|                          4.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.003+08:00|          3.0|          -3.0|          3.0|          3.0|                          6.0|                          0.0|                          9.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.004+08:00|          4.0|          -4.0|          4.0|          4.0|                          8.0|                          0.0|                         16.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.005+08:00|          5.0|          -5.0|          5.0|          5.0|                         10.0|                          0.0|                         25.0|                          1.0|                          0.0|
+-----------------------------+-------------+--------------+-------------+-------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
Total line number = 5
It costs 0.014s
```

### Time Series Generating Functions

The time series generating function takes several time series as input and outputs one time series. Unlike the aggregation function, the result set of the time series generating function has a timestamp column.

All time series generating functions can accept * as input.

IoTDB supports hybrid queries of time series generating function queries and raw data queries.

#### Mathematical Functions

Currently, IoTDB supports the following mathematical functions. The behavior of these mathematical functions is consistent with the behavior of these functions in the Java Math standard library.

| Function Name | Allowed Input Series Data Types | Output Series Data Type       | Corresponding Implementation in the Java Standard Library    |
| ------------- | ------------------------------- | ----------------------------- | ------------------------------------------------------------ |
| SIN           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#sin(double)                                             |
| COS           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#cos(double)                                             |
| TAN           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#tan(double)                                             |
| ASIN          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#asin(double)                                            |
| ACOS          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#acos(double)                                            |
| ATAN          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#atan(double)                                            |
| SINH          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#sinh(double)                                            |
| COSH          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#cosh(double)                                            |
| TANH          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#tanh(double)                                            |
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

Currently, IoTDB supports the following string processing functions:

| Function Name   | Allowed Input Series Data Types | Required Attributes                                          | Output Series Data Type | Description                                            |
| --------------- | ------------------------------- | ------------------------------------------------------------ | ----------------------- | ------------------------------------------------------ |
| STRING_CONTAINS | TEXT                            | `s`: the sequence to search for                              | BOOLEAN                 | Determine whether `s` is in the string                 |
| STRING_MATCHES  | TEXT                            | `regex`: the regular expression to which the string is to be matched | BOOLEAN                 | Determine whether the string can be matched by `regex` |

Example：

```   sql
select s1, string_contains(s1, 's'='warn'), string_matches(s1, 'regex'='[^\\s]+37229') from root.sg1.d4;
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

Currently, IoTDB supports the following selector functions:

| Function Name | Allowed Input Series Data Types       | Required Attributes                                          | Output Series Data Type       | Description                                                  |
| ------------- | ------------------------------------- | ------------------------------------------------------------ | ----------------------------- | ------------------------------------------------------------ |
| TOP_K         | INT32 / INT64 / FLOAT / DOUBLE / TEXT | `k`: the maximum number of selected data points, must be greater than 0 and less than or equal to 1000 | Same type as the input series | Returns `k` data points with the largest values in a time series. |
| BOTTOM_K      | INT32 / INT64 / FLOAT / DOUBLE / TEXT | `k`: the maximum number of selected data points, must be greater than 0 and less than or equal to 1000 | Same type as the input series | Returns `k` data points with the smallest values in a time series. |

Example：

```   sql
select s1, top_k(s1, 'k'='2'), bottom_k(s1, 'k'='2') from root.sg1.d2 where time > 2020-12-10T20:36:15.530+08:00;
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

Currently, IoTDB supports the following variation trend calculation functions:

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

#### Constant Timeseries Generating Functions

The constant timeseries generating function is used to generate a timeseries in which the values of all data points are the same.

The constant timeseries generating function accepts one or more timeseries inputs, and the timestamp set of the output data points is the union of the timestamp sets of the input timeseries.

Currently, IoTDB supports the following constant timeseries generating functions:

| Function Name | Required Attributes                                          | Output Series Data Type                      | Description                                                  |
| ------------- | ------------------------------------------------------------ | -------------------------------------------- | ------------------------------------------------------------ |
| CONST         | `value`: the value of the output data point <br />`type`: the type of the output data point, it can only be INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | Determined by the required attribute  `type` | Output the user-specified constant timeseries according to the  attributes `value` and `type`. |
| PI            | None                                                         | DOUBLE                                       | Data point value: a `double` value of  `π`, the ratio of the circumference of a circle to its diameter, which is equals to `Math.PI` in the *Java Standard Library*. |
| E             | None                                                         | DOUBLE                                       | Data point value: a `double` value of  `e`, the base of the natural logarithms, which is equals to `Math.E` in the *Java Standard Library*. |

Example:

```   sql
select s1, s2, const(s1, 'value'='1024', 'type'='INT64'), pi(s2), e(s1, s2) from root.sg1.d1; 
```

Result:

```
select s1, s2, const(s1, 'value'='1024', 'type'='INT64'), pi(s2), e(s1, s2) from root.sg1.d1; 
+-----------------------------+--------------+--------------+-----------------------------------------------------+------------------+---------------------------------+
|                         Time|root.sg1.d1.s1|root.sg1.d1.s2|const(root.sg1.d1.s1, "value"="1024", "type"="INT64")|pi(root.sg1.d1.s2)|e(root.sg1.d1.s1, root.sg1.d1.s2)|
+-----------------------------+--------------+--------------+-----------------------------------------------------+------------------+---------------------------------+
|1970-01-01T08:00:00.000+08:00|           0.0|           0.0|                                                 1024| 3.141592653589793|                2.718281828459045|
|1970-01-01T08:00:00.001+08:00|           1.0|          null|                                                 1024|              null|                2.718281828459045|
|1970-01-01T08:00:00.002+08:00|           2.0|          null|                                                 1024|              null|                2.718281828459045|
|1970-01-01T08:00:00.003+08:00|          null|           3.0|                                                 null| 3.141592653589793|                2.718281828459045|
|1970-01-01T08:00:00.004+08:00|          null|           4.0|                                                 null| 3.141592653589793|                2.718281828459045|
+-----------------------------+--------------+--------------+-----------------------------------------------------+------------------+---------------------------------+
Total line number = 5
It costs 0.005s
```
#### Data Type Conversion Function
The IoTDB currently supports 6 data types, including INT32, INT64 ,FLOAT, DOUBLE, BOOLEAN, TEXT. When we query or evaluate data, we may need to convert data types, such as TEXT to INT32, or improve the accuracy of the data, such as FLOAT to DOUBLE. Therefore, IoTDB supports the use of cast functions to convert data types.

| Function Name | Required Attributes                                          | Output Series Data Type                      | Series Data Type  Description                               |
| ------------- | ------------------------------------------------------------ | -------------------------------------------- | ----------------------------------------------------------- |
| CAST          | `type`: the type of the output data point, it can only be INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | Determined by the required attribute  `type` | Converts data to the type specified by the `type` argument. |



##### Notes
1. The value of type BOOLEAN is `true`, when data is converted to BOOLEAN if INT32 and INT64 are not 0, FLOAT and DOUBLE are not 0.0, TEXT is not empty string or "false", otherwise `false`.    
2. The value of type INT32, INT64, FLOAT, DOUBLE are 1 or 1.0 and TEXT is "true", when BOOLEAN data is true, otherwise 0, 0.0 or "false".  
3. When TEXT is converted to INT32, INT64, or FLOAT, the TEXT is first converted to DOUBLE and then to the corresponding type, which may cause loss of precision. It will skip directly if the data can not be converted.

##### Syntax
Example data:
```
IoTDB> select text from root.test;
+-----------------------------+--------------+
|                         Time|root.test.text|
+-----------------------------+--------------+
|1970-01-01T08:00:00.001+08:00|           1.1|
|1970-01-01T08:00:00.002+08:00|             1|
|1970-01-01T08:00:00.003+08:00|   hello world|
|1970-01-01T08:00:00.004+08:00|         false|
+-----------------------------+--------------+
```
SQL:
```sql
select cast(text, 'type'='BOOLEAN'), cast(text, 'type'='INT32'), cast(text, 'type'='INT64'), cast(text, 'type'='FLOAT'), cast(text, 'type'='DOUBLE') from root.test;
```
Result:
```
+-----------------------------+--------------------------------------+------------------------------------+------------------------------------+------------------------------------+-------------------------------------+
|                         Time|cast(root.test.text, "type"="BOOLEAN")|cast(root.test.text, "type"="INT32")|cast(root.test.text, "type"="INT64")|cast(root.test.text, "type"="FLOAT")|cast(root.test.text, "type"="DOUBLE")|
+-----------------------------+--------------------------------------+------------------------------------+------------------------------------+------------------------------------+-------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                  true|                                   1|                                   1|                                 1.1|                                  1.1|
|1970-01-01T08:00:00.002+08:00|                                  true|                                   1|                                   1|                                 1.0|                                  1.0|
|1970-01-01T08:00:00.003+08:00|                                  true|                                null|                                null|                                null|                                 null|
|1970-01-01T08:00:00.004+08:00|                                 false|                                null|                                null|                                null|                                 null|
+-----------------------------+--------------------------------------+------------------------------------+------------------------------------+------------------------------------+-------------------------------------+
Total line number = 4
It costs 0.078s
```
#### User Defined Timeseries Generating Functions

Please refer to [UDF (User Defined Function)](../Advanced-Features/UDF-User-Defined-Function.md).

Known Implementation UDF Libraries:

+ [IoTDB-Quality](https://thulab.github.io/iotdb-quality), a UDF library about data quality, including data profiling, data quality evalution and data repairing, etc.

### Nested Expressions

IoTDB supports the execution of arbitrary nested expressions consisting of **numbers, time series, arithmetic expressions, and time series generating functions (including user-defined functions)** in the `select` clause.

#### Syntax

The following is the syntax definition of the `select` clause:

```sql
selectClause
    : SELECT resultColumn (',' resultColumn)*
    ;

resultColumn
    : expression (AS ID)?
    ;

expression
    : '(' expression ')'
    | '-' expression
    | expression ('*' | '/' | '%') expression
    | expression ('+' | '-') expression
    | functionName '(' expression (',' expression)* functionAttribute* ')'
    | timeSeriesSuffixPath
    | number
    ;
```

#### Example

SQL:

```sql
select a,
       b,
       ((a + 1) * 2 - 1) % 2 + 1.5,
       sin(a + sin(a + sin(b))),
       -(a + b) * (sin(a + b) * sin(a + b) + cos(a + b) * cos(a + b))
from root.sg1.d1;
```

Result:

```
+-----------------------------+-------------+-------------+-------------------------------------------+------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                         Time|root.sg1.d1.a|root.sg1.d1.b|((((root.sg1.d1.a + 1) * 2) - 1) % 2) + 1.5|sin(root.sg1.d1.a + sin(root.sg1.d1.a + sin(root.sg1.d1.b)))|-root.sg1.d1.a + root.sg1.d1.b * ((sin(root.sg1.d1.a + root.sg1.d1.b) * sin(root.sg1.d1.a + root.sg1.d1.b)) + (cos(root.sg1.d1.a + root.sg1.d1.b) * cos(root.sg1.d1.a + root.sg1.d1.b)))|
+-----------------------------+-------------+-------------+-------------------------------------------+------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|            0|            0|                                        2.5|                                                         0.0|                                                                                                                                                                                    -0.0|
|1970-01-01T08:00:00.001+08:00|            1|            1|                                        2.5|                                          0.9238430524420609|                                                                                                                                                                                    -2.0|
|1970-01-01T08:00:00.002+08:00|            2|            2|                                        2.5|                                          0.7903505371876317|                                                                                                                                                                                    -4.0|
+-----------------------------+-------------+-------------+-------------------------------------------+------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
Total line number = 3
It costs 0.170s
```

### Fill Null Value

In the actual use of IoTDB, when doing the query operation of timeseries, situations where the value is null at some time points may appear, which will obstruct the further analysis by users. In order to better reflect the degree of data change, users expect missing values to be filled. Therefore, the IoTDB system introduces fill methods.

Fill methods refers to filling empty values according to the user's specified method and effective time range when performing timeseries queries for single or multiple columns. If the queried point's value is not null, the fill function will not work.

#### Fill Methods

IoTDB supports previous, linear, and value fill methods. Table 3-1 lists the data types and supported fill methods.

<center>

**Table 3-1 Data types and the supported fill methods**

|Data Type|Supported Fill Methods|
|:---|:---|
|boolean|previous, value|
|int32|previous, linear, value|
|int64|previous, linear, value|
|float|previous, linear, value|
|double|previous, linear, value|
|text|previous|
</center>

> Note: Only one Fill method can be specified in a Fill statement. Null value fill is compatible with version 0.12 and previous syntax (fill((<data_type>[<fill_method>(, <before_range>, <after_range>)?])+)), but the old syntax could not specify multiple fill methods at the same time

#### Single Fill Query

When data for a particular timestamp is null, the null values can be filled using single fill, as described below:

* Previous Function

When the value of the queried timestamp is null, the value of the previous timestamp is used to fill the blank. The formalized previous method is as follows:

```sql
select <path> from <prefixPath> where time = <T> fill(previous(, <before_range>)?)
```

Detailed descriptions of all parameters are given in Table 3-2.

<center>

**Table 3-2 Previous fill paramter list**


|Parameter name (case insensitive)|Interpretation|
|:---|:---|
|path, prefixPath|query path; mandatory field|
|T|query timestamp (only one can be specified); mandatory field|
|before\_range|represents the valid time range of the previous method. The previous method works when there are values in the [T-before\_range, T] range. When before\_range is not specified, before\_range takes the default value default\_fill\_interval; -1 represents infinit; optional field|
</center>

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

* Linear Method

When the value of the queried timestamp is null, the value of the previous and the next timestamp is used to fill the blank. The formalized linear method is as follows:

```sql
select <path> from <prefixPath> where time = <T> fill(linear(, <before_range>, <after_range>)?)
```
Detailed descriptions of all parameters are given in Table 3-3.

<center>

**Table 3-3 Linear fill paramter list**

|Parameter name (case insensitive)|Interpretation|
|:---|:---|
|path, prefixPath|query path; mandatory field|
|T|query timestamp (only one can be specified); mandatory field|
|before\_range, after\_range|represents the valid time range of the linear method. The previous method works when there are values in the [T-before\_range, T+after\_range] range. When before\_range and after\_range are not explicitly specified, default\_fill\_interval is used. -1 represents infinity; optional field|
</center>

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

* Value Method

When the value of the queried timestamp is null, given fill value is used to fill the blank. The formalized value method is as follows:

```sql
select <path> from <prefixPath> where time = <T> fill(constant)
```
Detailed descriptions of all parameters are given in Table 3-4.

<center>

**Table 3-4 Specific value fill paramter list**

|Parameter name (case insensitive)|Interpretation|
|:---|:---|
|path, prefixPath|query path; mandatory field|
|T|query timestamp (only one can be specified); mandatory field|
|constant|represents given fill value|
</center>

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

### Aggregate Query

This section mainly introduces the related examples of aggregate query.

#### Count Points

```sql
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

#### Aggregation By Level

Aggregation by level statement is used to group the query result whose name is the same at the given level. Keyword `LEVEL` is used to specify the level that need to be grouped.  By convention, `level=0` represents *root* level. 

For example：there are multiple series named `status` under different storage groups， like "root.ln.wf01.wt01.status", "root.ln.wf02.wt02.status", and "root.sgcc.wf03.wt01.status". If you need to count the number of data points of the `status` sequence under different storage groups, use the following query:

```sql
select count(status)
from root.**
group by level = 1
```

Result：

```
+-------------------------+---------------------------+
|count(root.ln.*.*.status)|count(root.sgcc.*.*.status)|
+-------------------------+---------------------------+
|                    20160|                      10080|
+-------------------------+---------------------------+
Total line number = 1
It costs 0.003s
```

Similarly，if you need to count the number of data points under different devices, you can specify level = 3,

```sql
select count(status)
from root.**
group by level = 3
```

Result：

```
+---------------------------+---------------------------+
|count(root.*.*.wt01.status)|count(root.*.*.wt02.status)|
+---------------------------+---------------------------+
|                      20160|                      10080|
+---------------------------+---------------------------+
Total line number = 1
It costs 0.003s
```

Attention，the devices named `wt01` under storage groups `ln` and `sgcc` are grouped together, since they are regarded as devices with the same name. If you need to further count the number of data points in different devices under different storage groups, you can use the following query:

```sql
select count(status)
from root.**
group by level = 1, 3
```

Result：

```
+----------------------------+----------------------------+------------------------------+
|count(root.ln.*.wt01.status)|count(root.ln.*.wt02.status)|count(root.sgcc.*.wt01.status)|
+----------------------------+----------------------------+------------------------------+
|                       10080|                       10080|                         10080|
+----------------------------+----------------------------+------------------------------+
Total line number = 1
It costs 0.003s
```



Assuming that you want to query the maximum value of temperature sensor under all time series, you can use the following query statement:

```sql
select max_value(temperature)
from root.**
group by level = 0
```

Result：

```
+---------------------------------+
|max_value(root.*.*.*.temperature)|
+---------------------------------+
|                             26.0|
+---------------------------------+
Total line number = 1
It costs 0.013s
```

The above queries are for a certain sensor. In particular, **if you want to query the total data points owned by all sensors at a certain level**, you need to explicitly specify `*` is selected.

```sql
select count(*)
from root.ln.**
group by level = 2
```

Result：

```
+----------------------+----------------------+
|count(root.*.wf01.*.*)|count(root.*.wf02.*.*)|
+----------------------+----------------------+
|                 20160|                 20160|
+----------------------+----------------------+
Total line number = 1
It costs 0.013s
```

All supported aggregation functions are: count, sum, avg, last_value, first_value, min_time, max_time, min_value, max_value, extreme.
When using four aggregations: sum, avg, min_value, max_value and extreme please make sure all the aggregated series have exactly the same data type. Otherwise, it will generate a syntax error.

#### Down-Frequency Aggregate Query

This section mainly introduces the related examples of down-frequency aggregation query, 
using the [GROUP BY clause](../Appendix/SQL-Reference.md), 
which is used to partition the result set according to the user's given partitioning conditions and aggregate the partitioned result set. 
IoTDB supports partitioning result sets according to time interval and customized sliding step which should not be smaller than the time interval and defaults to equal the time interval if not set. And by default results are sorted by time in ascending order. 
You can also use the [Java JDBC](../API/Programming-JDBC.md) standard interface to execute related queries.

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

##### Down-Frequency Aggregate Query without Specifying the Sliding Step Length

The SQL statement is:

```sql
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

##### Down-Frequency Aggregate Query Specifying the Sliding Step Length

The SQL statement is:

```sql
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

##### Down-Frequency Aggregate Query by Natural Month

The SQL statement is:

```sql
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

```sql
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

##### Left Open And Right Close Range

The SQL statement is:

```sql
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

#### Down-Frequency Aggregate Query with Fill Clause
IoTDB supports null value filling of original down-frequency aggregate results. Previous, Linear, and Value fill methods can be used in any aggregation operator in a query statement, but only one fill method can be used in a query statement. In addition, the following two points should be paid attention to when using:
- GroupByFill will not fill the aggregate result of count in any case, because in IoTDB, if there is no data in a time range, the aggregate result of count is 0
- GroupByFill will classify sum aggregation results. In IoTDB, if a query interval does not contain any data, sum aggregation result is null, and GroupByFill will fill sum. If the sum of a time range happens to be 0, GroupByFill will not fill the value

The syntax of down-frequency aggregate query is similar to that of single fill query. Simple examples and usage details are listed below:

##### Difference Between PREVIOUSUNTILLAST And PREVIOUS:

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

##### Fill The First And Last Null Value

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

##### ValueFill
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

#### Down-Frequency Aggregate Query with Level Clause

Level could be defined to show count the number of points of each node at the given level in current Metadata Tree.

This could be used to query the number of points under each device.

The SQL statement is:

Get down-frequency aggregate query by level.

```sql
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

```sql
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

#### Nested Expressions query with aggregations

IoTDB supports aggregation query nested by any other expressions.

##### Example

1. Aggregation query without `GROUP BY`.

Input:

```sql
select avg(temperature),
       sin(avg(temperature)),
       avg(temperature) + 1,
       -sum(hardware),
       avg(temperature) + sum(hardware)
from root.ln.wf01.wt01;
```

Result:

```
+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+--------------------------------------------------------------------+
|avg(root.ln.wf01.wt01.temperature)|sin(avg(root.ln.wf01.wt01.temperature))|avg(root.ln.wf01.wt01.temperature) + 1|-sum(root.ln.wf01.wt01.hardware)|avg(root.ln.wf01.wt01.temperature) + sum(root.ln.wf01.wt01.hardware)|
+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+--------------------------------------------------------------------+
|                15.927999999999999|                   -0.21826546964855045|                    16.927999999999997|                         -7426.0|                                                            7441.928|
+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+--------------------------------------------------------------------+
Total line number = 1
It costs 0.009s
```

Input:

```sql
select count(a),
       count(b),
       ((count(a) + 1) * 2 - 1) % 2 + 1.5,
       -(count(a) + count(b)) * (count(a) * count(b)) + count(a) / count(b)
from root.sg;
```

Result:

```
+----------------+----------------+----------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
|count(root.sg.a)|count(root.sg.b)|((((count(root.sg.a) + 1) * 2) - 1) % 2) + 1.5|(-count(root.sg.a) + count(root.sg.b) * (count(root.sg.a) * count(root.sg.b))) + (count(root.sg.a) / count(root.sg.b))|
+----------------+----------------+----------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
|               4|               3|                                           2.5|                                                                                                    -82.66666666666667|
+----------------+----------------+----------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
Total line number = 1
It costs 0.013s
```

2. Aggregation with `GROUP BY`.

Input:

```sql
select avg(temperature),
       sin(avg(temperature)),
       avg(temperature) + 1,
       -sum(hardware),
       avg(temperature) + sum(hardware) as custom_sum
from root.ln.wf01.wt01
GROUP BY([10, 90), 10ms);
```

Result:

```
+-----------------------------+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+----------+
|                         Time|avg(root.ln.wf01.wt01.temperature)|sin(avg(root.ln.wf01.wt01.temperature))|avg(root.ln.wf01.wt01.temperature) + 1|-sum(root.ln.wf01.wt01.hardware)|custom_sum|
+-----------------------------+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+----------+
|1970-01-01T08:00:00.010+08:00|                13.987499999999999|                     0.9888207947857667|                    14.987499999999999|                         -3211.0| 3224.9875|
|1970-01-01T08:00:00.020+08:00|                              29.6|                    -0.9701057337071853|                                  30.6|                         -3720.0|    3749.6|
|1970-01-01T08:00:00.030+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.040+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.050+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.060+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.070+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.080+08:00|                              null|                                   null|                                  null|                            null|      null|
+-----------------------------+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+----------+
Total line number = 8
It costs 0.012s
```

##### Note

> Automated fill (`FILL`) and grouped by level (`GROUP BY LEVEL`) are not supported in an aggregation query with expression nested. They may be supported in future versions.
>
> The aggregation expression must be the lowest level input of one expression tree. Any kind expressions except timeseries are not valid as aggregation function parameters。
>
> In a word, the following queries are not valid.
> ```sql
> SELECT avg(s1+1) FROM root.sg.d1; -- The aggregation function has expression parameters.
> SELECT avg(s1) + avg(s2) FROM root.sg.* GROUP BY LEVEL=1; -- Grouped by level
> SELECT avg(s1) + avg(s2) FROM root.sg.d1 GROUP BY([0, 10000), 1s) FILL(previous); -- Automated fill
> ```

### Last point Query

In scenarios when IoT devices updates data in a fast manner, users are more interested in the most recent point of IoT devices.

The Last point query is to return the most recent data point of the given timeseries in a three column format.

The SQL statement is defined as:

```sql
select last <Path> [COMMA <Path>]* from < PrefixPath > [COMMA < PrefixPath >]* <WhereClause>
```

which means: Query and return the last data points of timeseries prefixPath.path.

Only time filter with '>' or '>=' is supported in \<WhereClause\>. Any other filters given in the \<WhereClause\> will give an exception.

The result will be returned in a four column table format.

```
| Time | timeseries | value | dataType |
```

**Note:** The `value` colum will always return the value as `string` and thus also has `TSDataType.TEXT`. Therefore the colum `dataType` is returned also which contains the _real_ type how the value should be interpreted.

Example 1: get the last point of root.ln.wf01.wt01.status:

```
IoTDB> select last status from root.ln.wf01.wt01
+-----------------------------+------------------------+-----+--------+
|                         Time|              timeseries|value|dataType|
+-----------------------------+------------------------+-----+--------+
|2017-11-07T23:59:00.000+08:00|root.ln.wf01.wt01.status|false| BOOLEAN|
+-----------------------------+------------------------+-----+--------+
Total line number = 1
It costs 0.000s
```

Example 2: get the last status and temperature points of root.ln.wf01.wt01,
whose timestamp larger or equal to 2017-11-07T23:50:00。

```
IoTDB> select last status, temperature from root.ln.wf01.wt01 where time >= 2017-11-07T23:50:00
+-----------------------------+-----------------------------+---------+--------+
|                         Time|                   timeseries|    value|dataType|
+-----------------------------+-----------------------------+---------+--------+
|2017-11-07T23:59:00.000+08:00|     root.ln.wf01.wt01.status|    false| BOOLEAN|
|2017-11-07T23:59:00.000+08:00|root.ln.wf01.wt01.temperature|21.067368|  DOUBLE|
+-----------------------------+-----------------------------+---------+--------+
Total line number = 2
It costs 0.002s
```

### Fuzzy query

Fuzzy query is divided into Like statement and Regexp statement, both of which can support fuzzy matching of TEXT type data.

Like statement:

Example 1: Query data containing `'cc'` in `value` under `root.sg.device`. 
The percentage (`%`) wildcard matches any string of zero or more characters.


```
IoTDB> select * from root.sg.device where value like '%cc%'
+-----------------------------+--------------------+
|                         Time|root.sg.device.value|
+-----------------------------+--------------------+
|2017-11-07T23:59:00.000+08:00|            aabbccdd| 
|2017-11-07T23:59:00.000+08:00|                  cc|
+-----------------------------+--------------------+
Total line number = 2
It costs 0.002s
```

Example 2: Query data that consists of 3 characters and the second character is `'b'` in `value` under `root.sg.device`.
The underscore (`_`) wildcard matches any single character.

```
IoTDB> select * from root.sg.device where value like '_b_'
+-----------------------------+--------------------+
|                         Time|root.sg.device.value|
+-----------------------------+--------------------+
|2017-11-07T23:59:00.000+08:00|                 abc| 
+-----------------------------+--------------------+
Total line number = 1
It costs 0.002s
```

Regexp statement：

The filter conditions that need to be passed in are regular expressions in the Java standard library style

Example 1: Query a string composed of 26 English characters for the value under root.sg.device

```
IoTDB> select * from root.sg.device where value regexp '^[A-Za-z]+$'
+-----------------------------+--------------------+
|                         Time|root.sg.device.value|
+-----------------------------+--------------------+
|2017-11-07T23:59:00.000+08:00|            aabbccdd| 
|2017-11-07T23:59:00.000+08:00|                  cc|
+-----------------------------+--------------------+
Total line number = 2
It costs 0.002s
```

Example 2: Query root.sg.device where the value value is a string composed of 26 lowercase English characters and the time is greater than 100

```
IoTDB> select * from root.sg.device where value regexp '^[a-z]+$' and time > 100
+-----------------------------+--------------------+
|                         Time|root.sg.device.value|
+-----------------------------+--------------------+
|2017-11-07T23:59:00.000+08:00|            aabbccdd| 
|2017-11-07T23:59:00.000+08:00|                  cc|
+-----------------------------+--------------------+
Total line number = 2
It costs 0.002s
```

Examples of common regular matching:

```
All characters with a length of 3-20: ^.{3,20}$
Uppercase english characters: ^[A-Z]+$
Numbers and English characters: ^[A-Za-z0-9]+$
Beginning with a: ^a.*
```

For more syntax description, please read [SQL Reference](../Appendix/SQL-Reference.md).

### Alias

Since the unique data model of IoTDB, lots of additional information like device will be carried before each sensor. Sometimes, we want to query just one specific device, then these prefix information show frequently will be redundant in this situation, influencing the analysis of result set. At this time, we can use `AS` function provided by IoTDB, assign an alias to time series selected in query.  

For example：

```sql
select s1 as temperature, s2 as speed from root.ln.wf01.wt01;
```

The result set is：

| Time | temperature | speed |
| ---- | ----------- | ----- |
| ...  | ...         | ...   |

### Row and Column Control over Query Results

IoTDB provides [LIMIT/SLIMIT](../Appendix/SQL-Reference.md) clause and [OFFSET/SOFFSET](../Appendix/SQL-Reference.md) 
clause in order to make users have more control over query results. 
The use of LIMIT and SLIMIT clauses allows users to control the number of rows and columns of query results, 
and the use of OFFSET and SOFSET clauses allows users to set the starting position of the results for display.

Note that the LIMIT and OFFSET are not supported in group by query.

This chapter mainly introduces related examples of row and column control of query results. You can also use the [Java JDBC](../API/Programming-JDBC.md) standard interface to execute queries.

#### Row Control over Query Results

By using LIMIT and OFFSET clauses, users control the query results in a row-related manner. We demonstrate how to use LIMIT and OFFSET clauses through the following examples.

* Example 1: basic LIMIT clause

The SQL statement is:

```sql
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

```sql
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

```sql
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

```sql
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

```sql
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(previous, 1m) limit 10
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 401: line 1:107 mismatched input 'limit' expecting {<EOF>, ';'}
```

#### Column Control over Query Results

By using SLIMIT and SOFFSET clauses, users can control the query results in a column-related manner. We will demonstrate how to use SLIMIT and SOFFSET clauses through the following examples.

* Example 1: basic SLIMIT clause

The SQL statement is:

```sql
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

```sql
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

```sql
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

```sql
select * from root.sgcc.wf03.wt01 where time = 2017-11-01T16:35:00 fill(previous, 1m) slimit 1 soffset 1
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

```sql
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

####  Error Handling

If the parameter N/SN of LIMIT/SLIMIT exceeds the size of the result set, IoTDB returns all the results as expected. For example, the query result of the original SQL statement consists of six rows, and we select the first 100 rows through the LIMIT clause:

```sql
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

```sql
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 1234567890123456789
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 303: check metadata error: Out of range. LIMIT <N>: N should be Int32.
```

If the parameter N/SN of LIMIT/SLIMIT clause is not a positive intege, the system prompts errors. For example, executing the following SQL statement:

```sql
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 13.1
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 401: line 1:129 mismatched input '.' expecting {<EOF>, ';'}
```

If the parameter OFFSET of LIMIT clause exceeds the size of the result set, IoTDB will return an empty result set. For example, executing the following SQL statement:

```sql
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

```sql
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 2
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 411: Meet error in query process: The value of SOFFSET (2) is equal to or exceeds the number of sequences (2) that can actually be returned.
```

### Null Value Control over Query Results

* IoTDB will join all the sensor value by its time, and if some sensors don't have values in that timestamp, we will fill it with null. In some analysis scenarios, we only need the row if all the columns of it have value.

```sql
select * from root.ln.* where time <= 2017-11-01T00:01:00 WITHOUT NULL ANY
```

* In group by query, we will fill null for any group by interval if the columns don't have values in that group by interval. However, if all columns in that group by interval are null, maybe users don't need that RowRecord, so we can use `WITHOUT NULL ALL` to filter that row.

```sql
select * from root.ln.* where time <= 2017-11-01T00:01:00 WITHOUT NULL ALL
```

### Other ResultSet Formats

In addition, IoTDB supports two other results set format: 'align by device' and 'disable align'.

#### align by device

The 'align by device' indicates that the deviceId is considered as a column. Therefore, there are totally limited columns in the dataset. 

The SQL statement is:

```sql
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

For more syntax description, please read [SQL Reference](../Appendix/SQL-Reference.md).

#### disable align

The 'disable align' indicates that there are 2 columns for each time series in the result set. Disable Align Clause can only be used at the end of a query statement. Disable Align Clause cannot be used with Aggregation, Fill Statements, Group By or Group By Device Statements, but can with Limit Statements. The display principle of the result table is that only when the column (or row) has existing data will the column (or row) be shown, with nonexistent cells being empty.

The SQL statement is:

```sql
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


For more syntax description, please read [SQL Reference](../Appendix/SQL-Reference.md).

## DELETE

Users can delete data that meet the deletion condition in the specified timeseries by using the [DELETE statement](../Appendix/SQL-Reference.md). When deleting data, users can select one or more timeseries paths, prefix paths, or paths with star  to delete data within a certain time interval.

In a JAVA programming environment, you can use the [Java JDBC](../API/Programming-JDBC.md) to execute single or batch UPDATE statements.

### Delete Single Timeseries
Taking ln Group as an example, there exists such a usage scenario:

The wf02 plant's wt02 device has many segments of errors in its power supply status before 2017-11-01 16:26:00, and the data cannot be analyzed correctly. The erroneous data affected the correlation analysis with other devices. At this point, the data before this time point needs to be deleted. The SQL statement for this operation is

```sql
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

In case we hope to merely delete the data before 2017-11-01 16:26:00 in the year of 2017, The SQL statement is:
```sql
delete from root.ln.wf02.wt02.status where time>=2017-01-01T00:00:00 and time<=2017-11-01T16:26:00;
```

IoTDB supports to delete a range of timeseries points. Users can write SQL expressions as follows to specify the delete interval:

```sql
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

```sql
delete from root.ln.wf02.status
```


### Delete Multiple Timeseries
If both the power supply status and hardware version of the ln group wf02 plant wt02 device before 2017-11-01 16:26:00 need to be deleted, [the prefix path with broader meaning or the path with star](../Data-Concept/Data-Model-and-Terminology.md) can be used to delete the data. The SQL statement for this operation is:

```sql
delete from root.ln.wf02.wt02 where time <= 2017-11-01T16:26:00;
```
or

```sql
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```
It should be noted that when the deleted path does not exist, IoTDB will not prompt that the path does not exist, but that the execution is successful, because SQL is a declarative programming method. Unless it is a syntax error, insufficient permissions and so on, it is not considered an error, as shown below:
```
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
Msg: TimeSeries does not exist and its data cannot be deleted
```

### Delete Time Partition (experimental)
You may delete all data in a time partition of a storage group using the following grammar:

```sql
DELETE PARTITION root.ln 0,1,2
```

The `0,1,2` above is the id of the partition that is to be deleted, you can find it from the IoTDB
data folders or convert a timestamp manually to an id using `timestamp / partitionInterval
` (flooring), and the `partitionInterval` should be in your config (if time-partitioning is
supported in your version).

Please notice that this function is experimental and mainly for development, please use it with care.
