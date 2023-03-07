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

# Overview

## Syntax Definition

In IoTDB, `SELECT` statement is used to retrieve data from one or more selected time series. Here is the syntax definition of `SELECT` statement:

```sql
SELECT [LAST] selectExpr [, selectExpr] ...
    [INTO intoItem [, intoItem] ...]
    FROM prefixPath [, prefixPath] ...
    [WHERE whereCondition]
    [GROUP BY {
        ([startTime, endTime), interval [, slidingStep]) |
        LEVEL = levelNum [, levelNum] ... |
        TAGS(tagKey [, tagKey] ... )
        VARIATION(expression[,delta][,ignoreNull=true/false])|
        CONDITION(expression,[keep>/>=/=/</<=]threshold[,ignoreNull=true/false])|
        SESSION(timeInterval)
    }]
    [HAVING havingCondition]
    [ORDER BY sortKey {ASC | DESC}]
    [FILL ({PREVIOUS | LINEAR | constant})]
    [SLIMIT seriesLimit] [SOFFSET seriesOffset]
    [LIMIT rowLimit] [OFFSET rowOffset]
    [ALIGN BY {TIME | DEVICE}]
```

## Syntax Description

### `SELECT` clause

- The `SELECT` clause specifies the output of the query, consisting of several `selectExpr`.
- Each `selectExpr` defines one or more columns in the query result, which is an expression consisting of time series path suffixes, constants, functions, and operators.
- Supports using `AS` to specify aliases for columns in the query result set.
- Use the `LAST` keyword in the `SELECT` clause to specify that the query is the last query. For details and examples, see the document [Last Query](./Last-Query.md).
- For details and examples, see the document [Select Expression](./Select-Expression.md).

### `INTO` clause

- `SELECT INTO` is used to write query results into a series of specified time series. The `INTO` clause specifies the target time series to which query results are written.
- For detailed instructions and examples, see the document [SELECT INTO](Select-Into.md).

### `FROM` clause

- The `FROM` clause contains the path prefix of one or more time series to be queried, and wildcards are supported.
- When executing a query, the path prefix in the `FROM` clause and the suffix in the `SELECT` clause will be concatenated to obtain a complete query target time series.

### `WHERE` clause

- The `WHERE` clause specifies the filtering conditions for data rows, consisting of a `whereCondition`.
- `whereCondition` is a logical expression that evaluates to true for each row to be selected. If there is no `WHERE` clause, all rows will be selected.
- In `whereCondition`, any IOTDB-supported functions and operators can be used except aggregate functions.
- For details and examples, see the document [Where Condition](./Where-Condition.md).

### `GROUP BY` clause

- The `GROUP BY` clause specifies how the time series are aggregated by segment or group.
- Segmented aggregation refers to segmenting data in the row direction according to the time dimension, aiming at the time relationship between different data points in the same time series, and obtaining an aggregated value for each segment. Currently only **segmentation by time interval**、**group by variation**、**group by series** and **group by session** is supported, and more segmentation methods will be supported in the future.
- Group aggregation refers to grouping the potential business attributes of time series for different time series. Each group contains several time series, and each group gets an aggregated value. Support **group by path level** and **group by tag** two grouping methods.
- Segment aggregation and group aggregation can be mixed.
- For details and examples, see the document [Group By Aggregation](./Group-By.md).

### `HAVING` clause

- The `HAVING` clause specifies the filter conditions for the aggregation results, consisting of a `havingCondition`.
- `havingCondition` is a logical expression that evaluates to true for the aggregation results to be selected. If there is no `HAVING` clause, all aggregated results will be selected.
- `HAVING` is to be used with aggregate functions and the `GROUP BY` clause.
- For details and examples, see the document [Aggregation Result Filtering](./Having-Condition.md).

### `ORDER BY` clause

- The `ORDER BY` clause is used to specify how the result set is sorted.
- In ALIGN BY TIME mode: By default, they are sorted in ascending order of timestamp size, and `ORDER BY TIME DESC` can be used to specify that the result set is sorted in descending order of timestamp.
- In ALIGN BY DEVICE mode: arrange according to the device first, and sort each device in ascending order according to the timestamp. The ordering and priority can be adjusted by `ORDER BY` clause.
- For details and examples, see the document [Order By](./Order-By.md).

### `FILL` clause

- The `FILL` clause is used to specify the filling mode in the case of missing data, allowing users to fill in empty values ​​for the result set of any query according to a specific method.
- For details and examples, see the document [Fill Null Value](./Fill.md).

### `SLIMIT` and `SOFFSET` clauses

- `SLIMIT` specifies the number of columns of the query result, and `SOFFSET` specifies the starting column position of the query result display. `SLIMIT` and `SOFFSET` are only used to control value columns and have no effect on time and device columns.
- For details and examples of query result pagination, see the document [Result Set Pagination](./Pagination.md).

### `LIMIT` and `OFFSET` clauses

- `LIMIT` specifies the number of rows of the query result, and `OFFSET` specifies the starting row position of the query result display.
- For details and examples of query result pagination, see the document [Result Set Pagination](./Pagination.md).

### `ALIGN BY` clause

- The query result set is **ALIGN BY TIME** by default, including a time column and several value columns, and the timestamps of each column of data in each row are the same.
- It also supports  **ALIGN BY DEVICE**. The query result set contains a time column, a device column, and several value columns.
- For details and examples, see the document [Query Alignment Mode](./Align-By.md).

## Basic Examples

### Select a Column of Data Based on a Time Interval

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

### Select Multiple Columns of Data Based on a Time Interval

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

### Select Multiple Columns of Data for the Same Device According to Multiple Time Intervals

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


### Choose Multiple Columns of Data for Different Devices According to Multiple Time Intervals

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

### Order By Time Query
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

## Execution Interface

In IoTDB, there are two ways to execute data query:
- Execute queries using IoTDB-SQL.
- Efficient execution interfaces for common queries, including time-series raw data query, last query, and aggregation query.

### Execute queries using IoTDB-SQL

Data query statements can be used in SQL command-line terminals, JDBC, JAVA / C++ / Python / Go and other native APIs, and RESTful APIs.

- Execute the query statement in the SQL command line terminal: start the SQL command line terminal, and directly enter the query statement to execute, see [SQL command line terminal](../QuickStart/Command-Line-Interface.md).

- Execute query statements in JDBC, see [JDBC](../API/Programming-JDBC.md) for details.

- Execute query statements in native APIs such as JAVA / C++ / Python / Go. For details, please refer to the relevant documentation in the Application Programming Interface chapter. The interface prototype is as follows:

   ````java
   SessionDataSet executeQueryStatement(String sql)
   ````

- Used in RESTful API, see [HTTP API](../API/RestService.md) for details.

### Efficient execution interfaces

The native APIs provide efficient execution interfaces for commonly used queries, which can save time-consuming operations such as SQL parsing. include:

* Time-series raw data query with time range:
  - The specified query time range is a left-closed right-open interval, including the start time but excluding the end time.

```java
SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime);
```

* Last query:
  - Query the last data, whose timestamp is greater than or equal LastTime.

```java
SessionDataSet executeLastDataQuery(List<String> paths, long LastTime);
```

* Aggregation query:
  - Support specified query time range: The specified query time range is a left-closed right-open interval, including the start time but not the end time.
  - Support GROUP BY TIME.

```java
SessionDataSet executeAggregationQuery(List<String> paths, List<Aggregation> aggregations);

SessionDataSet executeAggregationQuery(
    List<String> paths, List<Aggregation> aggregations, long startTime, long endTime);

SessionDataSet executeAggregationQuery(
    List<String> paths,
    List<Aggregation> aggregations,
    long startTime,
    long endTime,
    long interval);

SessionDataSet executeAggregationQuery(
    List<String> paths,
    List<Aggregation> aggregations,
    long startTime,
    long endTime,
    long interval,
    long slidingStep);
```
