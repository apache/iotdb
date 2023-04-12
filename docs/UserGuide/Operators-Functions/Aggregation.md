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

# Aggregate Functions

Aggregate functions are many-to-one functions. They perform aggregate calculations on a set of values, resulting in a single aggregated result.

All aggregate functions except `COUNT()`, `COUNT_IF()` ignore null values and return null when there are no input rows or all values are null. For example, `SUM()` returns null instead of zero, and `AVG()` does not include null values in the count.

The aggregate functions supported by IoTDB are as follows:

| Function Name | Function Description                                                                                                                                                                                                                                                                              | Allowed Input Data Types | required attributes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Output Data Types                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ------------------------ |-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SUM           | Summation.                                                                                                                                                                                                                                                                                        | INT32 INT64 FLOAT DOUBLE | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | DOUBLE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| COUNT         | Counts the number of data points.                                                                                                                                                                                                                                                                 | All types                | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | INT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| AVG           | Average.                                                                                                                                                                                                                                                                                          | INT32 INT64 FLOAT DOUBLE | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | DOUBLE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| EXTREME       | Finds the value with the largest absolute value. Returns a positive value if the maximum absolute value of positive and negative values is equal.                                                                                                                                                 | INT32 INT64 FLOAT DOUBLE | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Consistent with the input data type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| MAX_VALUE     | Find the maximum value.                                                                                                                                                                                                                                                                           | INT32 INT64 FLOAT DOUBLE | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Consistent with the input data type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| MIN_VALUE     | Find the minimum value.                                                                                                                                                                                                                                                                           | INT32 INT64 FLOAT DOUBLE | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Consistent with the input data type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| FIRST_VALUE   | Find the value with the smallest timestamp.                                                                                                                                                                                                                                                       | All data types           | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Consistent with input data type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| LAST_VALUE    | Find the value with the largest timestamp.                                                                                                                                                                                                                                                        | All data types           | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Consistent with input data type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| MAX_TIME      | Find the maximum timestamp.                                                                                                                                                                                                                                                                       | All data Types           | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Timestamp                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| MIN_TIME      | Find the minimum timestamp.                                                                                                                                                                                                                                                                       | All data Types           | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Timestamp                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| COUNT_IF      | Find the number of data points that continuously meet a given condition and the number of data points that meet the condition (represented by keep) meet the specified threshold.                                                                                                                 | BOOLEAN                  | `[keep >=/>/=/!=/</<=]threshold`：The specified threshold or threshold condition, it is equivalent to `keep >= threshold` if `threshold` is used alone, type of `threshold` is `INT64`<br> `ignoreNull`：Optional, default value is `true`；If the value is `true`, null values are ignored, it means that if there is a null value in the middle, the value is ignored without interrupting the continuity. If the value is `true`, null values are not ignored, it means that if there are null values in the middle, continuity will be broken | INT64     |
| TIME_DURATION | Find the difference between the timestamp of the largest non-null value and the timestamp of the smallest non-null value in a column                                                                                                                                                              |  All data Types   | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | INT64                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|  MODE         | Find the mode. Note: <br>1.Having too many different values in the input series risks a memory exception; <br>2.If all the elements have the same number of occurrences, that is no Mode, return the value with earliest time; <br>3.If there are many Modes, return the Mode with earliest time. | All data Types                     | No                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Consistent with the input data type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
## COUNT

### example

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

## COUNT_IF

### Grammar
```sql
count_if(predicate, [keep >=/>/=/!=/</<=]threshold[, 'ignoreNull'='true/false'])
```
predicate: legal expression with `BOOLEAN` return type

use of threshold and ignoreNull can see above table

>Note: count_if is not supported to use with SlidingWindow in group by time now

### example

#### raw data

``` 
+-----------------------------+-------------+-------------+
|                         Time|root.db.d1.s1|root.db.d1.s2|
+-----------------------------+-------------+-------------+
|1970-01-01T08:00:00.001+08:00|            0|            0|
|1970-01-01T08:00:00.002+08:00|         null|            0|
|1970-01-01T08:00:00.003+08:00|            0|            0|
|1970-01-01T08:00:00.004+08:00|            0|            0|
|1970-01-01T08:00:00.005+08:00|            1|            0|
|1970-01-01T08:00:00.006+08:00|            1|            0|
|1970-01-01T08:00:00.007+08:00|            1|            0|
|1970-01-01T08:00:00.008+08:00|            0|            0|
|1970-01-01T08:00:00.009+08:00|            0|            0|
|1970-01-01T08:00:00.010+08:00|            0|            0|
+-----------------------------+-------------+-------------+
```

#### Not use `ignoreNull` attribute (Ignore Null)

SQL:
```sql
select count_if(s1=0 & s2=0, 3), count_if(s1=1 & s2=0, 3) from root.db.d1
```

Result:
```
+--------------------------------------------------+--------------------------------------------------+
|count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, 3)|count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, 3)|
+--------------------------------------------------+--------------------------------------------------+
|                                                 2|                                                 1|
+--------------------------------------------------+--------------------------------------------------
```

#### Use `ignoreNull` attribute

SQL:
```sql
select count_if(s1=0 & s2=0, 3, 'ignoreNull'='false'), count_if(s1=1 & s2=0, 3, 'ignoreNull'='false') from root.db.d1
```

Result:
```
+------------------------------------------------------------------------+------------------------------------------------------------------------+
|count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, 3, "ignoreNull"="false")|count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, 3, "ignoreNull"="false")|
+------------------------------------------------------------------------+------------------------------------------------------------------------+
|                                                                       1|                                                                       1|
+------------------------------------------------------------------------+------------------------------------------------------------------------+
```

## TIME_DURATION
### Grammar
```sql
    time_duration(Path)
```
### Example
#### raw data
```sql
+----------+-------------+
|      Time|root.db.d1.s1|
+----------+-------------+
|         1|           70|
|         3|           10|
|         4|          303|
|         6|          110|
|         7|          302|
|         8|          110|
|         9|           60|
|        10|           70|
|1677570934|           30|
+----------+-------------+
```
#### Insert sql
```sql
"CREATE DATABASE root.db",
"CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Beijing)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1, 2, 10, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(2, null, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(3, 10, 0, null)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(4, 303, 30, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(5, null, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(6, 110, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(7, 302, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(8, 110, null, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(9, 60, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(10,70, 20, null)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1677570934, 30, 0, true)",
```

SQL:
```sql
select time_duration(s1) from root.db.d1
```

Result:
```
+----------------------------+
|time_duration(root.db.d1.s1)|
+----------------------------+
|                  1677570933|
+----------------------------+
```
> Note: Returns 0 if there is only one data point, or null if the data point is null.