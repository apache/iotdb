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

## Introduction

When executing some queries, there may be no data for some columns in some rows, and data in these locations will be null, but this kind of null value is not conducive to data visualization and analysis, and the null value needs to be filled.

In IoTDB, users can use the FILL clause to specify the fill mode when data is missing. Fill null value allows the user to fill any query result with null values according to a specific method, such as taking the previous value that is not null, or linear interpolation. The query result after filling the null value can better reflect the data distribution, which is beneficial for users to perform data analysis.

## Syntax Definition

**The following is the syntax definition of the `FILL` clause:**

```sql
FILL '(' PREVIOUS | LINEAR | constant ')'
```

**Note:** 
- We can specify only one fill method in the `FILL` clause, and this method applies to all columns of the result set.
- Null value fill is not compatible with version 0.13 and previous syntax (`FILL((<data_type>[<fill_method>(, <before_range>, <after_range>)?])+)`) is not supported anymore.

## Fill Methods

**IoTDB supports the following three fill methods:**

- `PREVIOUS`: Fill with the previous non-null value of the column.
- `LINEAR`: Fill the column with a linear interpolation of the previous non-null value and the next non-null value of the column.
- Constant: Fill with the specified constant.

**Following table lists the data types and supported fill methods.**

| Data Type | Supported Fill Methods  |
| :-------- |:------------------------|
| boolean   | previous, value         |
| int32     | previous, linear, value |
| int64     | previous, linear, value |
| float     | previous, linear, value |
| double    | previous, linear, value |
| text      | previous, value         |

**Note:**  For columns whose data type does not support specifying the fill method, we neither fill it nor throw exception, just keep it as it is.

**For examples:**

If we don't use any fill methods:

```sql
select temperature, status from root.sgcc.wf03.wt01 where time >= 2017-11-01T16:37:00.000 and time <= 2017-11-01T16:40:00.000;
```

the original result will be like:

```
+-----------------------------+-------------------------------+--------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|root.sgcc.wf03.wt01.status|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:37:00.000+08:00|                          21.93|                      true|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:38:00.000+08:00|                           null|                     false|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:39:00.000+08:00|                          22.23|                      null|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:40:00.000+08:00|                          23.43|                      null|
+-----------------------------+-------------------------------+--------------------------+
Total line number = 4
```

### `PREVIOUS` Fill

**For null values in the query result set, fill with the previous non-null value of the column.**

**Note:** If the first value of this column is null, we will keep first value as null and won't fill it until we meet first non-null value

For example, with `PREVIOUS` fill, the SQL is as follows:

```sql
select temperature, status from root.sgcc.wf03.wt01 where time >= 2017-11-01T16:37:00.000 and time <= 2017-11-01T16:40:00.000 fill(previous);
```

result will be like:

```
+-----------------------------+-------------------------------+--------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|root.sgcc.wf03.wt01.status|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:37:00.000+08:00|                          21.93|                      true|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:38:00.000+08:00|                          21.93|                     false|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:39:00.000+08:00|                          22.23|                     false|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:40:00.000+08:00|                          23.43|                     false|
+-----------------------------+-------------------------------+--------------------------+
Total line number = 4
```

### `LINEAR` Fill

**For null values in the query result set, fill the column with a linear interpolation of the previous non-null value and the next non-null value of the column.**

**Note:**
- If all the values before current value are null or all the values after current value are null, we will keep current value as null and won't fill it.
- If the column's data type is boolean/text, we neither fill it nor throw exception, just keep it as it is.

Here we give an example of filling null values using the linear method. The SQL statement is as follows:

For example, with `LINEAR` fill, the SQL is as follows:

```sql
select temperature, status from root.sgcc.wf03.wt01 where time >= 2017-11-01T16:37:00.000 and time <= 2017-11-01T16:40:00.000 fill(linear);
```

result will be like:

```
+-----------------------------+-------------------------------+--------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|root.sgcc.wf03.wt01.status|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:37:00.000+08:00|                          21.93|                      true|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:38:00.000+08:00|                          22.08|                     false|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:39:00.000+08:00|                          22.23|                      null|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:40:00.000+08:00|                          23.43|                      null|
+-----------------------------+-------------------------------+--------------------------+
Total line number = 4
```

### Constant Fill

**For null values in the query result set, fill with the specified constant.**

**Note:** 
- When using the ValueFill, IoTDB neither fill the query result if the data type is different from the input constant nor throw exception, just keep it as it is.

   | Constant Value Data Type | Support Data Type                 |
   |:-------------------------|:----------------------------------|
   | `BOOLEAN` | `BOOLEAN` `TEXT` |
   | `INT64` | `INT32` `INT64` `FLOAT` `DOUBLE` `TEXT` |
   | `DOUBLE` | `FLOAT` `DOUBLE` `TEXT` |
   | `TEXT` | `TEXT` |
- If constant value is larger than Integer.MAX_VALUE, IoTDB neither fill the query result if the data type is int32 nor throw exception, just keep it as it is.

For example, with `FLOAT` constant fill, the SQL is as follows:

```sql
select temperature, status from root.sgcc.wf03.wt01 where time >= 2017-11-01T16:37:00.000 and time <= 2017-11-01T16:40:00.000 fill(2.0);
```

result will be like:

```
+-----------------------------+-------------------------------+--------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|root.sgcc.wf03.wt01.status|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:37:00.000+08:00|                          21.93|                      true|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:38:00.000+08:00|                            2.0|                     false|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:39:00.000+08:00|                          22.23|                      null|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:40:00.000+08:00|                          23.43|                      null|
+-----------------------------+-------------------------------+--------------------------+
Total line number = 4
```

For example, with `BOOLEAN` constant fill, the SQL is as follows:

```sql
select temperature, status from root.sgcc.wf03.wt01 where time >= 2017-11-01T16:37:00.000 and time <= 2017-11-01T16:40:00.000 fill(true);
```

result will be like:

```
+-----------------------------+-------------------------------+--------------------------+
|                         Time|root.sgcc.wf03.wt01.temperature|root.sgcc.wf03.wt01.status|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:37:00.000+08:00|                          21.93|                      true|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:38:00.000+08:00|                           null|                     false|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:39:00.000+08:00|                          22.23|                      true|
+-----------------------------+-------------------------------+--------------------------+
|2017-11-01T16:40:00.000+08:00|                          23.43|                      true|
+-----------------------------+-------------------------------+--------------------------+
Total line number = 4
```
