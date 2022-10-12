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

When executing some queries, there may be no data for some columns in some rows, and data in these locations will be null, but this kind of null value is not conducive to data visualization and analysis, and the null value needs to be filled.

Fill null value allows the user to fill any query result with null values according to a specific method, such as taking the previous value that is not null, or linear interpolation. The query result after filling the null value can better reflect the data distribution, which is beneficial for users to perform data analysis.

In IoTDB, users can use the FILL clause to specify the fill mode when data is missing. If the queried point's value is not null, the fill function will not work.

## Fill Methods

IoTDB supports previous, linear, and value fill methods. Following table lists the data types and supported fill methods.

| Data Type | Supported Fill Methods  |
| :-------- |:------------------------|
| boolean   | previous, value         |
| int32     | previous, linear, value |
| int64     | previous, linear, value |
| float     | previous, linear, value |
| double    | previous, linear, value |
| text      | previous, value         |
| </center> |                         |

> Note: Only one Fill method can be specified in a Fill statement. Null value fill is not compatible with version 0.13 and previous syntax (fill((<data_type>[<fill_method>(, <before_range>, <after_range>)?])+)) is not supported anymore.


### Previous Fill

When the value is null, the value of the previous timestamp is used to fill the blank. The formalized previous method is as follows:

```sql
fill(previous)
```

Here we give an example of filling null values using the previous method. The SQL statement is as follows:

```sql
select temperature, status from root.sgcc.wf03.wt01 where time >= 2017-11-01T16:37:00.000 and time <= 2017-11-01T16:40:00.000
```
if we don't use any fill methods, the original result will be like:

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

if we use previous fill, sql will be like:
```sql
select temperature from root.sgcc.wf03.wt01 where time >= 2017-11-01T16:37:00.000 and time <= 2017-11-01T16:40:00.000 fill(previous)
```

previous filled result will be like:

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

> Note: If the first value of this column is null, we will keep first value as null and won't fill it until we meet first non-null value

### Linear Fill

When the value in the queried timestamp is null, the value of the previous and the next timestamp is used to fill the blank. The formalized linear method is as follows:

```sql
fill(linear)
```

Here we give an example of filling null values using the linear method. The SQL statement is as follows:

```sql
select temperature from root.sgcc.wf03.wt01 where time >= 2017-11-01T16:37:00.000 and time <= 2017-11-01T16:40:00.000 fill(linear)
```

linear filled result will be like:

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


> Note: If all the values before current value are null or all the values after current value are null, we will keep current value as null and won't fill it.
> Note: If the column's data type is boolean/text, we neither fill it nor throw exception, just keep it as it is.

### Value Fill

When the value in the queried timestamp is null, given fill value is used to fill the blank. The formalized value method is as follows:

```sql
fill(constant)
```

Here we give an example of filling null values using the value method. The SQL statement is as follows:

```sql
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(2.0)
```

float constant filled result will be like:

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

```sql
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(true)
```

boolean constant filled result will be like:

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

> Note: When using the ValueFill, IoTDB neither fill the query result if the data type is different from the input constant nor throw exception, just keep it as it is.
> Note: If constant value is larger than Integer.MAX_VALUE, IoTDB neither fill the query result if the data type is int32 nor throw exception, just keep it as it is.

#### Constant Fill Type Consistency

| Constant Value Data Type | Support Data Type                 |
|:-------------------------|:----------------------------------|
| boolean                  | boolean, text                     |
| int64                    | int32, int64, float, double, text |
| double                   | float, double, text               |
| text                     | text                              |
| </center>                |                                   |