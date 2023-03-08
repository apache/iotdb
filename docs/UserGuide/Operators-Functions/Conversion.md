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

# Data Type Conversion Function

The IoTDB currently supports 6 data types, including INT32, INT64 ,FLOAT, DOUBLE, BOOLEAN, TEXT. When we query or evaluate data, we may need to convert data types, such as TEXT to INT32, or FLOAT to DOUBLE. IoTDB supports cast function to convert data types.

Syntax example:

```sql
SELECT cast(s1 as INT32)from root.sg
```

The syntax of the cast function is consistent with that of PostgreSQL. The data type specified after AS indicates the target type to be converted. Currently, all six data types supported by IoTDB can be used in the cast function. The conversion rules to be followed are shown in the following table. The row represents the original data type, and the column represents the target data type to be converted into:

|             | **INT32**                                                    | **INT64**                                                    | **FLOAT**                                                    | **DOUBLE**              | **BOOLEAN**                                                  | **TEXT**                         |
| ----------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------------- | ------------------------------------------------------------ | -------------------------------- |
| **INT32**   | No need to cast                                              | Cast directly                                                | Cast directly                                                | Cast directly           | !=0 : true<br />==0: false                                   | String.valueOf()                 |
| **INT64**   | Out of the range of INT32: throw Exception<br />Otherwise: Cast directly | No need to cast                                              | Cast directly                                                | Cast directly           | !=0L : true<br />==0: false                                  | String.valueOf()                 |
| **FLOAT**   | Out of the range of INT32: throw Exception<br />Otherwise: Math.round() | Out of the range of INT64: throw Exception<br />Otherwise: Math.round() | No need to cast                                              | Cast directly           | !=0.0f : true<br />==0: false                                | String.valueOf()                 |
| **DOUBLE**  | Out of the range of INT32: throw Exception<br />Otherwise: Math.round() | Out of the range of INT64: throw Exception<br />Otherwise: Math.round() | Out of the range of FLOAT：throw Exception<br />Otherwise: Cast directly | No need to cast         | !=0.0 : true<br />==0: false                                 | String.valueOf()                 |
| **BOOLEAN** | true: 1<br />false: 0                                        | true: 1L<br />false: 0                                       | true: 1.0f<br />false: 0                                     | true: 1.0<br />false: 0 | No need to cast                                              | true: "true"<br />false: "false" |
| **TEXT**    | Integer.parseInt()                                           | Long.parseLong()                                             | Float.parseFloat()                                           | Double.parseDouble()    | text.toLowerCase =="true" : true<br />text.toLowerCase =="false" : false<br />Otherwise: throw Exception | No need to cast                  |

#### Examples

```
// timeseries
IoTDB> show timeseries root.sg.d1.**
+-------------+-----+--------+--------+--------+-----------+----+----------+--------+------------------+
|   Timeseries|Alias|Database|DataType|Encoding|Compression|Tags|Attributes|Deadband|DeadbandParameters|
+-------------+-----+--------+--------+--------+-----------+----+----------+--------+------------------+
|root.sg.d1.s3| null| root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|    null|              null|
|root.sg.d1.s4| null| root.sg|  DOUBLE|   PLAIN|     SNAPPY|null|      null|    null|              null|
|root.sg.d1.s5| null| root.sg| BOOLEAN|   PLAIN|     SNAPPY|null|      null|    null|              null|
|root.sg.d1.s6| null| root.sg|    TEXT|   PLAIN|     SNAPPY|null|      null|    null|              null|
|root.sg.d1.s1| null| root.sg|   INT32|   PLAIN|     SNAPPY|null|      null|    null|              null|
|root.sg.d1.s2| null| root.sg|   INT64|   PLAIN|     SNAPPY|null|      null|    null|              null|
+-------------+-----+--------+--------+--------+-----------+----+----------+--------+------------------+

// data of timeseries
IoTDB> select * from root.sg.d1;
+-----------------------------+-------------+-------------+-------------+-------------+-------------+-------------+
|                         Time|root.sg.d1.s3|root.sg.d1.s4|root.sg.d1.s5|root.sg.d1.s6|root.sg.d1.s1|root.sg.d1.s2|
+-----------------------------+-------------+-------------+-------------+-------------+-------------+-------------+
|1970-01-01T08:00:00.000+08:00|          0.0|          0.0|        false|        10000|            0|            0|
|1970-01-01T08:00:00.001+08:00|          1.0|          1.0|        false|            3|            1|            1|
|1970-01-01T08:00:00.002+08:00|          2.7|          2.7|         true|         TRue|            2|            2|
|1970-01-01T08:00:00.003+08:00|         3.33|         3.33|         true|        faLse|            3|            3|
+-----------------------------+-------------+-------------+-------------+-------------+-------------+-------------+

// cast BOOLEAN to other types
IoTDB> select cast(s5 as INT32), cast(s5 as INT64),cast(s5 as FLOAT),cast(s5 as DOUBLE), cast(s5 as TEXT) from root.sg.d1
+-----------------------------+----------------------------+----------------------------+----------------------------+-----------------------------+---------------------------+
|                         Time|CAST(root.sg.d1.s5 AS INT32)|CAST(root.sg.d1.s5 AS INT64)|CAST(root.sg.d1.s5 AS FLOAT)|CAST(root.sg.d1.s5 AS DOUBLE)|CAST(root.sg.d1.s5 AS TEXT)|
+-----------------------------+----------------------------+----------------------------+----------------------------+-----------------------------+---------------------------+
|1970-01-01T08:00:00.000+08:00|                           0|                           0|                         0.0|                          0.0|                      false|
|1970-01-01T08:00:00.001+08:00|                           0|                           0|                         0.0|                          0.0|                      false|
|1970-01-01T08:00:00.002+08:00|                           1|                           1|                         1.0|                          1.0|                       true|
|1970-01-01T08:00:00.003+08:00|                           1|                           1|                         1.0|                          1.0|                       true|
+-----------------------------+----------------------------+----------------------------+----------------------------+-----------------------------+---------------------------+

// cast TEXT to numeric types
IoTDB> select cast(s6 as INT32), cast(s6 as INT64), cast(s6 as FLOAT), cast(s6 as DOUBLE) from root.sg.d1 where time < 2
+-----------------------------+----------------------------+----------------------------+----------------------------+-----------------------------+
|                         Time|CAST(root.sg.d1.s6 AS INT32)|CAST(root.sg.d1.s6 AS INT64)|CAST(root.sg.d1.s6 AS FLOAT)|CAST(root.sg.d1.s6 AS DOUBLE)|
+-----------------------------+----------------------------+----------------------------+----------------------------+-----------------------------+
|1970-01-01T08:00:00.000+08:00|                       10000|                       10000|                     10000.0|                      10000.0|
|1970-01-01T08:00:00.001+08:00|                           3|                           3|                         3.0|                          3.0|
+-----------------------------+----------------------------+----------------------------+----------------------------+-----------------------------+

// cast TEXT to BOOLEAN
IoTDB> select cast(s6 as BOOLEAN) from root.sg.d1 where time >= 2
+-----------------------------+------------------------------+
|                         Time|CAST(root.sg.d1.s6 AS BOOLEAN)|
+-----------------------------+------------------------------+
|1970-01-01T08:00:00.002+08:00|                          true|
|1970-01-01T08:00:00.003+08:00|                         false|
+-----------------------------+------------------------------+
```




