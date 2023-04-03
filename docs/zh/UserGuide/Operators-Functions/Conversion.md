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

## 数据类型转换

### CAST

#### 函数简介

当前 IoTDB 支持6种数据类型，其中包括 INT32、INT64、FLOAT、DOUBLE、BOOLEAN 以及 TEXT。当我们对数据进行查询或者计算时可能需要进行数据类型的转换， 比如说将 TEXT 转换为 INT32，或者提高数据精度，比如说将 FLOAT 转换为 DOUBLE。IoTDB 支持使用cast 函数对数据类型进行转换。

语法示例如下：

```sql
SELECT cast(s1 as INT32) from root.sg
```

cast 函数语法形式上与 PostgreSQL 一致，AS 后指定的数据类型表明要转换成的目标类型，目前 IoTDB 支持的六种数据类型均可以在 cast 函数中使用，遵循的转换规则如下表所示，其中行表示原始数据类型，列表示要转化成的目标数据类型：

|             | **INT32**                                                    | **INT64**                                                    | **FLOAT**                                       | **DOUBLE**              | **BOOLEAN**                                                  | **TEXT**                         |
| ----------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------------------------------------- | ----------------------- | ------------------------------------------------------------ | -------------------------------- |
| **INT32**   | 不转化                                                       | 直接转化                                                     | 直接转化                                        | 直接转化                | !=0 : true<br />==0: false                                   | String.valueOf()                 |
| **INT64**   | 超出 INT32 范围：执行抛异常<br />否则：直接转化              | 不转化                                                       | 直接转化                                        | 直接转化                | !=0L : true<br />==0: false                                  | String.valueOf()                 |
| **FLOAT**   | 超出 INT32 范围：执行抛异常<br />否则：四舍五入(Math.round()) | 超出 INT64 范围：执行抛异常<br />否则：四舍五入(Math.round()) | 不转化                                          | 直接转化                | !=0.0f : true<br />==0: false                                | String.valueOf()                 |
| **DOUBLE**  | 超出 INT32 范围：执行抛异常<br />否则：四舍五入(Math.round()) | 超出 INT64 范围：执行抛异常<br />否则：四舍五入(Math.round()) | 超出 FLOAT 范围：执行抛异常<br />否则：直接转化 | 不转化                  | !=0.0 : true<br />==0: false                                 | String.valueOf()                 |
| **BOOLEAN** | true: 1<br />false: 0                                        | true: 1L<br />false: 0                                       | true: 1.0f<br />false: 0                        | true: 1.0<br />false: 0 | 不转化                                                       | true: "true"<br />false: "false" |
| **TEXT**    | Integer.parseInt()                                           | Long.parseLong()                                             | Float.parseFloat()                              | Double.parseDouble()    | text.toLowerCase =="true" : true<br />text.toLowerCase =="false" : false<br />其它情况：执行抛异常 | 不转化                           |

#### 使用示例

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
