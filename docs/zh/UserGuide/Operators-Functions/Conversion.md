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

当前IoTDB支持6种数据类型，其中包括INT32、INT64、FLOAT、DOUBLE、BOOLEAN以及TEXT。当我们对数据进行查询或者计算时可能需要进行数据类型的转换， 比如说将TEXT转换为INT32，或者提高数据精度，比如说将FLOAT转换为DOUBLE。所以，IoTDB支持使用cast函数对数据类型进行转换。

| 函数名 | 必要的属性参数                                               | 输出序列类型             | 功能类型                           |
| ------ | ------------------------------------------------------------ | ------------------------ | ---------------------------------- |
| CAST   | `type`:输出的数据点的类型，只能是 INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | 由输入属性参数`type`决定 | 将数据转换为`type`参数指定的类型。 |

#### 类型转换说明

1.当INT32、INT64类型的值不为0时，FLOAT与DOUBLE类型的值不为0.0时，TEXT类型不为空字符串或者"false"时，转换为BOOLEAN类型时值为true，否则为false。

```
IoTDB> show timeseries root.sg.d1.*;
+-------------+-----+-------------+--------+--------+-----------+----+----------+
|   timeseries|alias|     database|dataType|encoding|compression|tags|attributes|
+-------------+-----+-------------+--------+--------+-----------+----+----------+
|root.sg.d1.s3| null|      root.sg|   FLOAT|     RLE|     SNAPPY|null|      null|
|root.sg.d1.s4| null|      root.sg|  DOUBLE|     RLE|     SNAPPY|null|      null|
|root.sg.d1.s5| null|      root.sg|    TEXT|   PLAIN|     SNAPPY|null|      null|
|root.sg.d1.s6| null|      root.sg| BOOLEAN|     RLE|     SNAPPY|null|      null|
|root.sg.d1.s1| null|      root.sg|   INT32|     RLE|     SNAPPY|null|      null|
|root.sg.d1.s2| null|      root.sg|   INT64|     RLE|     SNAPPY|null|      null|
+-------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 6
It costs 0.006s
IoTDB> select * from root.sg.d1;
+-----------------------------+-------------+-------------+-------------+-------------+-------------+-------------+
|                         Time|root.sg.d1.s3|root.sg.d1.s4|root.sg.d1.s5|root.sg.d1.s6|root.sg.d1.s1|root.sg.d1.s2|
+-----------------------------+-------------+-------------+-------------+-------------+-------------+-------------+
|1970-01-01T08:00:00.001+08:00|          1.1|          1.1|         test|        false|            1|            1|
|1970-01-01T08:00:00.002+08:00|         -2.2|         -2.2|        false|         true|           -2|           -2|
|1970-01-01T08:00:00.003+08:00|          0.0|          0.0|         true|         true|            0|            0|
+-----------------------------+-------------+-------------+-------------+-------------+-------------+-------------+
Total line number = 3
It costs 0.009s
IoTDB> select cast(s1, 'type'='BOOLEAN'), cast(s2, 'type'='BOOLEAN'), cast(s3, 'type'='BOOLEAN'), cast(s4, 'type'='BOOLEAN'), cast(s5, 'type'='BOOLEAN') from root.sg.d1;
+-----------------------------+-------------------------------------+-------------------------------------+-------------------------------------+-------------------------------------+-------------------------------------+
|                         Time|cast(root.sg.d1.s1, "type"="BOOLEAN")|cast(root.sg.d1.s2, "type"="BOOLEAN")|cast(root.sg.d1.s3, "type"="BOOLEAN")|cast(root.sg.d1.s4, "type"="BOOLEAN")|cast(root.sg.d1.s5, "type"="BOOLEAN")|
+-----------------------------+-------------------------------------+-------------------------------------+-------------------------------------+-------------------------------------+-------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                 true|                                 true|                                 true|                                 true|                                 true|
|1970-01-01T08:00:00.002+08:00|                                 true|                                 true|                                 true|                                 true|                                false|
|1970-01-01T08:00:00.003+08:00|                                false|                                false|                                false|                                false|                                 true|
+-----------------------------+-------------------------------------+-------------------------------------+-------------------------------------+-------------------------------------+-------------------------------------+
Total line number = 3
It costs 0.012s
```

2.当BOOLEAN类型的值为true时，转换为INT32与INT64类型的值为1，转换为FLOAT或者DOUBLE类型时值为1.0，转换为TEXT类型时值为"true"。当BOOLEAN类型的值为false时，转换为INT32与INT64类型的值为0，转换为FLOAT或者DOUBLE类型时值为0.0，转换为TEXT类型时值为"false"。

```
IoTDB> select cast(s6, 'type'='INT32'), cast(s6, 'type'='INT64'), cast(s6, 'type'='FLOAT'), cast(s6, 'type'='DOUBLE'), cast(s6, 'type'='TEXT') from root.sg.d1;
+-----------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------------------------+----------------------------------+
|                         Time|cast(root.sg.d1.s6, "type"="INT32")|cast(root.sg.d1.s6, "type"="INT64")|cast(root.sg.d1.s6, "type"="FLOAT")|cast(root.sg.d1.s6, "type"="DOUBLE")|cast(root.sg.d1.s6, "type"="TEXT")|
+-----------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------------------------+----------------------------------+
|1970-01-01T08:00:00.001+08:00|                                  0|                                  0|                                0.0|                                 0.0|                             false|
|1970-01-01T08:00:00.002+08:00|                                  1|                                  1|                                1.0|                                 1.0|                              true|
|1970-01-01T08:00:00.003+08:00|                                  1|                                  1|                                1.0|                                 1.0|                              true|
+-----------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------------------------+----------------------------------+
Total line number = 3
It costs 0.016s
```

3.当TEXT类型转换为INT32、INT64、FLOAT类型时，会先将TEXT类型的数据转换为DOUBLE类型，然后再转换为对应的类型，此时可能会存在损失精度的问题。如果无法转换的话则直接跳过。

```
IoTDB> select cast(s5, 'type'='INT32'), cast(s5, 'type'='INT64'), cast(s5, 'type'='FLOAT') from root.sg.d1;
+----+-----------------------------------+-----------------------------------+-----------------------------------+
|Time|cast(root.sg.d1.s5, "type"="INT32")|cast(root.sg.d1.s5, "type"="INT64")|cast(root.sg.d1.s5, "type"="FLOAT")|
+----+-----------------------------------+-----------------------------------+-----------------------------------+
+----+-----------------------------------+-----------------------------------+-----------------------------------+
Empty set.
It costs 0.009s
```

#### 使用示例

测试数据：
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
SQL语句：
```sql
select cast(text, 'type'='BOOLEAN'), cast(text, 'type'='INT32'), cast(text, 'type'='INT64'), cast(text, 'type'='FLOAT'), cast(text, 'type'='DOUBLE') from root.test;
```
结果：
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
