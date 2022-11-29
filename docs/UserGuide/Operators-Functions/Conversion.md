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

The IoTDB currently supports 6 data types, including INT32, INT64 ,FLOAT, DOUBLE, BOOLEAN, TEXT. When we query or evaluate data, we may need to convert data types, such as TEXT to INT32, or improve the accuracy of the data, such as FLOAT to DOUBLE. Therefore, IoTDB supports the use of cast functions to convert data types.

| Function Name | Required Attributes                                          | Output Series Data Type                      | Series Data Type  Description                               |
| ------------- | ------------------------------------------------------------ | -------------------------------------------- | ----------------------------------------------------------- |
| CAST          | `type`: the type of the output data point, it can only be INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | Determined by the required attribute  `type` | Converts data to the type specified by the `type` argument. |

#### Notes
1. The value of type BOOLEAN is `true`, when data is converted to BOOLEAN if INT32 and INT64 are not 0, FLOAT and DOUBLE are not 0.0, TEXT is not empty string or "false", otherwise `false`.    

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

2. The value of type INT32, INT64, FLOAT, DOUBLE are 1 or 1.0 and TEXT is "true", when BOOLEAN data is true, otherwise 0, 0.0 or "false".  

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

3. When TEXT is converted to INT32, INT64, or FLOAT, the TEXT is first converted to DOUBLE and then to the corresponding type, which may cause loss of precision. It will skip directly if the data can not be converted.

```
IoTDB> select cast(s5, 'type'='INT32'), cast(s5, 'type'='INT64'), cast(s5, 'type'='FLOAT') from root.sg.d1;
+----+-----------------------------------+-----------------------------------+-----------------------------------+
|Time|cast(root.sg.d1.s5, "type"="INT32")|cast(root.sg.d1.s5, "type"="INT64")|cast(root.sg.d1.s5, "type"="FLOAT")|
+----+-----------------------------------+-----------------------------------+-----------------------------------+
+----+-----------------------------------+-----------------------------------+-----------------------------------+
Empty set.
It costs 0.005s
```



#### Syntax
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
