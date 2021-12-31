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
# Skew

## 函数简介

本函数用于计算单列数值型数据的总体偏度

**函数名：** SKEW

**输入序列：** 仅支持单个输入序列，类型为 INT32 / INT64 / FLOAT / DOUBLE

**输出序列：** 输出单个序列，类型为DOUBLE，序列仅包含一个时间戳为0、值为总体偏度的数据点。

**提示：** 数据中的空值、缺失值和`NaN`将会被忽略。

## 使用示例

输入序列：

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:00.000+08:00|            1.0|
|2020-01-01T00:00:01.000+08:00|            2.0|
|2020-01-01T00:00:02.000+08:00|            3.0|
|2020-01-01T00:00:03.000+08:00|            4.0|
|2020-01-01T00:00:04.000+08:00|            5.0|
|2020-01-01T00:00:05.000+08:00|            6.0|
|2020-01-01T00:00:06.000+08:00|            7.0|
|2020-01-01T00:00:07.000+08:00|            8.0|
|2020-01-01T00:00:08.000+08:00|            9.0|
|2020-01-01T00:00:09.000+08:00|           10.0|
|2020-01-01T00:00:10.000+08:00|           10.0|
|2020-01-01T00:00:11.000+08:00|           10.0|
|2020-01-01T00:00:12.000+08:00|           10.0|
|2020-01-01T00:00:13.000+08:00|           10.0|
|2020-01-01T00:00:14.000+08:00|           10.0|
|2020-01-01T00:00:15.000+08:00|           10.0|
|2020-01-01T00:00:16.000+08:00|           10.0|
|2020-01-01T00:00:17.000+08:00|           10.0|
|2020-01-01T00:00:18.000+08:00|           10.0|
|2020-01-01T00:00:19.000+08:00|           10.0|
+-----------------------------+---------------+
```

用于查询的SQL语句：

```sql
select skew(s1) from root.test.d1
```

输出序列：

```
+-----------------------------+-----------------------+
|                         Time|  skew(root.test.d1.s1)|
+-----------------------------+-----------------------+
|1970-01-01T08:00:00.000+08:00|    -0.9998427402292644|
+-----------------------------+-----------------------+
```