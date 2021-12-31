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
# Dtw

## 函数简介

本函数用于计算两列数值型数据的DTW距离。

**函数名：** DTW

**输入序列：** 仅支持两个输入序列，类型均为 INT32 / INT64 / FLOAT / DOUBLE。

**输出序列：** 输出单个序列，类型为DOUBLE。序列仅包含一个时间戳为0、值为两个时间序列的DTW距离值。

**提示：** 

+ 如果某行数据中包含空值、缺失值或`NaN`，该行数据将会被忽略；
+ 如果数据中所有的行都被忽略，函数将会输出0。


## 使用示例

输入序列：

```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d2.s1|root.test.d2.s2|
+-----------------------------+---------------+---------------+
|1970-01-01T08:00:00.001+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.002+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.003+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.004+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.005+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.006+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.007+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.008+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.009+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.010+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.011+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.012+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.013+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.014+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.015+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.016+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.017+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.018+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.019+08:00|            1.0|            2.0|
|1970-01-01T08:00:00.020+08:00|            1.0|            2.0|
+-----------------------------+---------------+---------------+
```

用于查询的SQL语句：

```sql
select dtw(s1,s2) from root.test.d2
```

输出序列：

```
+-----------------------------+-------------------------------------+
|                         Time|dtw(root.test.d2.s1, root.test.d2.s2)|
+-----------------------------+-------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                 20.0|
+-----------------------------+-------------------------------------+
```
