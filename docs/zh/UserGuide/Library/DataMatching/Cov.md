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
# Cov

## 函数简介

本函数用于计算两列数值型数据的总体协方差。

**函数名：** COV

**输入序列：** 仅支持两个输入序列，类型均为 INT32 / INT64 / FLOAT / DOUBLE。

**输出序列：** 输出单个序列，类型为DOUBLE。序列仅包含一个时间戳为0、值为总体协方差的数据点。

**提示：** 

+ 如果某行数据中包含空值、缺失值或`NaN`，该行数据将会被忽略；
+ 如果数据中所有的行都被忽略，函数将会输出`NaN`。


## 使用示例

输入序列：

```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d2.s1|root.test.d2.s2|
+-----------------------------+---------------+---------------+
|2020-01-01T00:00:02.000+08:00|          100.0|          101.0|
|2020-01-01T00:00:03.000+08:00|          101.0|           null|
|2020-01-01T00:00:04.000+08:00|          102.0|          101.0|
|2020-01-01T00:00:06.000+08:00|          104.0|          102.0|
|2020-01-01T00:00:08.000+08:00|          126.0|          102.0|
|2020-01-01T00:00:10.000+08:00|          108.0|          103.0|
|2020-01-01T00:00:12.000+08:00|           null|          103.0|
|2020-01-01T00:00:14.000+08:00|          112.0|          104.0|
|2020-01-01T00:00:15.000+08:00|          113.0|           null|
|2020-01-01T00:00:16.000+08:00|          114.0|          104.0|
|2020-01-01T00:00:18.000+08:00|          116.0|          105.0|
|2020-01-01T00:00:20.000+08:00|          118.0|          105.0|
|2020-01-01T00:00:22.000+08:00|          100.0|          106.0|
|2020-01-01T00:00:26.000+08:00|          124.0|          108.0|
|2020-01-01T00:00:28.000+08:00|          126.0|          108.0|
|2020-01-01T00:00:30.000+08:00|            NaN|          108.0|
+-----------------------------+---------------+---------------+
```

用于查询的SQL语句：

```sql
select cov(s1,s2) from root.test.d2
```

输出序列：

```
+-----------------------------+-------------------------------------+
|                         Time|cov(root.test.d2.s1, root.test.d2.s2)|
+-----------------------------+-------------------------------------+
|1970-01-01T08:00:00.000+08:00|                   12.291666666666666|
+-----------------------------+-------------------------------------+
```