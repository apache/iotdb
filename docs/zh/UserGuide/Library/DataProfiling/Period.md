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
# Period

## 函数简介

本函数用于计算单列数值型数据的周期。

**函数名：** PERIOD

**输入序列：** 仅支持单个输入序列，类型为 INT32 / INT64 / FLOAT / DOUBLE

**输出序列：** 输出单个序列，类型为INT32，序列仅包含一个时间戳为0、值为周期的数据点。

## 使用示例

输入序列：

```
+-----------------------------+---------------+
|                         Time|root.test.d3.s1|
+-----------------------------+---------------+
|1970-01-01T08:00:00.001+08:00|            1.0|
|1970-01-01T08:00:00.002+08:00|            2.0|
|1970-01-01T08:00:00.003+08:00|            3.0|
|1970-01-01T08:00:00.004+08:00|            1.0|
|1970-01-01T08:00:00.005+08:00|            2.0|
|1970-01-01T08:00:00.006+08:00|            3.0|
|1970-01-01T08:00:00.007+08:00|            1.0|
|1970-01-01T08:00:00.008+08:00|            2.0|
|1970-01-01T08:00:00.009+08:00|            3.0|
+-----------------------------+---------------+
```

用于查询的SQL语句：

```sql
select period(s1) from root.test.d3
```

输出序列：

```
+-----------------------------+-----------------------+
|                         Time|period(root.test.d3.s1)|
+-----------------------------+-----------------------+
|1970-01-01T08:00:00.000+08:00|                      3|
+-----------------------------+-----------------------+
```
### Zeppelin示例
链接: <http://101.6.15.213:18181/#/notebook/2GEJBUSZ9>
