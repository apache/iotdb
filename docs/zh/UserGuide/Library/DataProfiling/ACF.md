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
# ACF

## 函数简介

本函数用于计算时间序列的自相关函数值，即序列与自身之间的互相关函数，详情参见`XCorr`函数文档。

**函数名：** ACF

**输入序列：** 仅支持单个输入序列，类型为 INT32 / INT64 / FLOAT / DOUBLE。

**输出序列：** 输出单个序列，类型为DOUBLE。序列中共包含$2N-1$个数据点，每个值的具体含义参见`XCorr`函数文档。

**提示：**

+ 序列中的`NaN`值会被忽略，在计算中表现为0。

## 使用示例

输入序列：
```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:01.000+08:00|              1|
|2020-01-01T00:00:02.000+08:00|            NaN|
|2020-01-01T00:00:03.000+08:00|              3|
|2020-01-01T00:00:04.000+08:00|            NaN|
|2020-01-01T00:00:05.000+08:00|              5|
+-----------------------------+---------------+
```


用于查询的SQL语句：

```sql
select acf(s1) from root.test.d1 where time <= 2020-01-01 00:00:05
```

输出序列：
```
+-----------------------------+--------------------+
|                         Time|acf(root.test.d1.s1)|
+-----------------------------+--------------------+
|1970-01-01T08:00:00.001+08:00|                 1.0|
|1970-01-01T08:00:00.002+08:00|                 0.0|
|1970-01-01T08:00:00.003+08:00|                 3.6|
|1970-01-01T08:00:00.004+08:00|                 0.0|
|1970-01-01T08:00:00.005+08:00|                 7.0|
|1970-01-01T08:00:00.006+08:00|                 0.0|
|1970-01-01T08:00:00.007+08:00|                 3.6|
|1970-01-01T08:00:00.008+08:00|                 0.0|
|1970-01-01T08:00:00.009+08:00|                 1.0|
+-----------------------------+--------------------+
```
### Zeppelin示例
链接: <http://101.6.15.213:18181/#/notebook/2GC91M5DY>
