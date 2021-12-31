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
# KSigma

## 函数简介

本函数利用动态K-Sigma算法进行异常检测。在一个窗口内，与平均值的差距超过k倍标准差的数据将被视作异常并输出。

**函数名：** KSIGMA

**输入序列：** 仅支持单个输入序列，类型为 INT32 / INT64 / FLOAT / DOUBLE

**参数：** 

+ `k`：在动态K-Sigma算法中，分布异常的标准差倍数阈值，默认值为3。
+ `window`：动态K-Sigma算法的滑动窗口大小，默认值为10000。


**输出序列：** 输出单个序列，类型与输入序列相同。

**提示：** k应大于0，否则将不做输出。

## 使用示例

### 指定k

输入序列：

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:02.000+08:00|            0.0|
|2020-01-01T00:00:03.000+08:00|           50.0|
|2020-01-01T00:00:04.000+08:00|          100.0|
|2020-01-01T00:00:06.000+08:00|          150.0|
|2020-01-01T00:00:08.000+08:00|          200.0|
|2020-01-01T00:00:10.000+08:00|          200.0|
|2020-01-01T00:00:14.000+08:00|          200.0|
|2020-01-01T00:00:15.000+08:00|          200.0|
|2020-01-01T00:00:16.000+08:00|          200.0|
|2020-01-01T00:00:18.000+08:00|          200.0|
|2020-01-01T00:00:20.000+08:00|          150.0|
|2020-01-01T00:00:22.000+08:00|          100.0|
|2020-01-01T00:00:26.000+08:00|           50.0|
|2020-01-01T00:00:28.000+08:00|            0.0|
|2020-01-01T00:00:30.000+08:00|            NaN|
+-----------------------------+---------------+
```

用于查询的SQL语句：

```sql
select ksigma(s1,"k"="1.0") from root.test.d1 where time <= 2020-01-01 00:00:30
```

输出序列：

```
+-----------------------------+---------------------------------+
|Time                         |ksigma(root.test.d1.s1,"k"="3.0")|
+-----------------------------+---------------------------------+
|2020-01-01T00:00:02.000+08:00|                              0.0|
|2020-01-01T00:00:03.000+08:00|                             50.0|
|2020-01-01T00:00:26.000+08:00|                             50.0|
|2020-01-01T00:00:28.000+08:00|                              0.0|
+-----------------------------+---------------------------------+
```

