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
# IntegralAvg

## 函数简介

本函数用于计算时间序列的函数均值，即在相同时间单位下的数值积分除以序列总的时间跨度。更多关于数值积分计算的信息请参考`Integral`函数。

**函数名：** INTEGRALAVG

**输入序列：** 仅支持单个输入序列，类型为 INT32 / INT64 / FLOAT / DOUBLE。

**输出序列：** 输出单个序列，类型为DOUBLE，序列仅包含一个时间戳为0、值为时间加权平均结果的数据点。

**提示：**

+ 时间加权的平均值等于在任意时间单位`unit`下计算的数值积分（即折线图中每相邻两个数据点和时间轴形成的直角梯形的面积之和），
  除以相同时间单位下输入序列的时间跨度，其值与具体采用的时间单位无关，默认与IoTDB时间单位一致。

+ 数据中的`NaN`将会被忽略。折线将以临近两个有值数据点为准。

+ 输入序列为空时，函数输出结果为0；仅有一个数据点时，输出结果为该点数值。

## 使用示例

输入序列：
```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:01.000+08:00|              1|
|2020-01-01T00:00:02.000+08:00|              2|
|2020-01-01T00:00:03.000+08:00|              5|
|2020-01-01T00:00:04.000+08:00|              6|
|2020-01-01T00:00:05.000+08:00|              7|
|2020-01-01T00:00:08.000+08:00|              8|
|2020-01-01T00:00:09.000+08:00|            NaN|
|2020-01-01T00:00:10.000+08:00|             10|
+-----------------------------+---------------+
```


用于查询的SQL语句：

```sql
select integralavg(s1) from root.test.d1 where time <= 2020-01-01 00:00:10
```

输出序列：
```
+-----------------------------+----------------------------+
|                         Time|integralavg(root.test.d1.s1)|
+-----------------------------+----------------------------+
|1970-01-01T08:00:00.000+08:00|                        5.75|
+-----------------------------+----------------------------+
```

其计算公式为：
$$\frac{1}{2}[(1+2)\times 1 + (2+5) \times 1 + (5+6) \times 1 + (6+7) \times 1 + (7+8) \times 3 + (8+10) \times 2] / 10 = 5.75$$
