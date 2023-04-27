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

## 死区处理

### 旋转门压缩

旋转门压缩（SDT）算法是一种死区处理算法。SDT 的计算复杂度较低，并使用线性趋势来表示大量数据。

在 IoTDB 中，SDT 在刷新到磁盘时会压缩并丢弃数据。

IoTDB 允许您在创建时间序列时指定 SDT 的属性，并支持以下三个属性：

* CompDev （Compression Deviation，压缩偏差）

CompDev 是 SDT 中最重要的参数，它表示当前样本与当前线性趋势之间的最大差值。CompDev 设置的值需要大于 0。

* CompMinTime （Compression Minimum Time Interval，最小压缩时间间隔）

CompMinTime 是测量两个存储的数据点之间的时间距离的参数，用于减少噪声。
如果当前点和最后存储的点之间的时间间隔小于或等于其值，则无论压缩偏差如何，都不会存储当前点。
默认值为 0，单位为毫秒。

* CompMaxTime （Compression Maximum Time Interval，最大压缩时间间隔）

CompMaxTime 是测量两个存储的数据点之间的时间距离的参数。
如果当前点和最后一个存储点之间的时间间隔大于或等于其值，
无论压缩偏差如何，都将存储当前点。
默认值为 9,223,372,036,854,775,807，单位为毫秒。

支持的数据类型：

* INT32（整型）
* INT64（长整型）
* FLOAT（单精度浮点数）
* DOUBLE（双精度浮点数）

SDT 的指定语法详见本文 [SQL 参考文档](../Reference/SQL-Reference.md)。

以下是使用 SDT 压缩的示例。

```
IoTDB> CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN,DEADBAND=SDT,COMPDEV=2
```

刷入磁盘和 SDT 压缩之前，结果如下所示：

```
IoTDB> SELECT s0 FROM root.sg1.d0
+-----------------------------+--------------+
|                         Time|root.sg1.d0.s0|
+-----------------------------+--------------+
|2017-11-01T00:06:00.001+08:00|             1|
|2017-11-01T00:06:00.002+08:00|             1|
|2017-11-01T00:06:00.003+08:00|             1|
|2017-11-01T00:06:00.004+08:00|             1|
|2017-11-01T00:06:00.005+08:00|             1|
|2017-11-01T00:06:00.006+08:00|             1|
|2017-11-01T00:06:00.007+08:00|             1|
|2017-11-01T00:06:00.015+08:00|            10|
|2017-11-01T00:06:00.016+08:00|            20|
|2017-11-01T00:06:00.017+08:00|             1|
|2017-11-01T00:06:00.018+08:00|            30|
+-----------------------------+--------------+
Total line number = 11
It costs 0.008s
```

刷入磁盘和 SDT 压缩之后，结果如下所示：
```
IoTDB> FLUSH
IoTDB> SELECT s0 FROM root.sg1.d0
+-----------------------------+--------------+
|                         Time|root.sg1.d0.s0|
+-----------------------------+--------------+
|2017-11-01T00:06:00.001+08:00|             1|
|2017-11-01T00:06:00.007+08:00|             1|
|2017-11-01T00:06:00.015+08:00|            10|
|2017-11-01T00:06:00.016+08:00|            20|
|2017-11-01T00:06:00.017+08:00|             1|
+-----------------------------+--------------+
Total line number = 5
It costs 0.044s
```

SDT 在刷新到磁盘时进行压缩。 SDT 算法始终存储第一个点，并且不存储最后一个点。

时间范围在 [2017-11-01T00:06:00.001, 2017-11-01T00:06:00.007] 的数据在压缩偏差内，因此被压缩和丢弃。
之所以存储时间为 2017-11-01T00:06:00.007 的数据点，是因为下一个数据点 2017-11-01T00:06:00.015 的值超过压缩偏差。
当一个数据点超过压缩偏差时，SDT 将存储上一个读取的数据点，并重新计算上下压缩边界。作为最后一个数据点，不存储时间 2017-11-01T00:06:00.018。
