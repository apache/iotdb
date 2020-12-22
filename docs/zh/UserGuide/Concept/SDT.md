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

# 旋转门压缩

旋转门压缩（SDT）算法是一种有损压缩算法。SDT的计算复杂度较低，并使用线性趋势来表示大量数据。

在IoTDB中，SDT在刷新到磁盘时会压缩并丢弃数据。

IoTDB允许您在创建时间序列时指定SDT的属性，并支持以下三个属性：

* CompDev (压缩偏差)

CompDev是SDT中最重要的参数，代表当前样本和当前线性之间的最大差趋势。

* CompMin (最小压缩间隔)

CompMin主要用于减少噪点。 CompMin测量两个存储的数据点之间的时间距离，如果当前点的时间到上一个存储的点的时间距离小于或等于compMin， 无论压缩偏差值，都不会存储当前数据点。

* CompMax (最大压缩间隔)

CompMax用于定期检查上一个存储的点到当前点之间的时间距离。它测量存储点之间的时间差。如果当前点时间到上一个存储点的时间距离 大于或等于compMax，无论压缩偏差值，都会存储当前数据点。

支持的数据类型:
* INT32（整型）
* INT64（长整型）
* FLOAT（单精度浮点数）
* DOUBLE（双精度浮点数）

SDT的指定语法详见本文[5.4节](../Operation%20Manual/SQL%20Reference.md)。

以下是使用SDT压缩的示例。

```
IoTDB> CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=2
```

刷入磁盘和SDT压缩之前，结果如下所示：

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

刷入磁盘和SDT压缩之后，结果如下所示：
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

SDT在刷新到磁盘时进行压缩。 SDT算法始终存储第一个点，并且不存储最后一个点。

时间范围在 [2017-11-01T00:06:00.001, 2017-11-01T00:06:00.007] 的数据在压缩偏差内，因此被压缩和丢弃。
之所以存储时间为 2017-11-01T00:06:00.007 的数据点，是因为下一个数据点 2017-11-01T00:06:00.015 的值超过压缩偏差。
当一个数据点超过压缩偏差时，SDT将存储上一个读取的数据点，并重新计算上下压缩边界。作为最后一个数据点，不存储时间 2017-11-01T00:06:00.018。