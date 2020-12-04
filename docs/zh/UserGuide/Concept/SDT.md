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

IoTDB允许您在创建时间序列时指定SDT的属性，并支持以下三个属性：

* CompDev (压缩偏差)

CompDev是SDT中最重要的参数，代表当前样本和当前线性之间的最大差趋势。

* CompMin (最小压缩间隔)

CompMin主要用于减少噪点。 CompMin测量两个存储的数据点之间的时间距离，如果当前点的时间到上一个存储的点的时间距离小于或等于compMin，
无论压缩偏差值，都不会存储当前数据点。

* CompMax (Compression Maximum)

CompMax用于定期检查上一个存储的点到当前点之间的时间距离。它测量存储点之间的时间差。如果当前点时间到上一个存储点的时间距离
大于或等于compMax，无论压缩偏差值，都会存储当前数据点。

SDT的指定语法详见本文[5.4节](../Operation%20Manual/SQL%20Reference.md)。
