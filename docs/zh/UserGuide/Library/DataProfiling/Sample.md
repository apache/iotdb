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
# Sample
## 函数简介
本函数对输入序列进行采样，即从输入序列中选取指定数量的数据点并输出。目前，本函数支持两种采样方法：**蓄水池采样法（reservoir sampling）** 对数据进行随机采样，所有数据点被采样的概率相同；**等距采样法（isometric sampling）** 按照相等的索引间隔对数据进行采样。

**函数名：** SAMPLE

**输入序列：** 仅支持单个输入序列，类型可以是任意的。

**参数：**

+ `method`：采样方法，取值为'reservoir'或'isometric'。在缺省情况下，采用蓄水池采样法。
+ `k`：采样数，它是一个正整数，在缺省情况下为1。

**输出序列：** 输出单个序列，类型与输入序列相同。该序列的长度为采样数，序列中的每一个数据点都来自于输入序列。

**提示：** 如果采样数大于序列长度，那么输入序列中所有的数据点都会被输出。

## 使用示例


### 蓄水池采样

当`method`参数为'reservoir'或缺省时，采用蓄水池采样法对输入序列进行采样。由于该采样方法具有随机性，下面展示的输出序列只是一种可能的结果。

输入序列：

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:01.000+08:00|            1.0|
|2020-01-01T00:00:02.000+08:00|            2.0|
|2020-01-01T00:00:03.000+08:00|            3.0|
|2020-01-01T00:00:04.000+08:00|            4.0|
|2020-01-01T00:00:05.000+08:00|            5.0|
|2020-01-01T00:00:06.000+08:00|            6.0|
|2020-01-01T00:00:07.000+08:00|            7.0|
|2020-01-01T00:00:08.000+08:00|            8.0|
|2020-01-01T00:00:09.000+08:00|            9.0|
|2020-01-01T00:00:10.000+08:00|           10.0|
+-----------------------------+---------------+
```


用于查询的SQL语句：

```sql
select sample(s1,'method'='reservoir','k'='5') from root.test.d1
```

输出序列：

```
+-----------------------------+------------------------------------------------------+
|                         Time|sample(root.test.d1.s1, "method"="reservoir", "k"="5")|
+-----------------------------+------------------------------------------------------+
|2020-01-01T00:00:02.000+08:00|                                                   2.0|
|2020-01-01T00:00:03.000+08:00|                                                   3.0|
|2020-01-01T00:00:05.000+08:00|                                                   5.0|
|2020-01-01T00:00:08.000+08:00|                                                   8.0|
|2020-01-01T00:00:10.000+08:00|                                                  10.0|
+-----------------------------+------------------------------------------------------+
```


### 等距采样
当`method`参数为'isometric'时，采用等距采样法对输入序列进行采样。

输入序列同上，用于查询的SQL语句如下：

```sql
select sample(s1,'method'='isometric','k'='5') from root.test.d1
```

输出序列：

```
+-----------------------------+------------------------------------------------------+
|                         Time|sample(root.test.d1.s1, "method"="isometric", "k"="5")|
+-----------------------------+------------------------------------------------------+
|2020-01-01T00:00:01.000+08:00|                                                   1.0|
|2020-01-01T00:00:03.000+08:00|                                                   3.0|
|2020-01-01T00:00:05.000+08:00|                                                   5.0|
|2020-01-01T00:00:07.000+08:00|                                                   7.0|
|2020-01-01T00:00:09.000+08:00|                                                   9.0|
+-----------------------------+------------------------------------------------------+
```
