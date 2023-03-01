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

## 机器学习

### AR

#### 函数简介

本函数用于学习数据的自回归模型系数。

**函数名:**  AR

**输入序列:** 仅支持单个输入序列，类型为 INT32 / INT64 / FLOAT / DOUBLE。

**参数:**

- `p`：自回归模型的阶数。默认为1。

**输出序列:** 输出单个序列，类型为 DOUBLE。第一行对应模型的一阶系数，以此类推。

**提示:** 

- `p`应为正整数。

- 序列中的大部分点为等间隔采样点。
- 序列中的缺失点通过线性插值进行填补后用于学习过程。

#### 使用示例

##### 指定阶数

输入序列：

```
+-----------------------------+---------------+
|                         Time|root.test.d0.s0|
+-----------------------------+---------------+
|2020-01-01T00:00:01.000+08:00|           -4.0|
|2020-01-01T00:00:02.000+08:00|           -3.0|
|2020-01-01T00:00:03.000+08:00|           -2.0|
|2020-01-01T00:00:04.000+08:00|           -1.0|
|2020-01-01T00:00:05.000+08:00|            0.0|
|2020-01-01T00:00:06.000+08:00|            1.0|
|2020-01-01T00:00:07.000+08:00|            2.0|
|2020-01-01T00:00:08.000+08:00|            3.0|
|2020-01-01T00:00:09.000+08:00|            4.0|
+-----------------------------+---------------+
```

用于查询的 SQL 语句：

```sql
select ar(s0,"p"="2") from root.test.d0
```

输出序列：

```
+-----------------------------+---------------------------+
|                         Time|ar(root.test.d0.s0,"p"="2")|
+-----------------------------+---------------------------+
|1970-01-01T08:00:00.001+08:00|                     0.9429|
|1970-01-01T08:00:00.002+08:00|                    -0.2571|
+-----------------------------+---------------------------+
```

