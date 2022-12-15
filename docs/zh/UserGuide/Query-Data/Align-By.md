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

# 查询对齐模式

在 IoTDB 中，查询结果集**默认按照时间对齐**，包含一列时间列和若干个值列，每一行数据各列的时间戳相同。

除按照时间对齐外，还支持以下对齐模式：

- 按设备对齐 `ALIGN BY DEVICE`

## 按设备对齐

在按设备对齐模式下，设备名会单独作为一列出现，查询结果集包含一列时间列、一列设备列和若干个值列。如果 `SELECT` 子句中选择了 `N` 列，则结果集包含 `N + 2` 列（时间列和设备名字列）。

在默认情况下，结果集按照 `Device` 进行排列，在每个 `Device` 内按照 `Time` 列升序排序。

当查询多个设备时，要求设备之间同名的列数据类型相同。

为便于理解，可以按照关系模型进行对应。设备可以视为关系模型中的表，选择的列可以视为表中的列，`Time + Device` 看做其主键。

**示例：**

```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 align by device;
```

执行如下：

```
+-----------------------------+-----------------+-----------+------+--------+
|                         Time|           Device|temperature|status|hardware|
+-----------------------------+-----------------+-----------+------+--------+
|2017-11-01T00:00:00.000+08:00|root.ln.wf01.wt01|      25.96|  true|    null|
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|      24.36|  true|    null|
|1970-01-01T08:00:00.001+08:00|root.ln.wf02.wt02|       null|  true|      v1|
|1970-01-01T08:00:00.002+08:00|root.ln.wf02.wt02|       null| false|      v2|
|2017-11-01T00:00:00.000+08:00|root.ln.wf02.wt02|       null|  true|      v2|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|       null|  true|      v2|
+-----------------------------+-----------------+-----------+------+--------+
Total line number = 6
It costs 0.012s
```
## 设备对齐模式下的排序
在设备对齐模式下，默认按照设备名的字典序升序排列，每个设备内部按照时间戳大小升序排列，可以通过 `ORDER BY` 子句调整设备列和时间列的排序优先级。

详细说明及示例见文档 [结果集排序](./Order-By.md)。 