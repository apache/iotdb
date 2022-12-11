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
### 设备视图下的排序
在设备视图下支持4种排序的模式子句
1. ``Order by device``: 按照device设备列名进行排序，排序方式为字典序排序，在这种情况下，包含相同列名的设备会以聚类的形式进行展示，便于观察相同设备下的差距

2. ``Order by time``: 按照time时间列进行排序，此时不同的设备会按照time的优先级 被打乱排序

3. ``Order by device,time``: 按照device设备列名进行排序，设备名相同的序列会通过time时间列进行排序

4. ``Order by time,device``: 按照time时间列进行排序，时间列相同的序列会通过device设备列名进行排序

> 为了保证结果的可观性，当不使用order by进行规定时，会为设备视图提供默认的排序方式，其中默认的排序视图为``order by device,time``，默认的排序顺序为`asc`。
仅使用`ALIGN BY DEVICE`时，结果集默认先按照 `Device` 列升序排列，在每个 `Device` 内再按照 `Time` 列升序排序。

在设备视图下支持两种排序键，`DEVICE`和`TIME`，靠前的排序键为主排序键。

当主排序键为`DEVICE`时，结果集的格式与默认情况类似：先按照`Device`列对结果进行排列，在`Device`内按照`Time`进行排序。示例代码如下：
```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 order by device desc,time asc align by device;
```
执行结果：

```
+-----------------------------+-----------------+--------+------+-----------+
|                         Time|           Device|hardware|status|temperature|
+-----------------------------+-----------------+--------+------+-----------+
|1970-01-01T08:00:00.001+08:00|root.ln.wf02.wt02|      v1|  true|       null|
|1970-01-01T08:00:00.002+08:00|root.ln.wf02.wt02|      v2| false|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf01.wt01|    null|  true|      25.96|
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|    null|  true|      24.36|
+-----------------------------+-----------------+--------+------+-----------+
Total line number = 6
```
主排序键为`Time`时，结果集会先按照`Time`进行排列，在`Time`相等的组内按照`Device`排序。
示例代码如下：
```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 order by time asc,device desc align by device;
```
执行结果：
```
+-----------------------------+-----------------+--------+------+-----------+
|                         Time|           Device|hardware|status|temperature|
+-----------------------------+-----------------+--------+------+-----------+
|1970-01-01T08:00:00.001+08:00|root.ln.wf02.wt02|      v1|  true|       null|
|1970-01-01T08:00:00.002+08:00|root.ln.wf02.wt02|      v2| false|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf01.wt01|    null|  true|      25.96|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|    null|  true|      24.36|
+-----------------------------+-----------------+--------+------+-----------+
Total line number = 6
```
当没有显式指定时，主排序键默认为`Device`，排序顺序默认为`ASC`，实例代码如下：
```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 order by device asc,time asc align by device;
```
结果如图所示，可以看出，`ORDER BY DEVICE ASC,TIME ASC`就是默认情况下的排序方式，由于`ASC`是默认排序顺序，此处可以省略。
```
+-----------------------------+-----------------+--------+------+-----------+
|                         Time|           Device|hardware|status|temperature|
+-----------------------------+-----------------+--------+------+-----------+
|2017-11-01T00:00:00.000+08:00|root.ln.wf01.wt01|    null|  true|      25.96|
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|    null|  true|      24.36|
|1970-01-01T08:00:00.001+08:00|root.ln.wf02.wt02|      v1|  true|       null|
|1970-01-01T08:00:00.002+08:00|root.ln.wf02.wt02|      v2| false|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
+-----------------------------+-----------------+--------+------+-----------+
Total line number = 6
```
 同样，可以在时间分区的`GROUP BY`中使用`ALIGN BY DEVICE`和`ORDER BY`子句，对按照时间分区聚合后的结果进行排序，示例代码如下所示：
```sql
select count(*) from root.ln.** group by ((2017-11-01T00:00:00.000+08:00,2017-11-01T00:03:00.000+08:00],1m) order by device asc,time asc align by device
```
执行结果：
```
+-----------------------------+-----------------+---------------+-------------+------------------+
|                         Time|           Device|count(hardware)|count(status)|count(temperature)|
+-----------------------------+-----------------+---------------+-------------+------------------+
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|           null|            1|                 1|
|2017-11-01T00:02:00.000+08:00|root.ln.wf01.wt01|           null|            0|                 0|
|2017-11-01T00:03:00.000+08:00|root.ln.wf01.wt01|           null|            0|                 0|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|              1|            1|              null|
|2017-11-01T00:02:00.000+08:00|root.ln.wf02.wt02|              0|            0|              null|
|2017-11-01T00:03:00.000+08:00|root.ln.wf02.wt02|              0|            0|              null|
+-----------------------------+-----------------+---------------+-------------+------------------+
Total line number = 6
```
 