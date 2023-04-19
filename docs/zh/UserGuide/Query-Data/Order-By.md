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

## 结果集排序

### 时间对齐模式下的排序
IoTDB的查询结果集默认按照时间对齐，可以使用`ORDER BY TIME`的子句指定时间戳的排列顺序。示例代码如下：
```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 order by time desc;
```
执行结果：

```
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf02.wt02.hardware|root.ln.wf02.wt02.status|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
|2017-11-01T00:01:00.000+08:00|                        v2|                    true|                        24.36|                    true|
|2017-11-01T00:00:00.000+08:00|                        v2|                    true|                        25.96|                    true|
|1970-01-01T08:00:00.002+08:00|                        v2|                   false|                         null|                    null|
|1970-01-01T08:00:00.001+08:00|                        v1|                    true|                         null|                    null|
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
Total line number = 4
```
### 设备对齐模式下的排序
当使用`ALIGN BY DEVICE`查询对齐模式下的结果集时，可以使用`ORDER BY`子句对返回的结果集顺序进行规定。

在设备对齐模式下支持4种排序模式的子句,其中包括两种排序键，`DEVICE`和`TIME`，靠前的排序键为主排序键，每种排序键都支持`ASC`和`DESC`两种排列顺序。
1. ``ORDER BY DEVICE``: 按照设备名的字典序进行排序，排序方式为字典序排序，在这种情况下，相同名的设备会以组的形式进行展示。

2. ``ORDER BY TIME``: 按照时间戳进行排序，此时不同的设备对应的数据点会按照时间戳的优先级被打乱排序。

3. ``ORDER BY DEVICE,TIME``: 按照设备名的字典序进行排序，设备名相同的数据点会通过时间戳进行排序。

4. ``ORDER BY TIME,DEVICE``: 按照时间戳进行排序，时间戳相同的数据点会通过设备名的字典序进行排序。

> 为了保证结果的可观性，当不使用`ORDER BY`子句，仅使用`ALIGN BY DEVICE`时，会为设备视图提供默认的排序方式。其中默认的排序视图为``ORDER BY DEVCE,TIME``，默认的排序顺序为`ASC`，
> 即结果集默认先按照设备名升序排列，在相同设备名内再按照时间戳升序排序。


当主排序键为`DEVICE`时，结果集的格式与默认情况类似：先按照设备名对结果进行排列，在相同的设备名下内按照时间戳进行排序。示例代码如下：
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
主排序键为`Time`时，结果集会先按照时间戳进行排序，在时间戳相等时按照设备名排序。
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
当没有显式指定时，主排序键默认为`Device`，排序顺序默认为`ASC`，示例代码如下：
```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 align by device;
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
同样，可以在聚合查询中使用`ALIGN BY DEVICE`和`ORDER BY`子句，对聚合后的结果进行排序，示例代码如下所示：
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