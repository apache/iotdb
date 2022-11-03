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

# 查询结果集格式

在 IoTDB 中，查询结果集默认按照时间对齐，同时支持另外一种结果返回形式：

- 设备时间对齐 `align by device`

注意：对齐方式子句只能用于查询语句句尾。

## 按设备对齐

在 `align by device` 对齐方式下，设备名会单独作为一列出现。如果 select 子句中有 `n` 列，最终结果就会有该 `n + 2` 列（时间列和设备名字列）。

> 注意：
>
> 1.可以将 'align by device' 的结果看成一张关系表，`Time + Device` 看做其主键。
>
> 2.结果集首先按照 `Device` 进行排序,然后按照 `Time` 排序。

SQL 形如：

```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 align by device;
```

结果如下：

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
