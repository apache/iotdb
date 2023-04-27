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

## 连续查询（Continuous Query, CQ）

我们可以通过 SQL 语句注册、或卸载一个 CQ 实例，以及查询到所有已经注册的 CQ 配置信息。

注意，目前连续查询尚未对分布式场景进行适配。敬请期待分布式版本。

### SQL 语句

#### 创建 CQ

##### 语法

```sql
CREATE (CONTINUOUS QUERY | CQ) <cq_id> 
[RESAMPLE EVERY <every_interval> FOR <for_interval> BOUNDARY <execution_boundary_time>] 
BEGIN 
SELECT <function>(<path_suffix>) INTO <full_path> | <node_name>
FROM <path_prefix>
GROUP BY time(<group_by_interval>) [, level = <level>] 
END
```

其中：

* `<cq_id>` 指定 CQ 全局唯一的 id。
* `<every_interval>` 指定查询执行时间间隔，支持 ns、us、ms、s、m、h、d、w 等单位，其值不应小于用户所配置的 `continuous_query_min_every_interval` 值。可选择指定。
* `<for_interval>` 指定每次查询的窗口大小，即查询时间范围为`[now() - <for_interval>, now())`，其中 `now()` 指查询时的时间戳。支持 ns、us、ms、s、m、h、d、w 等单位。可选择指定。 
* `<execution_boundary_time>` 是一个日期参数，表示**第一个窗口的起始时间**。
  * `<execution_boundary_time>` 可早于、等于、晚于**当前时间**。
  * 该参数可选择指定，不指定的情况下等价于输入 `BOUNDARY now()`。
  * **第一个窗口的结束时间**为  `<execution_boundary_time> + <for_interval>`。
  * 第 ` i (1 <= i)` 个窗口的**开始时间** `<execution_boundary_time> + <for_interval> + （i - 1） * <every_interval>`。
  * 第 ` i (1 <= i)` 个窗口的**结束时间** `<execution_boundary_time> + <for_interval> + i * <every_interval>`。
  * 如果**当前时间**小于等于**第一个窗口的结束时间** ，那么连续查询的第一个执行时刻为**第一个窗口的结束时间**。
  * 如果**当前时间**大于**第一个窗口的结束时间**，那么连续查询的第一个执行时刻为**第一个**大于等于**当前时间**的**窗口结束时间**。
  * 每一个执行时刻执行的**查询时间范围**为`[now() - <for_interval>, now())`。

* `<function>` 指定聚合函数，目前支持 `count`, `sum`, `avg`, `last_value`, `first_value`, `min_time`, `max_time`, `min_value`, `max_value` 等。
* `<path_prefix>` 与 `<path_suffix>` 拼接成完整的查询原时间序列。
* `<full_path>` 或 `<node_name>` 指定将查询出的数据写入的结果序列路径。
* `<group_by_interval>` 指定时间分组长度，支持 ns、us、ms、s、m、h、d、w、mo、y 等单位。
* `<level>`指按照序列第 `<level>` 层分组，将第 `<level>` 层同名的所有序列聚合。Group By Level 语句的具体语义及 `<level>` 的定义见 [路径层级分组聚合](../Query-Data/Aggregate-Query.html#聚合查询)。

注：

* `<for_interval>`, `<every_interval>` 可选择指定。如果用户没有指定其中的某一项，则未指定项的值按照`<group_by_interval>` 处理。
    * `<every_interval>`，`<for_interval>`，`<group_by_interval>` 的值均应大于 0。
    * `<group_by_interval>` 的值应小于`<for_interval>`的值，否则系统会按照等于`<for_interval>`的值处理。 
    * 用户应当结合实际需求指定合适的 `<for_interval>` 与 `<every_interval>`。
      * 若 `<for_interval>` 大于 `<every_interval>`，每次的查询窗口会有部分数据重叠，从查询性能角度这种配置不被建议。
      * 若 `<for_interval>` 小于 `<every_interval>`，每次的查询窗口之间可能会有未覆盖到的数据。
*  对于结果序列路径
     * 用户可以选择指定`<full_path>`，即以 `root` 开头的完整的时间序列路径，用户可以在路径中使用 `${x}` 变量来表示原始时间序列中 `level = x` 的节点名称，`x`应当大于等于 1 且小于等于 `<level>` 值
       （若未指定 `level`，则应小于等于 `<path_prefix>` 长度）。
    * 用户也可以仅指定`<node_name>`，即生成时间序列路径的最后一个结点名。
      * 若用户指定  `<level> = l`，则系统生成的结果时间序列路径为 `root.${1}. ... .${l}.<node_name>`
      * 若用户未指定 `<level>`，令原始时间序列最大层数为 `L`， 
      则系统生成的结果时间序列路径为 `root.${1}. ... .${L - 1}.<node_name>`。

##### 示例

###### 原始时间序列
````
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                   timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf02.wt02.temperature| null|      root.ln|   FLOAT| GORILLA|     SNAPPY|null|      null|
|root.ln.wf02.wt01.temperature| null|      root.ln|   FLOAT| GORILLA|     SNAPPY|null|      null|
|root.ln.wf01.wt02.temperature| null|      root.ln|   FLOAT| GORILLA|     SNAPPY|null|      null|
|root.ln.wf01.wt01.temperature| null|      root.ln|   FLOAT| GORILLA|     SNAPPY|null|      null|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
````

````
+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|                         Time|root.ln.wf02.wt02.temperature|root.ln.wf02.wt01.temperature|root.ln.wf01.wt02.temperature|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|2021-05-11T22:18:14.598+08:00|                        121.0|                         72.0|                        183.0|                        115.0|
|2021-05-11T22:18:19.941+08:00|                          0.0|                         68.0|                         68.0|                        103.0|
|2021-05-11T22:18:24.949+08:00|                        122.0|                         45.0|                         11.0|                         14.0|
|2021-05-11T22:18:29.967+08:00|                         47.0|                         14.0|                         59.0|                        181.0|
|2021-05-11T22:18:34.979+08:00|                        182.0|                        113.0|                         29.0|                        180.0|
|2021-05-11T22:18:39.990+08:00|                         42.0|                         11.0|                         52.0|                         19.0|
|2021-05-11T22:18:44.995+08:00|                         78.0|                         38.0|                        123.0|                         52.0|
|2021-05-11T22:18:49.999+08:00|                        137.0|                        172.0|                        135.0|                        193.0|
|2021-05-11T22:18:55.003+08:00|                         16.0|                        124.0|                        183.0|                         18.0|
+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
````

###### 结果序列配置举例说明

对于以上原始时间序列，若用户指定查询聚合层级为 `2`，聚合函数为 `avg`，
用户可以在 `INTO` 语句中仅指定生成序列的最后一个结点名，若用户将其指定为 `temperature_avg`，则系统生成的完整路径为 `root.${1}.${2}.temperature_avg`。
用户也可以在 `INTO` 语句中指定完整写入路径，用户可将其指定为 `root.${1}.${2}.temperature_avg`、`root.ln_cq.${2}.temperature_avg`、`root.${1}_cq.${2}.temperature_avg`、`root.${1}.${2}_cq.temperature_avg`等，
也可以按需要指定为 `root.${2}.${1}.temperature_avg` 等其它形式。
需要注意的是，`${x}` 中的 `x` 应当大于等于 `1` 且小于等于 `<level>` 值
（若未指定 `<level>`，则应小于等于 `<path_prefix>` 层级）。在上例中，`x` 应当小于等于 `2`。

###### 创建 `cq1`
````sql
CREATE CONTINUOUS QUERY cq1 
BEGIN 
  SELECT max_value(temperature) 
  INTO temperature_max 
  FROM root.ln.*.* 
  GROUP BY time(10s) 
END
````

每隔 10s 查询 `root.ln.*.*.temperature` 在前 10s 内的最大值（结果以 10s 为一组），
将结果写入到 `root.${1}.${2}.${3}.temperature_max` 中，
结果将产生 4 条新序列：

````
+---------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                       timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+---------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf02.wt02.temperature_max| null|      root.ln|   FLOAT| GORILLA|     SNAPPY|null|      null|
|root.ln.wf02.wt01.temperature_max| null|      root.ln|   FLOAT| GORILLA|     SNAPPY|null|      null|
|root.ln.wf01.wt02.temperature_max| null|      root.ln|   FLOAT| GORILLA|     SNAPPY|null|      null|
|root.ln.wf01.wt01.temperature_max| null|      root.ln|   FLOAT| GORILLA|     SNAPPY|null|      null|
+---------------------------------+-----+-------------+--------+--------+-----------+----+----------+
````
````
+-----------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+
|                         Time|root.ln.wf02.wt02.temperature_max|root.ln.wf02.wt01.temperature_max|root.ln.wf01.wt02.temperature_max|root.ln.wf01.wt01.temperature_max|
+-----------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+
|2021-05-11T22:18:16.964+08:00|                            122.0|                             68.0|                             68.0|                            103.0|
|2021-05-11T22:18:26.964+08:00|                            182.0|                            113.0|                             59.0|                            181.0|
|2021-05-11T22:18:36.964+08:00|                             78.0|                             38.0|                            123.0|                             52.0|
|2021-05-11T22:18:46.964+08:00|                            137.0|                            172.0|                            183.0|                            193.0|
+-----------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+
````
###### 创建 `cq2`
````sql
CREATE CONTINUOUS QUERY cq2 
RESAMPLE EVERY 20s FOR 20s 
BEGIN 
  SELECT avg(temperature) 
  INTO temperature_avg 
  FROM root.ln.*.* 
  GROUP BY time(10s), level=2 
END
````

每隔 20s 查询 `root.ln.*.*.temperature` 在前 20s 内的平均值（结果以 10s 为一组，按照第 2 层节点分组），
将结果写入到 `root.${1}.${2}.temperature_avg` 中。
结果将产生如下两条新序列，
其中 `root.ln.wf02.temperature_avg` 由 `root.ln.wf02.wt02.temperature` 和 `root.ln.wf02.wt01.temperature` 聚合计算生成，
`root.ln.wf01.temperature_avg` 由 `root.ln.wf01.wt02.temperature` 和 `root.ln.wf01.wt01.temperature` 聚合计算生成。

````
+----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                  timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf02.temperature_avg| null|      root.ln|  DOUBLE| GORILLA|     SNAPPY|null|      null|
|root.ln.wf01.temperature_avg| null|      root.ln|  DOUBLE| GORILLA|     SNAPPY|null|      null|
+----------------------------+-----+-------------+--------+--------+-----------+----+----------+
````
````
+-----------------------------+----------------------------+----------------------------+
|                         Time|root.ln.wf02.temperature_avg|root.ln.wf01.temperature_avg|
+-----------------------------+----------------------------+----------------------------+
|2021-05-11T22:18:16.969+08:00|                       58.75|                        49.0|
|2021-05-11T22:18:26.969+08:00|                        89.0|                      112.25|
|2021-05-11T22:18:36.969+08:00|                       42.25|                        61.5|
|2021-05-11T22:18:46.969+08:00|                      112.25|                      132.25|
+-----------------------------+----------------------------+----------------------------+
````
###### 创建 `cq3`
````sql
CREATE CONTINUOUS QUERY cq3 
RESAMPLE EVERY 20s FOR 20s 
BEGIN 
  SELECT avg(temperature) 
  INTO root.ln_cq.${2}.temperature_avg 
  FROM root.ln.*.* 
  GROUP BY time(10s), level=2 
END
````
查询模式与 cq2 相同，在这个例子中，用户自行指定结果写入到 `root.ln_cq.${2}.temperature_avg` 中。
结果将产生如下两条新序列，
其中 `root.ln_cq.wf02.temperature_avg` 由 `root.ln.wf02.wt02.temperature` 和 `root.ln.wf02.wt01.temperature` 聚合计算生成，
`root.ln_cq.wf01.temperature_avg` 由 `root.ln.wf01.wt02.temperature` 和 `root.ln.wf01.wt01.temperature` 聚合计算生成。

````
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                     timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln_cq.wf02.temperature_avg| null|   root.ln_cq|  DOUBLE| GORILLA|     SNAPPY|null|      null|
|root.ln_cq.wf01.temperature_avg| null|   root.ln_cq|  DOUBLE| GORILLA|     SNAPPY|null|      null|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
````
````
+-----------------------------+-------------------------------+-------------------------------+
|                         Time|root.ln_cq.wf02.temperature_avg|root.ln_cq.wf01.temperature_avg|
+-----------------------------+-------------------------------+-------------------------------+
|2021-05-11T22:18:16.971+08:00|                          58.75|                           49.0|
|2021-05-11T22:18:26.971+08:00|                           89.0|                         112.25|
|2021-05-11T22:18:36.971+08:00|                          42.25|                           61.5|
|2021-05-11T22:18:46.971+08:00|                         112.25|                         132.25|
+-----------------------------+-------------------------------+-------------------------------+
````

###### 创建 `cq4`

````sql
CREATE CONTINUOUS QUERY cq4 
RESAMPLE EVERY 20s FOR 20s BOUNDARY 2022-01-14T23:00:00.000+08:00 
BEGIN 
  SELECT avg(temperature) 
  INTO root.ln_cq.${2}.temperature_avg 
  FROM root.ln.*.* GROUP BY time(10s), level=2 
END
````

这个例子与创建 cq3 几乎完全相同。不同的是，在这个例子中用户自行指定了 `BOUNDARY 2022-01-14T23:00:00.000+08:00 ` 。

注意这个 CQ 的第一个执行时刻大于例子中的时间，因此 `2022-01-14T23:00:20.000+08:00` 为第一个执行时刻。递推地，`2022-01-14T23:00:40.000+08:00` 为第二个执行时刻，`2022-01-14T23:01:00.000+08:00` 为第三个执行时刻…… 

第一个执行时刻执行的 SQL 语句为 `select avg(temperature) from root.ln.*.* group by ([2022-01-14T23:00:00.000+08:00, 2022-01-14T23:00:20.000+08:00), 10s), level = 2`。

第二个执行时刻执行的 SQL 语句为 `select avg(temperature) from root.ln.*.* group by ([2022-01-14T23:00:20.000+08:00, 2022-01-14T23:00:40.000+08:00), 10s), level = 2`。

第三个执行时刻执行的 SQL 语句为 `select avg(temperature) from root.ln.*.* group by ([2022-01-14T23:00:40.000+08:00, 2022-01-14T23:01:00.000+08:00), 10s), level = 2`。

……


#### 展示 CQ 信息

##### 语法
````sql
SHOW (CONTINUOUS QUERIES | CQS) 
````
##### 结果示例
````
+-------+--------------+------------+-------------+----------------------------------------------------------------------------------------+-----------------------------------+
|cq name|every interval|for interval|     boundary|                                                                               query sql|                        target path|
+-------+--------------+------------+-------------+----------------------------------------------------------------------------------------+-----------------------------------+
|    cq1|         10000|       10000|1642166102238|     select max_value(temperature) from root.ln.*.* group by ([now() - 10s, now()), 10s)|root.${1}.${2}.${3}.temperature_max|
|    cq3|         20000|       20000|1642166118339|select avg(temperature) from root.ln.*.* group by ([now() - 20s, now()), 10s), level = 2|    root.ln_cq.${2}.temperature_avg|
|    cq2|         20000|       20000|1642166111493|select avg(temperature) from root.ln.*.* group by ([now() - 20s, now()), 10s), level = 2|     root.${1}.${2}.temperature_avg|
|    cq4|         20000|       20000|1642172400000|select avg(temperature) from root.ln.*.* group by ([now() - 20s, now()), 10s), level = 2|    root.ln_cq.${2}.temperature_avg|
+-------+--------------+------------+-------------+----------------------------------------------------------------------------------------+-----------------------------------+
````
#### 删除 CQ
##### 语法
````sql
DROP (CONTINUOUS QUERY | CQ) <cq_id> 
````
##### 示例
````sql
DROP CONTINUOUS QUERY cq3
````

``` sql
DROP CQ cq3
```

### 系统参数配置

| 参数名          | 描述           |  数据类型| 默认值 |
| :---------------------------------- |-------- | ----| -----|
| `continuous_query_execution_thread` | 执行连续查询任务的线程池的线程数 | int | max(1, CPU 核数 / 2)|
| `max_pending_continuous_query_tasks` | 队列中连续查询最大任务堆积数 | int | 64|
| `continuous_query_min_every_interval` | 连续查询执行时间间隔的最小值 | duration | 1s|

