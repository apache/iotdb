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

# 连续查询（Continuous Query, CQ）

我们可以通过 SQL 语句注册、或卸载一个 CQ 实例，以及查询到所有已经注册的 CQ 配置信息。

## SQL 语句

### 创建 CQ

#### 语法

```sql
CREATE CONTINUOUS QUERY <cq_id> 
[RESAMPLE EVERY <every_interval> FOR <for_interval>] 
BEGIN 
SELECT <function>(<path_suffix>) INTO <full_path> | <node_name>
FROM <path_prefix>
GROUP BY time(<group_by_interval>) [, level = <level>] 
END
```

其中：

* `<cq_id>` 指定 CQ 全局唯一的 id。
* `<every_interval>` 指定查询执行时间间隔，支持 ns、us、ms、s、m、h、d、w 等单位，其值不应小于用户所配置的 `continuous_query_min_every_interval` 值。可选择指定。
* `<for_interval>` 指定每次查询的窗口大小，即查询时间范围为`[now() - <for_interval>, now())`，其中 `now()` 指查询时的时间戳。支持 ns、us、ms、s、m、h
  、d、w 等单位。可选择指定。 
* `<function>` 指定聚合函数，目前支持 `count`, `sum`, `avg`, `last_value`, `first_value`, `min_time`, `max_time`, `min_value`, `max_value` 等。
* `<path_prefix>` 与 `<path_suffix>` 拼接成完整的查询原时间序列。
* `<full_path>` 或 `<node_name>` 指定将查询出的数据写入的结果序列路径。
* `<group_by_interval>` 指定时间分组长度，支持 ns、us、ms、s、m、h
  、d、w、mo、y 等单位。
* `<level>`指按照序列第 `<level>` 层分组，将第 `<level>` 层同名的所有序列聚合。Group By Level 语句的具体语义及 `<level>` 的定义见 [路径层级分组聚合](../IoTDB-SQL-Language/DML-Data-Manipulation-Language.md)。

注：

* `<for_interval>`,`<every_interval>` 可选择指定。如果用户没有指定其中的某一项，则未指定项的值按照`<group_by_interval>` 处理。
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

#### 示例

##### 原始时间序列
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

##### 结果序列配置举例说明

对于以上原始时间序列，若用户指定查询聚合层级为 `2`，聚合函数为 `avg`，
用户可以在 `INTO` 语句中仅指定生成序列的最后一个结点名，若用户将其指定为 `temperature_avg`，则系统生成的完整路径为 `root.${1}.${2}.temperature_avg`。
用户也可以在 `INTO` 语句中指定完整写入路径，用户可将其指定为 `root.${1}.${2}.temperature_avg`、`root.ln_cq.${2}.temperature_avg`、`root.${1}_cq.${2}.temperature_avg`、`root.${1}.${2}_cq.temperature_avg`等，
也可以按需要指定为 `root.${2}.${1}.temperature_avg` 等其它形式。
需要注意的是，`${x}` 中的 `x` 应当大于等于 `1` 且小于等于 `<level>` 值
（若未指定 `<level>`，则应小于等于 `<path_prefix>` 层级）。在上例中，`x` 应当小于等于 `2`。

##### 创建 `cq1`
````
CREATE CONTINUOUS QUERY cq1 BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.* GROUP BY time(10s) END
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
##### 创建 `cq2`
````
CREATE CONTINUOUS QUERY cq2 RESAMPLE EVERY 20s FOR 20s BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.*.* GROUP BY time(10s), level=2 END
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
##### 创建 `cq3`
````
CREATE CONTINUOUS QUERY cq3 RESAMPLE EVERY 20s FOR 20s BEGIN SELECT avg(temperature) INTO root.ln_cq.${2}.temperature_avg FROM root.ln.*.* GROUP BY time(10s), level=2 END
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

### 展示 CQ 信息
#### 语法
````
SHOW CONTINUOUS QUERIES 
````
#### 结果示例
````
+-------+--------------+------------+----------------------------------------------------------------------------------------+-----------------------------------+
|cq name|every interval|for interval|                                                                               query sql|                        target path|
+-------+--------------+------------+----------------------------------------------------------------------------------------+-----------------------------------+
|    cq1|         10000|       10000|     select max_value(temperature) from root.ln.*.* group by ([now() - 10s, now()), 10s)|root.${1}.${2}.${3}.temperature_max|
|    cq3|         20000|       20000|select avg(temperature) from root.ln.*.* group by ([now() - 20s, now()), 10s), level = 2|    root.ln_cq.${2}.temperature_avg|
|    cq2|         20000|       20000|select avg(temperature) from root.ln.*.* group by ([now() - 20s, now()), 10s), level = 2|     root.${1}.${2}.temperature_avg|
+-------+--------------+------------+----------------------------------------------------------------------------------------+-----------------------------------+
````
### 删除 CQ
#### 语法
````
DROP CONTINUOUS QUERY <cq_id> 
````
#### 示例
````
DROP CONTINUOUS QUERY cq3
````

## 系统参数配置
| 参数名          | 描述           |  数据类型| 默认值 |
| :---------------------------------- |-------- | ----| -----|
| `continuous_query_execution_thread` | 执行连续查询任务的线程池的线程数 | int | max(1, CPU 核数 / 2)|
| `max_pending_continuous_query_tasks` | 队列中连续查询最大任务堆积数 | int | 64|
| `continuous_query_min_every_interval` | 连续查询执行时间间隔的最小值 | duration | 1s|

