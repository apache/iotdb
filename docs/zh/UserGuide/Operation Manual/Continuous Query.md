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



# 持续查询（Continuous Query, ）

我们可以通过 SQL 语句注册、或卸载一个 CQ 实例，以及查询到所有已经注册的 CQ 配置信息。

### 注册 CQ

注册 CQ 的 SQL 语法如下：

```sql
CREATE CONTINUOUS QUERY <cq_id> 
[RESAMPLE EVERY <every_interval> FOR <for_interval>] 
BEGIN 
SELECT <function>(<path_suffix>) INTO <new_path> | <new_path_suffix>
FROM <path_prefix>  [WHERE <where_clause>] 
GROUP BY time(<group_by_interval>) [, level = <level>] 
END
```

其中：

* <cq_id> 指定 CQ 全局唯一的 id。
* <every_interval> 指定查询执行时间间隔。若用户不指定，
若用户指定了 <for_interval>，则与 <for_interval> 一致，
否则与 \<group_by_interval\> 一致。
* <for_interval> 指定每次查询的时间范围为[now() - <for_interval>, now())。
若用户不指定，若用户指定了 <every_interval>，
则与 <every_interval> 一致，否则与 \<group_by_interval\> 一致。
* <every_interval>的值应该大于0并小于<for_interval>的值，如果大于<for_interval>的值，
  系统会按照等于<for_interval>的值处理。
* <group_by_interval>的值应该大于0并小于<for_interval>的值，如果大于<for_interval>的值，
  系统会按照等于<for_interval>的值处理。  
* \<function\> 指定聚合函数。
* \<path_prefix\> 与 \<path_suffix\> 拼接成完整的查询原时间序列。
* \<new_path\> 或 \<new_path_suffix\> 指定将查询出的数据写入的时间序列路径。
    用户可以指定完整的时间序列路径，提供 ${x} 变量，
    表示 <path_prefix> 中 level = x 的节点名称。也可以仅指定时间序列的后缀，
    由 IoTDB 生成默认的完整的时间序列路径，
    生成规则为结果序列前\<level\>层与<path_prefix>前\<level\>层（包括）一致，
    第\<level\>层后添加用户指定的\<new_path_suffix\>。
* \<where_clause\> 指定数据过滤条件。
* \<group_by_interval\> 指定时间分组长度。
* \<level\>: 按照序列第 \<level\> 层分组，将第 \<level\> 层以下的所有序列聚合。
若用户不指定，则仅对单条序列聚合。

例如：

假设有原始时间序列：
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

创建 cq1: 
````
CREATE CONTINUOUS QUERY cq1 BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.* GROUP BY time(10s) END
````
原始数据：
````
+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|                         Time|root.ln.wf02.wt02.temperature|root.ln.wf02.wt01.temperature|root.ln.wf01.wt02.temperature|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|2021-04-19T23:48:15.498+08:00|                        196.0|                        143.0|                        114.0|                         75.0|
|2021-04-19T23:48:20.501+08:00|                         57.0|                         56.0|                        110.0|                        191.0|
|2021-04-19T23:48:25.506+08:00|                        183.0|                        136.0|                         29.0|                         59.0|
|2021-04-19T23:48:30.510+08:00|                         25.0|                         19.0|                        199.0|                         56.0|
+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
````
每隔 10s 查询 `root.ln.*.*.temperature` 在前 10s 内的最大值（结果以10s为一组），
将结果写入到 `root.ln.*.*.temperature_max` 中，其中 \* 与原序列同级节点一致。
结果将产生4条新序列：
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
|2021-04-19T23:48:10.629+08:00|                            196.0|                            143.0|                            114.0|                            191.0|
|2021-04-19T23:48:20.623+08:00|                            183.0|                            136.0|                            199.0|                             59.0|
+-----------------------------+---------------------------------+---------------------------------+---------------------------------+---------------------------------+
````

创建 cq2:
````
CREATE CONTINUOUS QUERY cq2 RESAMPLE EVERY 20s FOR 20s BEGIN SELECT avg(temperature) INTO temperature_avg FROM root.ln.*.* GROUP BY time(10s), level=1 END
````
原始数据：
````
+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|                         Time|root.ln.wf02.wt02.temperature|root.ln.wf02.wt01.temperature|root.ln.wf01.wt02.temperature|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|2021-04-19T23:49:50.571+08:00|                         39.0|                        103.0|                        168.0|                         55.0|
|2021-04-19T23:49:55.577+08:00|                         99.0|                         58.0|                        125.0|                        120.0|
|2021-04-19T23:50:00.581+08:00|                         97.0|                         51.0|                        108.0|                          8.0|
|2021-04-19T23:50:05.583+08:00|                          7.0|                         98.0|                         18.0|                        144.0|
|2021-04-19T23:50:10.586+08:00|                        164.0|                         23.0|                        130.0|                         85.0|
|2021-04-19T23:50:15.587+08:00|                        110.0|                        194.0|                         23.0|                         17.0|
|2021-04-19T23:50:20.593+08:00|                         28.0|                        136.0|                         91.0|                        116.0|
|2021-04-19T23:50:25.597+08:00|                          2.0|                         18.0|                        159.0|                        107.0|
|2021-04-19T23:50:30.602+08:00|                        181.0|                         74.0|                         19.0|                         33.0|
+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
````
每隔 20s 查询 `root.ln.*.*.temperature` 在前 20s 内的平均值（结果以10s为一组，按照第1层节点分组），
将结果写入到 `root.ln.temperature_avg` 中。

````
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+
|             timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.temperature_avg| null|      root.ln|  DOUBLE| GORILLA|     SNAPPY|null|      null|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+
````
````
+-----------------------------+-----------------------+
|                         Time|root.ln.temperature_avg|
+-----------------------------+-----------------------+
|2021-04-19T23:49:51.270+08:00|                  83.25|
|2021-04-19T23:50:01.270+08:00|                 83.625|
|2021-04-19T23:50:11.269+08:00|                 89.375|
|2021-04-19T23:50:21.269+08:00|                 74.125|
+-----------------------------+-----------------------+
````

创建 cq3:
````
CREATE CONTINUOUS QUERY cq3 BEGIN SELECT count(temperature) INTO temperature_cnt FROM root.ln.*.* WHERE temperature > 80.0 GROUP BY time(10s), level=2 END
````
每隔 10s 查询 `root.ln.*.*.temperature` 在前 10s 中大于 80.0 的值的个数（结果以10s为一组，按照第2层节点分组），
将结果写入到 `root.ln.*.temperature_cnt` 中，其中 \* 与原序列同级节点一致。
结果将产生2条新序列：
````
+----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                  timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf02.temperature_cnt| null|      root.ln|   INT64|     RLE|     SNAPPY|null|      null|
|root.ln.wf01.temperature_cnt| null|      root.ln|   INT64|     RLE|     SNAPPY|null|      null|
+----------------------------+-----+-------------+--------+--------+-----------+----+----------+
````
其中`root.ln.wf01.temperature_cnt`是
`root.ln.wf01.wt01.temperature` 和 `root.ln.wf01.wt02.temperature`
聚合的结果，`root.ln.wf02.wt02.temperature_cnt`
是
`root.ln.wf02.wt01.temperature` 和 `root.ln.wf02.wt02.temperature`
聚合的结果。


创建 cq4:
````
CREATE CONTINUOUS QUERY cq4 BEGIN SELECT count(temperature) INTO root.ln_cq.${2}.temperature_cnt FROM root.ln.*.* WHERE temperature > 80.0 GROUP BY time(10s), level=2 END
````
查询模式与 cq3 相同，
将结果写入到 `root.ln_cq.${2}.temperature_cnt` 中。
结果将产生2条新序列：
````
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                     timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln_cq.wf02.temperature_cnt| null|   root.ln_cq|   INT64|     RLE|     SNAPPY|null|      null|
|root.ln_cq.wf01.temperature_cnt| null|   root.ln_cq|   INT64|     RLE|     SNAPPY|null|      null|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
````
其中`root.ln_cq.wf01.temperature_cnt`是
`root.ln.wf01.wt01.temperature` 和 `root.ln.wf01.wt02.temperature`
聚合的结果，`root.ln_cq.wf02.temperature_cnt`
是
`root.ln.wf02.wt01.temperature` 和 `root.ln.wf02.wt02.temperature`
聚合的结果。

### 删除 CQ
````
DROP CONTINUOUS QUERY <cq_id> 
````

### 展示 CQ 信息
````
SHOW CONTINUOUS QUERIES 
````

````
+-------+--------------+------------+-------------------------------------------------------------------------------------------------------------------+-----------------------------------+
|cq name|every interval|for interval|                                                                                                          query sql|                        target path|
+-------+--------------+------------+-------------------------------------------------------------------------------------------------------------------+-----------------------------------+
|    cq1|         10000|       10000|                                select max_value(temperature) from root.ln.*.* group by ([now() - 10s, now()), 10s)|${0}.${1}.${2}.${3}.temperature_max|
|    cq3|         10000|       10000|select count(temperature) from root.ln.*.* where temperature > 80.0 group by ([now() - 10s, now()), 10s), level = 2|     ${0}.${1}.${2}.temperature_cnt|
|    cq2|         20000|       20000|                           select avg(temperature) from root.ln.*.* group by ([now() - 20s, now()), 10s), level = 1|          ${0}.${1}.temperature_avg|
|    cq4|         10000|       10000|select count(temperature) from root.ln.*.* where temperature > 80.0 group by ([now() - 10s, now()), 10s), level = 2|    root.ln_cq.${2}.temperature_cnt|
+-------+--------------+------------+-------------------------------------------------------------------------------------------------------------------+-----------------------------------+
````




