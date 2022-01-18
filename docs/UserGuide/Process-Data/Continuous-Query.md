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

# Continuous Query, CQ

We can create, drop a CQ, and query all registered CQ configuration information through SQL statements.

Note that the current distributed version of IoTDB does not support continuous queries. Please stay tuned.

## SQL statements

### Create CQ

#### Syntax

```sql
CREATE (CONTINUOUS QUERY | CQ) <cq_id> 
[RESAMPLE EVERY <every_interval> FOR <for_interval>]
BEGIN
SELECT <function>(<path_suffix>) INTO <full_path> | <node_name>
FROM <path_prefix>
GROUP BY time(<group_by_interval>) [, level = <level>]
END
```

* `<cq_id>` specifies the globally unique id of CQ.
* `<every_interval>` specifies the query execution time interval. We currently support the units of ns, us, ms, s, m, h, d, w, and its value should not be lower than the minimum threshold configured by the user. 
* `<for_interval>` specifies the time range of each query as `[now()-<for_interval>, now())`. We currently support the units of ns, us, ms, s, m, h, d, w.
* `<execution_boundary_time>` is a date that represents **the start time of the first window**.
  * `<execution_boundary_time>` can be earlier than, equals to, later than **current time**.
  * This parameter is optional. If not specified, it is equivalent to `BOUNDARY now()`.
  * **The end time of the first window** is `<execution_boundary_time> + <for_interval>`.
  * The **start time** of the `i (1 <= i)`th window is `<execution_boundary_time> + <for_interval> + (i - 1) * <every_interval>`.
  * The **end time** of the `i (1 <= i)`th window is`<execution_boundary_time> + <for_interval> + i * <every_interval>`.
  * If **current time** is earlier than or equal to **the end time of the first window**, then the first execution moment of the continuous query is **the end time of the first window**.
  * If **current time** is later than **the end time of the first window**, then the first execution moment of the continuous query is the **end time of the first window whose end time is later than or equal to the current time** .
  * The **query time range**  at each execution moment is `[now() - <for_interval>, now())`.

* `<function>` specifies the aggregate function.
* `<path_prefix>` and `<path_suffix>` are spliced into the queried time series path.
* `<full_path>` or `<node_name>` specifies the result time series path.
* `<group_by_interval>` specifies the time grouping length. We currently support the units of ns, us, ms, s, m, h, d, w, mo, y.
* `<level>` refers to grouping according to the `<level>` level of the time series, and grouping the aggregation result of  time series with the same name below the `<level>` level. For the specific semantics of the Group By Level statement and the definition of `<level>`, see [aggregation-by-level](../Write-And-Delete-Data/Delete-Data.md)


Note:
* `<for_interval>`,`<every_interval>` can optionally be specified. If the user does not specify one of them, the value of the unspecified item will be processed equal to `<group_by_interval>`.
    * The values of `<every_interval>`, `<for_interval>` and `<group_by_interval>` should all be greater than 0.
    * The value of `<group_by_interval>` should be less than the value of `<for_interval>`, otherwise the system will process the value equal to `<for_interval>`.
    * The user should specify the appropriate `<for_interval>` and `<every_interval>` according to actual needs.
        * If `<for_interval>` is greater than `<every_interval>`, there will be partial data overlap in each query window. This configuration is not recommended from the perspective of query performance.
        * If `<for_interval>` is less than `<every_interval>`, there may be uncovered data between each query window.
* For the result series path
    * The user can choose to specify `<full_path>`, which is the complete time series path starting with `root`. The user can use the `${x}` variable in the path to represent the node name of `level = x` in the original time series. `x` should be greater than or equal to 0 and less than or equal to the value of `<level>`
      (If `level` is not specified, it should be less than or equal to the level, i.e. length, of `<path_prefix>`).
    * The user can also specify only `<node_name>`, which is the last node name of the result time series path.
        * If the user specifies `<level> = l`, the result time series path generated by the system is `root.${1}. ... .${l}.<node_name>`
        * If the user does not specify `<level>`, let the maximum level of the original time series be `L`,
          Then the result time series path generated by the system is `root.${1}. ... .${L-1}.<node_name>`.

#### Examples

##### Original Data
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


##### Result time series path configuration example

For the above original time series, if the user specifies that the query aggregation level is `2`, the aggregation function is `avg`,
The user can specify only the last node name of the generated time series in the `INTO` clause. If the user specifies it as `temperature_avg`, the full path generated by the system will be `root.${1}.${2}.temperature_avg` .
The user can also specify the full path in the `INTO` clause, and the user can specify it as `root.${1}.${2}.temperature_avg`, `root.ln_cq.${2}.temperature_avg`, `root.${1}_cq.${2}.temperature_avg`, `root.${1}.${2}_cq.temperature_avg` etc.,
It can also be specified as `root.${2}.${1}.temperature_avg` and others as needed.
It should be noted that the `x` in `${x}` should be greater than or equal to `1` and less than or equal to the value of `<level>`
(If `<level>` is not specified, it should be less than or equal to the length of `<path_prefix>`). In the above example, `x` should be less than or equal to `2`.

##### Create `cq1`
````sql
CREATE CONTINUOUS QUERY cq1 
BEGIN 
  SELECT max_value(temperature) 
  INTO temperature_max 
  FROM root.ln.*.* 
  GROUP BY time(10s) 
END
````

Query the maximum value of `root.ln.*.*.temperature` in the previous 10s every 10s (the results are grouped by 10s),
 and the results will be written to `root.${1}.${2}.${3}.temperature_max`,
As a result, 4 new time series will be generated.
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
##### Create `cq2`
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
Query the average value of `root.ln.*.*.temperature` in the previous 20s every 20s (the results are grouped by 10s),
 and the results will be written to `root.${1}.${2}.temperature_avg`,
As a result, 2 new time series will be generated.
Among them, `root.ln.wf02.temperature_avg` is generated by the aggregation calculation of `root.ln.wf02.wt02.temperature` and `root.ln.wf02.wt01.temperature`,
and `root.ln.wf01.temperature_avg` is generated by the aggregation calculation of `root.ln.wf01.wt02.temperature` and `root.ln.wf01.wt01.temperature`.
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
##### Create `cq3`
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

The query mode is the same as `cq2`,
and the results will be written to `root.ln_cq.${2}.temperature_avg`.
As a result, 2 new time series will be generated.
Among them, `root.ln_cq.wf02.temperature_avg` is generated by the aggregation calculation of `root.ln.wf02.wt02.temperature` and `root.ln.wf02.wt01.temperature`,
and `root.ln_cq.wf01.temperature_avg` is generated by the aggregation calculation of `root.ln.wf01.wt02.temperature` and `root.ln.wf01.wt01.temperature`.
    
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

##### 创建 `cq4`

````sql
CREATE CONTINUOUS QUERY cq4 
RESAMPLE EVERY 20s FOR 20s BOUNDARY 2022-01-14T23:00:00.000+08:00 
BEGIN 
  SELECT avg(temperature) 
  INTO root.ln_cq.${2}.temperature_avg 
  FROM root.ln.*.* GROUP BY time(10s), level=2 
END
````

This example is almost identical to creating cq3. The difference is that in this example the user specified `BOUNDARY 2022-01-14T23:00:00.000+08:00 `.

Note that the first execution time of this CQ is later than the time in the example, so `2022-01-14T23:00:20.000+08:00` is the first execution time. Recursively, `2022-01-14T23:00:40.000+08:00` is the second execution moment, `2022-01-14T23:01:00.000+08:00` is the third execution moment...

The SQL statement executed at the first execution moment is `select avg(temperature) from root.ln.*.* group by ([2022-01-14T23:00:00.000+08:00, 2022-01-14T23:00: 20.000+08:00), 10s), level = 2`.

The SQL statement executed at the second execution moment is `select avg(temperature) from root.ln.*.* group by ([2022-01-14T23:00:20.000+08:00, 2022-01-14T23:00: 40.000+08:00), 10s), level = 2`.

The SQL statement executed at the third execution moment is `select avg(temperature) from root.ln.*.* group by ([2022-01-14T23:00:40.000+08:00, 2022-01-14T23:01: 00.000+08:00), 10s), level = 2`.

...

### Show CQ Information

#### Syntax
````sql
SHOW (CONTINUOUS QUERIES | CQS) 
````

#### Example Result
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

### Drop CQ
#### Syntax
````sql
DROP (CONTINUOUS QUERY | CQ) <cq_id> 
````

#### Example

````sql
DROP CONTINUOUS QUERY cq3
````

````sql
DROP CQ cq3
````
## System Parameter Configuration
| Name | Description | Data Type | Default Value |
| :---------------------------------- |-------- | ---- | -----|
| `continuous_query_execution_thread` | The number of threads in the thread pool that executes continuous query tasks | int | max(1, CPU core number / 2)|
| `max_pending_continuous_query_tasks` | The maximum number of continuous query tasks pending in queue | int | 64|
| `continuous_query_min_every_interval` | The minimum value of the continuous query execution time interval | duration | 1s|