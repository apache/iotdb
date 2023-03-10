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

## 聚合函数

聚合函数是多对一函数。它们对一组值进行聚合计算，得到单个聚合结果。

除了 `COUNT()`, `COUNT_IF()`之外，其他所有聚合函数都忽略空值，并在没有输入行或所有值为空时返回空值。 例如，`SUM()` 返回 null 而不是零，而 `AVG()` 在计数中不包括 null 值。

IoTDB 支持的聚合函数如下：

| 函数名           | 功能描述                                          | 允许的输入类型                  | 必要的属性参数                                                                                                                                                                                                                    | 输出类型      |
|---------------|-----------------------------------------------|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| SUM           | 求和。                                           | INT32 INT64 FLOAT DOUBLE | 无                                                                                                                                                                                                                          | DOUBLE    |
| COUNT         | 计算数据点数。                                       | 所有类型                     | 无                                                                                                                                                                                                                          | INT64     |
| AVG           | 求平均值。                                         | INT32 INT64 FLOAT DOUBLE | 无                                                                                                                                                                                                                          | DOUBLE    |
| EXTREME       | 求具有最大绝对值的值。如果正值和负值的最大绝对值相等，则返回正值。             | INT32 INT64 FLOAT DOUBLE | 无                                                                                                                                                                                                                          | 与输入类型一致   |
| MAX_VALUE     | 求最大值。                                         | INT32 INT64 FLOAT DOUBLE | 无                                                                                                                                                                                                                          | 与输入类型一致   |
| MIN_VALUE     | 求最小值。                                         | INT32 INT64 FLOAT DOUBLE | 无                                                                                                                                                                                                                          | 与输入类型一致   |
| FIRST_VALUE   | 求时间戳最小的值。                                     | 所有类型                     | 无                                                                                                                                                                                                                          | 与输入类型一致   |
| LAST_VALUE    | 求时间戳最大的值。                                     | 所有类型                     | 无                                                                                                                                                                                                                          | 与输入类型一致   |
| MAX_TIME      | 求最大时间戳。                                       | 所有类型                     | 无                                                                                                                                                                                                                          | Timestamp |
| MIN_TIME      | 求最小时间戳。                                       | 所有类型                     | 无                                                                                                                                                                                                                          | Timestamp |
| COUNT_IF      | 求数据点连续满足某一给定条件，且满足条件的数据点个数（用keep表示）满足指定阈值的次数。 | BOOLEAN                  | `[keep >=/>/=/!=/</<=]threshold`：被指定的阈值或阈值条件，若只使用`threshold`则等价于`keep >= threshold`,`threshold`类型为`INT64`<br/> `ignoreNull`：可选，默认为`true`；为`true`表示忽略null值，即如果中间出现null值，直接忽略，不会打断连续性；为`false`表示不忽略null值，即如果中间出现null值，会打断连续性 | INT64     |
| TIME_DURATION | 求某一列最大一个不为NULL的值所在时间戳与最小一个不为NULL的值所在时间戳的时间戳差  | 所有类型                     | 无                                                                                                                                                                                                                          |   INT64        |

### COUNT_IF

#### 语法
```sql
count_if(predicate, [keep >=/>/=/!=/</<=]threshold[, 'ignoreNull'='true/false'])
```
predicate: 返回类型为`BOOLEAN`的合法表达式

threshold 及 ignoreNull 用法见上表

>注意: count_if 当前暂不支持与 group by time 的 SlidingWindow 一起使用

#### 使用示例

##### 原始数据

``` 
+-----------------------------+-------------+-------------+
|                         Time|root.db.d1.s1|root.db.d1.s2|
+-----------------------------+-------------+-------------+
|1970-01-01T08:00:00.001+08:00|            0|            0|
|1970-01-01T08:00:00.002+08:00|         null|            0|
|1970-01-01T08:00:00.003+08:00|            0|            0|
|1970-01-01T08:00:00.004+08:00|            0|            0|
|1970-01-01T08:00:00.005+08:00|            1|            0|
|1970-01-01T08:00:00.006+08:00|            1|            0|
|1970-01-01T08:00:00.007+08:00|            1|            0|
|1970-01-01T08:00:00.008+08:00|            0|            0|
|1970-01-01T08:00:00.009+08:00|            0|            0|
|1970-01-01T08:00:00.010+08:00|            0|            0|
+-----------------------------+-------------+-------------+
```

##### 不使用ignoreNull参数(忽略null)

SQL:
```sql
select count_if(s1=0 & s2=0, 3), count_if(s1=1 & s2=0, 3) from root.db.d1
```

输出:
```
+--------------------------------------------------+--------------------------------------------------+
|count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, 3)|count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, 3)|
+--------------------------------------------------+--------------------------------------------------+
|                                                 2|                                                 1|
+--------------------------------------------------+--------------------------------------------------
```

##### 使用ignoreNull参数

SQL:
```sql
select count_if(s1=0 & s2=0, 3, 'ignoreNull'='false'), count_if(s1=1 & s2=0, 3, 'ignoreNull'='false') from root.db.d1
```

输出:
```
+------------------------------------------------------------------------+------------------------------------------------------------------------+
|count_if(root.db.d1.s1 = 0 & root.db.d1.s2 = 0, 3, "ignoreNull"="false")|count_if(root.db.d1.s1 = 1 & root.db.d1.s2 = 0, 3, "ignoreNull"="false")|
+------------------------------------------------------------------------+------------------------------------------------------------------------+
|                                                                       1|                                                                       1|
+------------------------------------------------------------------------+------------------------------------------------------------------------+
```

## TIME_DURATION
### 语法
```sql
    time_duration(Path)
```
### 使用示例
#### 准备数据
```
+----------+-------------+
|      Time|root.db.d1.s1|
+----------+-------------+
|         1|           70|
|         3|           10|
|         4|          303|
|         6|          110|
|         7|          302|
|         8|          110|
|         9|           60|
|        10|           70|
|1677570934|           30|
+----------+-------------+
```
#### 写入语句
```sql
"CREATE DATABASE root.db",
"CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN tags(city=Beijing)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1, 2, 10, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(2, null, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(3, 10, 0, null)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(4, 303, 30, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(5, null, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(6, 110, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(7, 302, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(8, 110, null, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(9, 60, 20, true)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(10,70, 20, null)",
"INSERT INTO root.db.d1(timestamp,s1,s2,s3) values(1677570934, 30, 0, true)",
```

查询：
```sql
select time_duration(s1) from root.db.d1
```

输出
```
+----------------------------+
|time_duration(root.db.d1.s1)|
+----------------------------+
|                  1677570933|
+----------------------------+
```
