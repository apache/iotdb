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

## 比较运算符和函数

### 基本比较运算符

- 输入数据类型： `INT32`, `INT64`, `FLOAT`, `DOUBLE`。
- 注意：会将所有数据转换为`DOUBLE`类型后进行比较。`==`和`!=`可以直接比较两个`BOOLEAN`。
- 返回类型：`BOOLEAN`。

|运算符                       |含义|
|----------------------------|-----------|
|`>`                         |大于|
|`>=`                        |大于等于|
|`<`                         |小于|
|`<=`                        |小于等于|
|`==`                        |等于|
|`!=` / `<>`                 |不等于|

**示例：**

```sql
select a, b, a > 10, a <= b, !(a <= b), a > 10 && a > b from root.test;
```

运行结果
```
IoTDB> select a, b, a > 10, a <= b, !(a <= b), a > 10 && a > b from root.test;
+-----------------------------+-----------+-----------+----------------+--------------------------+---------------------------+------------------------------------------------+
|                         Time|root.test.a|root.test.b|root.test.a > 10|root.test.a <= root.test.b|!root.test.a <= root.test.b|(root.test.a > 10) & (root.test.a > root.test.b)|
+-----------------------------+-----------+-----------+----------------+--------------------------+---------------------------+------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|         23|       10.0|            true|                     false|                       true|                                            true|
|1970-01-01T08:00:00.002+08:00|         33|       21.0|            true|                     false|                       true|                                            true|
|1970-01-01T08:00:00.004+08:00|         13|       15.0|            true|                      true|                      false|                                           false|
|1970-01-01T08:00:00.005+08:00|         26|        0.0|            true|                     false|                       true|                                            true|
|1970-01-01T08:00:00.008+08:00|          1|       22.0|           false|                      true|                      false|                                           false|
|1970-01-01T08:00:00.010+08:00|         23|       12.0|            true|                     false|                       true|                                            true|
+-----------------------------+-----------+-----------+----------------+--------------------------+---------------------------+------------------------------------------------+
```

### `BETWEEN ... AND ...` 运算符

|运算符                       |含义|
|----------------------------|-----------|
|`BETWEEN ... AND ...`       |在指定范围内|
|`NOT BETWEEN ... AND ...`   |不在指定范围内|

**示例：** 选择区间 [36.5,40] 内或之外的数据：

```sql
select temperature from root.sg1.d1 where temperature between 36.5 and 40;
```

```sql
select temperature from root.sg1.d1 where temperature not between 36.5 and 40;
```

### 模糊匹配运算符

对于 TEXT 类型的数据，支持使用 `Like` 和 `Regexp` 运算符对数据进行模糊匹配

|运算符                       |含义|
|----------------------------|-----------|
|`LIKE`                      |匹配简单模式|
|`NOT LIKE`                  |无法匹配简单模式|
|`REGEXP`                    |匹配正则表达式|
|`NOT REGEXP`                |无法匹配正则表达式|

输入数据类型：`TEXT`

返回类型：`BOOLEAN`

#### 使用 `Like` 进行模糊匹配

**匹配规则：**

- `%` 表示任意0个或多个字符。
- `_` 表示任意单个字符。

**示例 1：** 查询 `root.sg.d1` 下 `value` 含有`'cc'`的数据。

```shell
IoTDB> select * from root.sg.d1 where value like '%cc%'
+-----------------------------+----------------+
|                         Time|root.sg.d1.value|
+-----------------------------+----------------+
|2017-11-01T00:00:00.000+08:00|        aabbccdd| 
|2017-11-01T00:00:01.000+08:00|              cc|
+-----------------------------+----------------+
Total line number = 2
It costs 0.002s
```

**示例 2：** 查询 `root.sg.d1` 下 `value` 中间为 `'b'`、前后为任意单个字符的数据。

```shell
IoTDB> select * from root.sg.device where value like '_b_'
+-----------------------------+----------------+
|                         Time|root.sg.d1.value|
+-----------------------------+----------------+
|2017-11-01T00:00:02.000+08:00|             abc| 
+-----------------------------+----------------+
Total line number = 1
It costs 0.002s
```

#### 使用 `Regexp` 进行模糊匹配

需要传入的过滤条件为 **Java 标准库风格的正则表达式**。

**常见的正则匹配举例：**

```
长度为3-20的所有字符：^.{3,20}$
大写英文字符：^[A-Z]+$
数字和英文字符：^[A-Za-z0-9]+$
以a开头的：^a.*
```

**示例 1：** 查询 root.sg.d1 下 value 值为26个英文字符组成的字符串。

```shell
IoTDB> select * from root.sg.d1 where value regexp '^[A-Za-z]+$'
+-----------------------------+----------------+
|                         Time|root.sg.d1.value|
+-----------------------------+----------------+
|2017-11-01T00:00:00.000+08:00|        aabbccdd| 
|2017-11-01T00:00:01.000+08:00|              cc|
+-----------------------------+----------------+
Total line number = 2
It costs 0.002s
```

**示例 2：** 查询 root.sg.d1 下 value 值为26个小写英文字符组成的字符串且时间大于100的。

```shell
IoTDB> select * from root.sg.d1 where value regexp '^[a-z]+$' and time > 100
+-----------------------------+----------------+
|                         Time|root.sg.d1.value|
+-----------------------------+----------------+
|2017-11-01T00:00:00.000+08:00|        aabbccdd| 
|2017-11-01T00:00:01.000+08:00|              cc|
+-----------------------------+----------------+
Total line number = 2
It costs 0.002s
```

**示例 3：**

```sql
select b, b like '1%', b regexp '[0-2]' from root.test;
```

运行结果
```
+-----------------------------+-----------+-------------------------+--------------------------+
|                         Time|root.test.b|root.test.b LIKE '^1.*?$'|root.test.b REGEXP '[0-2]'|
+-----------------------------+-----------+-------------------------+--------------------------+
|1970-01-01T08:00:00.001+08:00| 111test111|                     true|                      true|
|1970-01-01T08:00:00.003+08:00| 333test333|                    false|                     false|
+-----------------------------+-----------+-------------------------+--------------------------+
```

### `IS NULL` 运算符

|运算符                       |含义|
|----------------------------|-----------|
|`IS NULL`                   |是空值|
|`IS NOT NULL`               |不是空值|

**示例 1：** 选择值为空的数据:

```sql
select code from root.sg1.d1 where temperature is null;
```

**示例 2：** 选择值为非空的数据:

```sql
select code from root.sg1.d1 where temperature is not null;
```

### `IN` 运算符

|运算符                       |含义|
|----------------------------|-----------|
|`IN` / `CONTAINS`           |是指定列表中的值|
|`NOT IN` / `NOT CONTAINS`   |不是指定列表中的值|

输入数据类型：`All Types`

返回类型 `BOOLEAN`

**注意：请确保集合中的值可以被转为输入数据的类型。**
> 例如：
>
>`s1 in (1, 2, 3, 'test')`，`s1`的数据类型是`INT32`
>
> 我们将会抛出异常，因为`'test'`不能被转为`INT32`类型

**示例 1：** 选择值在特定范围内的数据：

```sql
select code from root.sg1.d1 where code in ('200', '300', '400', '500');
```

**示例 2：** 选择值在特定范围外的数据：

```sql
select code from root.sg1.d1 where code not in ('200', '300', '400', '500');
```

**示例 3：**

```sql
select a, a in (1, 2) from root.test;
```

输出2:
```
+-----------------------------+-----------+--------------------+
|                         Time|root.test.a|root.test.a IN (1,2)|
+-----------------------------+-----------+--------------------+
|1970-01-01T08:00:00.001+08:00|          1|                true|
|1970-01-01T08:00:00.003+08:00|          3|               false|
+-----------------------------+-----------+--------------------+
```

### 条件函数 

条件函数针对每个数据点进行条件判断，返回布尔值。

| 函数名      | 可接收的输入序列类型                     | 必要的属性参数                               | 输出序列类型     | 功能类型                                             |
|----------|--------------------------------|---------------------------------------|------------|--------------------------------------------------|
| ON_OFF   | INT32 / INT64 / FLOAT / DOUBLE | `threshold`:DOUBLE类型                  | BOOLEAN 类型 | 返回`ts_value >= threshold`的bool值                  |
| IN_RANGE | INT32 / INT64 / FLOAT / DOUBLE | `lower`:DOUBLE类型<br/>`upper`:DOUBLE类型 | BOOLEAN类型  | 返回`ts_value >= lower && ts_value <= upper`的bool值 |                                                    |

测试数据：

```
IoTDB> select ts from root.test;
+-----------------------------+------------+
|                         Time|root.test.ts|
+-----------------------------+------------+
|1970-01-01T08:00:00.001+08:00|           1|
|1970-01-01T08:00:00.002+08:00|           2|
|1970-01-01T08:00:00.003+08:00|           3|
|1970-01-01T08:00:00.004+08:00|           4|
+-----------------------------+------------+
```

**示例 1：**

SQL语句：
```sql
select ts, on_off(ts, 'threshold'='2') from root.test;
```

输出：
```
IoTDB> select ts, on_off(ts, 'threshold'='2') from root.test;
+-----------------------------+------------+-------------------------------------+
|                         Time|root.test.ts|on_off(root.test.ts, "threshold"="2")|
+-----------------------------+------------+-------------------------------------+
|1970-01-01T08:00:00.001+08:00|           1|                                false|
|1970-01-01T08:00:00.002+08:00|           2|                                 true|
|1970-01-01T08:00:00.003+08:00|           3|                                 true|
|1970-01-01T08:00:00.004+08:00|           4|                                 true|
+-----------------------------+------------+-------------------------------------+
```

**示例 2：**

Sql语句：
```sql
select ts, in_range(ts, 'lower'='2', 'upper'='3.1') from root.test;
```

输出：
```
IoTDB> select ts, in_range(ts, 'lower'='2', 'upper'='3.1') from root.test;
+-----------------------------+------------+--------------------------------------------------+
|                         Time|root.test.ts|in_range(root.test.ts, "lower"="2", "upper"="3.1")|
+-----------------------------+------------+--------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|           1|                                             false|
|1970-01-01T08:00:00.002+08:00|           2|                                              true|
|1970-01-01T08:00:00.003+08:00|           3|                                              true|
|1970-01-01T08:00:00.004+08:00|           4|                                             false|
+-----------------------------+------------+--------------------------------------------------+
```