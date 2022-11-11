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


## 逻辑运算符

条件函数针对每个数据点进行条件判断，返回布尔值。

目前IoTDB支持以下条件函数

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

##### 测试1

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

##### 测试2
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

#### 一元逻辑运算符
支持运算符 `!`

输入数据类型：`BOOLEAN`

输出数据类型：`BOOLEAN`

注意：`!`的优先级很高，记得使用括号调整优先级

#### 二元比较运算符

支持运算符 `>`, `>=`, `<`, `<=`, `==`, `!=`

输入数据类型： `INT32`, `INT64`, `FLOAT`, `DOUBLE`

注意：会将所有数据转换为`DOUBLE`类型后进行比较。`==`和`!=`可以直接比较两个`BOOLEAN`

返回类型：`BOOLEAN`

#### 二元逻辑运算符

支持运算符 AND:`and`,`&`, `&&`; OR:`or`,`|`,`||`

输入数据类型：`BOOLEAN`

返回类型 `BOOLEAN`

注意：当某个时间戳下左操作数和右操作数都为`BOOLEAN`类型时，二元逻辑操作才会有输出结果

#### IN 运算符

支持运算符 `IN`

输入数据类型：`All Types`

返回类型 `BOOLEAN`

#### 字符串匹配运算符

支持运算符 `LIKE`, `REGEXP`

输入数据类型：`TEXT`

返回类型：`BOOLEAN`

### 使用示例
输入1：
```sql
select a, b, a > 10, a <= b, !(a <= b), a > 10 && a > b from root.test;
```
输出1:
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

输入2:
```sql
select a, b, a in (1, 2), b like '1%', b regexp '[0-2]' from root.test;
```

输出2:
```
+-----------------------------+-----------+-----------+--------------------+-------------------------+--------------------------+
|                         Time|root.test.a|root.test.b|root.test.a IN (1,2)|root.test.b LIKE '^1.*?$'|root.test.b REGEXP '[0-2]'|
+-----------------------------+-----------+-----------+--------------------+-------------------------+--------------------------+
|1970-01-01T08:00:00.001+08:00|          1| 111test111|                true|                     true|                      true|
|1970-01-01T08:00:00.003+08:00|          3| 333test333|               false|                    false|                     false|
+-----------------------------+-----------+-----------+--------------------+-------------------------+--------------------------+
```
