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

## 条件表达式

### CASE

#### 介绍

CASE表达式是一种条件表达式，可用于根据特定条件返回不同的值，功能类似于其它语言中的if-else。
CASE表达式由以下部分组成：
- CASE关键字：表示开始CASE表达式。
- WHEN-THEN子句：可能存在多个，用于定义条件与给出结果。此子句又分为WHEN和THEN两个部分，WHEN部分表示条件，THEN部分表示结果表达式。如果WHEN条件为真，则返回对应的THEN结果。
- ELSE子句：如果没有任何WHEN-THEN子句的条件为真，则返回ELSE子句中的结果。可以不存在ELSE子句。
- END关键字：表示结束CASE表达式。

CASE表达式是一种标量运算，可以配合任何其它的标量运算或聚合函数使用。
  
下文把所有THEN部分和ELSE子句并称为结果子句。

#### 语法示例

CASE表达式支持两种格式。

语法示例如下：
- 格式1：
```sql
  CASE
    WHEN condition1 THEN expression1
    [WHEN condition2 THEN expression2] ...
    [ELSE expression_end]
  END
```
  从上至下检查WHEN子句中的condition。

  condition为真时返回对应THEN子句中的expression，condition为假时继续检查下一个WHEN子句中的condition。
- 格式2：
```sql
  CASE caseValue
    WHEN whenValue1 THEN expression1
    [WHEN whenValue2 THEN expression2] ...
    [ELSE expression_end]
  END
```

  从上至下检查WHEN子句中的whenValue是否与caseValue相等。

  满足caseValue=whenValue时返回对应THEN子句中的expression，不满足时继续检查下一个WHEN子句中的whenValue。

  格式2会被iotdb转换成等效的格式1，例如以上sql语句会转换成：
```sql
  CASE
    WHEN caseValue=whenValue1 THEN expression1
    [WHEN caseValue=whenValue1 THEN expression1] ...
    [ELSE expression_end]
  END
```

如果格式1中的condition均不为真，或格式2中均不满足caseVaule=whenValue，则返回ELSE子句中的expression_end；不存在ELSE子句则返回null。

#### 注意事项

- 格式1中，所有WHEN子句必须返回BOOLEAN类型。
- 格式2中，所有WHEN子句必须能够与CASE子句进行判等。
- 一个CASE表达式中所有结果子句的返回值类型需要满足一定的条件：
  - BOOLEAN类型不能与其它类型共存，存在其它类型会报错。
  - TEXT类型不能与其它类型共存，存在其它类型会报错。
  - 其它四种数值类型可以共存，最终结果会为DOUBLE类型，转换过程可能会存在精度损失。
- CASE表达式没有实现惰性计算，即所有子句都会被计算。
- CASE表达式不支持与UDF混用。
- CASE表达式内部不能存在聚合函数，但CASE表达式的结果可以提供给聚合函数。
- 使用CLI时，由于CASE表达式字符串较长，推荐用as为表达式提供别名。

#### 使用示例

##### 示例1

CASE表达式可对数据进行直观地分析，例如：

- 某种化学产品的制备需要温度和压力都处于特定范围之内
- 在制备过程中传感器会侦测温度和压力，在iotdb中形成T(temperature)和P(pressure)两个时间序列

这种应用场景下，CASE表达式可以指出哪些时间的参数是合适的，哪些时间的参数不合适，以及为什么不合适。
  
数据：
```sql
IoTDB> select * from root.test1
+-----------------------------+------------+------------+
|                         Time|root.test1.P|root.test1.T|
+-----------------------------+------------+------------+
|2023-03-29T11:25:54.724+08:00|   1000000.0|      1025.0|
|2023-03-29T11:26:13.445+08:00|   1000094.0|      1040.0|
|2023-03-29T11:27:36.988+08:00|   1000095.0|      1041.0|
|2023-03-29T11:27:56.446+08:00|   1000095.0|      1059.0|
|2023-03-29T11:28:20.838+08:00|   1200000.0|      1040.0|
+-----------------------------+------------+------------+
```

SQL语句：
```sql
select T, P, case
when 1000<T and T<1050 and 1000000<P and P<1100000 then "good!"
when T<=1000 or T>=1050 then "bad temperature"
when P<=1000000 or P>=1100000 then "bad pressure"
end as `result`
from root.test1
```


输出：
```
+-----------------------------+------------+------------+---------------+
|                         Time|root.test1.T|root.test1.P|         result|
+-----------------------------+------------+------------+---------------+
|2023-03-29T11:25:54.724+08:00|      1025.0|   1000000.0|   bad pressure|
|2023-03-29T11:26:13.445+08:00|      1040.0|   1000094.0|          good!|
|2023-03-29T11:27:36.988+08:00|      1041.0|   1000095.0|          good!|
|2023-03-29T11:27:56.446+08:00|      1059.0|   1000095.0|bad temperature|
|2023-03-29T11:28:20.838+08:00|      1040.0|   1200000.0|   bad pressure|
+-----------------------------+------------+------------+---------------+
```


##### 示例2

CASE表达式可实现结果的自由转换，例如将具有某种模式的字符串转换成另一种字符串。

数据：
```sql
IoTDB> select * from root.test2
+-----------------------------+--------------+
|                         Time|root.test2.str|
+-----------------------------+--------------+
|2023-03-27T18:23:33.427+08:00|         abccd|
|2023-03-27T18:23:39.389+08:00|         abcdd|
|2023-03-27T18:23:43.463+08:00|       abcdefg|
+-----------------------------+--------------+
```

SQL语句：
```sql
select str, case
when str like "%cc%" then "has cc"
when str like "%dd%" then "has dd"
else "no cc and dd" end as `result`
from root.test2
```

输出：
```
+-----------------------------+--------------+------------+
|                         Time|root.test2.str|      result|
+-----------------------------+--------------+------------+
|2023-03-27T18:23:33.427+08:00|         abccd|      has cc|
|2023-03-27T18:23:39.389+08:00|         abcdd|      has dd|
|2023-03-27T18:23:43.463+08:00|       abcdefg|no cc and dd|
+-----------------------------+--------------+------------+
```

##### 示例3：搭配聚合函数

###### 合法：聚合函数←CASE表达式

CASE表达式可作为聚合函数的参数。例如，与聚合函数COUNT搭配，可实现同时按多个条件进行数据统计。

数据：
```sql
IoTDB> select * from root.test3
+-----------------------------+------------+
|                         Time|root.test3.x|
+-----------------------------+------------+
|2023-03-27T18:11:11.300+08:00|         0.0|
|2023-03-27T18:11:14.658+08:00|         1.0|
|2023-03-27T18:11:15.981+08:00|         2.0|
|2023-03-27T18:11:17.668+08:00|         3.0|
|2023-03-27T18:11:19.112+08:00|         4.0|
|2023-03-27T18:11:20.822+08:00|         5.0|
|2023-03-27T18:11:22.462+08:00|         6.0|
|2023-03-27T18:11:24.174+08:00|         7.0|
|2023-03-27T18:11:25.858+08:00|         8.0|
|2023-03-27T18:11:27.979+08:00|         9.0|
+-----------------------------+------------+
```

SQL语句：

```sql
select
count(case when x<=1 then 1 end) as `(-∞,1]`,
count(case when 1<x and x<=3 then 1 end) as `(1,3]`,
count(case when 3<x and x<=7 then 1 end) as `(3,7]`,
count(case when 7<x then 1 end) as `(7,+∞)`
from root.test3
```

输出：
```
+------+-----+-----+------+
|(-∞,1]|(1,3]|(3,7]|(7,+∞)|
+------+-----+-----+------+
|     2|    2|    4|     2|
+------+-----+-----+------+
```

###### 非法：CASE表达式←聚合函数

不支持在CASE表达式内部使用聚合函数。

SQL语句：
```sql
select case when x<=1 then avg(x) else sum(x) end from root.test3
```

输出：
```
Msg: 701: Raw data and aggregation result hybrid calculation is not supported.
```

##### 示例4：格式2

一个使用格式2的简单例子。如果所有条件都为判等，则推荐使用格式2，以简化SQL语句。

数据：
```sql
IoTDB> select * from root.test4
+-----------------------------+------------+
|                         Time|root.test4.x|
+-----------------------------+------------+
|1970-01-01T08:00:00.001+08:00|         1.0|
|1970-01-01T08:00:00.002+08:00|         2.0|
|1970-01-01T08:00:00.003+08:00|         3.0|
|1970-01-01T08:00:00.004+08:00|         4.0|
+-----------------------------+------------+
```

SQL语句：
```sql
select x, case x when 1 then "one" when 2 then "two" else "other" end from root.test4
```

输出：
```
+-----------------------------+------------+-----------------------------------------------------------------------------------+
|                         Time|root.test4.x|CASE WHEN root.test4.x = 1 THEN "one" WHEN root.test4.x = 2 THEN "two" ELSE "other"|
+-----------------------------+------------+-----------------------------------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|         1.0|                                                                                one|
|1970-01-01T08:00:00.002+08:00|         2.0|                                                                                two|
|1970-01-01T08:00:00.003+08:00|         3.0|                                                                              other|
|1970-01-01T08:00:00.004+08:00|         4.0|                                                                              other|
+-----------------------------+------------+-----------------------------------------------------------------------------------+
```

##### 示例5：结果子句类型

CASE表达式的结果子句的返回值需要满足一定的类型限制。

此示例中，继续使用示例4中的数据。

###### 非法：BOOLEAN与其它类型共存

SQL语句：
```sql
select x, case x when 1 then true when 2 then 2 end from root.test4
```

输出：
```
Msg: 701: CASE expression: BOOLEAN and other types cannot exist at same time
```

###### 合法：只存在BOOLEAN类型

SQL语句：
```sql
select x, case x when 1 then true when 2 then false end as `result` from root.test4
```

输出：
```
+-----------------------------+------------+------+
|                         Time|root.test4.x|result|
+-----------------------------+------------+------+
|1970-01-01T08:00:00.001+08:00|         1.0|  true|
|1970-01-01T08:00:00.002+08:00|         2.0| false|
|1970-01-01T08:00:00.003+08:00|         3.0|  null|
|1970-01-01T08:00:00.004+08:00|         4.0|  null|
+-----------------------------+------------+------+
```

###### 非法：TEXT与其它类型共存

SQL语句：
```sql
select x, case x when 1 then 1 when 2 then "str" end from root.test4
```

输出：
```
Msg: 701: CASE expression: TEXT and other types cannot exist at same time
```

###### 合法：只存在TEXT类型

见示例1。

###### 合法：数值类型共存

SQL语句：
```sql
select x, case x
when 1 then 1
when 2 then 222222222222222
when 3 then 3.3
when 4 then 4.4444444444444
end as `result`
from root.test4
```

输出：
```
+-----------------------------+------------+-------------------+
|                         Time|root.test4.x|             result|
+-----------------------------+------------+-------------------+
|1970-01-01T08:00:00.001+08:00|         1.0|                1.0|
|1970-01-01T08:00:00.002+08:00|         2.0|2.22222222222222E14|
|1970-01-01T08:00:00.003+08:00|         3.0|  3.299999952316284|
|1970-01-01T08:00:00.004+08:00|         4.0|   4.44444465637207|
+-----------------------------+------------+-------------------+
```