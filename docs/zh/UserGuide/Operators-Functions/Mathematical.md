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

## 算数运算符和函数

### 算数运算符

#### 一元算数运算符

支持的运算符：`+`, `-`

输入数据类型要求：`INT32`, `INT64`, `FLOAT`, `DOUBLE`

输出数据类型：与输入数据类型一致

#### 二元算数运算符

支持的运算符：`+`, `-`, `*`, `/`,  `%`

输入数据类型要求：`INT32`, `INT64`, `FLOAT`和`DOUBLE`

输出数据类型：`DOUBLE`

注意：当某个时间戳下左操作数和右操作数都不为空（`null`）时，二元运算操作才会有输出结果

#### 使用示例

例如：

```sql
select s1, - s1, s2, + s2, s1 + s2, s1 - s2, s1 * s2, s1 / s2, s1 % s2 from root.sg.d1
```

结果：

``` 
+-----------------------------+-------------+--------------+-------------+-------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|                         Time|root.sg.d1.s1|-root.sg.d1.s1|root.sg.d1.s2|root.sg.d1.s2|root.sg.d1.s1 + root.sg.d1.s2|root.sg.d1.s1 - root.sg.d1.s2|root.sg.d1.s1 * root.sg.d1.s2|root.sg.d1.s1 / root.sg.d1.s2|root.sg.d1.s1 % root.sg.d1.s2|
+-----------------------------+-------------+--------------+-------------+-------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|1970-01-01T08:00:00.001+08:00|          1.0|          -1.0|          1.0|          1.0|                          2.0|                          0.0|                          1.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.002+08:00|          2.0|          -2.0|          2.0|          2.0|                          4.0|                          0.0|                          4.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.003+08:00|          3.0|          -3.0|          3.0|          3.0|                          6.0|                          0.0|                          9.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.004+08:00|          4.0|          -4.0|          4.0|          4.0|                          8.0|                          0.0|                         16.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.005+08:00|          5.0|          -5.0|          5.0|          5.0|                         10.0|                          0.0|                         25.0|                          1.0|                          0.0|
+-----------------------------+-------------+--------------+-------------+-------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
Total line number = 5
It costs 0.014s
```

### 数学函数

目前 IoTDB 支持下列数学函数，这些数学函数的行为与这些函数在 Java Math 标准库中对应实现的行为一致。

| 函数名  | 输入序列类型                   | 输出序列类型             | 必要属性参数    | Java 标准库中的对应实现                                      |
| ------- | ------------------------------ | ------------------------ |-----------| ------------------------------------------------------------ |
| SIN     | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#sin(double)                                             |
| COS     | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#cos(double)                                             |
| TAN     | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#tan(double)                                             |
| ASIN    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#asin(double)                                            |
| ACOS    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#acos(double)                                            |
| ATAN    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#atan(double)                                            |
| SINH    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#sinh(double)                                            |
| COSH    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#cosh(double)                                            |
| TANH    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#tanh(double)                                            |
| DEGREES | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#toDegrees(double)                                       |
| RADIANS | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#toRadians(double)                                       |
| ABS     | INT32 / INT64 / FLOAT / DOUBLE | 与输入序列的实际类型一致 |           | Math#abs(int) / Math#abs(long) /Math#abs(float) /Math#abs(double) |
| SIGN    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#signum(double)                                          |
| CEIL    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#ceil(double)                                            |
| FLOOR   | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#floor(double)                                           |
| ROUND   | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |`places`:四舍五入有效位数，正数为小数点后面的有效位数，负数为整数位的有效位数 | Math#rint(Math#pow(10,places))/Math#pow(10,places)|
| EXP     | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#exp(double)                                             |
| LN      | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#log(double)                                             |
| LOG10   | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#log10(double)                                           |
| SQRT    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   |           | Math#sqrt(double)                                            |

例如：

```   sql
select s1, sin(s1), cos(s1), tan(s1) from root.sg1.d1 limit 5 offset 1000;
```

结果：

```
+-----------------------------+-------------------+-------------------+--------------------+-------------------+
|                         Time|     root.sg1.d1.s1|sin(root.sg1.d1.s1)| cos(root.sg1.d1.s1)|tan(root.sg1.d1.s1)|
+-----------------------------+-------------------+-------------------+--------------------+-------------------+
|2020-12-10T17:11:49.037+08:00|7360723084922759782| 0.8133527237573284|  0.5817708713544664| 1.3980636773094157|
|2020-12-10T17:11:49.038+08:00|4377791063319964531|-0.8938962705202537|  0.4482738644511651| -1.994085181866842|
|2020-12-10T17:11:49.039+08:00|7972485567734642915| 0.9627757585308978|-0.27030138509681073|-3.5618602479083545|
|2020-12-10T17:11:49.040+08:00|2508858212791964081|-0.6073417341629443| -0.7944406950452296| 0.7644897069734913|
|2020-12-10T17:11:49.041+08:00|2817297431185141819|-0.8419358900502509| -0.5395775727782725| 1.5603611649667768|
+-----------------------------+-------------------+-------------------+--------------------+-------------------+
Total line number = 5
It costs 0.008s
```
#### ROUND
例如：
```sql
select s4,round(s4),round(s4,2),round(s4,-1) from root.sg1.d1 
```

```sql
+-----------------------------+-------------+--------------------+----------------------+-----------------------+
|                         Time|root.db.d1.s4|ROUND(root.db.d1.s4)|ROUND(root.db.d1.s4,2)|ROUND(root.db.d1.s4,-1)|
+-----------------------------+-------------+--------------------+----------------------+-----------------------+
|1970-01-01T08:00:00.001+08:00|    101.14345|               101.0|                101.14|                  100.0|
|1970-01-01T08:00:00.002+08:00|    20.144346|                20.0|                 20.14|                   20.0|
|1970-01-01T08:00:00.003+08:00|    20.614372|                21.0|                 20.61|                   20.0|
|1970-01-01T08:00:00.005+08:00|    20.814346|                21.0|                 20.81|                   20.0|
|1970-01-01T08:00:00.006+08:00|     60.71443|                61.0|                 60.71|                   60.0|
|2023-03-13T16:16:19.764+08:00|    10.143425|                10.0|                 10.14|                   10.0|
+-----------------------------+-------------+--------------------+----------------------+-----------------------+
Total line number = 6
It costs 0.059s
```
