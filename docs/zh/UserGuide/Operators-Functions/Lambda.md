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

# Lambda 表达式

## JEXL 自定义函数

### 函数简介

Java Expression Language (JEXL) 是一个表达式语言引擎。我们使用 JEXL 来扩展 UDF，在命令行中，通过简易的 lambda 表达式来实现 UDF。

lambda 表达式中支持的运算符详见链接 [JEXL 中 lambda 表达式支持的运算符](https://commons.apache.org/proper/commons-jexl/apidocs/org/apache/commons/jexl3/package-summary.html#customization) 。

| 函数名      | 可接收的输入序列类型                     | 必要的属性参数                               | 输出序列类型     | 功能类型                                             |
|----------|--------------------------------|---------------------------------------|------------|--------------------------------------------------|
| JEXL   | INT32 / INT64 / FLOAT / DOUBLE / TEXT / BOOLEAN | `expr`是一个支持标准的一元或多元参数的lambda表达式，符合`x -> {...}`或`(x, y, z) -> {...}`的格式，例如`x -> {x * 2}`, `(x, y, z) -> {x + y * z}`| INT32 / INT64 / FLOAT / DOUBLE / TEXT / BOOLEAN | 返回将输入的时间序列通过lambda表达式变换的序列             |

### 使用示例

输入序列：
```
IoTDB> select * from root.ln.wf01.wt01;
+-----------------------------+---------------------+--------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.str|root.ln.wf01.wt01.st|root.ln.wf01.wt01.temperature|
+-----------------------------+---------------------+--------------------+-----------------------------+
|1970-01-01T08:00:00.000+08:00|                  str|                10.0|                          0.0|
|1970-01-01T08:00:00.001+08:00|                  str|                20.0|                          1.0|
|1970-01-01T08:00:00.002+08:00|                  str|                30.0|                          2.0|
|1970-01-01T08:00:00.003+08:00|                  str|                40.0|                          3.0|
|1970-01-01T08:00:00.004+08:00|                  str|                50.0|                          4.0|
|1970-01-01T08:00:00.005+08:00|                  str|                60.0|                          5.0|
|1970-01-01T08:00:00.006+08:00|                  str|                70.0|                          6.0|
|1970-01-01T08:00:00.007+08:00|                  str|                80.0|                          7.0|
|1970-01-01T08:00:00.008+08:00|                  str|                90.0|                          8.0|
|1970-01-01T08:00:00.009+08:00|                  str|               100.0|                          9.0|
|1970-01-01T08:00:00.010+08:00|                  str|               110.0|                         10.0|
+-----------------------------+---------------------+--------------------+-----------------------------+
```

用于查询的SQL语句：
```sql
select jexl(temperature, 'expr'='x -> {x + x}') as jexl1, jexl(temperature, 'expr'='x -> {x * 3}') as jexl2, jexl(temperature, 'expr'='x -> {x * x}') as jexl3, jexl(temperature, 'expr'='x -> {multiply(x, 100)}') as jexl4, jexl(temperature, st, 'expr'='(x, y) -> {x + y}') as jexl5, jexl(temperature, st, str, 'expr'='(x, y, z) -> {x + y + z}') as jexl6 from root.ln.wf01.wt01;
```

输出序列：
```
+-----------------------------+-----+-----+-----+------+-----+--------+
|                         Time|jexl1|jexl2|jexl3| jexl4|jexl5|   jexl6|
+-----------------------------+-----+-----+-----+------+-----+--------+
|1970-01-01T08:00:00.000+08:00|  0.0|  0.0|  0.0|   0.0| 10.0| 10.0str|
|1970-01-01T08:00:00.001+08:00|  2.0|  3.0|  1.0| 100.0| 21.0| 21.0str|
|1970-01-01T08:00:00.002+08:00|  4.0|  6.0|  4.0| 200.0| 32.0| 32.0str|
|1970-01-01T08:00:00.003+08:00|  6.0|  9.0|  9.0| 300.0| 43.0| 43.0str|
|1970-01-01T08:00:00.004+08:00|  8.0| 12.0| 16.0| 400.0| 54.0| 54.0str|
|1970-01-01T08:00:00.005+08:00| 10.0| 15.0| 25.0| 500.0| 65.0| 65.0str|
|1970-01-01T08:00:00.006+08:00| 12.0| 18.0| 36.0| 600.0| 76.0| 76.0str|
|1970-01-01T08:00:00.007+08:00| 14.0| 21.0| 49.0| 700.0| 87.0| 87.0str|
|1970-01-01T08:00:00.008+08:00| 16.0| 24.0| 64.0| 800.0| 98.0| 98.0str|
|1970-01-01T08:00:00.009+08:00| 18.0| 27.0| 81.0| 900.0|109.0|109.0str|
|1970-01-01T08:00:00.010+08:00| 20.0| 30.0|100.0|1000.0|120.0|120.0str|
+-----------------------------+-----+-----+-----+------+-----+--------+
Total line number = 11
It costs 0.118s
```

