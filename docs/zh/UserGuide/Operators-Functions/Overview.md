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

# 运算符和函数

本章介绍 IoTDB 支持的运算符和函数。IoTDB 提供了丰富的内置运算符和函数来满足您的计算需求，同时支持通过[用户自定义函数](./User-Defined-Function.md)能力进行扩展。

可以使用 `SHOW FUNCTIONS` 显示所有可用函数的列表，包括内置函数和自定义函数。

关于运算符和函数在 SQL 中的行为，可以查看文档 [选择表达式](../Query-Data/Select-Expression.md)。

## 运算符列表

### 算数运算符
|运算符                       |含义|
|----------------------------|-----------|
|`+`                         |取正（单目）|
|`-`                         |取负（单目）|
|`*`                         |乘|
|`/`                         |除|
|`%`                         |取余|
|`+`                         |加|
|`-`                         |减|

### 比较运算符
|运算符                       |含义|
|----------------------------|-----------|
|`>`                         |大于|
|`>=`                        |大于等于|
|`<`                         |小于|
|`<=`                        |小于等于|
|`==`                        |等于|
|`!=` / `<>`                 |不等于|
|`BETWEEN ... AND ...`       |在指定范围内|
|`NOT BETWEEN ... AND ...`   |不在指定范围内|
|`LIKE`                      |匹配简单模式|
|`NOT LIKE`                  |无法匹配简单模式|
|`REGEXP`                    |匹配正则表达式|
|`NOT REGEXP`                |无法匹配正则表达式|
|`IS NULL`                   |是空值|
|`IS NOT NULL`               |不是空值|
|`IN` / `CONTAINS`           |是指定列表中的值|
|`NOT IN` / `NOT CONTAINS`   |不是指定列表中的值|

### 逻辑运算符
|运算符                       |含义|
|----------------------------|-----------|
|`NOT` / `!`                 |取非（单目）|
|`AND` / `&` / `&&`          |逻辑与|
|`OR`/ &#124; / &#124;&#124; |逻辑或|
<!--- &#124;即管道符 转义不能用在``里, 表格内不允许使用管道符 -->

### 运算符优先级

运算符的优先级从高到低如下所示排列，同一行的运算符具有相同的优先级。
```sql
!, - (单目), + (单目)
*, /, DIV, %, MOD
-, +
=, ==, <=>, >=, >, <=, <, <>, !=
LIKE, REGEXP, NOT LIKE, NOT REGEXP
BETWEEN ... AND ..., NOT BETWEEN ... AND ...
IS NULL, IS NOT NULL
IN, CONTAINS, NOT IN, NOT CONTAINS
AND, &, &&
OR, |, ||
```

## 内置函数列表

### 数学函数 

### 字符串处理函数

### 选择函数

### 趋势计算函数

### 常序列生成函数

### 数据类型转换函数

### 比较函数

### 区间查询函数

### 采样函数

## 数据质量函数库

对基于时序数据的应用而言，数据质量至关重要。基于用户自定义函数能力，IoTDB 提供了一系列关于数据质量的函数，包括数据画像、数据质量评估与修复等，能够满足工业领域对数据质量的需求。

**该函数库中的函数不是内置函数，使用前要先加载到系统中。** 操作流程如下：
1. 下载包含全部依赖的 jar 包和注册脚本（下载地址：XXX）；
2. 将 jar 包复制到 IoTDB 程序目录的 `ext\udf` 目录下；
3. 运行`sbin\start-server.bat`（在 Windows 下）或`sbin\start-server.sh`（在 Linux 或 MacOS 下）以启动 IoTDB 服务器；
4. 将注册脚本复制到 IoTDB 的程序目录下（与`sbin`目录同级的根目录下），修改脚本中的参数（如果需要）并运行注册脚本以注册 UDF。

### 数据画像

### 异常监测

### 数据匹配

### 频域分析

### 数据质量

### 数据修复

### 序列发现
