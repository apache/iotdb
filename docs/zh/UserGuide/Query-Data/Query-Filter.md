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

## 查询过滤条件

在 IoTDB 查询语句中，支持使用**时间过滤**和**值过滤**两种过滤条件。

支持的运算符如下：

- 比较运算符：大于（`>`）、大于等于（ `>=`）、等于（ `=` 或 `==`）、不等于（ `!=` 或 `<>`）、小于等于（ `<=`）、小于（ `<`）。
- 范围包含运算符：包含（ `IN` ）。
- 逻辑运算符：与（ `AND` 或 `&` 或 `&&`）、或（ `OR` 或 `|` 或 `||`）、非（ `NOT` 或 `!`）。

### 时间过滤条件

使用时间过滤条件可以筛选特定时间范围的数据。对于时间戳支持的格式，请参考 [时间戳类型](../Data-Concept/Data-Type.md) 。

示例如下：

1. 选择时间戳大于 2022-01-01T00:05:00.000 的数据：

   ```sql
   select s1 from root.sg1.d1 where time > 2022-01-01T00:05:00.000;
   ```

2. 选择时间戳等于 2022-01-01T00:05:00.000 的数据：

   ```sql
   select s1 from root.sg1.d1 where time = 2022-01-01T00:05:00.000;
   ```

3. 选择时间区间 [2017-11-01T00:05:00.000, 2017-11-01T00:12:00.000) 内的数据：

   ```sql
   select s1 from root.sg1.d1 where time >= 2022-01-01T00:05:00.000 and time < 2017-11-01T00:12:00.000;
   ```

注：在上述示例中，`time` 也可写做 `timestamp`。

### 值过滤条件

使用值过滤条件可以筛选数据值满足特定条件的数据。**允许**使用 select 子句中未选择的时间序列作为值过滤条件。

示例如下：

1. 选择值大于 36.5 的数据：

   ```sql
   select temperature from root.sg1.d1 where temperature > 36.5;
   ```

2. 选择值等于 true 的数据：

   ```sql
   select status from root.sg1.d1 where status = true;

3. 选择区间 (36.5,40] 的数据：

   ```sql
   select temperature from root.sg1.d1 where temperature > 36.5 and temperature < 40;
   ```

4. 选择值在特定范围内的数据：

   ```sql
   select code from root.sg1.d1 where code in ('200', '300', '400', '500');
   ```

5. 选择值在特定范围外的数据：

   ```sql
   select code from root.sg1.d1 where code not in ('200', '300', '400', '500');
   ```

### 模糊查询

在值过滤条件中，对于 TEXT 类型的数据，支持使用 `Like` 和 `Regexp` 运算符对数据进行模糊匹配

#### 使用 `Like` 进行模糊匹配 

**匹配规则：**

- `%` 表示任意0个或多个字符。
- `_` 表示任意单个字符。

**示例 1：** 查询 `root.sg.d1` 下 `value` 含有`'cc'`的数据。

```
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

```
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
