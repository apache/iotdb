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

# 字面值常量

该部分对 IoTDB 中支持的字面值常量进行说明，包括字符串常量、数值型常量、时间戳常量、布尔型常量和空值。

## 字符串常量

在 IoTDB 中，字符串是由**单引号（`'`）或双引号（`"`）字符括起来的字符序列**。示例如下：

```
'a string'
"another string"
```

### 使用场景

- `INSERT` 或者 `SELECT` 中用于表达 `TEXT` 类型数据的场景。

  ```sql
  # insert 示例
  insert into root.ln.wf02.wt02(timestamp,hardware) values(1, 'v1')
  insert into root.ln.wf02.wt02(timestamp,hardware) values(2, '\\')
  
  +-----------------------------+--------------------------+
  |                         Time|root.ln.wf02.wt02.hardware|
  +-----------------------------+--------------------------+
  |1970-01-01T08:00:00.001+08:00|                        v1|
  +-----------------------------+--------------------------+
  |1970-01-01T08:00:00.002+08:00|                        \\|
  +-----------------------------+--------------------------+
  
  # select 示例
  select code from root.sg1.d1 where code in ('string1', 'string2');
  ```

- `LOAD` / `REMOVE` / `SETTLE` 指令中的文件路径。

  ```sql
  # load 示例
  LOAD 'examplePath'
  
  # remove 示例
  REMOVE 'examplePath'
  
  # SETTLE 示例
  SETTLE 'examplePath'
  ```

- 用户密码。

  ```sql
  # 示例，write_pwd 即为用户密码
  CREATE USER ln_write_user 'write_pwd'
  ```

- 触发器和 UDF 中的类全类名，示例如下：

  ```sql
  # 触发器示例，AS 后使用字符串表示类全类名
  CREATE TRIGGER `alert-listener-sg1d1s1`
  AFTER INSERT
  ON root.sg1.d1.s1
  AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
  WITH (
    'lo' = '0', 
    'hi' = '100.0'
  )
  
  # UDF 示例，AS 后使用字符串表示类全类名
  CREATE FUNCTION example AS 'org.apache.iotdb.udf.UDTFExample'
  ```

- Select 子句中可以为结果集中的值指定别名，别名可以被定义为字符串或者标识符，示例如下：

  ```sql
  select s1 as 'temperature', s2 as 'speed' from root.ln.wf01.wt01;
  
  # 表头如下所示
  +-----------------------------+-----------|-----+
  |                         Time|temperature|speed|
  +-----------------------------+-----------|-----+
  ```

- 用于表示键值对，键值对的键和值可以被定义成常量（包括字符串）或者标识符，具体请参考键值对章节。

### 如何在字符串内使用引号

- 在单引号引起的字符串内，双引号无需特殊处理。同理，在双引号引起的字符串内，单引号无需特殊处理。
- 在单引号引起的字符串里，可以通过双写单引号来表示一个单引号，即单引号 ' 可以表示为 ''。
- 在双引号引起的字符串里，可以通过双写双引号来表示一个双引号，即双引号 " 可以表示为 ""。

字符串内使用引号的示例如下：

```
'string'  // string
'"string"'  // "string"
'""string""'  // ""string""
'''string'  // 'string

"string" // string
"'string'"  // 'string'
"''string''"  // ''string''
"""string"  // "string
```

## 数值型常量

数值型常量包括整型和浮点型。

整型常量是一个数字序列。可以以 `+` 或 `-` 开头表示正负。例如：`1`, `-1`。

带有小数部分或由科学计数法表示的为浮点型常量，例如：`.1`, `3.14`, `-2.23`, `+1.70`, `1.2E3`, `1.2E-3`, `-1.2E3`, `-1.2E-3`。

在 IoTDB 中，`INT32` 和 `INT64` 表示整数类型（计算是准确的），`FLOAT` 和 `DOUBLE` 表示浮点数类型（计算是近似的）。

在浮点上下文中可以使用整数，它会被解释为等效的浮点数。

## 时间戳常量

时间戳是一个数据到来的时间点，在 IoTDB 中分为绝对时间戳和相对时间戳。详细信息可参考 [数据类型文档](https://iotdb.apache.org/zh/UserGuide/Master/Data-Concept/Data-Type.html)。

特别地，`NOW()`表示语句开始执行时的服务端系统时间戳。

## 布尔型常量

布尔值常量 `TRUE` 和 `FALSE` 分别等价于 `1` 和 `0`，它们对大小写不敏感。

## 空值

`NULL`值表示没有数据。`NULL`对大小写不敏感。