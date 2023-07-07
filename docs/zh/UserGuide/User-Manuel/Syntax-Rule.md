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

# 语法约定
## 字面值常量

该部分对 IoTDB 中支持的字面值常量进行说明，包括字符串常量、数值型常量、时间戳常量、布尔型常量和空值。

### 字符串常量

在 IoTDB 中，字符串是由**单引号（`'`）或双引号（`"`）字符括起来的字符序列**。示例如下：

```Plain%20Text
'a string'
"another string"
```

#### 使用场景

- `INSERT` 或者 `SELECT` 中用于表达 `TEXT` 类型数据的场景。

  ```SQL
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

  ```SQL
  # load 示例
  LOAD 'examplePath'
  
  # remove 示例
  REMOVE 'examplePath'
  
  # SETTLE 示例
  SETTLE 'examplePath'
  ```

- 用户密码。

  ```SQL
  # 示例，write_pwd 即为用户密码
  CREATE USER ln_write_user 'write_pwd'
  ```

- 触发器和 UDF 中的类全类名，示例如下：

  ```SQL
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

  ```SQL
  select s1 as 'temperature', s2 as 'speed' from root.ln.wf01.wt01;
  
  # 表头如下所示
  +-----------------------------+-----------|-----+
  |                         Time|temperature|speed|
  +-----------------------------+-----------|-----+
  ```

- 用于表示键值对，键值对的键和值可以被定义成常量（包括字符串）或者标识符，具体请参考键值对章节。

#### 如何在字符串内使用引号

- 在单引号引起的字符串内，双引号无需特殊处理。同理，在双引号引起的字符串内，单引号无需特殊处理。
- 在单引号引起的字符串里，可以通过双写单引号来表示一个单引号，即单引号 ' 可以表示为 ''。
- 在双引号引起的字符串里，可以通过双写双引号来表示一个双引号，即双引号 " 可以表示为 ""。

字符串内使用引号的示例如下：

```Plain%20Text
'string'  // string
'"string"'  // "string"
'""string""'  // ""string""
'''string'  // 'string

"string" // string
"'string'"  // 'string'
"''string''"  // ''string''
"""string"  // "string
```

### 数值型常量

数值型常量包括整型和浮点型。

整型常量是一个数字序列。可以以 `+` 或 `-` 开头表示正负。例如：`1`, `-1`。

带有小数部分或由科学计数法表示的为浮点型常量，例如：`.1`, `3.14`, `-2.23`, `+1.70`, `1.2E3`, `1.2E-3`, `-1.2E3`, `-1.2E-3`。

在 IoTDB 中，`INT32` 和 `INT64` 表示整数类型（计算是准确的），`FLOAT` 和 `DOUBLE` 表示浮点数类型（计算是近似的）。

在浮点上下文中可以使用整数，它会被解释为等效的浮点数。

### 时间戳常量

时间戳是一个数据到来的时间点，在 IoTDB 中分为绝对时间戳和相对时间戳。详细信息可参考 [数据类型文档](https://iotdb.apache.org/zh/UserGuide/Master/Data-Concept/Data-Type.html)。

特别地，`NOW()`表示语句开始执行时的服务端系统时间戳。

### 布尔型常量

布尔值常量 `TRUE` 和 `FALSE` 分别等价于 `1` 和 `0`，它们对大小写不敏感。

### 空值

`NULL`值表示没有数据。`NULL`对大小写不敏感。

## 标识符

### 使用场景

在 IoTDB 中，触发器名称、UDF函数名、元数据模板名称、用户与角色名、连续查询标识、Pipe、PipeSink、键值对中的键和值、别名等可以作为标识符。

### 约束

请注意，此处约束是标识符的通用约束，具体标识符可能还附带其它约束条件，如用户名限制字符数大于等于4，更严格的约束请参考具体标识符相关的说明文档。

**标识符命名有以下约束：**

- 不使用反引号括起的标识符中，允许出现以下字符：
  - [ 0-9 a-z A-Z _ ] （字母，数字，下划线）
  - ['\u2E80'..'\u9FFF'] （UNICODE 中文字符）

- 标识符允许使用数字开头、不使用反引号括起的标识符不能全部为数字。

- 标识符是大小写敏感的。

- 标识符允许为关键字。

**如果出现如下情况，标识符需要使用反引号进行引用：**

- 标识符包含不允许的特殊字符。
- 标识符为实数。

### 如何在反引号引起的标识符中使用引号

**在反引号引起的标识符中可以直接使用单引号和双引号。**

**在用反引号引用的标识符中，可以通过双写反引号的方式使用反引号，即 ` 可以表示为 ``**，示例如下：

```SQL
# 创建模板 t1`t
create schema template `t1``t` 
(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)

# 创建模板 t1't"t
create schema template `t1't"t` 
(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

### 特殊情况示例

需要使用反引号进行引用的部分情况示例：

- 触发器名称出现上述特殊情况时需使用反引号引用：

  ```sql
  # 创建触发器 alert.`listener-sg1d1s1
  CREATE TRIGGER `alert.``listener-sg1d1s1`
  AFTER INSERT
  ON root.sg1.d1.s1
  AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
  WITH (
    'lo' = '0', 
    'hi' = '100.0'
  )
  ```

- UDF 名称出现上述特殊情况时需使用反引号引用：

  ```sql
  # 创建名为 111 的 UDF，111 为实数，所以需要用反引号引用。
  CREATE FUNCTION `111` AS 'org.apache.iotdb.udf.UDTFExample'
  ```

- 元数据模板名称出现上述特殊情况时需使用反引号引用：

  ```sql
  # 创建名为 111 的元数据模板，111 为实数，需要用反引号引用。
  create schema template `111` 
  (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
  ```

- 用户名、角色名出现上述特殊情况时需使用反引号引用，同时无论是否使用反引号引用，用户名、角色名中均不允许出现空格，具体请参考权限管理章节中的说明。

  ```sql
  # 创建用户 special`user.
  CREATE USER `special``user.` 'write_pwd'
  
  # 创建角色 111
  CREATE ROLE `111`
  ```

- 连续查询标识出现上述特殊情况时需使用反引号引用：

  ```sql
  # 创建连续查询 test.cq
  CREATE CONTINUOUS QUERY `test.cq` 
  BEGIN 
    SELECT max_value(temperature) 
    INTO temperature_max 
    FROM root.ln.*.* 
    GROUP BY time(10s) 
  END
  ```

- Pipe、PipeSink 名称出现上述特殊情况时需使用反引号引用：

  ```sql
  # 创建 PipeSink test.*1
  CREATE PIPESINK `test.*1` AS IoTDB ('ip' = '输入你的IP')
  
  # 创建 Pipe test.*2
  CREATE PIPE `test.*2` TO `test.*1` FROM 
  (select ** from root WHERE time>=yyyy-mm-dd HH:MM:SS) WITH 'SyncDelOp' = 'true'
  ```

- Select 子句中可以结果集中的值指定别名，别名可以被定义为字符串或者标识符，示例如下：

  ```sql
  select s1 as temperature, s2 as speed from root.ln.wf01.wt01;
  # 表头如下所示
  +-----------------------------+-----------+-----+
  |                         Time|temperature|speed|
  +-----------------------------+-----------+-----+
  ```

- 用于表示键值对，键值对的键和值可以被定义成常量（包括字符串）或者标识符，具体请参考键值对章节。

## 关键字

关键字是在 SQL 具有特定含义的词，可以作为标识符。保留字是关键字的一个子集，保留字不能用于标识符。

关于 IoTDB 的关键字列表，可以查看 [关键字](https://iotdb.apache.org/zh/UserGuide/Master/Reference/Keywords.html) 。

## 词法与文法详细定义

请阅读代码仓库中的词法和语法描述文件：

词法文件：`antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlLexer.g4`

语法文件：`antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlParser.g4`
