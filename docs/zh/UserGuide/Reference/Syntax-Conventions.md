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

字符串是由单引号（`'`）或双引号（`"`）字符括起来的字符序列。示例如下：
```js
'a string'
"another string"
```

字符串字面值的使用场景：

- `INSERT` 或者 `SELECT` 中用于表达 `TEXT` 类型数据的场景
- SQL 中 UDF 和 Trigger 的 Java 类全类名
- `CREATE TRIGGER` 语句中描述触发器属性的键值对
- UDF 函数输入参数中的属性
- `LOAD` / `REMOVE` / `SETTLE` 指令中的文件路径
- 用户密码

通过以下几种方式可以在字符串内使用引号：

- 在引号前使用转义符 (\\)。
- 在单引号括的的字符串内，双引号无需特殊处理。同理，在双引号括的的字符串内，单引号无需特殊处理。

关于引号和转义字符的使用示例如下：
```js
'string'  // string
'"string"'  // "string"
'""string""'  // ""string""
'str\'ing'  // str'ing
'\'string'  // 'string
"string" // string
"'string'"  // 'string'
"''string''"  // ''string''
"str\"ing"  // str"ing
"\"string"  // "string
```

### 数值型常量

数值型常量包括整型和浮点型。

整型常量是一个数字序列。可以以 `+` 或 `-` 开头表示正负。例如：`1`, `-1`。

带有小数部分或由科学计数法表示的为浮点型常量，例如：`.1`, `3.14`, `-2.23`, `+1.70`, `1.2E3`, `1.2E-3`, `-1.2E3`, `-1.2E-3`。

在 IoTDB 中，`INT32` 和 `INT64` 表示整数类型（计算是准确的），`FLOAT` 和 `DOUBLE` 表示浮点数类型（计算是近似的）。

在浮点上下文中可以使用整数，它会被解释为等效的浮点数。

### 时间戳常量

时间戳是一个数据到来的时间点，在 IoTDB 中分为绝对时间戳和相对时间戳。详细信息可参考 [数据类型文档](../Data-Concept/Data-Type.md)。

特别地，`NOW()`表示语句开始执行时的服务端系统时间戳。

### 布尔型常量

布尔值常量 `TRUE` 和 `FALSE` 分别等价于 `1` 和 `0`，它们对大小写不敏感。

### 空值

`NULL`值表示没有数据。`NULL`对大小写不敏感。

## 标识符

在 IoTDB 中，触发器名称、UDF函数名、元数据模板名称、用户与角色名等被称为标识符。

标识符命名有以下约束：

- 在不含引用的标识符中，允许出现以下字符：
  - [0-9 a-z A-Z _ : @ # $ { }] （字母，数字，部分特殊字符）
  - ['\u2E80'..'\u9FFF'] （UNICODE 中文字符）
- 标识符允许使用数字开头、允许全部为数字（**不推荐！**）。
- 标识符是大小写敏感的。
- 注意：用户与角色名对大小写不敏感，并且不允许转义特殊字符。

如果标识符要包含不允许的特殊字符，或者使用系统关键字，需要用反引号（`）对标识符进行引用。反引号引用的标识符中出现反引号需要反斜杠转义。

示例如下：
```sql
id  // 合法，被解析为 id
ID  // 合法，被解析为 ID，与 id 不同
id0  // 合法，被解析为 id0
_id  // 合法，被解析为 _id
0id  // 合法，被解析为 0id
233  // 合法，被解析为 233 (不推荐！)
ab!  // 不合法，包含不被允许的特殊字符
`ab!`  // 合法，被解析为 ab!
`"ab"`  // 合法，被解析为 "ab"
`a`b`  // 不合法，反引号应使用反斜杠进行转义
`a\`b`  // 合法，被解析为 a`b
```

## 路径节点名

我们称一个路径中由 `.` 分割的部分叫做节点（node name）。

路径节点名的约束与标识符基本一致，但要额外注意以下几点：

- `root` 只允许出现时间序列的开头，若其他层级出现 `root`，则无法解析，提示报错。
- 无论是否使用反引号引用，路径分隔符（`.`）都不能出现在路径节点名中。 如果路径节点名中一定要出现 `.` （不推荐！），需要用单引号或双引号括起。在这种情况下，为避免引发歧义，引号被系统视为节点名的一部分。
- 在反引号括起的路径节点名中，单引号和双引号需要使用反斜杠进行转义。
- 特别地，如果系统在 Windows 系统上部署，那么存储组层级名称是**大小写不敏感**的。例如，同时创建 `root.ln` 和 `root.LN` 是不被允许的。

示例如下：

```sql
CREATE TIMESERIES root.a.b.s1+s2/s3.c WITH DATATYPE=INT32, ENCODING=RLE
// 解析失败！

CREATE TIMESERIES root.a.b.`s1+s2/s3`.c WITH DATATYPE=INT32, ENCODING=RLE
// root.a.b.`s1+s2/s3`.c 将被解析为 Path[root, a, b, s1+s2/s3, c]
```

```sql
CREATE TIMESERIES root.a.b.select WITH DATATYPE=INT32, ENCODING=RLE
// 解析失败！

CREATE TIMESERIES root.a.b.`select` WITH DATATYPE=INT32, ENCODING=RLE
// root.a.b.`select` 将被解析为 Path[root, a, b, select]
```

```sql
CREATE TIMESERIES root.a.b.`s1.s2`.c WITH DATATYPE=INT32, ENCODING=RLE
// 解析失败！

CREATE TIMESERIES root.a.b."s1.s2".c WITH DATATYPE=INT32, ENCODING=RLE
// root.a.b."s1.s2".c 将被解析为 Path[root, a, b, "s1.s2", c]
```

```sql
CREATE TIMESERIES root.a.b.`s1"s2`.c WITH DATATYPE=INT32, ENCODING=RLE
// 解析失败！

CREATE TIMESERIES root.a.b.`s1\"s2`.c WITH DATATYPE=INT32, ENCODING=RLE
// root.a.b.`s1\"s2`.c 将被解析为 Path[root, a, b, s1\"s2, c]
```

## 关键字和保留字

关键字是在 SQL 具有特定含义的词，不能直接用于标识符或路径节点名，需要使用反引号进行转义。保留字是关键字的一个子集，保留字不能用于标识符或路径节点名（即使进行了转义）。

关于 IoTDB 的关键字和保留字列表，可以查看 [关键字和保留字](Keywords.md) 。

## 表达式

IoTDB 支持在 `select` 子句中执行由数字常量、时间序列、算数运算表达式和时间序列生成函数（包括用户自定义函数）组成的任意嵌套表达式。

注意：当参与表达式的路径节点名由纯数字、单引号、或双引号组成（不推荐！）时，必须使用反引号（`）括起，以免引起歧义。示例如下：  

```sql
-- 存在时间序列： root.sg.d.0, root.sg.d.'a' 和 root.sg."d".b
select 0 from root.sg.d  -- 存在歧义，解析失败
select 'a' from root.sg.d -- 存在歧义，解析失败
select "d".b from root.sg -- 存在歧义，解析失败
select `0` from root.sg.d  -- 对时间序列 root.sg.d.0 进行查询
select `0` + 0 from root.sg.d -- 表达式，对时间序列 root.sg.d.0 的每一个查询结果加 0
select myudf(`'a'`, 'x') from root.sg.d -- 表达式，调用函数 myudf，第一个参数为时间序列 root.sg.d.'a'，第二个参数为字符串常量 'x'
```

## 引用符号

### 双引号（"）、单引号（'）

双引号、单引号的使用场景如下：

1. 字符串字面值由单引号或双引号括起的字符串表示。
2. 如果要在路径节点名中使用路径分隔符（`.`），则需要将路径节点名用单引号或双引号括起。在这种情况下，为避免引发歧义，引号被系统视为节点名的一部分。

### 反引号（\`）

反引号的使用场景如下：

1. 在标识符中使用特殊字符时，标识符需要使用反引号括起。
2. 在路径节点名中使用除路径分隔符之外的特殊字符时，路径节点名需要使用反引号括起。在这种情况下，反引号不会被系统视为节点名的一部分。

### 反斜杠（\）

反斜杠的使用场景如下：
- 在字符串常量中，出现双引号或单引号时，要使用反斜杠进行转义。
  - 如："str\\"ing" 解析为 str"ing、'str\\'ing' 解析为 str'ing。
- 在标识符中，出现反引号时，要使用反斜杠进行转义。
  - 如：\`na\\\`me\` 解析为 na\`me。
- 在路径节点名中，出现双引号或单引号时，要使用反斜杠进行转义。注意，为了避免歧义，反斜杠会被系统视为节点名的一部分。 
  - 如：root.sg1.d1."a\\"b" 解析为 Path[root, sg1, d1, "a\\"b"]、root.sg1.d1.'a\\'b' 解析为 Path[root, sg1, d1, 'a\\'b']、root.sg1.d1.\`a\\"b\` 解析为 Path[root, sg1, d1, a\\"b]、root.sg1.d1.\`a\\'b\` 解析为 Path[root, sg1, d1, a\\'b]。


## 了解更多

请阅读代码仓库中的词法和语法描述文件：

词法文件：`antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlLexer.g4`

语法文件：`antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlParser.g4`
