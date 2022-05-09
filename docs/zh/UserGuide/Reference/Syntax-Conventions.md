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

```Plain%20Text
'a string'
"another string"
```

除文件路径以外，我们会对字符串常量做反转义处理，具体使用可以参考使用场景中的示例。

转义字符可以参考链接：[Characters (The Java™ Tutorials > Learning the Java Language > Numbers and Strings)](https://docs.oracle.com/javase/tutorial/java/data/characters.html)

#### 使用场景

- `INSERT` 或者 `SELECT` 中用于表达 `TEXT` 类型数据的场景。

  ```SQL
  # insert 示例
  insert into root.ln.wf02.wt02(timestamp,hardware) values(1, 'v1')
  insert into root.ln.wf02.wt02(timestamp,hardware) values(2, '\\')
  
  # 查询 root.ln.wf02.wt02的数据，结果如下，可以看到\\被转义为了\
  +-----------------------------+--------------------------+
  |                         Time|root.ln.wf02.wt02.hardware|
  +-----------------------------+--------------------------+
  |1970-01-01T08:00:00.001+08:00|                        v1|
  +-----------------------------+--------------------------+
  |1970-01-01T08:00:00.002+08:00|                         \|
  +-----------------------------+--------------------------+
  
  # select 示例
  select code from root.sg1.d1 where code in ('string1', 'string2');
  ```

- `LOAD` / `REMOVE` / `SETTLE` 指令中的文件路径。由于windows系统使用反斜杠\作为路径分隔符，文件路径我们不会做反转义处理。

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
  # 示例，'write_pwd'即为用户密码
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

- 用于表示键值对，键值对的键可以被定义成字符串或者标识符，键值对的值可以被定义成常量（包括字符串）或者标识符，更推荐将键值对表示为字符串。示例如下：

  1. 触发器中表示触发器属性的键值对。参考示例语句中 WITH 后的属性键值对。

  ```SQL
  # 示例
  CREATE TRIGGER `alert-listener-sg1d1s1`
  AFTER INSERT
  ON root.sg1.d1.s1
  AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
  WITH (
    'lo' = '0', 
    'hi' = '100.0'
  )
  ```

  2. UDF 中函数输入参数中的属性键值对。参考示例语句中 SELECT 子句中的属性键值对。

  ```SQL
  # 示例
  SELECT example(s1, s2, 'key1'='value1', 'key2'='value2') FROM root.sg.d1;
  ```

  3. 时间序列中用于表示标签和属性的键值对。

  ```SQL
  # 创建时间序列时设定标签和属性
  CREATE timeseries root.turbine.d1.s1(temprature) 
  WITH datatype=FLOAT, encoding=RLE, compression=SNAPPY, 'max_point_number' = '5'
  TAGS('tag1' = 'v1', 'tag2'= 'v2') ATTRIBUTES('attr1' = 'v1', 'attr2' = 'v2')
  
  # 修改时间序列的标签和属性
  ALTER timeseries root.turbine.d1.s1 SET 'newTag1' = 'newV1', 'attr1' = 'newV1'
  
  # 修改标签名
  ALTER timeseries root.turbine.d1.s1 RENAME 'tag1' TO 'newTag1'
  
  # 插入别名、标签、属性
  ALTER timeseries root.turbine.d1.s1 UPSERT 
  ALIAS='newAlias' TAGS('tag2' = 'newV2', tag3=v3) ATTRIBUTES('attr3' ='v3', 'attr4'='v4')
  
  # 添加新的标签
  ALTER timeseries root.turbine.d1.s1 ADD TAGS 'tag3' = 'v3', 'tag4' = 'v4'
  
  # 添加新的属性
  ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES 'attr3' = 'v3', 'attr4' = 'v4'
  
  # 查询符合条件的时间序列信息
  SHOW timeseries root.ln.** WHRER 'unit' = 'c'
  ```

  4. 创建 Pipe 以及 PipeSink 时表示属性的键值对。

  ```SQL
  # 创建 PipeSink 时表示属性
  CREATE PIPESINK my_iotdb AS IoTDB ('ip' = '输入你的IP')
  
  # 创建 Pipe 时在 WITH 子句中表示属性
  CREATE PIPE my_pipe TO my_iotdb FROM 
  (select ** from root WHERE time>=yyyy-mm-dd HH:MM:SS) WITH 'SyncDelOp' = 'true'
  ```

#### 如何在字符串内使用引号

- 在单引号引起的字符串内，双引号无需特殊处理。同理，在双引号引起的字符串内，单引号无需特殊处理。

- 在引号前使用转义符 (\)。

- 在单引号引起的字符串里，可以通过双写单引号来表示一个单引号，即单引号 ' 可以表示为 ''。

- 在双引号引起的字符串里，可以通过双写双引号来表示一个双引号，即双引号 " 可以表示为 ""。

字符串内使用引号的示例如下：

```Plain%20Text
'string'  // string
'"string"'  // "string"
'""string""'  // ""string""
'str\'ing'  // str'ing
'\'string'  // 'string
'''string'  // 'string

"string" // string
"'string'"  // 'string'
"''string''"  // ''string''
"str\"ing"  // str"ing
"\"string"  // "string
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

在 IoTDB 中，触发器名称、UDF函数名、元数据模板名称、用户与角色名、连续查询标识、Pipe、PipeSink、键值对中的键和值、别名等被称为标识符。

### 约束

请注意，此处约束是标识符的通用约束，具体标识符可能还附带其它约束条件，如用户名限制字符数大于等于4，更严格的约束请参考具体标识符相关的说明文档。

标识符命名有以下约束：

- 不使用反引号括起的标识符中，允许出现以下字符：

  - [ 0-9 a-z A-Z _ : @ # $ { } ] （字母，数字，部分特殊字符）

  - ['\u2E80'..'\u9FFF'] （UNICODE 中文字符）

- 标识符允许使用数字开头、不使用反引号括起的标识符不能全部为数字。

- 标识符是大小写敏感的。

如果出现如下情况，标识符需要使用反引号进行引用：

- 标识符包含不允许的特殊字符。
- 标识符为系统关键字。
- 标识符为纯数字。

### 如何在反引号引起的标识符中使用引号

在反引号引起的标识符中可以直接使用单引号和双引号。

在用反引号引用的标识符中，可以通过双写反引号的方式使用反引号，即 ` 可以表示为 ``，示例如下：

```SQL
# 创建模板 t1't"t
create schema template `t1't"t` 
(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)

# 创建模板 t1`t
create schema template `t1``t` 
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
  # 创建名为 select 的 UDF，select 为系统关键字，所以需要用反引号引用
  CREATE FUNCTION `select` AS 'org.apache.iotdb.udf.UDTFExample'
  ```

- 元数据模板名称出现上述特殊情况时需使用反引号引用：

  ```sql
  # 创建名为 111 的元数据模板，111 为纯数字，需要用反引号引用
  create schema template `111` 
  (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
  ```

- 用户名、角色名出现上述特殊情况时需使用反引号引用，同时无论是否使用反引号引用，用户名、角色名中均不允许出现空格，具体请参考权限管理章节中的说明。

  ```sql
  # 创建用户 special`user.
  CREATE USER `special``user.` 'write_pwd'
  
  # 创建角色 `select`
  CREATE ROLE `select`
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
  +-----------------------------+-----------|-----+
  |                         Time|temperature|speed|
  +-----------------------------+-----------|-----+
  ```

- 用于表示键值对，键值对的键可以被定义成字符串或者标识符，键值对的值可以被定义成常量（包括字符串）或者标识符，更推荐将键值对表示为字符串。键值对的使用范围和字符串常量中提到的一致，下面以时间序列中用于表示标签和属性的键值对作为示例：

  ```SQL
  # 创建时间序列时设定标签和属性
  CREATE timeseries root.turbine.d1.s1(temprature) 
  WITH datatype=FLOAT, encoding=RLE, compression=SNAPPY, max_point_number = 5
  TAGS(tag1 = v1, tag2= v2) ATTRIBUTES(attr1 = v1, attr2 = v2)
  
  # 修改时间序列的标签和属性
  ALTER timeseries root.turbine.d1.s1 SET newTag1 = newV1, attr1 = newV1
  
  # 修改标签名
  ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO newTag1
  
  # 插入别名、标签、属性
  ALTER timeseries root.turbine.d1.s1 UPSERT 
  ALIAS = newAlias TAGS(tag2 = newV2, tag3 = v3) ATTRIBUTES(attr3 = v3, attr4 = v4)
  
  # 添加新的标签
  ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3 = v3, tag4 = v4
  
  # 添加新的属性
  ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES attr3 = v3, attr4 = v4
  
  # 查询符合条件的时间序列信息
  SHOW timeseries root.ln.** WHRER unit = c
  ```

## 路径结点名

路径结点名是特殊的标识符，其还可以是通配符 \* 或 \*\*。在创建时间序列时，各层级的路径结点名不能为通配符 \* 或 \*\*。在查询语句中，可以用通配符 \* 或 \*\* 来表示路径结点名，以匹配一层或多层路径。

### 通配符

`*`在路径中表示一层。例如`root.vehicle.*.sensor1`代表的是以`root.vehicle`为前缀，以`sensor1`为后缀，层次等于 4 层的路径。

`**`在路径中表示是（`*`）+，即为一层或多层`*`。例如`root.vehicle.device1.**`代表的是`root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`等所有以`root.vehicle.device1`为前缀路径的大于等于 4 层的路径；`root.vehicle.**.sensor1`代表的是以`root.vehicle`为前缀，以`sensor1`为后缀，层次大于等于 4 层的路径。

由于通配符 * 在查询表达式中也可以表示乘法符号，下述例子用于帮助您区分两种情况：

```SQL
# 创建时间序列 root.sg.a*b
create timeseries root.sg.`a*b` with datatype=FLOAT,encoding=PLAIN;
# 请注意，如标识符部分所述，a*b包含特殊字符，需要用``括起来使用
# create timeseries root.sg.a*b with datatype=FLOAT,encoding=PLAIN 是错误用法

# 创建时间序列 root.sg.a
create timeseries root.sg.a with datatype=FLOAT,encoding=PLAIN;

# 创建时间序列 root.sg.b
create timeseries root.sg.b with datatype=FLOAT,encoding=PLAIN;

# 查询时间序列 root.sg.a*b
select `a*b` from root.sg
# 其结果集表头为
|Time|root.sg.a*b|

# 查询时间序列 root.sg.a 和 root.sg.b的乘积
select a*b from root.sg
# 其结果集表头为
|Time|root.sg.a * root.sg.b|
```

### 标识符

路径结点名不为通配符时，使用方法和标识符一致。

使用反引号引起的路径结点名，若其中含有特殊字符 . `，在结果集中展示时会添加反引号，其它情况下会正常展示，具体请参考特殊情况示例中结果集的示例。

需要使用反引号进行引用的部分特殊情况示例：

- 创建时间序列时，如下情况需要使用反引号对特殊节点名进行引用：

```SQL
# 路径结点名中包含特殊字符，时间序列各结点为["root","sg","www.`baidu.com"]
create timeseries root.sg.`www.``baidu.com`.a with datatype=FLOAT,encoding=PLAIN;

# 路径结点名为系统关键字
create timeseries root.sg.`select`.a with datatype=FLOAT,encoding=PLAIN;

# 路径结点名为纯数字
create timeseries root.sg.`111` with datatype=FLOAT,encoding=PLAIN;
```

依次执行示例中语句后，执行 show timeseries，结果如下：

```SQL
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                 timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
|           root.sg.select.a| null|      root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|
|              root.sg.111.a| null|      root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|
|root.sg.`www.``baidu.com`.a| null|      root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
```

- 插入数据时，如下情况需要使用反引号对特殊节点名进行引用：

```SQL
# 路径结点名中包含特殊字符 . 和 `
insert into root.sg.`www.``baidu.com`(timestamp, a) values(1, 2);

# 路径结点名为系统关键字
insert into root.sg.`select`(timestamp, a) values (1, 2);

# 路径结点名为纯数字
insert into root.sg(timestamp, `111`) values (1, 2);
```

- 查询数据时，如下情况需要使用反引号对特殊节点名进行引用：

```SQL
# 路径结点名中包含特殊字符 . 和 `
select a from root.sg.`www.``baidu.com`;

# 路径结点名为系统关键字
select a from root.sg.`select`

# 路径结点名为纯数字
select `111` from root.sg
```

结果集分别为：

```SQL
# select a from root.sg.`www.``baidu.com` 结果集
+-----------------------------+---------------------------+
|                         Time|root.sg.`www.``baidu.com`.a|
+-----------------------------+---------------------------+
|1970-01-01T08:00:00.001+08:00|                        2.0|
+-----------------------------+---------------------------+

# select a from root.sg.`select` 结果集
+-----------------------------+----------------+
|                         Time|root.sg.select.a|
+-----------------------------+----------------+
|1970-01-01T08:00:00.001+08:00|             2.0|
+-----------------------------+----------------+

# select `111` from root.sg 结果集
+-----------------------------+-----------+
|                         Time|root.sg.111|
+-----------------------------+-----------+
|1970-01-01T08:00:00.001+08:00|        2.0|
+-----------------------------+-----------+
```

## 关键字和保留字

关键字是在 SQL 具有特定含义的词，不能直接用于标识符，需要使用反引号进行转义。保留字是关键字的一个子集，保留字不能用于标识符（即使进行了转义）。

关于 IoTDB 的关键字和保留字列表，可以查看 [关键字和保留字](https://iotdb.apache.org/zh/UserGuide/Master/Reference/Keywords.html) 。

## 了解更多

请阅读代码仓库中的词法和语法描述文件：

词法文件：`antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlLexer.g4`

语法文件：`antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlParser.g4`
