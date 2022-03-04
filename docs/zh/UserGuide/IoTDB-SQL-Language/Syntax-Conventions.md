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

## 双引号（\"）、反引号（\`）

双引号和反引号内引用的字符串被解释为标识符（ID），被引用的字符串一般包含特殊字符。需要注意的是，被引用的字符串不可带有 `.` 字符。

标识符（ID）的定义为：

```sql
ID
    : FIRST_NAME_CHAR NAME_CHAR*
    | '"' (~('"' | '.') | '""')+ '"'
    | '`' (~('`' | '.') | '``')+ '`'
    ;

fragment NAME_CHAR
    : 'A'..'Z'
    | 'a'..'z'
    | '0'..'9'
    | '_'
    | ':'
    | '@'
    | '#'
    | '$'
    | '{'
    | '}'
    | CN_CHAR
    ;

fragment FIRST_NAME_CHAR
    : 'A'..'Z'
    | 'a'..'z'
    | '_'
    | ':'
    | '@'
    | '#'
    | '$'
    | '{'
    | '}'
    | CN_CHAR
    ;

fragment CN_CHAR
    : '\u2E80'..'\u9FFF'
    ;
```

标志符的使用场景：
* `TRIGGER`，`FUNCTION`(UDF)，`CONTINUOUS QUERY`，`USER`，`ROLE` 等的名字。
* 时间序列路径的表达：除了时间序列的开头的层级（`root`）和存储组层级外，层级还支持使用被  \`  或者 ` " ` 符号引用的特殊字符串作为其名称。

例子：

```sql
CREATE FUNCTION "udfname:""actual-name""" AS 'org.apache.iotdb.db.query.udf.example.Counter'
# "udfname:""actual-name""" 会被解析成 udfname:"actual-name"

CREATE FUNCTION `udfname:actual-name` AS 'org.apache.iotdb.db.query.udf.example.Counter'
# `udfname:actual-name` 会被解析成 udfname:actual-name

CREATE TIMESERIES root.a.b.`s1+s2/s3`.c WITH DATATYPE=INT32,ENCODING=RLE
# root.a.b.`s1+s2/s3`.c 会被解析成 root.a.b.s1+s2/s3.c
```



## 单引号（\'）

字符串字面值只能由单引号（`'`）字符包围的字符串表示。

字符串字面值（`STRING_LITERAL`）的定义为：

```sql
STRING_LITERAL
    : '\'' ((~'\'') | '\'\'')* '\''
    ;
```

字符串字面值的使用场景：

* `INSERT` 或者 `SELECT` 中用于表达 `TEXT` 类型数据的场景
* SQL 中 UDF 和 Trigger 的 Java 类全类名
* `CREATE TRIGGER` 语句中描述触发器属性的键值对
* UDF 函数输入参数中的属性
* `LOAD` / `REMOVE` / `SETTLE` 指令中的文件路径
* 用户密码

例子：

```sql
SELECT `my-udf`(s1, s2, 'key'='value') FROM root.sg.d;

CREATE TRIGGER trigger_name BEFORE INSERT ON root.a.b.`s1+s2/s3`.c AS 'org.apache.iotdb.db.engine.trigger.example.Counter'

CREATE USER `my-%+-*/user&name` 'my-pass''word'''
# 密码是 my-pass'word'
```



## 了解更多

请阅读代码仓库中的词法和语法描述文件：

词法文件：`antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlLexer.g4`

语法文件：`antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlParser.g4`

