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

# Syntax Conventions

## Double-quotes (\") & Backquotes (\`)

Strings quoted in double-quotes and backquotes are interpreted as identifiers (ID), and the quoted strings generally contain special characters. It should be noted that the quoted strings cannot contain `.` characters.

The definition of identifier (ID) is:

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

Usages of identifiers:

* The names of `TRIGGER`, `FUNCTION`(UDF), `CONTINUOUS QUERY`, `USER`, `ROLE`, etc.
* In time series paths: In addition to the beginning level of the time series (`root`) and the storage group level, other levels also support strings quoted by  \` or `" ` as their names.

Examples:

```sql
CREATE FUNCTION "udfname:""actual-name""" AS 'org.apache.iotdb.db.query.udf.example.Counter'
# "udfname:""actual-name""" will be parsed as udfname:"actual-name"

CREATE FUNCTION `udfname:actual-name` AS 'org.apache.iotdb.db.query.udf.example.Counter'
# `udfname:actual-name` will be parsed as udfname:actual-name

CREATE TIMESERIES root.a.b.`s1+s2/s3`.c WITH DATATYPE=INT32,ENCODING=RLE
# root.a.b.`s1+s2/s3`.c will be parsed as root.a.b.s1+s2/s3.c
```



## Single-quotes (\')

The literal value of a string can only be represented by a string quoted by `'` characters.

The definition of string literal (`STRING_LITERAL`) is:

```sql
STRING_LITERAL
    : '\'' ((~'\'') | '\'\'')* '\''
    ;
```

Usages of string literals:

* Values of  `TEXT` type data in `INSERT` or `SELECT` statements
* Full Java class names in UDF and trigger management statements
* Attribute fields (including attribute keys and attribute values) in UDF / trigger execution or management statements
* File paths in `LOAD` / `REMOVE` / `SETTLE` statements
* Password fields in user management statements

Examples:

```sql
SELECT `my-udf`(s1, s2, 'key'='value') FROM root.sg.d;

CREATE TRIGGER trigger_name BEFORE INSERT ON root.a.b.`s1+s2/s3`.c AS 'org.apache.iotdb.db.engine.trigger.example.Counter'

CREATE USER `my-%+-*/user&name` 'my-pass''word'''
# The password is my-pass'word'
```



## Learn More

Please read the lexical and grammar description files in our code repository:

Lexical file: `antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlLexer.g4`

Grammer file: `antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlParser.g4`