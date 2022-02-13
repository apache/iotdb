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

## Literal Values

This section describes how to write literal values in IoTDB. These include strings, numbers, timestamp values, boolean values, and NULL.

### String Literals

A string is a sequence of characters, enclosed within either single quote (`'`) or double quote (`"`) characters. Examples:
```js
'a string'
"another string"
```

Usages of string literals:

- Values of  `TEXT` type data in `INSERT` or `SELECT` statements 
- Full Java class names in UDF and trigger management statements 
- Attribute fields (including attribute keys and attribute values) in UDF / trigger execution or management statements 
- File paths in `LOAD` / `REMOVE` / `SETTLE` statements 
- Password fields in user management statements

There are several ways to include quote characters within a string:

 - A `'` inside a string quoted with `'` may be written as `''`.
 - A `"` inside a string quoted with `"` may be written as `""`.
 - Precede the quote character by an escape character (\\).
 - `'` inside a string quoted with `"` needs no special treatment and need not be doubled or escaped. In the same way, `"` inside a string quoted with `'` needs no special treatment. (but escaping still works)

The following examples demonstrate how quoting and escaping work:
```js
'string'  // string
'"string"'  // "string"
'""string""'  // ""string""
'str''ing'  // str'ing
'\'string'  // 'string
"string" // string
"'string'"  // 'string'
"''string''"  // ''string''
"str""ing"  // str"ing
"\"string"  // "string
```

### Numeric Literals

Number literals include integer (exact-value) literals and floating-point (approximate-value) literals.

Integers are represented as a sequence of digits. Numbers may be preceded by `-` or `+` to indicate a negative or positive value, respectively. Examples: `1`, `-1`.

Numbers with fractional part or represented in scientific notation with a mantissa and exponent are approximate-value numbers. Examples: `.1`, `3.14`, `-2.23`, `+1.70`, `1.2E3`, `1.2E-3`, `-1.2E3`, `-1.2E-3`.

The `INT32` and `INT64` data types are integer types and calculations are exact.

The `FLOAT` and `DOUBLE` data types are floating-point types and calculations are approximate.

An integer may be used in floating-point context; it is interpreted as the equivalent floating-point number.

### Timestamp Literals

The timestamp is the time point at which data is produced. It includes absolute timestamps and relative timestamps in IoTDB. For information about timestamp support in IoTDB, see [Data Type Doc](../Data-Concept/Data-Type.md).

Specially, `NOW()` represents a constant timestamp that indicates the system time at which the statement began to execute.

### Boolean Literals

The constants `TRUE` and `FALSE` evaluate to 1 and 0, respectively. The constant names can be written in any lettercase.

### NULL Values

The `NULL` value means “no data.” `NULL` can be written in any lettercase.


## Identifiers

Certain objects within IoTDB, including `TRIGGER`, `FUNCTION`(UDF), `CONTINUOUS QUERY`, `SCHEMA TEMPLATE`, `USER`, `ROLE` and other object names are known as identifiers.

What you need to know about identifiers:

- Permitted characters in unquoted identifiers:
  - [0-9 a-z A-Z _ : @ # $ { }] (letters, digits, some special characters)
  - ['\u2E80'..'\u9FFF'] (UNICODE Chinese characters)
- Identifiers may begin with a digit and consist solely of digits, **which is not recommended!**
- Identifiers are case sensitive.
- Note: User and role names are not case-sensitive, and special characters are not allowed to be escaped.

If an identifier contains special characters or is a keyword, you must quote it whenever you refer to it.
The identifier quote character is the backtick (`):
```sql
id  // parsed as id
ID  // parsed as ID
id0  // parsed as id0
_id  // parsed as _id
0id  // parsed as 0id
233  // parsed as 233 (not recommended!)
ab!  // invalid
`ab!`  // parsed as ab!
`"ab"`  // parsed as "ab"
```

Identifier quote characters can be included within an identifier if you quote the identifier. If the character to be included within the identifier is the same as that used to quote the identifier itself, then you need to double the character.
```sql
`a``b`  // parsed as a`b
`a\`b`  // parsed as a`b
```

## Node Names in Path

We call the part of a path divided by `.` as a `node`. 

The constraints of node names are almost the same as the identifiers, but you should note the following points:

- `root` is a reserved word, and it is only allowed to appear at the beginning layer of the time series. If `root` appears in other layers, it cannot be parsed and an error will be reported.
- Character `.` is not permitted in unquoted or quoted node names. If you must do it (even if it is not recommended), you can enclose it within either single quote (`'`) or double quote (`"`). In this case, quotes are recognized as part of the node name to avoid ambiguity.
- In particular, if the system is deployed on a Windows machine, the storage group layer name will be **case-insensitive**. For example, creating both `root.ln` and `root.LN` at the same time is not allowed.

Examples:

```sql
CREATE TIMESERIES root.a.b.s1+s2/s3.c WITH DATATYPE=INT32, ENCODING=RLE
// invalid!

CREATE TIMESERIES root.a.b.`s1+s2/s3`.c WITH DATATYPE=INT32, ENCODING=RLE
// root.a.b.`s1+s2/s3`.c will be parsed as Path[root, a, b, s1+s2/s3, c]
```

```sql
CREATE TIMESERIES root.a.b.select WITH DATATYPE=INT32, ENCODING=RLE
// invalid!

CREATE TIMESERIES root.a.b.`select` WITH DATATYPE=INT32, ENCODING=RLE
// root.a.b.`select` will be parsed as Path[root, a, b, `select`]
```

```sql
CREATE TIMESERIES root.a.b.`s1.s2`.c WITH DATATYPE=INT32, ENCODING=RLE
// invalid!

CREATE TIMESERIES root.a.b."s1.s2".c WITH DATATYPE=INT32, ENCODING=RLE
// root.a.b."s1.s2".c will be parsed as Path[root, a, b, "s1.s2", c]
```

Note: When escaping the path separator (`.`) with quotes, to avoid ambiguity, you must follow the following rules:

- Within a path node name enclosed in backticks, it cannot start or end with single and double quotes.
- Within a path node name enclosed in single quotation marks, single quotes cannot be used before and after the path separator. Similarly, in the path node name enclosed in double quotation marks, double quotes cannot be used before and after the path separator.

```sql
CREATE TIMESERIES root.sg."s1\".\"s2" WITH DATATYPE=INT32, ENCODING=RLE
// invalid! ambiguity with root.sg."s1"."s2"

CREATE TIMESERIES root.sg.`"a`.`"` WITH DATATYPE=INT32, ENCODING=RLE
// invalid! ambiguity with root.sg."a."

CREATE TIMESERIES root.sg.`"ab`.`cd"` WITH DATATYPE=INT32, ENCODING=RLE
// invalid! ambiguity with root.sg."ab.cd"
```

# Keywords and Reserved Words

Keywords are words that have significance in SQL require special treatment for use as identifiers and node names, and need to be escaped with backticks.
Certain keywords, such as TIME and ROOT, are reserved and cannot use as identifiers and node names (even after escaping).

[Keywords and Reserved Words](Keywords.md) shows the keywords and reserved words in IoTDB 0.13.

## Expressions

IoTDB supports the execution of arbitrary nested expressions consisting of numbers, time series, arithmetic expressions, and time series generating functions (including user-defined functions) in the `select` clause.

Note: Node names that consist solely of digits, `'` and `"` in an expression must be enclosed in backticks (`).
```sql
-- There exists timeseries: root.sg.d.0, root.sg.d.'a' and root.sg."d".b
select 0 from root.sg.d  -- ambiguity exists, parsing failed
select 'a' from root.sg.d -- ambiguity exists, parsing failed
select "d".b from root.sg -- ambiguity exists, parsing failed
select `0` from root.sg.d  -- query from root.sg.d.0
select `0` + 0 from root.sg.d -- valid expression, add number 0 to each point of root.sg.d.0
select myudf(`'a'`, 'x') from root.sg.d -- valid expression, call function myudf with timeseries root.sg.d.'a' as the 1st parameter, and a string constant 'x' as the 2nd parameter
```

## Learn More

Please read the lexical and grammar description files in our code repository:

Lexical file: `antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlLexer.g4`

Grammer file: `antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlParser.g4`
