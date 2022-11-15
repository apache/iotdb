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


## Literal Values

This section describes how to write literal values in IoTDB. These include strings, numbers, timestamp values, boolean values, and NULL.

### String Literals

> We refer to MySQL's definition of string：A string is a sequence of bytes or characters, enclosed within either single quote (`'`) or double quote (`"`) characters.

Definition of string in MySQL could be found here：[MySQL :: MySQL 8.0 Reference Manual :: 9.1.1 String Literals](https://dev.mysql.com/doc/refman/8.0/en/string-literals.html)

So in IoTDB, **A string is a sequence of bytes or characters, enclosed within either single quote (`'`) or double quote (`"`) characters.** Examples：

```js
'a string'
"another string"
```

#### Usage Scenarios

Usages of string literals:

- Values of  `TEXT` type data in `INSERT` or `SELECT` statements 

  ```SQL
  # insert
  insert into root.ln.wf02.wt02(timestamp,hardware) values(1, 'v1')
  insert into root.ln.wf02.wt02(timestamp,hardware) values(2, '\\')
  
  +-----------------------------+--------------------------+
  |                         Time|root.ln.wf02.wt02.hardware|
  +-----------------------------+--------------------------+
  |1970-01-01T08:00:00.001+08:00|                        v1|
  +-----------------------------+--------------------------+
  |1970-01-01T08:00:00.002+08:00|                        \\|
  +-----------------------------+--------------------------+
  
  # select
  select code from root.sg1.d1 where code in ('string1', 'string2');
  ```
  
- Used in`LOAD` / `REMOVE` / `SETTLE` instructions to represent file path.

  ```SQL
  # load
  LOAD 'examplePath'
  
  # remove
  REMOVE 'examplePath'
  
  # SETTLE
  SETTLE 'examplePath'
  ```

- Password fields in user management statements

  ```SQL
  # write_pwd is the password
  CREATE USER ln_write_user 'write_pwd'
  ```

- Full Java class names in UDF and trigger management statements 

  ```SQL
  # Trigger example. Full java class names after 'AS' should be string literals.
  CREATE TRIGGER `alert-listener-sg1d1s1`
  AFTER INSERT
  ON root.sg1.d1.s1
  AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
  WITH (
    'lo' = '0', 
    'hi' = '100.0'
  )
  
  # UDF example. Full java class names after 'AS' should be string literals.
  CREATE FUNCTION example AS 'org.apache.iotdb.udf.UDTFExample'
  ```

- `AS` function provided by IoTDB can assign an alias to time series selected in query. Alias can be constant(including string) or identifier.

  ```SQL
  select s1 as 'temperature', s2 as 'speed' from root.ln.wf01.wt01;
  
  # Header of dataset
  +-----------------------------+-----------|-----+
  |                         Time|temperature|speed|
  +-----------------------------+-----------|-----+
  ```

- The key/value of an attribute can be String Literal and identifier, more details can be found at **key-value pair** part. 


#### How to use quotation marks in String Literals

There are several ways to include quote characters within a string:

 - `'` inside a string quoted with `"` needs no special treatment and need not be doubled or escaped. In the same way, `"` inside a string quoted with `'` needs no special treatment.
 - A `'` inside a string quoted with `'` may be written as `''`.
- A `"` inside a string quoted with `"` may be written as `""`.

The following examples demonstrate how quoting and escaping work:
```js
'string'  // string
'"string"'  // "string"
'""string""'  // ""string""
'''string'  // 'string

"string" // string
"'string'"  // 'string'
"''string''"  // ''string''
"""string"  // "string
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

### Usage scenarios

Certain objects within IoTDB, including `TRIGGER`, `FUNCTION`(UDF), `CONTINUOUS QUERY`, `SCHEMA TEMPLATE`, `USER`, `ROLE`,`Pipe`,`PipeSink`,`alias` and other object names are known as identifiers.

### Constraints

Below are basic constraints of identifiers, specific identifiers may have other constraints, for example, `user` should consists of more than 4 characters. 

- Permitted characters in unquoted identifiers:
  - [0-9 a-z A-Z _ ] (letters, digits and underscore)
  - ['\u2E80'..'\u9FFF'] (UNICODE Chinese characters)
- Identifiers may begin with a digit, unquoted identifiers can not be a real number.
- Identifiers are case sensitive.
- Key words can be used as an identifier.

**You need to quote the identifier with back quote(`) in the following cases:**

- Identifier contains special characters.
- Identifier that is a real number.

### How to use quotations marks in quoted identifiers

`'` and `"` can be used directly in quoted identifiers.

` may be written as `` in quoted  identifiers. See the example below:

```SQL
# create template t1't"t
create schema template `t1't"t` 
(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)

# create template t1`t
create schema template `t1``t` 
(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

### Examples

Examples of case in which quoted identifier is used ：

- Trigger name should be quoted in cases described above ：

  ```sql
  # create trigger named alert.`listener-sg1d1s1
  CREATE TRIGGER `alert.``listener-sg1d1s1`
  AFTER INSERT
  ON root.sg1.d1.s1
  AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
  WITH (
    'lo' = '0', 
    'hi' = '100.0'
  )
  ```

- UDF name should be quoted in cases described above ：

  ```sql
  # create a funciton named 111, 111 is a real number.
  CREATE FUNCTION `111` AS 'org.apache.iotdb.udf.UDTFExample'
  ```

- Template name should be quoted in cases described above ：

  ```sql
  # create a template named 111, 111 is a real number.
  create schema template `111` 
  (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
  ```

- User and Role name should be quoted in cases described above, blank space is not allow in User and Role name whether quoted or not ：

  ```sql
  # create user special`user.
  CREATE USER `special``user.` 'write_pwd'
  
  # create role 111
  CREATE ROLE `111`
  ```

- Continuous query name should be quoted in cases described above ：

  ```sql
  # create continuous query test.cq
  CREATE CONTINUOUS QUERY `test.cq` 
  BEGIN 
    SELECT max_value(temperature) 
    INTO temperature_max 
    FROM root.ln.*.* 
    GROUP BY time(10s) 
  END
  ```

- Pipe、PipeSink should be quoted in cases described above ：

  ```sql
  # create PipeSink test.*1
  CREATE PIPESINK `test.*1` AS IoTDB ('ip' = '输入你的IP')
  
  # create Pipe test.*2
  CREATE PIPE `test.*2` TO `test.*1` FROM 
  (select ** from root WHERE time>=yyyy-mm-dd HH:MM:SS) WITH 'SyncDelOp' = 'true'
  ```

- `AS` function provided by IoTDB can assign an alias to time series selected in query. Alias can be constant(including string) or identifier.

  ```sql
  select s1 as temperature, s2 as speed from root.ln.wf01.wt01;
  
  # Header of result dataset
  +-----------------------------+-----------|-----+
  |                         Time|temperature|speed|
  +-----------------------------+-----------|-----+
  ```

- The key/value of an attribute can be String Literal and identifier, more details can be found at **key-value pair** part. 


## Node Names in Path

Node name is a special identifier, it can also be wildcard `*` and `**`. When creating timeseries, node name can not be wildcard. In query statment, you can use wildcard to match one or more nodes of path.

### Wildcard

`*` represents one node. For example, `root.vehicle.*.sensor1` represents a 4-node path which is prefixed with `root.vehicle` and suffixed with `sensor1`.

`**` represents (`*`)+, which is one or more nodes of `*`. For example, `root.vehicle.device1.**` represents all paths prefixed by `root.vehicle.device1` with nodes num greater than or equal to 4, like `root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`, etc; `root.vehicle.**.sensor1` represents a path which is prefixed with `root.vehicle` and suffixed with `sensor1` and has at least 4 nodes.

As `*` can also be used in expressions of select clause to represent multiplication, below are examples to help you better understand the usage of `* `:

```SQL
# create timeseries root.sg.`a*b`
create timeseries root.sg.`a*b` with datatype=FLOAT,encoding=PLAIN;

# As described in Identifier part, a*b should be quoted.
# "create timeseries root.sg.a*b with datatype=FLOAT,encoding=PLAIN" is wrong. 

# create timeseries root.sg.a
create timeseries root.sg.a with datatype=FLOAT,encoding=PLAIN;

# create timeseries root.sg.b
create timeseries root.sg.b with datatype=FLOAT,encoding=PLAIN;

# query data of root.sg.`a*b`
select `a*b` from root.sg
# Header of result dataset
|Time|root.sg.a*b|

# multiplication of root.sg.a and root.sg.b
select a*b from root.sg
# Header of result dataset
|Time|root.sg.a * root.sg.b|
```

### Identifier

When node name is not wildcard, it is a identifier, which means the constraints on it is the same as described in Identifier part.

- Create timeseries statement:

```SQL
# Node name contains special characters like ` and .,all nodes of this timeseries are: ["root","sg","www.`baidu.com"]
create timeseries root.sg.`www.``baidu.com`.a with datatype=FLOAT,encoding=PLAIN;

# Node name is a real number.
create timeseries root.sg.`111` with datatype=FLOAT,encoding=PLAIN;
```

After executing above statments, execute "show timeseries"，below is the result：

```SQL
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                 timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
|            root.sg.`111`.a| null|      root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|
|root.sg.`www.``baidu.com`.a| null|      root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
```

- Insert statment:

```SQL
# Node name contains special characters like . and `
insert into root.sg.`www.``baidu.com`(timestamp, a) values(1, 2);

# Node name is a real number.
insert into root.sg(timestamp, `111`) values (1, 2);
```

- Query statement:

```SQL
# Node name contains special characters like . and `
select a from root.sg.`www.``baidu.com`;

# Node name is a real number.
select `111` from root.sg
```

Results:

```SQL
# select a from root.sg.`www.``baidu.com`
+-----------------------------+---------------------------+
|                         Time|root.sg.`www.``baidu.com`.a|
+-----------------------------+---------------------------+
|1970-01-01T08:00:00.001+08:00|                        2.0|
+-----------------------------+---------------------------+

# select `111` from root.sg
+-----------------------------+-----------+
|                         Time|root.sg.111|
+-----------------------------+-----------+
|1970-01-01T08:00:00.001+08:00|        2.0|
+-----------------------------+-----------+
```

## Key-Value Pair

**The key/value of an attribute can be constant(including string) and identifier. **

Below are usage scenarios of key-value pair:

- Attributes fields of trigger. See the attributes after `With` clause in the example below:

```SQL
# 以字符串形式表示键值对
CREATE TRIGGER `alert-listener-sg1d1s1`
AFTER INSERT
ON root.sg1.d1.s1
AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
WITH (
  'lo' = '0', 
  'hi' = '100.0'
)

# 以标识符和常量形式表示键值对
CREATE TRIGGER `alert-listener-sg1d1s1`
AFTER INSERT
ON root.sg1.d1.s1
AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
WITH (
  lo = 0, 
  hi = 100.0
)
```

- Key-value pair to represent tag/attributes in timeseries:

```sql
# create timeseries using string as key/value
CREATE timeseries root.turbine.d1.s1(temprature) 
WITH datatype = FLOAT, encoding = RLE, compression = SNAPPY, 'max_point_number' = '5'
TAGS('tag1' = 'v1', 'tag2'= 'v2') ATTRIBUTES('attr1' = 'v1', 'attr2' = 'v2')

# create timeseries using constant as key/value
CREATE timeseries root.turbine.d1.s1(temprature) 
WITH datatype = FLOAT, encoding = RLE, compression = SNAPPY, max_point_number = 5
TAGS(tag1 = v1, tag2 = v2) ATTRIBUTES(attr1 = v1, attr2 = v2)
```

```sql
# alter tags and attributes of timeseries
ALTER timeseries root.turbine.d1.s1 SET 'newTag1' = 'newV1', 'attr1' = 'newV1'

ALTER timeseries root.turbine.d1.s1 SET newTag1 = newV1, attr1 = newV1
```

```sql
# rename tag
ALTER timeseries root.turbine.d1.s1 RENAME 'tag1' TO 'newTag1'

ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO newTag1
```

```sql
# upsert alias, tags, attributes
ALTER timeseries root.turbine.d1.s1 UPSERT 
ALIAS='newAlias' TAGS('tag2' = 'newV2', 'tag3' = 'v3') ATTRIBUTES('attr3' ='v3', 'attr4'='v4')

ALTER timeseries root.turbine.d1.s1 UPSERT 
ALIAS = newAlias TAGS(tag2 = newV2, tag3 = v3) ATTRIBUTES(attr3 = v3, attr4 = v4)
```

```sql
# add new tags
ALTER timeseries root.turbine.d1.s1 ADD TAGS 'tag3' = 'v3', 'tag4' = 'v4'

ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3 = v3, tag4 = v4
```

```sql
# add new attributes
ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES 'attr3' = 'v3', 'attr4' = 'v4'

ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES attr3 = v3, attr4 = v4
```

```sql
# query for timeseries
SHOW timeseries root.ln.** WHRER 'unit' = 'c'

SHOW timeseries root.ln.** WHRER unit = c
```

- Attributes fields of Pipe and PipeSink.

```SQL
# PipeSink example 
CREATE PIPESINK my_iotdb AS IoTDB ('ip' = '输入你的IP')

# Pipe example 
CREATE PIPE my_pipe TO my_iotdb FROM 
(select ** from root WHERE time>=yyyy-mm-dd HH:MM:SS) WITH 'SyncDelOp' = 'true'
```

## Keywords and Reserved Words

Keywords are words that have significance in SQL. Keywords can be used as an identifier. Certain keywords, such as TIME/TIMESTAMP and ROOT, are reserved and cannot use as identifiers.

[Keywords and Reserved Words](Keywords.md) shows the keywords and reserved words in IoTDB.

## Session、TsFile API

When using the Session and TsFile APIs, if the method you call requires parameters such as measurement, device, database, path in the form of String, **please ensure that the parameters passed in the input string is the same as when using the SQL statement**, here are some examples to help you understand. Code example could be found at: `example/session/src/main/java/org/apache/iotdb/SyntaxConventionRelatedExample.java`

1. Take creating a time series createTimeseries as an example:

```Java
public void createTimeseries(
    String path,
    TSDataType dataType,
    TSEncoding encoding,
    CompressionType compressor)
    throws IoTDBConnectionException, StatementExecutionException;
```

If you wish to create the time series root.sg.a, root.sg.\`a.\`\`"b\`, root.sg.\`111\`, the SQL statement you use should look like this:

```SQL
create timeseries root.sg.a with datatype=FLOAT,encoding=PLAIN,compressor=SNAPPY;

# node names contain special characters, each node in the time series is ["root","sg","a.`\"b"]
create timeseries root.sg.`a.``"b` with datatype=FLOAT,encoding=PLAIN,compressor=SNAPPY;

# node names are pure numbers
create timeseries root.sg.`111` with datatype=FLOAT,encoding=PLAIN,compressor=SNAPPY;
```

When you call the createTimeseries method, you should assign the path string as follows to ensure that the content of the path string is the same as when using SQL:

```Java
// timeseries root.sg.a
String path = "root.sg.a";

// timeseries root.sg.`a``"b`
String path = "root.sg.`a``\"b`";

// timeseries root.sg.`111`
String path = "root.sg.`111`";
```

2. Take inserting data insertRecord as an example:

```Java
public void insertRecord(
    String deviceId,
    long time,
    List<String> measurements,
    List<TSDataType> types,
    Object... values)
    throws IoTDBConnectionException, StatementExecutionException;
```

If you want to insert data into the time series root.sg.a, root.sg.\`a.\`\`"b\`, root.sg.\`111\`, the SQL statement you use should be as follows:

```SQL
insert into root.sg(timestamp, a, `a.``"b`, `111`) values (1, 2, 2, 2);
```

When you call the insertRecord method, you should assign deviceId and measurements as follows:

```Java
// deviceId is root.sg
String deviceId = "root.sg";

// measurements
String[] measurements = new String[]{"a", "`a.``\"b`", "`111`"};
List<String> measurementList = Arrays.asList(measurements);
```

3. Take executeRawDataQuery as an example:

```Java
public SessionDataSet executeRawDataQuery(
    List<String> paths, 
    long startTime, 
    long endTime)
    throws StatementExecutionException, IoTDBConnectionException;
```

If you wish to query the data of the time series root.sg.a, root.sg.\`a.\`\`"b\`, root.sg.\`111\`, the SQL statement you use should be as follows :

```SQL
select a from root.sg

# node name contains special characters
select `a.``"b` from root.sg;

# node names are pure numbers
select `111` from root.sg
```

When you call the executeRawDataQuery method, you should assign paths as follows:

```Java
// paths
String[] paths = new String[]{"root.sg.a", "root.sg.`a.``\"b`", "root.sg.`111`"};
List<String> pathList = Arrays.asList(paths);
```

## Detailed Definitions of Lexical and Grammar

Please read the lexical and grammar description files in our code repository:

Lexical file: `antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlLexer.g4`

Grammer file: `antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IoTDBSqlParser.g4`
