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

# Chapter 4: Operation Manual

In this part, we will introduce you IoTDB's Query Language. IoTDB offers you a SQL-like query language for interacting with IoTDB, the query language can be devided into 4 major parts:

* Schema Statement: statements about schema management are all listed in this section.
* Data Management Statement: statements about data management (such as: data insertion, data query, etc.) are all listed in this section.
* Database Management Statement: statements about database management and authentication are all listed in this section.
* Functions: functions that IoTDB offers are all listed in this section.

All of these statements are write in IoTDB's own syntax, for details about the syntax composition, please check the `Reference` section.

## IoTDB Query Language


### Schema Statement

#### Set Storage Group




SET STORAGE GROUP TO &lt;PrefixPath>

Eg: 
``` SQL
IoTDB > SET STORAGE GROUP TO root.ln.wf01.wt01
```
>Note: PrefixPath can not include `*`

#### Create Timeseries Statement
 

CREATE TIMESERIES &lt;Timeseries> WITH &lt;AttributeClauses>

AttributeClauses : DATATYPE=&lt;DataTypeValue> COMMA ENCODING=&lt;EncodingValue> [COMMA &lt;ExtraAttributeClause>]*

DataTypeValue: BOOLEAN | DOUBLE | FLOAT | INT32 | INT64 | TEXT
EncodingValue: GORILLA | PLAIN | RLE | TS_2DIFF | REGULAR

ExtraAttributeClause: {
	COMPRESSOR = &lt;CompressorValue>
	MAX_POINT_NUMBER = Integer
}

CompressorValue: UNCOMPRESSED | SNAPPY

Eg: 
```
IoTDB > CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
IoTDB > CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
IoTDB > CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSOR=SNAPPY, MAX_POINT_NUMBER=3
```
>Note: Datatype and encoding type must be corresponding. Please check Chapter 3 Encoding Section for details.


#### Delete Timeseries Statement


DELETE TIMESERIES &lt;PrefixPath> [COMMA &lt;PrefixPath>]*

Eg: 
```
IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.status
IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature
IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.*
```

#### Show All Timeseries Statement

SHOW TIMESERIES

Eg:
```
IoTDB > SHOW TIMESERIES
```
>Note: This statement can only be used in IoTDB Client. If you need to show all timeseries in JDBC, please use `DataBaseMetadata` interface.

#### Show Specific Timeseries Statement


SHOW TIMESERIES &lt;Path>

Eg: 
```
IoTDB > SHOW TIMESERIES root
IoTDB > SHOW TIMESERIES root.ln
IoTDB > SHOW TIMESERIES root.ln.*.*.status
IoTDB > SHOW TIMESERIES root.ln.wf01.wt01.status
```
>Note: The path can be prefix path, star path or timeseries path

>Note: This statement can be used in IoTDB Client and JDBC.


#### Show Storage Group Statement

SHOW STORAGE GROUP

Eg:
``` 
IoTDB > SHOW STORAGE GROUP
```
>Note: This statement can be used in IoTDB Client and JDBC.

### Data Management Statement

#### Insert Record Statement


INSERT INTO &lt;PrefixPath> LPAREN TIMESTAMP COMMA &lt;Sensor> [COMMA &lt;Sensor>]* RPAREN VALUES LPAREN &lt;TimeValue>, &lt;PointValue> [COMMA &lt;PointValue>]* RPAREN

Sensor : Identifier

Eg: 
```
IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp,status) VALUES(NOW(), false)
IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp,temperature) VALUES(2017-11-01T00:17:00.000+08:00,24.22028)
IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp, status, temperature) VALUES (1509466680000, false, 20.060787);
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>

>Note: The order of Sensor and PointValue need one-to-one correspondence


#### Update Record Statement

UPDATE &lt;UpdateClause> SET &lt;SetClause> WHERE &lt;WhereClause>

UpdateClause: &lt;prefixPath>

SetClause: &lt;SetExpression> 

SetExpression: &lt;Path> EQUAL &lt;PointValue>

WhereClause : &lt;Condition> [(AND | OR) &lt;Condition>]*

Condition  : &lt;Expression> [(AND | OR) &lt;Expression>]*

Expression : [NOT | !]? TIME PrecedenceEqualOperator &lt;TimeValue>

Eg: 
```
IoTDB > UPDATE root.ln.wf01.wt01 SET temperature = 23 WHERE time < NOW() and time > 2017-11-1T00:15:00+08:00
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>


#### Delete Record Statement

DELETE FROM &lt;PrefixPath> [COMMA &lt;PrefixPath>]* WHERE TIME LESSTHAN &lt;TimeValue>

Eg:
```
DELETE FROM root.ln.wf01.wt01.temperature WHERE time < 2017-11-1T00:05:00+08:00
DELETE FROM root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature WHERE time < NOW()
DELETE FROM root.ln.wf01.wt01.* WHERE time < 1509466140000
```

#### Select Record Statement


SELECT &lt;SelectClause> FROM &lt;FromClause> [WHERE &lt;WhereClause>]?

SelectClause : &lt;SelectPath> (COMMA &lt;SelectPath>)*

SelectPath : &lt;FUNCTION> LPAREN &lt;Path> RPAREN | &lt;Path>

FUNCTION : ‘COUNT’ , ‘MIN_TIME’, ‘MAX_TIME’, ‘MIN_VALUE’, ‘MAX_VALUE’

FromClause : &lt;PrefixPath> (COMMA &lt;PrefixPath>)?

WhereClause : &lt;Condition> [(AND | OR) &lt;Condition>]*

Condition  : &lt;Expression> [(AND | OR) &lt;Expression>]*

Expression : [NOT | !]? &lt;TimeExpr> | [NOT | !]? &lt;SensorExpr>

TimeExpr : TIME PrecedenceEqualOperator &lt;TimeValue>

SensorExpr : (&lt;Timeseries> | &lt;Path>) PrecedenceEqualOperator &lt;PointValue>

Eg: 
```
IoTDB > SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00
IoTDB > SELECT * FROM root
IoTDB > SELECT COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
IoTDB > SELECT MIN_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
IoTDB > SELECT MAX_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature > 24
IoTDB > SELECT MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature > 23
IoTDB > SELECT MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
```
>Note: the statement needs to satisfy this constraint: &lt;Path>(SelectClause) + &lt;PrefixPath>(FromClause) = &lt;Timeseries>

>Note: If the &lt;SensorExpr>(WhereClause) is started with &lt;Path> and not with ROOT, the statement needs to satisfy this constraint: &lt;PrefixPath>(FromClause) + &lt;Path>(SensorExpr) = &lt;Timeseries>

>Note: In Version 0.7.0, if &lt;WhereClause> includes `OR`, time filter can not be used.


#### Group By Statement


SELECT &lt;SelectClause> FROM &lt;FromClause> WHERE  &lt;WhereClause> GROUP BY &lt;GroupByClause>

SelectClause : &lt;Function> [COMMA &lt; Function >]*

Function : &lt;AggregationFunction> LPAREN &lt;Path> RPAREN

FromClause : &lt;PrefixPath>

WhereClause : &lt;Condition> [(AND | OR) &lt;Condition>]*

Condition  : &lt;Expression> [(AND | OR) &lt;Expression>]*

Expression : [NOT | !]? &lt;TimeExpr> | [NOT | !]? &lt;SensorExpr>

TimeExpr : TIME PrecedenceEqualOperator &lt;TimeValue>

SensorExpr : (&lt;Timeseries> | &lt;Path>) PrecedenceEqualOperator &lt;PointValue>

GroupByClause : LPAREN &lt;TimeUnit> (COMMA TimeValue)? COMMA &lt;TimeInterval> (COMMA &lt;TimeInterval>)* RPAREN

TimeUnit : Integer &lt;DurationUnit>

DurationUnit : "ms" | "s" | "m" | "h" | "d" | "w"

TimeInterval: LBRACKET &lt;TimeValue> COMMA &lt;TimeValue> RBRACKET

Eg: 
```
SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 where temperature < 24 GROUP BY(5m, [1509465720000, 1509466380000])
SELECT COUNT (status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE time < 1509466500000 GROUP BY(5m, 1509465660000, [1509465720000, 1509466380000])
SELECT MIN_TIME(status), MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE temperature < 25 and time < 1509466800000 GROUP BY (3m, 1509465600000, [1509466140000, 1509466380000], [1509466440000, 1509466620000])
```

>Note: the statement needs to satisfy this constraint: &lt;Path>(SelectClause) + &lt;PrefixPath>(FromClause) = &lt;Timeseries>

>Note: If the &lt;SensorExpr>(WhereClause) is started with &lt;Path> and not with ROOT, the statement needs to satisfy this constraint: &lt;PrefixPath>(FromClause) + &lt;Path>(SensorExpr) = &lt;Timeseries>

>Note: &lt;TimeValue>(TimeInterval) needs to be greater than 0

>Note: First &lt;TimeValue>(TimeInterval) in needs to be smaller than second &lt;TimeValue>(TimeInterval)

#### Fill Statement


SELECT &lt;SelectClause> FROM &lt;FromClause> WHERE &lt;WhereClause> FILL &lt;FillClause>

SelectClause : &lt;Path> [COMMA &lt;Path>]*

FromClause : &lt; PrefixPath > [COMMA &lt; PrefixPath >]*

WhereClause : &lt;WhereExpression>

WhereExpression : TIME EQUAL &lt;TimeValue>

FillClause : LPAREN &lt;TypeClause> [COMMA &lt;TypeClause>]* RPAREN

TypeClause : &lt;Int32Clause> | &lt;Int64Clause> | &lt;FloatClause> | &lt;DoubleClause> | &lt;BoolClause> | &lt;TextClause>

Int32Clause: INT32 LBRACKET (&lt;LinearClause> | &lt;PreviousClause>)  RBRACKET

Int64Clause: INT64 LBRACKET (&lt;LinearClause> | &lt;PreviousClause>)  RBRACKET

FloatClause: FLOAT LBRACKET (&lt;LinearClause> | &lt;PreviousClause>)  RBRACKET

DoubleClause: DOUBLE LBRACKET (&lt;LinearClause> | &lt;PreviousClause>)  RBRACKET

BoolClause: BOOLEAN LBRACKET (&lt;LinearClause> | &lt;PreviousClause>)  RBRACKET

TextClause: TEXT LBRACKET (&lt;LinearClause> | &lt;PreviousClause>)  RBRACKET

PreviousClause : PREVIOUS [COMMA &lt;ValidPreviousTime>]?

LinearClause : LINEAR [COMMA &lt;ValidPreviousTime> COMMA &lt;ValidBehindTime>]?

ValidPreviousTime, ValidBehindTime: &lt;TimeUnit>

TimeUnit : Integer &lt;DurationUnit>

DurationUnit : "ms" | "s" | "m" | "h" | "d" | "w"

Eg: 
```
SELECT temperature FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL(float[previous, 1m])
SELECT temperature,status FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL (float[linear, 1m, 1m], boolean[previous, 1m])
SELECT temperature,status,hardware FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL (float[linear, 1m, 1m], boolean[previous, 1m], text[previous])
SELECT temperature,status,hardware FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL (float[linear], boolean[previous, 1m], text[previous])
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath>(FromClause) + &lt;Path>(SelectClause) = &lt;Timeseries>

>Note: Integer in &lt;TimeUnit> needs to be greater than 0

#### Limit Statement


SELECT &lt;SelectClause> FROM &lt;FromClause> [WHERE &lt;WhereClause>] [LIMIT &lt;LIMITClause>] [SLIMIT &lt;SLIMITClause>]

SelectClause : [&lt;Path> | Function]+

Function : &lt;AggregationFunction> LPAREN &lt;Path> RPAREN

FromClause : &lt;Path>

WhereClause : &lt;Condition> [(AND | OR) &lt;Condition>]*

Condition : &lt;Expression> [(AND | OR) &lt;Expression>]*

Expression: [NOT|!]?&lt;TimeExpr> | [NOT|!]?&lt;SensorExpr>

TimeExpr : TIME PrecedenceEqualOperator &lt;TimeValue>

SensorExpr : (&lt;Timeseries>|&lt;Path>) PrecedenceEqualOperator &lt;PointValue>

LIMITClause : &lt;N> [OFFSETClause]?

N : NonNegativeInteger

OFFSETClause : OFFSET &lt;OFFSETValue>

OFFSETValue : NonNegativeInteger

SLIMITClause : &lt;SN> [SOFFSETClause]?

SN : NonNegativeInteger

SOFFSETClause : SOFFSET &lt;SOFFSETValue>

SOFFSETValue : NonNegativeInteger

NonNegativeInteger:= ('+')? Digit+

Eg: 
```
IoTDB > SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00 LIMIT 3 OFFSET 2
IoTDB > SELECT COUNT (status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE time < 1509466500000 GROUP BY(5m, 1509465660000, [1509465720000, 1509466380000]) LIMIT 3
```
>Note: The order of &lt;LIMITClause> and &lt;SLIMITClause> does not affect the grammatical correctness.

>Note: &lt;SLIMITClause> can only effect in Prefixpath and StarPath.

>Note: &lt;FillClause> can not use &lt;LIMITClause> but not &lt;SLIMITClause>.


### Database Management Statement

#### Create User


CREATE USER &lt;userName> &lt;password>;  

userName:=identifier  

password:=identifier

Eg: 
```
IoTDB > CREATE USER thulab pwd;
```

#### Delete User


DROP USER &lt;userName>;  

userName:=identifier

Eg:
```
IoTDB > DROP USER xiaoming;
```

#### Create Role


CREATE ROLE &lt;roleName>;  

roleName:=identifie

Eg: 
```
IoTDB > CREATE ROLE admin;
```

#### Delete Role

DROP ROLE &lt;roleName>;  

roleName:=identifier

Eg: 
```
IoTDB > DROP ROLE admin;
```

#### Grant User Privileges


GRANT USER &lt;userName> PRIVILEGES &lt;privileges> ON &lt;nodeName>;  

userName:=identifier  

nodeName:=identifier (DOT identifier)*  

privileges:= string (COMMA string)*

Eg: 
```
IoTDB > GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.ln;
```

#### Grant Role Privileges

GRANT ROLE &lt;roleName> PRIVILEGES &lt;privileges> ON &lt;nodeName>;  

privileges:= string (COMMA string)*  

roleName:=identifier  

nodeName:=identifier (DOT identifier)*

Eg: 
```
IoTDB > GRANT ROLE temprole PRIVILEGES 'DELETE_TIMESERIES' ON root.ln;
```

#### Grant User Role

GRANT &lt;roleName> TO &lt;userName>;  

roleName:=identifier  

userName:=identifier

Eg: 
```
IoTDB > GRANT temprole TO tempuser;
```

#### Revoke User Privileges

REVOKE USER &lt;userName> PRIVILEGES &lt;privileges> ON &lt;nodeName>;   

privileges:= string (COMMA string)*  

userName:=identifier  

nodeName:=identifier (DOT identifier)*

Eg: 
```
IoTDB > REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.ln;
```

#### Revoke Role Privileges

REVOKE ROLE &lt;roleName> PRIVILEGES &lt;privileges> ON &lt;nodeName>;  

privileges:= string (COMMA string)*  

roleName:= identifier  

nodeName:=identifier (DOT identifier)*

Eg: 
```
IoTDB > REVOKE ROLE temprole PRIVILEGES 'DELETE_TIMESERIES' ON root.ln;
```

#### Revoke Role From User

REVOKE &lt;roleName> FROM &lt;userName>;

roleName:=identifier

userName:=identifier

Eg: 
```
IoTDB > REVOKE temproleFROM tempuser;
```

#### List Users

LIST USER

Eg: 
```
IoTDB > LIST USER
```

#### List Roles

LIST ROLE

Eg:
```
IoTDB > LIST ROLE
```

#### List Privileges

LIST PRIVILEGES USER  &lt;username> ON &lt;path>;    

username:=identifier    

path=‘root’ (DOT identifier)*

Eg:
```
IoTDB > LIST PRIVIEGES USER sgcc_wirte_user ON root.sgcc;
```

#### List Privileges of Roles(On Specific Path)

LIST PRIVILEGES ROLE &lt;roleName> ON &lt;path>;    

roleName:=identifier  

path=‘root’ (DOT identifier)*

Eg: 
```
IoTDB > LIST PRIVIEGES ROLE wirte_role ON root.sgcc;
```

#### List Privileges of Users


LIST USER PRIVILEGES &lt;username> ;   

username:=identifier  

Eg: 
```
IoTDB > LIST USER PRIVIEGES tempuser;
```

#### List Privileges of Roles

LIST ROLE PRIVILEGES &lt;roleName>

roleName:=identifier

Eg: 
```
IoTDB > LIST ROLE PRIVIEGES actor;
```

#### List Roles of Users

LIST ALL ROLE OF USER &lt;username> ;  

username:=identifier

Eg: 
```
IoTDB > LIST ALL ROLE OF USER tempuser;
```

#### List Users of Role

LIST ALL USER OF ROLE &lt;roleName>;

roleName:=identifier

Eg: 
```
IoTDB > LIST ALL USER OF ROLE roleuser;
```

#### Update Password


UPDATE USER &lt;username> SET PASSWORD &lt;password>;

roleName:=identifier

password:=identifier

Eg: 
```
IoTDB > UPDATE USER tempuser SET PASSWORD newpwd;
```

### Functions

#### COUNT

The COUNT function returns the value number of timeseries(one or more) non-null values selected by the SELECT statement. The result is a signed 64-bit integer. If there are no matching rows, COUNT () returns 0.


SELECT COUNT(Path) (COMMA COUNT(Path))* FROM &lt;FromClause> [WHERE &lt;WhereClause>]?

Eg. 
```
SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>

* FIRST

The FIRST function returns the first point value of the choosen timeseries(one or more).

SELECT FIRST (Path) (COMMA FIRST (Path))* FROM &lt;FromClause> [WHERE &lt;WhereClause>]?

Eg. 
```
SELECT FIRST (status), FIRST (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>

#### MAX_TIME

The MAX_TIME function returns the maximum timestamp of the choosen timeseries(one or more). The result is a signed 64-bit integer, greater than 0.

SELECT MAX_TIME (Path) (COMMA MAX_TIME (Path))* FROM &lt;FromClause> [WHERE &lt;WhereClause>]?

Eg. 
```
SELECT MAX_TIME(status), MAX_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>


#### MAX_VALUE

The MAX_VALUE function returns the maximum value(lexicographically ordered) of the choosen timeseries (one or more). 

SELECT MAX_VALUE (Path) (COMMA MAX_VALUE (Path))* FROM &lt;FromClause> [WHERE &lt;WhereClause>]?

Eg. 
```
SELECT MAX_VALUE(status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>

#### MEAN

The MEAN function returns the arithmetic mean value of the choosen timeseries over a specified period of time. The timeseries must be int32, int64, float, double type, and the other types are not to be calculated. The result is a double type number.

SELECT MEAN (Path) (COMMA MEAN (Path))* FROM &lt;FromClause> [WHERE &lt;WhereClause>]?

Eg. 
```
SELECT MEAN (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>

#### MIN_TIME

The MIN_TIME function returns the minimum timestamp of the choosen timeseries(one or more). The result is a signed 64-bit integer, greater than 0.

SELECT MIN_TIME (Path) (COMMA MIN_TIME (Path))* FROM &lt;FromClause> [WHERE &lt;WhereClause>]?

Eg. 
```
SELECT MIN_TIME(status), MIN_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>

#### MIN_VALUE

The MIN_VALUE function returns the minimum value(lexicographically ordered) of the choosen timeseries (one or more). 


SELECT MIN_VALUE (Path) (COMMA MIN_VALUE (Path))* FROM &lt;FromClause> [WHERE &lt;WhereClause>]?

Eg. 
```
SELECT MIN_VALUE(status),MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>

#### NOW

The NOW function returns the current timestamp. This function can be used in the data operation statement to represent time. The result is a signed 64-bit integer, greater than 0. 


NOW()

Eg.
```
INSERT INTO root.ln.wf01.wt01(timestamp,status) VALUES(NOW(), false) 
UPDATE root.ln.wf01.wt01 SET temperature = 23 WHERE time < NOW()
DELETE FROM root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature WHERE time < NOW()
SELECT * FROM root WHERE time < NOW()
SELECT COUNT(temperature) FROM root.ln.wf01.wt01 WHERE time < NOW()
```
#### SUM

The SUM function returns the sum of the choosen timeseries (one or more) over a specified period of time. The timeseries must be int32, int64, float, double type, and the other types are not to be calculated. The result is a double type number. 

SELECT SUM(Path) (COMMA SUM(Path))* FROM &lt;FromClause> [WHERE &lt;WhereClause>]?

Eg. 
```
SELECT SUM(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
```
>Note: the statement needs to satisfy this constraint: &lt;PrefixPath> + &lt;Path> = &lt;Timeseries>

## Reference

### Keywords

```
Keywords for IoTDB (case insensitive):
ADD, BY, COMPRESSOR, CREATE, DATATYPE, DELETE, DESCRIBE, DROP, ENCODING, EXIT, FROM, GRANT, GROUP, LABLE, LINK, INDEX, INSERT, INTO, LOAD, MAX_POINT_NUMBER, MERGE, METADATA, ON, ORDER, PASSWORD, PRIVILEGES, PROPERTY, QUIT, REVOKE, ROLE, ROOT, SELECT, SET, SHOW, STORAGE, TIME, TIMESERIES, TIMESTAMP, TO, UNLINK, UPDATE, USER, USING, VALUE, VALUES, WHERE, WITH

Keywords with special meanings (case sensitive):
* Data Types: BOOLEAN, DOUBLE, FLOAT, INT32, INT64, TEXT (Only capitals is acceptable)
* Encoding Methods: BITMAP, DFT, GORILLA, PLAIN, RLE, TS_2DIFF (Only capitals is acceptable)
* Compression Methods: UNCOMPRESSED, SNAPPY (Only capitals is acceptable)
* Logical symbol: AND, &, &&, OR, | , ||, NOT, !, TRUE, FALSE
```

### Identifiers

```
QUOTE := '\'';
DOT := '.';
COLON : ':' ;
COMMA := ',' ;
SEMICOLON := ';' ;
LPAREN := '(' ;
RPAREN := ')' ;
LBRACKET := '[';
RBRACKET := ']';
EQUAL := '=' | '==';
NOTEQUAL := '<>' | '!=';
LESSTHANOREQUALTO := '<=';
LESSTHAN := '<';
GREATERTHANOREQUALTO := '>=';
GREATERTHAN := '>';
DIVIDE := '/';
PLUS := '+';
MINUS := '-';
STAR := '*';
Letter := 'a'..'z' | 'A'..'Z';
HexDigit := 'a'..'f' | 'A'..'F';
Digit := '0'..'9';
Boolean := TRUE | FALSE | 0 | 1 (case insensitive)

```

```
StringLiteral := ( '\'' ( ~('\'') )* '\'' | '\"' ( ~('\"') )* '\"');
eg. â€˜abcâ€?
eg. â€œabcâ€?
```

```
Integer := ('-' | '+')? Digit+;
eg. 123
eg. -222
```

```
Float := ('-' | '+')? Digit+ DOT Digit+ (('e' | 'E') ('-' | '+')? Digit+)?;
eg. 3.1415
eg. 1.2E10
eg. -1.33
```

```
Identifier := (Letter | '_') (Letter | Digit | '_' | MINUS)*;
eg. a123
eg. _abc123

```

### Literals


```
PointValue : Integer | Float | StringLiteral | Boolean
```
```
TimeValue : Integer | DateTime | ISO8601 | NOW()
Note: Integer means timestamp type.

DateTime : 
eg. 2016-11-16T16:22:33+08:00
eg. 2016-11-16 16:22:33+08:00
eg. 2016-11-16T16:22:33.000+08:00
eg. 2016-11-16 16:22:33.000+08:00
Note: DateTime Type can support several types, see Chapter 3 Datetime section for details.
```
```
PrecedenceEqualOperator : EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
```
```
Timeseries : ROOT [DOT <LayerName>]* DOT <SensorName>
LayerName : Identifier
SensorName : Identifier
eg. root.ln.wf01.wt01.status
eg. root.sgcc.wf03.wt01.temperature
Note: Timeseries must be start with `root`(case insensitive) and end with sensor name.
```

```
PrefixPath : ROOT (DOT <LayerName>)*
LayerName : Identifier | STAR
eg. root.sgcc
eg. root.*
```
```
Path: (ROOT | <LayerName>) (DOT <LayerName>)* 
LayerName: Identifier | STAR
eg. root.ln.wf01.wt01.status
eg. root.*.wf01.wt01.status
eg. root.ln.wf01.wt01.*
eg. *.wt01.*
eg. *
```
