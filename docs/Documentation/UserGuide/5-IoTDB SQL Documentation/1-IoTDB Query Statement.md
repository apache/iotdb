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

# Chapter 5: IoTDB SQL Documentation

In this part, we will introduce you IoTDB's Query Language. IoTDB offers you a SQL-like query language for interacting with IoTDB, the query language can be devided into 4 major parts:

* Schema Statement: statements about schema management are all listed in this section.
* Data Management Statement: statements about data management (such as: data insertion, data query, etc.) are all listed in this section.
* Database Management Statement: statements about database management and authentication are all listed in this section.
* Functions: functions that IoTDB offers are all listed in this section.

All of these statements are write in IoTDB's own syntax, for details about the syntax composition, please check the `Reference` section.

## IoTDB Query Statement


### Schema Statement

* Set Storage Group

``` SQL
SET STORAGE GROUP TO <PrefixPath>
Eg: IoTDB > SET STORAGE GROUP TO root.ln.wf01.wt01
Note: PrefixPath can not include `*`
```
* Create Timeseries Statement

```
CREATE TIMESERIES <Timeseries> WITH <AttributeClauses>
AttributeClauses : DATATYPE=<DataTypeValue> COMMA ENCODING=<EncodingValue> [COMMA <ExtraAttributeClause>]*
DataTypeValue: BOOLEAN | DOUBLE | FLOAT | INT32 | INT64 | TEXT
EncodingValue: GORILLA | PLAIN | RLE | TS_2DIFF | REGULAR
ExtraAttributeClause: {
	COMPRESSOR = <CompressorValue>
	MAX_POINT_NUMBER = Integer
}
CompressorValue: UNCOMPRESSED | SNAPPY
Eg: IoTDB > CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
Eg: IoTDB > CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
Eg: IoTDB > CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSOR=SNAPPY, MAX_POINT_NUMBER=3
Note: Datatype and encoding type must be corresponding. Please check Chapter 3 Encoding Section for details.
```

* Delete Timeseries Statement

```
DELETE TIMESERIES <PrefixPath> [COMMA <PrefixPath>]*
Eg: IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.status
Eg: IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature
Eg: IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.*
```

* Show All Timeseries Statement

```
SHOW TIMESERIES
Eg: IoTDB > SHOW TIMESERIES
Note: This statement can only be used in IoTDB Client. If you need to show all timeseries in JDBC, please use `DataBaseMetadata` interface.
```

* Show Specific Timeseries Statement

```
SHOW TIMESERIES <Path>
Eg: IoTDB > SHOW TIMESERIES root
Eg: IoTDB > SHOW TIMESERIES root.ln
Eg: IoTDB > SHOW TIMESERIES root.ln.*.*.status
Eg: IoTDB > SHOW TIMESERIES root.ln.wf01.wt01.status
Note: The path can be prefix path, star path or timeseries path
Note: This statement can be used in IoTDB Client and JDBC.
```

* Show Storage Group Statement

```
SHOW STORAGE GROUP
Eg: IoTDB > SHOW STORAGE GROUP
Note: This statement can be used in IoTDB Client and JDBC.
```

### Data Management Statement

* Insert Record Statement

```
INSERT INTO <PrefixPath> LPAREN TIMESTAMP COMMA <Sensor> [COMMA <Sensor>]* RPAREN VALUES LPAREN <TimeValue>, <PointValue> [COMMA <PointValue>]* RPAREN
Sensor : Identifier
Eg: IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
Eg: IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp,status) VALUES(NOW(), false)
Eg: IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp,temperature) VALUES(2017-11-01T00:17:00.000+08:00,24.22028)
Eg: IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp, status, temperature) VALUES (1509466680000, false, 20.060787);
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
Note: The order of Sensor and PointValue need one-to-one correspondence
```

* Update Record Statement

```
UPDATE <UpdateClause> SET <SetClause> WHERE <WhereClause>
UpdateClause: <prefixPath>
SetClause: <SetExpression> 
SetExpression: <Path> EQUAL <PointValue>
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition  : <Expression> [(AND | OR) <Expression>]*
Expression : [NOT | !]? TIME PrecedenceEqualOperator <TimeValue>
Eg: IoTDB > UPDATE root.ln.wf01.wt01 SET temperature = 23 WHERE time < NOW() and time > 2017-11-1T00:15:00+08:00
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* Delete Record Statement

```
DELETE FROM <PrefixPath> [COMMA <PrefixPath>]* WHERE TIME LESSTHAN <TimeValue>
Eg: DELETE FROM root.ln.wf01.wt01.temperature WHERE time < 2017-11-1T00:05:00+08:00
Eg: DELETE FROM root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature WHERE time < NOW()
Eg: DELETE FROM root.ln.wf01.wt01.* WHERE time < 1509466140000
```

* Select Record Statement

```
SELECT <SelectClause> FROM <FromClause> [WHERE <WhereClause>]?
SelectClause : <SelectPath> (COMMA <SelectPath>)*
SelectPath : <FUNCTION> LPAREN <Path> RPAREN | <Path>
FUNCTION : ‘COUNT’ , ‘MIN_TIME’, ‘MAX_TIME’, ‘MIN_VALUE’, ‘MAX_VALUE’
FromClause : <PrefixPath> (COMMA <PrefixPath>)?
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition  : <Expression> [(AND | OR) <Expression>]*
Expression : [NOT | !]? <TimeExpr> | [NOT | !]? <SensorExpr>
TimeExpr : TIME PrecedenceEqualOperator <TimeValue>
SensorExpr : (<Timeseries> | <Path>) PrecedenceEqualOperator <PointValue>
Eg: IoTDB > SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00
Eg. IoTDB > SELECT * FROM root
Eg. IoTDB > SELECT COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
Eg. IoTDB > SELECT MIN_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
Eg. IoTDB > SELECT MAX_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature > 24
Eg. IoTDB > SELECT MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature > 23
Eg. IoTDB > SELECT MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
Note: the statement needs to satisfy this constraint: <Path>(SelectClause) + <PrefixPath>(FromClause) = <Timeseries>
Note: If the <SensorExpr>(WhereClause) is started with <Path> and not with ROOT, the statement needs to satisfy this constraint: <PrefixPath>(FromClause) + <Path>(SensorExpr) = <Timeseries>
Note: In Version 0.7.0, if <WhereClause> includes `OR`, time filter can not be used.
```

* Group By Statement

```
SELECT <SelectClause> FROM <FromClause> WHERE  <WhereClause> GROUP BY <GroupByClause>
SelectClause : <Function> [COMMA < Function >]*
Function : <AggregationFunction> LPAREN <Path> RPAREN
FromClause : <PrefixPath>
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition  : <Expression> [(AND | OR) <Expression>]*
Expression : [NOT | !]? <TimeExpr> | [NOT | !]? <SensorExpr>
TimeExpr : TIME PrecedenceEqualOperator <TimeValue>
SensorExpr : (<Timeseries> | <Path>) PrecedenceEqualOperator <PointValue>
GroupByClause : LPAREN <TimeUnit> (COMMA TimeValue)? COMMA <TimeInterval> (COMMA <TimeInterval>)* RPAREN
TimeUnit : Integer <DurationUnit>
DurationUnit : "ms" | "s" | "m" | "h" | "d" | "w"
TimeInterval: LBRACKET <TimeValue> COMMA <TimeValue> RBRACKET
Eg: SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 where temperature < 24 GROUP BY(5m, [1509465720000, 1509466380000])
Eg. SELECT COUNT (status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE time < 1509466500000 GROUP BY(5m, 1509465660000, [1509465720000, 1509466380000])
Eg. SELECT MIN_TIME(status), MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE temperature < 25 and time < 1509466800000 GROUP BY (3m, 1509465600000, [1509466140000, 1509466380000], [1509466440000, 1509466620000])
Note: the statement needs to satisfy this constraint: <Path>(SelectClause) + <PrefixPath>(FromClause) = <Timeseries>
Note: If the <SensorExpr>(WhereClause) is started with <Path> and not with ROOT, the statement needs to satisfy this constraint: <PrefixPath>(FromClause) + <Path>(SensorExpr) = <Timeseries>
Note: <TimeValue>(TimeInterval) needs to be greater than 0
Note: First <TimeValue>(TimeInterval) in needs to be smaller than second <TimeValue>(TimeInterval)
```

* Fill Statement

```
SELECT <SelectClause> FROM <FromClause> WHERE <WhereClause> FILL <FillClause>
SelectClause : <Path> [COMMA <Path>]*
FromClause : < PrefixPath > [COMMA < PrefixPath >]*
WhereClause : <WhereExpression>
WhereExpression : TIME EQUAL <TimeValue>
FillClause : LPAREN <TypeClause> [COMMA <TypeClause>]* RPAREN
TypeClause : <Int32Clause> | <Int64Clause> | <FloatClause> | <DoubleClause> | <BoolClause> | <TextClause>
Int32Clause: INT32 LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
Int64Clause: INT64 LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
FloatClause: FLOAT LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
DoubleClause: DOUBLE LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
BoolClause: BOOLEAN LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
TextClause: TEXT LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
PreviousClause : PREVIOUS [COMMA <ValidPreviousTime>]?
LinearClause : LINEAR [COMMA <ValidPreviousTime> COMMA <ValidBehindTime>]?
ValidPreviousTime, ValidBehindTime: <TimeUnit>
TimeUnit : Integer <DurationUnit>
DurationUnit : "ms" | "s" | "m" | "h" | "d" | "w"
Eg: SELECT temperature FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL(float[previous, 1m])
Eg: SELECT temperature,status FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL (float[linear, 1m, 1m], boolean[previous, 1m])
Eg: SELECT temperature,status,hardware FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL (float[linear, 1m, 1m], boolean[previous, 1m], text[previous])
Eg: SELECT temperature,status,hardware FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL (float[linear], boolean[previous, 1m], text[previous])
Note: the statement needs to satisfy this constraint: <PrefixPath>(FromClause) + <Path>(SelectClause) = <Timeseries>
Note: Integer in <TimeUnit> needs to be greater than 0
```

* Limit Statement

```
SELECT <SelectClause> FROM <FromClause> [WHERE <WhereClause>] [LIMIT <LIMITClause>] [SLIMIT <SLIMITClause>]
SelectClause : [<Path> | Function]+
Function : <AggregationFunction> LPAREN <Path> RPAREN
FromClause : <Path>
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition : <Expression> [(AND | OR) <Expression>]*
Expression: [NOT|!]?<TimeExpr> | [NOT|!]?<SensorExpr>
TimeExpr : TIME PrecedenceEqualOperator <TimeValue>
SensorExpr : (<Timeseries>|<Path>) PrecedenceEqualOperator <PointValue>
LIMITClause : <N> [OFFSETClause]?
N : NonNegativeInteger
OFFSETClause : OFFSET <OFFSETValue>
OFFSETValue : NonNegativeInteger
SLIMITClause : <SN> [SOFFSETClause]?
SN : NonNegativeInteger
SOFFSETClause : SOFFSET <SOFFSETValue>
SOFFSETValue : NonNegativeInteger
NonNegativeInteger:= ('+')? Digit+
Eg: IoTDB > SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00 LIMIT 3 OFFSET 2
Eg. IoTDB > SELECT COUNT (status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE time < 1509466500000 GROUP BY(5m, 1509465660000, [1509465720000, 1509466380000]) LIMIT 3
Note: The order of <LIMITClause> and <SLIMITClause> does not affect the grammatical correctness.
Note: <SLIMITClause> can only effect in Prefixpath and StarPath.
Note: <FillClause> can not use <LIMITClause> but not <SLIMITClause>.
```

### Database Management Statement

* Create User

```
CREATE USER <userName> <password>;  
userName:=identifier  
password:=identifier
Eg: IoTDB > CREATE USER thulab pwd;
```

* Delete User

```
DROP USER <userName>;  
userName:=identifier
Eg: IoTDB > DROP USER xiaoming;
```

* Create Role

```
CREATE ROLE <roleName>;  
roleName:=identifie
Eg: IoTDB > CREATE ROLE admin;
```

* Delete Role

```
DROP ROLE <roleName>;  
roleName:=identifier
Eg: IoTDB > DROP ROLE admin;
```

* Grant User Privileges

```
GRANT USER <userName> PRIVILEGES <privileges> ON <nodeName>;  
userName:=identifier  
nodeName:=identifier (DOT identifier)*  
privileges:= string (COMMA string)*
Eg: IoTDB > GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.ln;
```

* Grant Role Privileges

```
GRANT ROLE <roleName> PRIVILEGES <privileges> ON <nodeName>;  
privileges:= string (COMMA string)*  
roleName:=identifier  
nodeName:=identifier (DOT identifier)*
Eg: IoTDB > GRANT ROLE temprole PRIVILEGES 'DELETE_TIMESERIES' ON root.ln;
```

* Grant User Role

```
GRANT <roleName> TO <userName>;  
roleName:=identifier  
userName:=identifier
Eg: IoTDB > GRANT temprole TO tempuser;
```

* Revoke User Privileges

```
REVOKE USER <userName> PRIVILEGES <privileges> ON <nodeName>;   
privileges:= string (COMMA string)*  
userName:=identifier  
nodeName:=identifier (DOT identifier)*
Eg: IoTDB > REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.ln;
```

* Revoke Role Privileges

```
REVOKE ROLE <roleName> PRIVILEGES <privileges> ON <nodeName>;  
privileges:= string (COMMA string)*  
roleName:= identifier  
nodeName:=identifier (DOT identifier)*
Eg: IoTDB > REVOKE ROLE temprole PRIVILEGES 'DELETE_TIMESERIES' ON root.ln;
```

* Revoke Role From User

```
REVOKE <roleName> FROM <userName>;
roleName:=identifier
userName:=identifier
Eg: IoTDB > REVOKE temproleFROM tempuser;
```

* List Users

```
LIST USER
Eg: IoTDB > LIST USER
```

* List Roles

```
LIST ROLE
Eg: IoTDB > LIST ROLE
```

* List Privileges

```
LIST PRIVILEGES USER  <username> ON <path>;    
username:=identifier    
path=‘root’ (DOT identifier)*
Eg: IoTDB > LIST PRIVIEGES USER sgcc_wirte_user ON root.sgcc;
```

* List Privileges of Roles(On Specific Path)

```
LIST PRIVILEGES ROLE <roleName> ON <path>;    
roleName:=identifier  
path=‘root’ (DOT identifier)*
Eg: IoTDB > LIST PRIVIEGES ROLE wirte_role ON root.sgcc;
```

* List Privileges of Users

```
LIST USER PRIVILEGES <username> ;   
username:=identifier  
Eg: IoTDB > LIST USER PRIVIEGES tempuser;
```

* List Privileges of Roles

```
LIST ROLE PRIVILEGES <roleName>
roleName:=identifier
Eg: IoTDB > LIST ROLE PRIVIEGES actor;
```

* List Roles of Users

```
LIST ALL ROLE OF USER <username> ;  
username:=identifier
Eg: IoTDB > LIST ALL ROLE OF USER tempuser;
```

* List Users of Role

```
LIST ALL USER OF ROLE <roleName>;
roleName:=identifier
Eg: IoTDB > LIST ALL USER OF ROLE roleuser;
```

* Update Password

```
UPDATE USER <username> SET PASSWORD <password>;
roleName:=identifier
password:=identifier
Eg: IoTDB > UPDATE USER tempuser SET PASSWORD newpwd;
```

### Functions

* COUNT

The COUNT function returns the value number of timeseries(one or more) non-null values selected by the SELECT statement. The result is a signed 64-bit integer. If there are no matching rows, COUNT () returns 0.

```
SELECT COUNT(Path) (COMMA COUNT(Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* FIRST

The FIRST function returns the first point value of the choosen timeseries(one or more).

```
SELECT FIRST (Path) (COMMA FIRST (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT FIRST (status), FIRST (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MAX_TIME

The MAX_TIME function returns the maximum timestamp of the choosen timeseries(one or more). The result is a signed 64-bit integer, greater than 0.

```
SELECT MAX_TIME (Path) (COMMA MAX_TIME (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MAX_TIME(status), MAX_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MAX_VALUE

The MAX_VALUE function returns the maximum value(lexicographically ordered) of the choosen timeseries (one or more). 

```
SELECT MAX_VALUE (Path) (COMMA MAX_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MAX_VALUE(status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MEAN

The MEAN function returns the arithmetic mean value of the choosen timeseries over a specified period of time. The timeseries must be int32, int64, float, double type, and the other types are not to be calculated. The result is a double type number.

```
SELECT MEAN (Path) (COMMA MEAN (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MEAN (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MIN_TIME

The MIN_TIME function returns the minimum timestamp of the choosen timeseries(one or more). The result is a signed 64-bit integer, greater than 0.

```
SELECT MIN_TIME (Path) (COMMA MIN_TIME (Path))*FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MIN_TIME(status), MIN_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MIN_VALUE

The MIN_VALUE function returns the minimum value(lexicographically ordered) of the choosen timeseries (one or more). 

```
SELECT MIN_VALUE (Path) (COMMA MIN_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MIN_VALUE(status),MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* NOW

The NOW function returns the current timestamp. This function can be used in the data operation statement to represent time. The result is a signed 64-bit integer, greater than 0. 

```
NOW()
Eg. INSERT INTO root.ln.wf01.wt01(timestamp,status) VALUES(NOW(), false) 
Eg. UPDATE root.ln.wf01.wt01 SET temperature = 23 WHERE time < NOW()
Eg. DELETE FROM root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature WHERE time < NOW()
Eg. SELECT * FROM root WHERE time < NOW()
Eg. SELECT COUNT(temperature) FROM root.ln.wf01.wt01 WHERE time < NOW()
```
* SUM

The SUM function returns the sum of the choosen timeseries (one or more) over a specified period of time. The timeseries must be int32, int64, float, double type, and the other types are not to be calculated. The result is a double type number. 

```
SELECT SUM(Path) (COMMA SUM(Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT SUM(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

### TTL
IoTDB supports storage-level TTL settings, which means it is able to delete old data
automatically and periodically. The beneficial of using TTL is that hopefully you can control the 
total disk space usage to prevent the machine from running out of disk. Moreover, the query
performance may downgrade as the total number of files goes up. Timely removing such files also
helps to keep at a high query performance level. The TTL operations in IoTDB are supported by the
following two statements:

* Set TTL
```
SET TTL TO StorageGroupName TTLTime
Eg. SET TTL TO root.group1 3600000
This example means that for data in root.group1, only that of the latest 1 hour will remain, the
older one is removed or made invisible. 
Note: TTLTime should be millisecond timestamp. When TTL is set, insertions that fall
out of TTL will be rejected.
```

* Unset TTL
```
UNSET TTL TO StorageGroupName
Eg. UNSET TTL TO root.group1
This example means that data of all time will be stored in this group. 
```

Notice: When you set TTL to some storage groups, data out of the TTL will be made invisible
immediately, but because the data files may contain both out-dated and living data or the data files may
be being used by queries, the physical removal of data is stale. If you increase or unset TTL
just after setting it previously, some previously invisible data may be seen again, but the
physically removed one is lost forever. So we recommend that you do not change the TTL once it is
set or do not reset it frequently, unless you are determined to suffer this unpredictability. 