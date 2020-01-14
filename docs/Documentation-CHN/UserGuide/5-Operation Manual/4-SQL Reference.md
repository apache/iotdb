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

# 第5章 IoTDB操作指南
## SQL 参考文档

### 显示版本号

```sql
show version
```

```
+---------------+
|        version|
+---------------+
|0.10.0-SNAPSHOT|
+---------------+
Total line number = 1
It costs 0.417s
```

### Schema语句

* 设置存储组

``` SQL
SET STORAGE GROUP TO <PrefixPath>
Eg: IoTDB > SET STORAGE GROUP TO root.ln.wf01.wt01
Note: PrefixPath can not include `*`
```
* 删除存储组

```
DELETE STORAGE GROUP <PrefixPath> [COMMA <PrefixPath>]*
Eg: IoTDB > DELETE STORAGE GROUP root.ln.wf01.wt01
Eg: IoTDB > DELETE STORAGE GROUP root.ln.wf01.wt01, root.ln.wf01.wt02
Note: PrefixPath can not include `*`
```

* 创建时间序列语句

```
CREATE TIMESERIES <Timeseries> WITH <AttributeClauses>
AttributeClauses : DATATYPE=<DataTypeValue> COMMA ENCODING=<EncodingValue> [COMMA <ExtraAttributeClause>]*
DataTypeValue: BOOLEAN | DOUBLE | FLOAT | INT32 | INT64 | TEXT
EncodingValue: GORILLA | PLAIN | RLE | TS_2DIFF | REGULAR
ExtraAttributeClause: {
	COMPRESSOR | COMPRESSION = <CompressorValue>
	MAX_POINT_NUMBER = Integer
}
CompressorValue: UNCOMPRESSED | SNAPPY
Eg: IoTDB > CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
Eg: IoTDB > CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
Eg: IoTDB > CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSOR=SNAPPY, MAX_POINT_NUMBER=3
Note: Datatype and encoding type must be corresponding. Please check Chapter 3 Encoding Section for details.
```

* 删除时间序列语句

```
DELETE TIMESERIES <PrefixPath> [COMMA <PrefixPath>]*
Eg: IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.status
Eg: IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature
Eg: IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.*
```

* 显示所有时间序列语句

```
SHOW TIMESERIES
Eg: IoTDB > SHOW TIMESERIES
Note: This statement can only be used in IoTDB Client. If you need to show all timeseries in JDBC, please use `DataBaseMetadata` interface.
```

* 显示特定时间序列语句

```
SHOW TIMESERIES <Path>
Eg: IoTDB > SHOW TIMESERIES root
Eg: IoTDB > SHOW TIMESERIES root.ln
Eg: IoTDB > SHOW TIMESERIES root.ln.*.*.status
Eg: IoTDB > SHOW TIMESERIES root.ln.wf01.wt01.status
Note: The path can be prefix path, star path or timeseries path
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示存储组语句

```
SHOW STORAGE GROUP
Eg: IoTDB > SHOW STORAGE GROUP
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示指定路径下时间序列数语句

```
COUNT TIMESERIES <Path>
Eg: IoTDB > COUNT TIMESERIES root
Eg: IoTDB > COUNT TIMESERIES root.ln
Eg: IoTDB > COUNT TIMESERIES root.ln.*.*.status
Eg: IoTDB > COUNT TIMESERIES root.ln.wf01.wt01.status
Note: The path can be prefix path, star path or timeseries path.
Note: This statement can be used in IoTDB Client and JDBC.
```

```
COUNT TIMESERIES <Path> GROUP BY LEVEL=<INTEGER>
Eg: IoTDB > COUNT TIMESERIES root GROUP BY LEVEL=1
Eg: IoTDB > COUNT TIMESERIES root.ln GROUP BY LEVEL=2
Eg: IoTDB > COUNT TIMESERIES root.ln.wf01 GROUP BY LEVEL=3
Note: The path can be prefix path or timeseries path.
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示指定路径下特定层级的节点数语句

```
COUNT NODES <Path> LEVEL=<INTEGER>
Eg: IoTDB > COUNT NODES root LEVEL=2
Eg: IoTDB > COUNT NODES root.ln LEVEL=2
Eg: IoTDB > COUNT NODES root.ln.wf01 LEVEL=3
Note: The path can be prefix path or timeseries path.
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示设备语句

```
SHOW DEVICES
Eg: IoTDB > SHOW DEVICES
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示ROOT节点的子节点名称语句

```
SHOW CHILD PATHS
Eg: IoTDB > SHOW CHILD PATHS
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示子节点名称语句

```
SHOW CHILD PATHS <Path>
Eg: IoTDB > SHOW CHILD PATHS root
Eg: IoTDB > SHOW CHILD PATHS root.ln
Eg: IoTDB > SHOW CHILD PATHS root.ln.wf01
Note: The path can only be prefix path.
Note: This statement can be used in IoTDB Client and JDBC.
```
### 数据管理语句

* 插入记录语句

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

* 更新记录语句

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

* 删除记录语句

```
DELETE FROM <PrefixPath> [COMMA <PrefixPath>]* WHERE TIME LESSTHAN <TimeValue>
Eg: DELETE FROM root.ln.wf01.wt01.temperature WHERE time < 2017-11-1T00:05:00+08:00
Eg: DELETE FROM root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature WHERE time < NOW()
Eg: DELETE FROM root.ln.wf01.wt01.* WHERE time < 1509466140000
```

* 选择记录语句

```
SELECT <SelectClause> FROM <FromClause> [WHERE <WhereClause>]?
SelectClause : <SelectPath> (COMMA <SelectPath>)*
SelectPath : <FUNCTION> LPAREN <Path> RPAREN | <Path>
FUNCTION : ‘COUNT’ , ‘MIN_TIME’, ‘MAX_TIME’, ‘MIN_VALUE’, ‘MAX_VALUE’
FromClause : <PrefixPath> (COMMA <PrefixPath>)?
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition  : <Expression> [(AND | OR) <Expression>]*
Expression : [NOT | !]? <TimeExpr> | [NOT | !]? <SensorExpr>
TimeExpr : TIME PrecedenceEqualOperator (<TimeValue> | <RelativeTime>)
RelativeTimeDurationUnit = Integer ('Y'|'MO'|'W'|'D'|'H'|'M'|'S'|'MS'|'US'|'NS')
RelativeTime : (now() | <TimeValue>) [(+|-) RelativeTimeDurationUnit]+
SensorExpr : (<Timeseries> | <Path>) PrecedenceEqualOperator <PointValue>
Eg: IoTDB > SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00
Eg. IoTDB > SELECT * FROM root
Eg. IoTDB > SELECT * FROM root where time > now() - 5m
Eg. IoTDB > SELECT COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
Eg. IoTDB > SELECT MIN_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
Eg. IoTDB > SELECT MAX_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature > 24
Eg. IoTDB > SELECT MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature > 23
Eg. IoTDB > SELECT MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
Note: the statement needs to satisfy this constraint: <Path>(SelectClause) + <PrefixPath>(FromClause) = <Timeseries>
Note: If the <SensorExpr>(WhereClause) is started with <Path> and not with ROOT, the statement needs to satisfy this constraint: <PrefixPath>(FromClause) + <Path>(SensorExpr) = <Timeseries>
Note: In Version 0.7.0, if <WhereClause> includes `OR`, time filter can not be used.
Note: There must be a space on both sides of the plus and minus operator appearing in the time expression 
```

* Group By语句

```
SELECT <SelectClause> FROM <FromClause> WHERE  <WhereClause> GROUP BY <GroupByClause>
SelectClause : <Function> [COMMA < Function >]*
Function : <AggregationFunction> LPAREN <Path> RPAREN
FromClause : <PrefixPath>
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition  : <Expression> [(AND | OR) <Expression>]*
Expression : [NOT | !]? <TimeExpr> | [NOT | !]? <SensorExpr>
TimeExpr : TIME PrecedenceEqualOperator (<TimeValue> | <RelativeTime>)
RelativeTimeDurationUnit = Integer ('Y'|'MO'|'W'|'D'|'H'|'M'|'S'|'MS'|'US'|'NS')
RelativeTime : (now() | <TimeValue>) [(+|-) RelativeTimeDurationUnit]+
SensorExpr : (<Timeseries> | <Path>) PrecedenceEqualOperator <PointValue>
GroupByClause : LPAREN <TimeInterval> COMMA <TimeUnit> (COMMA <TimeUnit>)? RPAREN
TimeInterval: LBRACKET <TimeValue> COMMA <TimeValue> RBRACKET
TimeUnit : Integer <DurationUnit>
DurationUnit : "ms" | "s" | "m" | "h" | "d" | "w"
Eg: SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 where temperature < 24 GROUP BY([1509465720000, 1509466380000], 5m)
Eg. SELECT COUNT (status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE time < 1509466500000 GROUP BY([1509465720000, 1509466380000], 5m, 10m)
Eg. SELECT MIN_TIME(status), MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE temperature < 25 GROUP BY ([1509466140000, 1509466380000], 3m, 5ms)
Note: the statement needs to satisfy this constraint: <Path>(SelectClause) + <PrefixPath>(FromClause) = <Timeseries>
Note: If the <SensorExpr>(WhereClause) is started with <Path> and not with ROOT, the statement needs to satisfy this constraint: <PrefixPath>(FromClause) + <Path>(SensorExpr) = <Timeseries>
Note: <TimeValue>(TimeInterval) needs to be greater than 0
Note: First <TimeValue>(TimeInterval) in needs to be smaller than second <TimeValue>(TimeInterval)
Note: <TimeUnit> needs to be greater than 0
Note: Third <TimeUnit> if set shouldn't be smaller than second <TimeUnit>
```

* Fill语句

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

* Limit语句

```
SELECT <SelectClause> FROM <FromClause> [WHERE <WhereClause>] [<LIMITClause>] [<SLIMITClause>]
SelectClause : [<Path> | Function]+
Function : <AggregationFunction> LPAREN <Path> RPAREN
FromClause : <Path>
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition : <Expression> [(AND | OR) <Expression>]*
Expression: [NOT|!]?<TimeExpr> | [NOT|!]?<SensorExpr>
TimeExpr : TIME PrecedenceEqualOperator (<TimeValue> | <RelativeTime>)
RelativeTimeDurationUnit = Integer ('Y'|'MO'|'W'|'D'|'H'|'M'|'S'|'MS'|'US'|'NS')
RelativeTime : (now() | <TimeValue>) [(+|-) RelativeTimeDurationUnit]+
SensorExpr : (<Timeseries>|<Path>) PrecedenceEqualOperator <PointValue>
LIMITClause : LIMIT <N> [OFFSETClause]?
N : Integer
OFFSETClause : OFFSET <OFFSETValue>
OFFSETValue : Integer
SLIMITClause : SLIMIT <SN> [SOFFSETClause]?
SN : Integer
SOFFSETClause : SOFFSET <SOFFSETValue>
SOFFSETValue : Integer
Eg: IoTDB > SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00 LIMIT 3 OFFSET 2
Eg. IoTDB > SELECT COUNT (status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE time < 1509466500000 GROUP BY([1509465720000, 1509466380000], 5m) LIMIT 3
Note: N, OFFSETValue, SN and SOFFSETValue must be greater than 0.
Note: The order of <LIMITClause> and <SLIMITClause> does not affect the grammatical correctness.
Note: <FillClause> can not use <LIMITClause> but not <SLIMITClause>.
```

* Disable align语句
```
规则:  
1. 大小写均可.  
Correct example: select * from root.sg1 disable align  
Correct example: select * from root.sg1 DISABLE ALIGN  

2. Disable Align只能用于查询语句句尾.  
Correct example: select * from root.sg1 where time > 10 disable align 
Wrong example: select * from root.sg1 disable align where time > 10 

3. Disable Align 不能用于聚合查询、Fill语句、Group by或Group by device语句，但可用于Limit语句。
Correct example: select * from root.sg1 limit 3 offset 2 disable align
Correct example: select * from root.sg1 slimit 3 soffset 2 disable align
Wrong example: select count(s0),count(s1) from root.sg1.d1 disable align
Wrong example: select * from root.vehicle where root.vehicle.d0.s0>0 disable align
Wrong example: select * from root.vehicle group by device disable align

4. 结果显示若无数据显示为空白.

查询结果样式如下表:
| Time | root.sg.d0.s1 | Time | root.sg.d0.s2 | Time | root.sg.d1.s1 |
| ---  | ---           | ---  | ---           | ---  | ---           |
|  1   | 100           | 20   | 300           | 400  | 600           |
|  2   | 300           | 40   | 800           | 700  | 900           |
|  4   | 500           |      |               | 800  | 1000          |
|      |               |      |               | 900  | 8000          |

5. 一些正确使用样例: 
   - select * from root.vehicle disable align
   - select s0,s0,s1 from root.vehicle.* disable align
   - select s0,s1 from root.vehicle.* limit 10 offset 1 disable align
   - select * from root.vehicle slimit 10 soffset 2 disable align
   - select * from root.vehicle where time > 10 disable align


```

### 数据库管理语句

* 创建用户

```
CREATE USER <userName> <password>;  
userName:=identifier  
password:=string
Eg: IoTDB > CREATE USER thulab 'pwd';
```

* 删除用户

```
DROP USER <userName>;  
userName:=identifier
Eg: IoTDB > DROP USER xiaoming;
```

* 创建角色

```
CREATE ROLE <roleName>;  
roleName:=identifie
Eg: IoTDB > CREATE ROLE admin;
```

* 删除角色

```
DROP ROLE <roleName>;  
roleName:=identifier
Eg: IoTDB > DROP ROLE admin;
```

* 赋予用户权限

```
GRANT USER <userName> PRIVILEGES <privileges> ON <nodeName>;  
userName:=identifier  
nodeName:=identifier (DOT identifier)*  
privileges:= string (COMMA string)*
Eg: IoTDB > GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.ln;
```

* 赋予角色权限

```
GRANT ROLE <roleName> PRIVILEGES <privileges> ON <nodeName>;  
privileges:= string (COMMA string)*  
roleName:=identifier  
nodeName:=identifier (DOT identifier)*
Eg: IoTDB > GRANT ROLE temprole PRIVILEGES 'DELETE_TIMESERIES' ON root.ln;
```

* 赋予用户角色

```
GRANT <roleName> TO <userName>;  
roleName:=identifier  
userName:=identifier
Eg: IoTDB > GRANT temprole TO tempuser;
```

* 撤销用户权限

```
REVOKE USER <userName> PRIVILEGES <privileges> ON <nodeName>;   
privileges:= string (COMMA string)*  
userName:=identifier  
nodeName:=identifier (DOT identifier)*
Eg: IoTDB > REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.ln;
```

* 撤销角色权限

```
REVOKE ROLE <roleName> PRIVILEGES <privileges> ON <nodeName>;  
privileges:= string (COMMA string)*  
roleName:= identifier  
nodeName:=identifier (DOT identifier)*
Eg: IoTDB > REVOKE ROLE temprole PRIVILEGES 'DELETE_TIMESERIES' ON root.ln;
```

* 撤销用户角色

```
REVOKE <roleName> FROM <userName>;
roleName:=identifier
userName:=identifier
Eg: IoTDB > REVOKE temproleFROM tempuser;
```

* 列出用户

```
LIST USER
Eg: IoTDB > LIST USER
```

* 列出角色

```
LIST ROLE
Eg: IoTDB > LIST ROLE
```

* 列出权限

```
LIST PRIVILEGES USER  <username> ON <path>;    
username:=identifier    
path=‘root’ (DOT identifier)*
Eg: IoTDB > LIST PRIVIEGES USER sgcc_wirte_user ON root.sgcc;
```

* 列出角色权限

```
LIST PRIVILEGES ROLE <roleName> ON <path>;    
roleName:=identifier  
path=‘root’ (DOT identifier)*
Eg: IoTDB > LIST PRIVIEGES ROLE wirte_role ON root.sgcc;
```

* 列出用户权限

```
LIST USER PRIVILEGES <username> ;   
username:=identifier  
Eg: IoTDB > LIST USER PRIVIEGES tempuser;
```

* 列出角色权限

```
LIST ROLE PRIVILEGES <roleName>
roleName:=identifier
Eg: IoTDB > LIST ROLE PRIVIEGES actor;
```

* 列出用户角色 

```
LIST ALL ROLE OF USER <username> ;  
username:=identifier
Eg: IoTDB > LIST ALL ROLE OF USER tempuser;
```

* 列出角色用户

```
LIST ALL USER OF ROLE <roleName>;
roleName:=identifier
Eg: IoTDB > LIST ALL USER OF ROLE roleuser;
```

* 更新密码 

```
ALTER USER <username> SET PASSWORD <password>;
roleName:=identifier
password:=string
Eg: IoTDB > ALTER USER tempuser SET PASSWORD newpwd;
```

### 功能

* COUNT

```
SELECT COUNT(Path) (COMMA COUNT(Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* FIRST_VALUE
原有的 `FIRST` 方法在 `v0.10.0` 版本更名为 `FIRST_VALUE`。
```
SELECT FIRST_VALUE (Path) (COMMA FIRST_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT FIRST_VALUE (status), FIRST_VALUE (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* LAST_VALUE
原有的 `LAST` 方法在 `v0.10.0` 版本更名为 `LAST_VALUE`。
```
SELECT LAST_VALUE (Path) (COMMA LAST_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT LAST_VALUE (status), LAST_VALUE (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MAX_TIME

```
SELECT MAX_TIME (Path) (COMMA MAX_TIME (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MAX_TIME(status), MAX_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MAX_VALUE

```
SELECT MAX_VALUE (Path) (COMMA MAX_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MAX_VALUE(status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* AVG
原有的 `MEAN` 方法在 `v0.9.0` 版本更名为 `AVG`。
```
SELECT AVG (Path) (COMMA AVG (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT AVG (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MIN_TIME

```
SELECT MIN_TIME (Path) (COMMA MIN_TIME (Path))*FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MIN_TIME(status), MIN_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MIN_VALUE

```
SELECT MIN_VALUE (Path) (COMMA MIN_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MIN_VALUE(status),MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* NOW

```
NOW()
Eg. INSERT INTO root.ln.wf01.wt01(timestamp,status) VALUES(NOW(), false) 
Eg. UPDATE root.ln.wf01.wt01 SET temperature = 23 WHERE time < NOW()
Eg. DELETE FROM root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature WHERE time < NOW()
Eg. SELECT * FROM root WHERE time < NOW()
Eg. SELECT COUNT(temperature) FROM root.ln.wf01.wt01 WHERE time < NOW()
```

* SUM

```
SELECT SUM(Path) (COMMA SUM(Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT SUM(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

### TTL
IoTDB支持对存储组级别设置数据存活时间（TTL），这使得IoTDB可以定期、自动地删除一定时间之前的数据。合理使用TTL
可以帮助您控制IoTDB占用的总磁盘空间以避免出现磁盘写满等异常。并且，随着文件数量的增多，查询性能往往随之下降,
内存占用也会有所提高。及时地删除一些较老的文件有助于使查询性能维持在一个较高的水平和减少内存资源的占用。
IoTDB中的TTL操作通可以由以下的语句进行实现：

* 设置 TTL
```
SET TTL TO StorageGroupName TTLTime
Eg. SET TTL TO root.group1 3600000
这个例子展示了如何使得root.group1这个存储组只保留近一个小时的数据，一个小时前的数据会被删除或者进入不可见状态。
注意: TTLTime 应是毫秒时间戳。一旦TTL被设置，超过TTL时间范围的写入将被拒绝。
```

* 取消 TTL
```
UNSET TTL TO StorageGroupName
Eg. UNSET TTL TO root.group1
这个例子展示了如何取消存储组root.group1的TTL，这将使得该存储组接受任意时刻的数据。
```

* 显示 TTL
```
SHOW ALL TTL
SHOW TTL ON StorageGroupNames
Eg.1 SHOW ALL TTL
这个例子会给出所有存储组的TTL。
Eg.2 SHOW TTL ON root.group1,root.group2,root.group3
这个例子会显示指定的三个存储组的TTL。
注意: 没有设置TTL的存储组的TTL将显示为null。
```

注意：当您对某个存储组设置TTL的时候，超过TTL范围的数据将会立即不可见。但由于数据文件可能混合包含处在TTL范围内
与范围外的数据，同时数据文件可能正在接受查询，数据文件的物理删除不会立即进行。如果你在此时取消或者调大TTL，
一部分之前不可见的数据可能重新可见，而那些已经被物理删除的数据则将永久丢失。也就是说，TTL操作不会原子性地删除
对应的数据。因此我们不推荐您频繁修改TTL，除非您能接受该操作带来的一定程度的不可预知性。

## 参考

### 关键字

```
Keywords for IoTDB (case insensitive):
ADD, BY, COMPRESSOR, CREATE, DATATYPE, DELETE, DESCRIBE, DROP, ENCODING, EXIT, FROM, GRANT, GROUP, LABLE, LINK, INDEX, INSERT, INTO, LOAD, MAX_POINT_NUMBER, MERGE, METADATA, ON, ORDER, PASSWORD, PRIVILEGES, PROPERTY, QUIT, REVOKE, ROLE, ROOT, SELECT, SET, SHOW, STORAGE, TIME, TIMESERIES, TIMESTAMP, TO, UNLINK, UPDATE, USER, USING, VALUE, VALUES, WHERE, WITH

Keywords with special meanings (case insensitive):
* Data Types: BOOLEAN, DOUBLE, FLOAT, INT32, INT64, TEXT 
* Encoding Methods: BITMAP, DFT, GORILLA, PLAIN, RLE, TS_2DIFF
* Compression Methods: UNCOMPRESSED, SNAPPY
* Logical symbol: AND, &, &&, OR, | , ||, NOT, !, TRUE, FALSE
```

### 标识符

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
eg. 'abc'
eg. "abc"
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

### 常量


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

