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
# Chapter 5: Operation Manual

## SQL Reference
In this part, we will introduce you IoTDB's Query Language. IoTDB offers you a SQL-like query language for interacting with IoTDB, the query language can be devided into 4 major parts:

* Schema Statement: statements about schema management are all listed in this section.
* Data Management Statement: statements about data management (such as: data insertion, data query, etc.) are all listed in this section.
* Database Management Statement: statements about database management and authentication are all listed in this section.
* Functions: functions that IoTDB offers are all listed in this section.

All of these statements are write in IoTDB's own syntax, for details about the syntax composition, please check the `Reference` section.

### Show Version

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

### Schema Statement

* Set Storage Group

``` SQL
SET STORAGE GROUP TO <PrefixPath>
Eg: IoTDB > SET STORAGE GROUP TO root.ln.wf01.wt01
Note: PrefixPath can not include `*`
```

* Delete Storage Group

```
DELETE STORAGE GROUP <PrefixPath> [COMMA <PrefixPath>]*
Eg: IoTDB > DELETE STORAGE GROUP root.ln.wf01.wt01
Eg: IoTDB > DELETE STORAGE GROUP root.ln.wf01.wt01, root.ln.wf01.wt02
Note: PrefixPath can not include `*`
```

* Create Timeseries Statement

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

* Count Timeseries Statement

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

* Count Nodes Statement

```
COUNT NODES <Path> LEVEL=<INTEGER>
Eg: IoTDB > COUNT NODES root LEVEL=2
Eg: IoTDB > COUNT NODES root.ln LEVEL=2
Eg: IoTDB > COUNT NODES root.ln.wf01 LEVEL=3
Note: The path can be prefix path or timeseries path.
Note: This statement can be used in IoTDB Client and JDBC.
```

* Show Devices Statement
```
SHOW DEVICES
Eg: IoTDB > SHOW DEVICES
Note: This statement can be used in IoTDB Client and JDBC.
```

* Show Child Paths of Root Statement
```
SHOW CHILD PATHS
Eg: IoTDB > SHOW CHILD PATHS
Note: This statement can be used in IoTDB Client and JDBC.
```

* Show Child Paths Statement
```
SHOW CHILD PATHS <Path>
Eg: IoTDB > SHOW CHILD PATHS root
Eg: IoTDB > SHOW CHILD PATHS root.ln
Eg: IoTDB > SHOW CHILD PATHS root.ln.wf01
Note: The path can only be prefix path.
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

* Group By Statement

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

* Group By Device Statement
```
GroupbyDeviceClause : GROUP BY DEVICE

Rules:  
1. Both uppercase and lowercase are ok.  
Correct example: select * from root.sg1 group by device  
Correct example: select * from root.sg1 GROUP BY DEVICE  

2. GroupbyDeviceClause can only be used at the end of a query statement.  
Correct example: select * from root.sg1 where time > 10 group by device  
Wrong example: select * from root.sg1 group by device where time > 10  

3. The paths of the SELECT clause can only be single level. In other words, the paths of the SELECT clause can only be measurements or STAR, without DOT.
Correct example: select s0,s1 from root.sg1.* group by device  
Correct example: select s0,s1 from root.sg1.d0, root.sg1.d1 group by device  
Correct example: select * from root.sg1.* group by device  
Correct example: select * from root group by device  
Correct example: select s0,s1,* from root.*.* group by device  
Wrong example: select d0.s1, d0.s2, d1.s0 from root.sg1 group by device  
Wrong example: select *.s0, *.s1 from root.* group by device  
Wrong example: select *.*.* from root group by device

4. The data types of the same measurement column should be the same across devices. 
Note that when it comes to aggregated paths, the data type of the measurement column will reflect 
the aggregation function rather than the original timeseries.

Correct example: select s0 from root.sg1.d0,root.sg1.d1 group by device   
root.sg1.d0.s0 and root.sg1.d1.s0 are both INT32.  

Correct example: select count(s0) from root.sg1.d0,root.sg1.d1 group by device   
count(root.sg1.d0.s0) and count(root.sg1.d1.s0) are both INT64.  

Wrong example: select s0 from root.sg1.d0, root.sg2.d3 group by device  
root.sg1.d0.s0 is INT32 while root.sg2.d3.s0 is FLOAT. 

5. The display principle of the result table is that only when the column (or row) has existing data will the column (or row) be shown, with nonexistent cells being null.   
For example, "select s0,s1,s2 from root.sg.d0, root.sg.d1, root.sg.d2 group by device". Suppose that the actual existing timeseries are as follows:  
- root.sg.d0.s0
- root.sg.d0.s1
- root.sg.d1.s0

Then the header of the result table will be: [Time, Device, s0, s1].  
And you could expect a table like:  

| Time | Device   | s0 | s1 |
| ---  | ---      | ---| ---|
|  1   |root.sg.d0| 20 | 2.5|
|  2   |root.sg.d0| 23 | 3.1|
| ...  | ...      | ...| ...|
|  1   |root.sg.d1| 12 |null|
|  2   |root.sg.d1| 19 |null|
| ...  | ...      | ...| ...|

Note that the cells of measurement 's0' and device 'root.sg.d1' are all null.    
Also note that the column of 's2' and the rows of 'root.sg.d2' are not existent.  

6. The duplicated devices in the prefix paths are neglected.  
For example, "select s0,s1 from root.sg.d0,root.sg.d0,root.sg.d1 group by device" is equal to "select s0,s1 from root.sg.d0,root.sg.d1 group by device".  
For example. "select s0,s1 from root.sg.*,root.sg.d0 group by device" is equal to "select s0,s1 from root.sg.* group by device".  

7. The duplicated measurements in the suffix paths are not neglected.  
For example, "select s0,s0,s1 from root.sg.* group by device" is not equal to "select s0,s1 from root.sg.* group by device".

8. More correct examples: 
   - select * from root.vehicle group by device
   - select s0,s0,s1 from root.vehicle.* group by device
   - select s0,s1 from root.vehicle.* limit 10 offset 1 group by device
   - select * from root.vehicle slimit 10 soffset 2 group by device
   - select * from root.vehicle where time > 10 group by device
   - select * from root.vehicle where root.vehicle.d0.s0>0 group by device
   - select count(*) from root.vehicle group by device
   - select sum(*) from root.vehicle GROUP BY (20ms,0,[2,50]) group by device
   - select * from root.vehicle where time = 3 Fill(int32[previous, 5ms]) group by device
```
* Disable Align Statement
```
Disable Align Clause: DISABLE ALIGN

Rules:  
1. Both uppercase and lowercase are ok.  
Correct example: select * from root.sg1 disable align  
Correct example: select * from root.sg1 DISABLE ALIGN  

2. Disable Align Clause can only be used at the end of a query statement.  
Correct example: select * from root.sg1 where time > 10 disable align 
Wrong example: select * from root.sg1 disable align where time > 10 

3. Disable Align Clause cannot be used with Aggregation, Fill Statements, Group By or Group By Device Statements, but can with Limit Statements.
Correct example: select * from root.sg1 limit 3 offset 2 disable align
Correct example: select * from root.sg1 slimit 3 soffset 2 disable align
Wrong example: select count(s0),count(s1) from root.sg1.d1 disable align
Wrong example: select * from root.vehicle where root.vehicle.d0.s0>0 disable align
Wrong example: select * from root.vehicle group by device disable align

4. The display principle of the result table is that only when the column (or row) has existing data will the column (or row) be shown, with nonexistent cells being empty.

You could expect a table like:
| Time | root.sg.d0.s1 | Time | root.sg.d0.s2 | Time | root.sg.d1.s1 |
| ---  | ---           | ---  | ---           | ---  | ---           |
|  1   | 100           | 20   | 300           | 400  | 600           |
|  2   | 300           | 40   | 800           | 700  | 900           |
|  4   | 500           |      |               | 800  | 1000          |
|      |               |      |               | 900  | 8000          |

5. More correct examples: 
   - select * from root.vehicle disable align
   - select s0,s0,s1 from root.vehicle.* disable align
   - select s0,s1 from root.vehicle.* limit 10 offset 1 disable align
   - select * from root.vehicle slimit 10 soffset 2 disable align
   - select * from root.vehicle where time > 10 disable align

```

### Database Management Statement

* Create User

```
CREATE USER <userName> <password>;  
userName:=identifier  
password:=string
Eg: IoTDB > CREATE USER thulab 'pwd';
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

* Alter Password

```
ALTER USER <username> SET PASSWORD <password>;
roleName:=identifier
password:=identifier
Eg: IoTDB > ALTER USER tempuser SET PASSWORD 'newpwd';
```

### Functions

* COUNT

The COUNT function returns the value number of timeseries(one or more) non-null values selected by the SELECT statement. The result is a signed 64-bit integer. If there are no matching rows, COUNT () returns 0.

```
SELECT COUNT(Path) (COMMA COUNT(Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* FIRST_VALUE(Rename from `FIRST` at `V0.10.0`)

The FIRST_VALUE function returns the first point value of the choosen timeseries(one or more).

```
SELECT FIRST_VALUE (Path) (COMMA FIRST_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT FIRST_VALUE (status), FIRST_VALUE (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* LAST_VALUE(Rename from `LAST` at `V0.10.0`)

The LAST_VALUE function returns the last point value of the choosen timeseries(one or more).

```
SELECT LAST_VALUE (Path) (COMMA LAST_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT LAST_VALUE (status), LAST_VALUE (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
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

* AVG(Rename from `MEAN` at `V0.9.0`)

The AVG function returns the arithmetic mean value of the choosen timeseries over a specified period of time. The timeseries must be int32, int64, float, double type, and the other types are not to be calculated. The result is a double type number.

```
SELECT AVG (Path) (COMMA AVG (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT AVG (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
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
automatically and periodically. The benefit of using TTL is that hopefully you can control the 
total disk space usage and prevent the machine from running out of disks. Moreover, the query
performance may downgrade as the total number of files goes up and the memory usage also increase
as there are more files. Timely removing such files helps to keep at a high query performance
level and reduce memory usage. The TTL operations in IoTDB are supported by the following three
statements:

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
This example means that data of all time will be accepted in this group. 
```

* Show TTL
```
SHOW ALL TTL
SHOW TTL ON StorageGroupNames
Eg.1 SHOW ALL TTL
This example will show TTLs of all storage groups.
Eg.2 SHOW TTL ON root.group1,root.group2,root.group3
This example will show TTLs of the specified 3 groups.
Notice: storage groups without TTL will show a "null"
```

Notice: When you set TTL to some storage groups, data out of the TTL will be made invisible
immediately, but because the data files may contain both out-dated and living data or the data files may
be being used by queries, the physical removal of data is stale. If you increase or unset TTL
just after setting it previously, some previously invisible data may be seen again, but the
physically removed one is lost forever. In other words, different from delete statement, the
atomicity of data deletion is not guaranteed for efficiency concerns. So we recommend that you do
not change the TTL once it is set or at least do not reset it frequently, unless you are determined 
to suffer the unpredictability. 

## Reference

### Keywords

```
Keywords for IoTDB (case insensitive):
ADD, BY, COMPRESSOR, CREATE, DATATYPE, DELETE, DESCRIBE, DROP, ENCODING, EXIT, FROM, GRANT, GROUP, LABLE, LINK, INDEX, INSERT, INTO, LOAD, MAX_POINT_NUMBER, MERGE, METADATA, ON, ORDER, PASSWORD, PRIVILEGES, PROPERTY, QUIT, REVOKE, ROLE, ROOT, SELECT, SET, SHOW, STORAGE, TIME, TIMESERIES, TIMESTAMP, TO, UNLINK, UPDATE, USER, USING, VALUE, VALUES, WHERE, WITH

Keywords with special meanings (case insensitive):
* Data Types: BOOLEAN, DOUBLE, FLOAT, INT32, INT64, TEXT 
* Encoding Methods: BITMAP, DFT, GORILLA, PLAIN, RLE, TS_2DIFF 
* Compression Methods: UNCOMPRESSED, SNAPPY, GZIP, LZ0, ZDT, PAA, PLA
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
eg. 'abc'
eg. 'abc'
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
