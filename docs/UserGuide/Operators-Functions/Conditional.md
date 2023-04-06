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

## Conditional Expressions

### CASE

#### Introduction

The CASE expression is a kind of conditional expression that can be used to return different values based on specific conditions, similar to the if-else statements in other languages.

The CASE expression consists of the following parts:

- CASE keyword: Indicates the start of the CASE expression.
- WHEN-THEN clauses: There may be multiple clauses used to define conditions and give results. This clause is divided into two parts, WHEN and THEN. The WHEN part defines the condition, and the THEN part defines the result expression. If the WHEN condition is true, the corresponding THEN result is returned.
- ELSE clause: If none of the WHEN conditions is true, the result in the ELSE clause will be returned. The ELSE clause can be omitted.
- END keyword: Indicates the end of the CASE expression.

The CASE expression is a scalar operation that can be used in combination with any other scalar operation or aggregate function.

In the following text, all THEN parts and ELSE clauses will be collectively referred to as result clauses.

#### Syntax

The CASE expression supports two formats.

- Format 1:
    ```sql
    CASE 
        WHEN condition1 THEN expression1
        [WHEN condition2 THEN expression2] ...
        [ELSE expression_end]
    END
    ```
  The `condition`s will be evaluated one by one.

  The first `condition` that is true will return the corresponding expression.

- Format 2:
    ```sql
    CASE caseValue
        WHEN whenValue1 THEN expression1
        [WHEN whenValue2 THEN expression2] ...
        [ELSE expression_end]
    END
    ```
  The `caseValue` will be evaluated first, and then the `whenValue`s will be evaluated one by one. The first `whenValue` that is equal to the `caseValue` will return the corresponding `expression`.

  Format 2 will be transformed into an equivalent Format 1 by iotdb.

  For example, the above SQL statement will be transformed into:

    ```sql
    CASE 
        WHEN caseValue=whenValue1 THEN expression1
        [WHEN caseValue=whenValue1 THEN expression1] ...
        [ELSE expression_end]
    END
    ```

If none of the conditions are true, or if none of the `whenValue`s match the `caseValue`, the `expression_end` will be returned.

If there is no ELSE clause, `null` will be returned.

#### Notes

- In format 1, all WHEN clauses must return a BOOLEAN type.
- In format 2, all WHEN clauses must be able to be compared to the CASE clause.
- All result clauses in a CASE expression must satisfy certain conditions for their return value types:
  - BOOLEAN types cannot coexist with other types and will cause an error if present.
  - TEXT types cannot coexist with other types and will cause an error if present.
  - The other four numeric types can coexist, and the final result will be of DOUBLE type, with possible precision loss during conversion.
  - If necessary, you can use the CAST function to convert the result to a type that can coexist with others.
- The CASE expression does not implement lazy evaluation, meaning that all clauses will be evaluated.
- The CASE expression does not support mixing with UDFs.
- Aggregate functions cannot be used within a CASE expression, but the result of a CASE expression can be used as input for an aggregate function.
- When using the CLI, because the CASE expression string can be lengthy, it is recommended to provide an alias for the expression using AS.

#### Using Examples

##### Example 1

The CASE expression can be used to analyze data in a visual way. For example:
- The preparation of a certain chemical product requires that the temperature and pressure be within specific ranges.
- During the preparation process, sensors will detect the temperature and pressure, forming two time-series T (temperature) and P (pressure) in IoTDB.
In this application scenario, the CASE expression can indicate which time parameters are appropriate, which are not, and why they are not.

data:
```sql
IoTDB> select * from root.test1
+-----------------------------+------------+------------+
|                         Time|root.test1.P|root.test1.T|
+-----------------------------+------------+------------+
|2023-03-29T11:25:54.724+08:00|   1000000.0|      1025.0|
|2023-03-29T11:26:13.445+08:00|   1000094.0|      1040.0|
|2023-03-29T11:27:36.988+08:00|   1000095.0|      1041.0|
|2023-03-29T11:27:56.446+08:00|   1000095.0|      1059.0|
|2023-03-29T11:28:20.838+08:00|   1200000.0|      1040.0|
+-----------------------------+------------+------------+
```

SQL statements:
```sql
select T, P, case
when 1000<T and T<1050 and 1000000<P and P<1100000 then "good!"
when T<=1000 or T>=1050 then "bad temperature"
when P<=1000000 or P>=1100000 then "bad pressure"
end as `result`
from root.test1
```


output:
```
+-----------------------------+------------+------------+---------------+
|                         Time|root.test1.T|root.test1.P|         result|
+-----------------------------+------------+------------+---------------+
|2023-03-29T11:25:54.724+08:00|      1025.0|   1000000.0|   bad pressure|
|2023-03-29T11:26:13.445+08:00|      1040.0|   1000094.0|          good!|
|2023-03-29T11:27:36.988+08:00|      1041.0|   1000095.0|          good!|
|2023-03-29T11:27:56.446+08:00|      1059.0|   1000095.0|bad temperature|
|2023-03-29T11:28:20.838+08:00|      1040.0|   1200000.0|   bad pressure|
+-----------------------------+------------+------------+---------------+
```


##### Example 2

The CASE expression can achieve flexible result transformation, such as converting strings with a certain pattern to other strings.

data:
```sql
IoTDB> select * from root.test2
+-----------------------------+--------------+
|                         Time|root.test2.str|
+-----------------------------+--------------+
|2023-03-27T18:23:33.427+08:00|         abccd|
|2023-03-27T18:23:39.389+08:00|         abcdd|
|2023-03-27T18:23:43.463+08:00|       abcdefg|
+-----------------------------+--------------+
```

SQL statements:
```sql
select str, case
when str like "%cc%" then "has cc"
when str like "%dd%" then "has dd"
else "no cc and dd" end as `result`
from root.test2
```

output:
```
+-----------------------------+--------------+------------+
|                         Time|root.test2.str|      result|
+-----------------------------+--------------+------------+
|2023-03-27T18:23:33.427+08:00|         abccd|      has cc|
|2023-03-27T18:23:39.389+08:00|         abcdd|      has dd|
|2023-03-27T18:23:43.463+08:00|       abcdefg|no cc and dd|
+-----------------------------+--------------+------------+
```

##### Example 3: work with aggregation functions

###### valid: aggregation function ← CASE expression

The CASE expression can be used as a parameter for aggregate functions. For example, used in conjunction with the COUNT function, it can implement statistics based on multiple conditions simultaneously.

data:
```sql
IoTDB> select * from root.test3
+-----------------------------+------------+
|                         Time|root.test3.x|
+-----------------------------+------------+
|2023-03-27T18:11:11.300+08:00|         0.0|
|2023-03-27T18:11:14.658+08:00|         1.0|
|2023-03-27T18:11:15.981+08:00|         2.0|
|2023-03-27T18:11:17.668+08:00|         3.0|
|2023-03-27T18:11:19.112+08:00|         4.0|
|2023-03-27T18:11:20.822+08:00|         5.0|
|2023-03-27T18:11:22.462+08:00|         6.0|
|2023-03-27T18:11:24.174+08:00|         7.0|
|2023-03-27T18:11:25.858+08:00|         8.0|
|2023-03-27T18:11:27.979+08:00|         9.0|
+-----------------------------+------------+
```

SQL statements:

```sql
select
count(case when x<=1 then 1 end) as `(-∞,1]`,
count(case when 1<x and x<=3 then 1 end) as `(1,3]`,
count(case when 3<x and x<=7 then 1 end) as `(3,7]`,
count(case when 7<x then 1 end) as `(7,+∞)`
from root.test3
```

output:
```
+------+-----+-----+------+
|(-∞,1]|(1,3]|(3,7]|(7,+∞)|
+------+-----+-----+------+
|     2|    2|    4|     2|
+------+-----+-----+------+
```

###### invalid: CASE expression ← aggregation function 

Using aggregation function in CASE expression is not supported

SQL statements:
```sql
select case when x<=1 then avg(x) else sum(x) end from root.test3
```

output:
```
Msg: 701: Raw data and aggregation result hybrid calculation is not supported.
```

##### Example 4: kind 2

Here is a simple example that uses the format 2 syntax. 
If all conditions are equality tests, it is recommended to use format 2 to simplify SQL statements.

data:
```sql
IoTDB> select * from root.test4
+-----------------------------+------------+
|                         Time|root.test4.x|
+-----------------------------+------------+
|1970-01-01T08:00:00.001+08:00|         1.0|
|1970-01-01T08:00:00.002+08:00|         2.0|
|1970-01-01T08:00:00.003+08:00|         3.0|
|1970-01-01T08:00:00.004+08:00|         4.0|
+-----------------------------+------------+
```

SQL statements:
```sql
select x, case x when 1 then "one" when 2 then "two" else "other" end from root.test4
```

output:
```
+-----------------------------+------------+-----------------------------------------------------------------------------------+
|                         Time|root.test4.x|CASE WHEN root.test4.x = 1 THEN "one" WHEN root.test4.x = 2 THEN "two" ELSE "other"|
+-----------------------------+------------+-----------------------------------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|         1.0|                                                                                one|
|1970-01-01T08:00:00.002+08:00|         2.0|                                                                                two|
|1970-01-01T08:00:00.003+08:00|         3.0|                                                                              other|
|1970-01-01T08:00:00.004+08:00|         4.0|                                                                              other|
+-----------------------------+------------+-----------------------------------------------------------------------------------+
```

##### Example 5: type of return clauses

The result clause of a CASE expression needs to satisfy certain type restrictions.

In this example, we continue to use the data from Example 4.

###### Invalid: BOOLEAN cannot coexist with other types

SQL statements:
```sql
select x, case x when 1 then true when 2 then 2 end from root.test4
```

output:
```
Msg: 701: CASE expression: BOOLEAN and other types cannot exist at same time
```

###### Valid: Only BOOLEAN type exists

SQL statements:
```sql
select x, case x when 1 then true when 2 then false end as `result` from root.test4
```

output:
```
+-----------------------------+------------+------+
|                         Time|root.test4.x|result|
+-----------------------------+------------+------+
|1970-01-01T08:00:00.001+08:00|         1.0|  true|
|1970-01-01T08:00:00.002+08:00|         2.0| false|
|1970-01-01T08:00:00.003+08:00|         3.0|  null|
|1970-01-01T08:00:00.004+08:00|         4.0|  null|
+-----------------------------+------------+------+
```

###### Invalid:TEXT cannot coexist with other types

SQL statements:
```sql
select x, case x when 1 then 1 when 2 then "str" end from root.test4
```

output:
```
Msg: 701: CASE expression: TEXT and other types cannot exist at same time
```

###### Valid: Only TEXT type exists

See in Example 1.

###### Valid: Numerical types coexist

SQL statements:
```sql
select x, case x
when 1 then 1
when 2 then 222222222222222
when 3 then 3.3
when 4 then 4.4444444444444
end as `result`
from root.test4
```

output:
```
+-----------------------------+------------+-------------------+
|                         Time|root.test4.x|             result|
+-----------------------------+------------+-------------------+
|1970-01-01T08:00:00.001+08:00|         1.0|                1.0|
|1970-01-01T08:00:00.002+08:00|         2.0|2.22222222222222E14|
|1970-01-01T08:00:00.003+08:00|         3.0|  3.299999952316284|
|1970-01-01T08:00:00.004+08:00|         4.0|   4.44444465637207|
+-----------------------------+------------+-------------------+
```