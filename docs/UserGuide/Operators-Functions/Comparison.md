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

# Comparison Operators and Functions

## Basic comparison operators

Supported operators `>`, `>=`, `<`, `<=`, `==`, `!=` (or  `<>` )

Supported input data types: `INT32`, `INT64`, `FLOAT` and `DOUBLE` 

Note: It will transform all type to `DOUBLE` then do computation. 

Output data type: `BOOLEAN`

**Example:**

```sql
select a, b, a > 10, a <= b, !(a <= b), a > 10 && a > b from root.test;
```

```
IoTDB> select a, b, a > 10, a <= b, !(a <= b), a > 10 && a > b from root.test;
+-----------------------------+-----------+-----------+----------------+--------------------------+---------------------------+------------------------------------------------+
|                         Time|root.test.a|root.test.b|root.test.a > 10|root.test.a <= root.test.b|!root.test.a <= root.test.b|(root.test.a > 10) & (root.test.a > root.test.b)|
+-----------------------------+-----------+-----------+----------------+--------------------------+---------------------------+------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|         23|       10.0|            true|                     false|                       true|                                            true|
|1970-01-01T08:00:00.002+08:00|         33|       21.0|            true|                     false|                       true|                                            true|
|1970-01-01T08:00:00.004+08:00|         13|       15.0|            true|                      true|                      false|                                           false|
|1970-01-01T08:00:00.005+08:00|         26|        0.0|            true|                     false|                       true|                                            true|
|1970-01-01T08:00:00.008+08:00|          1|       22.0|           false|                      true|                      false|                                           false|
|1970-01-01T08:00:00.010+08:00|         23|       12.0|            true|                     false|                       true|                                            true|
+-----------------------------+-----------+-----------+----------------+--------------------------+---------------------------+------------------------------------------------+
```

## `BETWEEN ... AND ...` operator

|operator |meaning|
|-----------------------------|-----------|
|`BETWEEN ... AND ...` |within the specified range|
|`NOT BETWEEN ... AND ...` |Not within the specified range|

**Example:** Select data within or outside the interval [36.5,40]:

```sql
select temperature from root.sg1.d1 where temperature between 36.5 and 40;
```

```sql
select temperature from root.sg1.d1 where temperature not between 36.5 and 40;
```

## Fuzzy matching operator

For TEXT type data, support fuzzy matching of data using `Like` and `Regexp` operators.

|operator |meaning|
|-----------------------------|-----------|
|`LIKE` | matches simple patterns|
|`NOT LIKE` |cannot match simple pattern|
|`REGEXP` | Match regular expression|
|`NOT REGEXP` |Cannot match regular expression|

Input data type: `TEXT`

Return type: `BOOLEAN`

### Use `Like` for fuzzy matching

**Matching rules:**

- `%` means any 0 or more characters.
- `_` means any single character.

**Example 1:** Query the data under `root.sg.d1` that contains `'cc'` in `value`.

```shell
IoTDB> select * from root.sg.d1 where value like '%cc%'
+--------------------------+----------------+
| Time|root.sg.d1.value|
+--------------------------+----------------+
|2017-11-01T00:00:00.000+08:00| aabbccdd|
|2017-11-01T00:00:01.000+08:00| cc|
+--------------------------+----------------+
Total line number = 2
It costs 0.002s
```

**Example 2:** Query the data under `root.sg.d1` with `'b'` in the middle of `value` and any single character before and after.

```shell
IoTDB> select * from root.sg.device where value like '_b_'
+--------------------------+----------------+
| Time|root.sg.d1.value|
+--------------------------+----------------+
|2017-11-01T00:00:02.000+08:00|abc|
+--------------------------+----------------+
Total line number = 1
It costs 0.002s
```

### Use `Regexp` for fuzzy matching

The filter condition that needs to be passed in is **Java standard library style regular expression**.

**Common regular matching examples:**

```
All characters with a length of 3-20: ^.{3,20}$
Uppercase English characters: ^[A-Z]+$
Numbers and English characters: ^[A-Za-z0-9]+$
Starting with a: ^a.*
```

**Example 1:** Query the string of 26 English characters for value under root.sg.d1.

```shell
IoTDB> select * from root.sg.d1 where value regexp '^[A-Za-z]+$'
+--------------------------+----------------+
| Time|root.sg.d1.value|
+--------------------------+----------------+
|2017-11-01T00:00:00.000+08:00| aabbccdd|
|2017-11-01T00:00:01.000+08:00| cc|
+--------------------------+----------------+
Total line number = 2
It costs 0.002s
```

**Example 2:** Query root.sg.d1 where the value is a string consisting of 26 lowercase English characters and the time is greater than 100.

```shell
IoTDB> select * from root.sg.d1 where value regexp '^[a-z]+$' and time > 100
+--------------------------+----------------+
| Time|root.sg.d1.value|
+--------------------------+----------------+
|2017-11-01T00:00:00.000+08:00| aabbccdd|
|2017-11-01T00:00:01.000+08:00| cc|
+--------------------------+----------------+
Total line number = 2
It costs 0.002s
```

**Example 3:**

```sql
select b, b like '1%', b regexp '[0-2]' from root.test;
```

operation result
```
+-----------------------------+-----------+------- ------------------+--------------------------+
| Time|root.test.b|root.test.b LIKE '^1.*?$'|root.test.b REGEXP '[0-2]'|
+-----------------------------+-----------+------- ------------------+--------------------------+
|1970-01-01T08:00:00.001+08:00| 111test111| true| true|
|1970-01-01T08:00:00.003+08:00| 333test333| false| false|
+-----------------------------+-----------+------- ------------------+--------------------------+
```

## `IS NULL` operator

|operator |meaning|
|-----------------------------|-----------|
|`IS NULL` |is a null value|
|`IS NOT NULL` |is not a null value|

**Example 1:** Select data with empty values:

```sql
select code from root.sg1.d1 where temperature is null;
```

**Example 2:** Select data with non-null values:

```sql
select code from root.sg1.d1 where temperature is not null;
```

## `IN` operator

|operator |meaning|
|-----------------------------|-----------|
|`IN` / `CONTAINS` | are the values ​​in the specified list|
|`NOT IN` / `NOT CONTAINS` |not a value in the specified list|

Input data type: `All Types`

return type `BOOLEAN`

**Note: Please ensure that the values ​​in the collection can be converted to the type of the input data. **
> 
> For example:
>
> `s1 in (1, 2, 3, 'test')`, the data type of `s1` is `INT32`
>
> We will throw an exception because `'test'` cannot be converted to type `INT32`
> 
**Example 1:** Select data with values ​​within a certain range:

```sql
select code from root.sg1.d1 where code in ('200', '300', '400', '500');
```

**Example 2:** Select data with values ​​outside a certain range:

```sql
select code from root.sg1.d1 where code not in ('200', '300', '400', '500');
```

**Example 3:**

```sql
select a, a in (1, 2) from root.test;
```

Output 2:
```
+-----------------------------+-----------+------- -------------+
| Time|root.test.a|root.test.a IN (1,2)|
+-----------------------------+-----------+------- -------------+
|1970-01-01T08:00:00.001+08:00| 1| true|
|1970-01-01T08:00:00.003+08:00| 3| false|
+-----------------------------+-----------+------- -------------+
```

## Condition Functions

Condition functions are used to check whether timeseries data points satisfy some specific condition. 

They return BOOLEANs.

Currently, IoTDB supports the following condition functions:

| Function Name | Allowed Input Series Data Types | Required Attributes                           | Output Series Data Type | Series Data Type  Description                 |
|---------------|---------------------------------|-----------------------------------------------|-------------------------|-----------------------------------------------|
| ON_OFF        | INT32 / INT64 / FLOAT / DOUBLE  | `threshold`: a double type variate            | BOOLEAN                 | Return `ts_value >= threshold`.               |
| IN_RANGR      | INT32 / INT64 / FLOAT / DOUBLE  | `lower`: DOUBLE type<br/>`upper`: DOUBLE type | BOOLEAN                 | Return `ts_value >= lower && value <= upper`. |

Example Data:
```
IoTDB> select ts from root.test;
+-----------------------------+------------+
|                         Time|root.test.ts|
+-----------------------------+------------+
|1970-01-01T08:00:00.001+08:00|           1|
|1970-01-01T08:00:00.002+08:00|           2|
|1970-01-01T08:00:00.003+08:00|           3|
|1970-01-01T08:00:00.004+08:00|           4|
+-----------------------------+------------+
```

##### Test 1
SQL:
```sql
select ts, on_off(ts, 'threshold'='2') from root.test;
```

Output:
```
IoTDB> select ts, on_off(ts, 'threshold'='2') from root.test;
+-----------------------------+------------+-------------------------------------+
|                         Time|root.test.ts|on_off(root.test.ts, "threshold"="2")|
+-----------------------------+------------+-------------------------------------+
|1970-01-01T08:00:00.001+08:00|           1|                                false|
|1970-01-01T08:00:00.002+08:00|           2|                                 true|
|1970-01-01T08:00:00.003+08:00|           3|                                 true|
|1970-01-01T08:00:00.004+08:00|           4|                                 true|
+-----------------------------+------------+-------------------------------------+
```

##### Test 2
Sql:
```sql
select ts, in_range(ts, 'lower'='2', 'upper'='3.1') from root.test;
```

Output:
```
IoTDB> select ts, in_range(ts,'lower'='2', 'upper'='3.1') from root.test;
+-----------------------------+------------+--------------------------------------------------+
|                         Time|root.test.ts|in_range(root.test.ts, "lower"="2", "upper"="3.1")|
+-----------------------------+------------+--------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|           1|                                             false|
|1970-01-01T08:00:00.002+08:00|           2|                                              true|
|1970-01-01T08:00:00.003+08:00|           3|                                              true|
|1970-01-01T08:00:00.004+08:00|           4|                                             false|
+-----------------------------+------------+--------------------------------------------------+
```