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

# Query Filter

In IoTDB query statements, two filter conditions, **time filter** and **value filter**, are supported.

The supported operators are as follows:

- Comparison operators: greater than (`>`), greater than or equal ( `>=`), equal ( `=` or `==`), not equal ( `!=` or `<>`), less than or equal ( `<=`), less than ( `<`).
- Logical operators: and ( `AND` or `&` or `&&`), or ( `OR` or `|` or `||`), not ( `NOT` or `!`).
- Range contains operator: contains ( `IN` ).
- String matches operator: `LIKE`, `REGEXP`.

## Time Filter

Use time filters to filter data for a specific time range. For supported formats of timestamps, please refer to [Timestamp](../Data-Concept/Data-Type.md) .

An example is as follows:

1. Select data with timestamp greater than 2022-01-01T00:05:00.000:

    ```sql
    select s1 from root.sg1.d1 where time > 2022-01-01T00:05:00.000;
    ````

2. Select data with timestamp equal to 2022-01-01T00:05:00.000:

    ```sql
    select s1 from root.sg1.d1 where time = 2022-01-01T00:05:00.000;
    ````

3. Select the data in the time interval [2017-11-01T00:05:00.000, 2017-11-01T00:12:00.000):

    ```sql
    select s1 from root.sg1.d1 where time >= 2022-01-01T00:05:00.000 and time < 2017-11-01T00:12:00.000;
    ````

Note: In the above example, `time` can also be written as `timestamp`.

## Value Filter

Use value filters to filter data whose data values meet certain criteria. **Allow** to use a time series not selected in the select clause as a value filter.

An example is as follows:

1. Select data with a value greater than 36.5:

    ```sql
    select temperature from root.sg1.d1 where temperature > 36.5;
    ````

2. Select data with value equal to true:

    ```sql
    select status from root.sg1.d1 where status = true;
    ````

3. Select data for the interval [36.5,40] or not:

    ```sql
    select temperature from root.sg1.d1 where temperature between 36.5 and 40;
    ````
    ```sql
    select temperature from root.sg1.d1 where temperature not between 36.5 and 40;
    ````

4. Select data with values within a specific range:

    ```sql
    select code from root.sg1.d1 where code in ('200', '300', '400', '500');
    ````

5. Select data with values outside a certain range:

    ```sql
    select code from root.sg1.d1 where code not in ('200', '300', '400', '500');
    ````

6. Select data with values is null:

    ```sql
    select code from root.sg1.d1 where temperature is null;
    ````

7. Select data with values is not null:

    ```sql
    select code from root.sg1.d1 where temperature is not null;
    ````

## Fuzzy Query

Fuzzy query is divided into Like statement and Regexp statement, both of which can support fuzzy matching of TEXT type data.

Like statement:

### Fuzzy matching using `Like`

In the value filter condition, for TEXT type data, use `Like` and `Regexp` operators to perform fuzzy matching on data.

**Matching rules:**

- The percentage (`%`) wildcard matches any string of zero or more characters.
- The underscore (`_`) wildcard matches any single character.

**Example 1:** Query data containing `'cc'` in `value` under `root.sg.d1`. 

```
IoTDB> select * from root.sg.d1 where value like '%cc%'
+-----------------------------+----------------+
|                         Time|root.sg.d1.value|
+-----------------------------+----------------+
|2017-11-01T00:00:00.000+08:00|        aabbccdd| 
|2017-11-01T00:00:01.000+08:00|              cc|
+-----------------------------+----------------+
Total line number = 2
It costs 0.002s
```

**Example 2:** Query data that consists of 3 characters and the second character is `'b'` in `value` under `root.sg.d1`.

```
IoTDB> select * from root.sg.device where value like '_b_'
+-----------------------------+----------------+
|                         Time|root.sg.d1.value|
+-----------------------------+----------------+
|2017-11-01T00:00:02.000+08:00|             abc| 
+-----------------------------+----------------+
Total line number = 1
It costs 0.002s
```

### Fuzzy matching using `Regexp`

The filter conditions that need to be passed in are regular expressions in the Java standard library style.

**Examples of common regular matching:**

```
All characters with a length of 3-20: ^.{3,20}$
Uppercase english characters: ^[A-Z]+$
Numbers and English characters: ^[A-Za-z0-9]+$
Beginning with a: ^a.*
```

**Example 1:** Query a string composed of 26 English characters for the value under root.sg.d1

```
IoTDB> select * from root.sg.d1 where value regexp '^[A-Za-z]+$'
+-----------------------------+----------------+
|                         Time|root.sg.d1.value|
+-----------------------------+----------------+
|2017-11-01T00:00:00.000+08:00|        aabbccdd| 
|2017-11-01T00:00:01.000+08:00|              cc|
+-----------------------------+----------------+
Total line number = 2
It costs 0.002s
```

**Example 2:** Query root.sg.d1 where the value value is a string composed of 26 lowercase English characters and the time is greater than 100

```
IoTDB> select * from root.sg.d1 where value regexp '^[a-z]+$' and time > 100
+-----------------------------+----------------+
|                         Time|root.sg.d1.value|
+-----------------------------+----------------+
|2017-11-01T00:00:00.000+08:00|        aabbccdd| 
|2017-11-01T00:00:01.000+08:00|              cc|
+-----------------------------+----------------+
Total line number = 2
It costs 0.002s
```
