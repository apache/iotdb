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

The CASE expression is similar to the if/else statement in other languages.

The syntax is as follows:

- Format 1：
    ```sql
    CASE 
        WHEN condition1 THEN expression1
        [WHEN condition2 THEN expression2] ...
        [ELSE expression_end]
    END
    ```
  The `condition`s are evaluated one by one. 
- The first `condition` that is true will return the corresponding `expression`.
- 格式2：
    ```sql
    CASE caseValue
        WHEN whenValue1 THEN expression1
        [WHEN whenValue2 THEN expression2] ...
        [ELSE expression_end]
    END
    ```
  The `caseValue` is evaluated first, and then the `whenValue`s are evaluated one by one. The first `whenValue` that is equal to the `caseValue` will return the corresponding `expression`.

  Format 2 will be transformed into an equivalent Format 1 by iotdb. 
  For example, the above SQL statement will be transformed into:
    ```sql
    CASE 
        WHEN caseValue=whenValue1 THEN expression1
        [WHEN caseValue=whenValue1 THEN expression1] ...
        [ELSE expression_end]
    END
    ```

如果condition均不为真，或均不满足caseVaule=whenValue，则返回expression_end；不存在ELSE子句则返回null。

If none of the conditions are true, or if none of the `whenValue`s match the `caseValue`, the `expression_end` will be returned.
If there is no ELSE clause, `null` will be returned.

#### Example Usage

##### Test Data

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

##### Example 1

SQL statement：

```sql
select ts, case when ts<=1 then "ts <= 1" else "1 < ts" end from root.test
```

output：

```
+-----------------------------+------------+--------------------------------------------------------+
|                         Time|root.test.ts|CASE WHEN root.test.ts <= 1 THEN "ts <= 1" ELSE "1 < ts"|
+-----------------------------+------------+--------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|         1.0|                                                 ts <= 1|
|1970-01-01T08:00:00.002+08:00|         2.0|                                                  1 < ts|
|1970-01-01T08:00:00.003+08:00|         3.0|                                                  1 < ts|
|1970-01-01T08:00:00.004+08:00|         4.0|                                                  1 < ts|
+-----------------------------+------------+--------------------------------------------------------+
```

##### Example 2

The CASE expression can be nested in other expressions.

SQL statement：

```sql
select ts, case ts when 2 then 20 else 30 end + case ts when 3 then 40 else 50 end as data from root.test
```

output：

```
+-----------------------------+------------+----+
|                         Time|root.test.ts|data|
+-----------------------------+------------+----+
|1970-01-01T08:00:00.001+08:00|         1.0|80.0|
|1970-01-01T08:00:00.002+08:00|         2.0|70.0|
|1970-01-01T08:00:00.003+08:00|         3.0|70.0|
|1970-01-01T08:00:00.004+08:00|         4.0|80.0|
+-----------------------------+------------+----+
```

##### Example 3

If none of the conditions match and there is no ELSE clause, 
the output will be `null`.

SQL statement：

```sql
select ts, case ts when 1 then true end from root.test
```

output：

```
+-----------------------------+------------+-------------------------------------+
|                         Time|root.test.ts|CASE WHEN root.test.ts = 1 THEN true |
+-----------------------------+------------+-------------------------------------+
|1970-01-01T08:00:00.001+08:00|         1.0|                                 true|
|1970-01-01T08:00:00.002+08:00|         2.0|                                 null|
|1970-01-01T08:00:00.003+08:00|         3.0|                                 null|
|1970-01-01T08:00:00.004+08:00|         4.0|                                 null|
+-----------------------------+------------+-------------------------------------+
```


