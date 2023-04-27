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

# Select Expression

The `SELECT` clause specifies the output of the query, consisting of several `selectExpr`. Each `selectExpr` defines one or more columns in the query result.

**`selectExpr` is an expression consisting of time series path suffixes, constants, functions, and operators. That is, `selectExpr` can contain: **
- Time series path suffix (wildcards are supported)
- operator
   - Arithmetic operators
   - comparison operators
   - Logical Operators
- function
   - aggregate functions
   - Time series generation functions (including built-in functions and user-defined functions)
- constant

## Use Alias

Since the unique data model of IoTDB, lots of additional information like device will be carried before each sensor. Sometimes, we want to query just one specific device, then these prefix information show frequently will be redundant in this situation, influencing the analysis of result set. At this time, we can use `AS` function provided by IoTDB, assign an alias to time series selected in query.  

For example：

```sql
select s1 as temperature, s2 as speed from root.ln.wf01.wt01;
```

The result set is：

| Time | temperature | speed |
| ---- | ----------- | ----- |
| ...  | ...         | ...   |


## Operator

See the documentation [Operators and Functions](../Operators-Functions/Overview.md) for a list of operators supported in IoTDB.

## Function

### aggregate functions

Aggregate functions are many-to-one functions. They perform aggregate calculations on a set of values, resulting in a single aggregated result.

**A query that contains an aggregate function is called an aggregate query**, otherwise, it is called a time series query.

> Please note that mixed use of `Aggregate Query` and `Timeseries Query` is not allowed. Below are examples for queries that are not allowed.
>
> ```
> select a, count(a) from root.sg 
> select sin(a), count(a) from root.sg
> select a, count(a) from root.sg group by ([10,100),10ms)
> ```

For the aggregation functions supported by IoTDB, see the document [Aggregation Functions](../Operators-Functions/Aggregation.md).

### Time series generation function

A time series generation function takes several raw time series as input and produces a list of time series as output. Unlike aggregate functions, time series generators have a timestamp column in their result sets.

All time series generation functions accept * as input, and all can be mixed with raw time series queries.

#### Built-in time series generation functions

See the documentation [Operators and Functions](../Operators-Functions/Overview.md) for a list of built-in functions supported in IoTDB.

#### User-Defined time series generation function

IoTDB supports function extension through User Defined Function (click for [User-Defined Function](../Operators-Functions/User-Defined-Function.md)) capability.

## Nested Expressions

IoTDB supports the calculation of arbitrary nested expressions. Since time series query and aggregation query can not be used in a query statement at the same time, we divide nested expressions into two types, which are nested expressions with time series query and nested expressions with aggregation query. 

The following is the syntax definition of the `select` clause:

```sql
selectClause
    : SELECT resultColumn (',' resultColumn)*
    ;

resultColumn
    : expression (AS ID)?
    ;

expression
    : '(' expression ')'
    | '-' expression
    | expression ('*' | '/' | '%') expression
    | expression ('+' | '-') expression
    | functionName '(' expression (',' expression)* functionAttribute* ')'
    | timeSeriesSuffixPath
    | number
    ;
```

### Nested Expressions with Time Series Query

IoTDB supports the calculation of arbitrary nested expressions consisting of **numbers, time series, time series generating functions (including user-defined functions) and arithmetic expressions** in the `select` clause.

##### Example

Input1：

```sql
select a,
       b,
       ((a + 1) * 2 - 1) % 2 + 1.5,
       sin(a + sin(a + sin(b))),
       -(a + b) * (sin(a + b) * sin(a + b) + cos(a + b) * cos(a + b)) + 1
from root.sg1;
```

Result1：

```
+-----------------------------+----------+----------+----------------------------------------+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                         Time|root.sg1.a|root.sg1.b|((((root.sg1.a + 1) * 2) - 1) % 2) + 1.5|sin(root.sg1.a + sin(root.sg1.a + sin(root.sg1.b)))|(-root.sg1.a + root.sg1.b * ((sin(root.sg1.a + root.sg1.b) * sin(root.sg1.a + root.sg1.b)) + (cos(root.sg1.a + root.sg1.b) * cos(root.sg1.a + root.sg1.b)))) + 1|
+-----------------------------+----------+----------+----------------------------------------+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.010+08:00|         1|         1|                                     2.5|                                 0.9238430524420609|                                                                                                                      -1.0|
|1970-01-01T08:00:00.020+08:00|         2|         2|                                     2.5|                                 0.7903505371876317|                                                                                                                      -3.0|
|1970-01-01T08:00:00.030+08:00|         3|         3|                                     2.5|                                0.14065207680386618|                                                                                                                      -5.0|
|1970-01-01T08:00:00.040+08:00|         4|      null|                                     2.5|                                               null|                                                                                                                      null|
|1970-01-01T08:00:00.050+08:00|      null|         5|                                    null|                                               null|                                                                                                                      null|
|1970-01-01T08:00:00.060+08:00|         6|         6|                                     2.5|                                -0.7288037411970916|                                                                                                                     -11.0|
+-----------------------------+----------+----------+----------------------------------------+---------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+
Total line number = 6
It costs 0.048s
```

Input2：

```sql
select (a + b) * 2 + sin(a) from root.sg
```

Result2：

```
+-----------------------------+----------------------------------------------+
|                         Time|((root.sg.a + root.sg.b) * 2) + sin(root.sg.a)|
+-----------------------------+----------------------------------------------+
|1970-01-01T08:00:00.010+08:00|                             59.45597888911063|
|1970-01-01T08:00:00.020+08:00|                            100.91294525072763|
|1970-01-01T08:00:00.030+08:00|                            139.01196837590714|
|1970-01-01T08:00:00.040+08:00|                            180.74511316047935|
|1970-01-01T08:00:00.050+08:00|                            219.73762514629607|
|1970-01-01T08:00:00.060+08:00|                             259.6951893788978|
|1970-01-01T08:00:00.070+08:00|                             300.7738906815579|
|1970-01-01T08:00:00.090+08:00|                             39.45597888911063|
|1970-01-01T08:00:00.100+08:00|                             39.45597888911063|
+-----------------------------+----------------------------------------------+
Total line number = 9
It costs 0.011s
```

Input3：

```sql
select (a + *) / 2  from root.sg1
```

Result3：

```
+-----------------------------+-----------------------------+-----------------------------+
|                         Time|(root.sg1.a + root.sg1.a) / 2|(root.sg1.a + root.sg1.b) / 2|
+-----------------------------+-----------------------------+-----------------------------+
|1970-01-01T08:00:00.010+08:00|                          1.0|                          1.0|
|1970-01-01T08:00:00.020+08:00|                          2.0|                          2.0|
|1970-01-01T08:00:00.030+08:00|                          3.0|                          3.0|
|1970-01-01T08:00:00.040+08:00|                          4.0|                         null|
|1970-01-01T08:00:00.060+08:00|                          6.0|                          6.0|
+-----------------------------+-----------------------------+-----------------------------+
Total line number = 5
It costs 0.011s
```

Input4：

```sql
select (a + b) * 3 from root.sg, root.ln
```

Result4：

```
+-----------------------------+---------------------------+---------------------------+---------------------------+---------------------------+
|                         Time|(root.sg.a + root.sg.b) * 3|(root.sg.a + root.ln.b) * 3|(root.ln.a + root.sg.b) * 3|(root.ln.a + root.ln.b) * 3|
+-----------------------------+---------------------------+---------------------------+---------------------------+---------------------------+
|1970-01-01T08:00:00.010+08:00|                       90.0|                      270.0|                      360.0|                      540.0|
|1970-01-01T08:00:00.020+08:00|                      150.0|                      330.0|                      690.0|                      870.0|
|1970-01-01T08:00:00.030+08:00|                      210.0|                      450.0|                      570.0|                      810.0|
|1970-01-01T08:00:00.040+08:00|                      270.0|                      240.0|                      690.0|                      660.0|
|1970-01-01T08:00:00.050+08:00|                      330.0|                       null|                       null|                       null|
|1970-01-01T08:00:00.060+08:00|                      390.0|                       null|                       null|                       null|
|1970-01-01T08:00:00.070+08:00|                      450.0|                       null|                       null|                       null|
|1970-01-01T08:00:00.090+08:00|                       60.0|                       null|                       null|                       null|
|1970-01-01T08:00:00.100+08:00|                       60.0|                       null|                       null|                       null|
+-----------------------------+---------------------------+---------------------------+---------------------------+---------------------------+
Total line number = 9
It costs 0.014s
```

##### Explanation

- Only when the left operand and the right operand under a certain timestamp are not `null`, the nested expressions will have an output value. Otherwise this row will not be included in the result. 
  - In Result1 of the Example part, the value of time series `root.sg.a` at time 40 is 4, while the value of time series `root.sg.b` is `null`. So at time 40, the value of nested expressions `(a + b) * 2 + sin(a)` is `null`. So in Result2, this row is not included in the result.
- If one operand in the nested expressions can be translated into multiple time series (For example, `*`), the result of each time series will be included in the result (Cartesian product). Please refer to Input3, Input4 and corresponding Result3 and Result4 in Example.

##### Note

> Please note that Aligned Time Series has not been supported in Nested Expressions with Time Series Query yet. An error message is expected if you use it with Aligned Time Series selected in a query statement.

### Nested Expressions query with aggregations

IoTDB supports the calculation of arbitrary nested expressions consisting of **numbers, aggregations and arithmetic expressions** in the `select` clause.

##### Example

Aggregation query without `GROUP BY`.

Input1:

```sql
select avg(temperature),
       sin(avg(temperature)),
       avg(temperature) + 1,
       -sum(hardware),
       avg(temperature) + sum(hardware)
from root.ln.wf01.wt01;
```

Result1:

```
+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+--------------------------------------------------------------------+
|avg(root.ln.wf01.wt01.temperature)|sin(avg(root.ln.wf01.wt01.temperature))|avg(root.ln.wf01.wt01.temperature) + 1|-sum(root.ln.wf01.wt01.hardware)|avg(root.ln.wf01.wt01.temperature) + sum(root.ln.wf01.wt01.hardware)|
+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+--------------------------------------------------------------------+
|                15.927999999999999|                   -0.21826546964855045|                    16.927999999999997|                         -7426.0|                                                            7441.928|
+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+--------------------------------------------------------------------+
Total line number = 1
It costs 0.009s
```

Input2:

```sql
select avg(*), 
	   (avg(*) + 1) * 3 / 2 -1 
from root.sg1
```

Result2:

```
+---------------+---------------+-------------------------------------+-------------------------------------+
|avg(root.sg1.a)|avg(root.sg1.b)|(avg(root.sg1.a) + 1) * 3 / 2 - 1    |(avg(root.sg1.b) + 1) * 3 / 2 - 1    |
+---------------+---------------+-------------------------------------+-------------------------------------+
|            3.2|            3.4|                    5.300000000000001|                   5.6000000000000005|
+---------------+---------------+-------------------------------------+-------------------------------------+
Total line number = 1
It costs 0.007s
```

Aggregation with `GROUP BY`.

Input3:

```sql
select avg(temperature),
       sin(avg(temperature)),
       avg(temperature) + 1,
       -sum(hardware),
       avg(temperature) + sum(hardware) as custom_sum
from root.ln.wf01.wt01
GROUP BY([10, 90), 10ms);
```

Result3:

```
+-----------------------------+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+----------+
|                         Time|avg(root.ln.wf01.wt01.temperature)|sin(avg(root.ln.wf01.wt01.temperature))|avg(root.ln.wf01.wt01.temperature) + 1|-sum(root.ln.wf01.wt01.hardware)|custom_sum|
+-----------------------------+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+----------+
|1970-01-01T08:00:00.010+08:00|                13.987499999999999|                     0.9888207947857667|                    14.987499999999999|                         -3211.0| 3224.9875|
|1970-01-01T08:00:00.020+08:00|                              29.6|                    -0.9701057337071853|                                  30.6|                         -3720.0|    3749.6|
|1970-01-01T08:00:00.030+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.040+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.050+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.060+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.070+08:00|                              null|                                   null|                                  null|                            null|      null|
|1970-01-01T08:00:00.080+08:00|                              null|                                   null|                                  null|                            null|      null|
+-----------------------------+----------------------------------+---------------------------------------+--------------------------------------+--------------------------------+----------+
Total line number = 8
It costs 0.012s
```

##### Explanation

- Only when the left operand and the right operand under a certain timestamp are not `null`, the nested expressions will have an output value. Otherwise this row will not be included in the result. But for nested expressions with `GROUP BY` clause, it is better to show the result of all time intervals. Please refer to Input3 and corresponding Result3 in Example.
- If one operand in the nested expressions can be translated into multiple time series (For example, `*`), the result of each time series will be included in the result (Cartesian product). Please refer to Input2 and corresponding Result2 in Example.



