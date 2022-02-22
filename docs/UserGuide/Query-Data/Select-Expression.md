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

## Syntax Definition

A selection expression (`selectExpr`) is a component of a SELECT clause, each `selectExpr` corresponds to a column in the query result set, and its syntax is defined as follows:

```sql
selectClause
    : SELECT influxResultColumn (',' influxResultColumn)*
    ;

influxResultColumn
    : selectExpr (AS alias)?
    ;

selectExpr
    : '(' selectExpr ')'
    | '-' selectExpr
    | selectExpr ('*' | '/' | '%') selectExpr
    | selectExpr ('+' | '-') selectExpr
    | functionName '(' selectExpr (',' selectExpr)* functionAttribute* ')'
    | timeSeriesSuffixPath
    | number
    ;
```

From this syntax definition, `selectExpr` can contain:

- suffix of time series path 
- function
   - Built-in aggregation functions, see [Aggregate Query](./Aggregate-Query.md) for details.
   - Time series generation function
   - User-defined functions, see [UDF](../Process-Data/UDF-User-Defined-Function.md) for details.
- expressions
   - Arithmetic operation expressions
   - Time series generating nested expressions
   - Aggregate query nested expressions
- Numeric constants (could be used in expressions only)

## Arithmetic Query

> Please note that Aligned Timeseries has not been supported in Arithmetic Query yet. An error message is expected if you use Arithmetic Query with Aligned Timeseries selected.

### Operators

#### Unary Arithmetic Operators

Supported operators: `+`, `-`

Supported input data types: `INT32`, `INT64`, `FLOAT` and `DOUBLE`

Output data type: consistent with the input data type

#### Binary Arithmetic Operators

Supported operators: `+`, `-`, `*`, `/`, `%`

Supported input data types: `INT32`, `INT64`, `FLOAT` and `DOUBLE`

Output data type: `DOUBLE`

Note: Only when the left operand and the right operand under a certain timestamp are not  `null`, the binary arithmetic operation will have an output value.

### Example

```sql
select s1, - s1, s2, + s2, s1 + s2, s1 - s2, s1 * s2, s1 / s2, s1 % s2 from root.sg.d1
```

Result:

```
+-----------------------------+-------------+--------------+-------------+-------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|                         Time|root.sg.d1.s1|-root.sg.d1.s1|root.sg.d1.s2|root.sg.d1.s2|root.sg.d1.s1 + root.sg.d1.s2|root.sg.d1.s1 - root.sg.d1.s2|root.sg.d1.s1 * root.sg.d1.s2|root.sg.d1.s1 / root.sg.d1.s2|root.sg.d1.s1 % root.sg.d1.s2|
+-----------------------------+-------------+--------------+-------------+-------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
|1970-01-01T08:00:00.001+08:00|          1.0|          -1.0|          1.0|          1.0|                          2.0|                          0.0|                          1.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.002+08:00|          2.0|          -2.0|          2.0|          2.0|                          4.0|                          0.0|                          4.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.003+08:00|          3.0|          -3.0|          3.0|          3.0|                          6.0|                          0.0|                          9.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.004+08:00|          4.0|          -4.0|          4.0|          4.0|                          8.0|                          0.0|                         16.0|                          1.0|                          0.0|
|1970-01-01T08:00:00.005+08:00|          5.0|          -5.0|          5.0|          5.0|                         10.0|                          0.0|                         25.0|                          1.0|                          0.0|
+-----------------------------+-------------+--------------+-------------+-------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+
Total line number = 5
It costs 0.014s
```

## Time Series Generating Functions

The time series generating function takes several time series as input and outputs one time series. Unlike the aggregation function, the result set of the time series generating function has a timestamp column.

All time series generating functions can accept * as input.

IoTDB supports hybrid queries of time series generating function queries and raw data queries.

> Please note that Aligned Timeseries has not been supported in queries with hybrid functions yet. An error message is expected if you use hybrid functions with Aligned Timeseries selected in a query statement.

### Mathematical Functions

Currently, IoTDB supports the following mathematical functions. The behavior of these mathematical functions is consistent with the behavior of these functions in the Java Math standard library.

| Function Name | Allowed Input Series Data Types | Output Series Data Type       | Corresponding Implementation in the Java Standard Library    |
| ------------- | ------------------------------- | ----------------------------- | ------------------------------------------------------------ |
| SIN           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#sin(double)                                             |
| COS           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#cos(double)                                             |
| TAN           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#tan(double)                                             |
| ASIN          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#asin(double)                                            |
| ACOS          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#acos(double)                                            |
| ATAN          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#atan(double)                                            |
| SINH          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#sinh(double)                                            |
| COSH          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#cosh(double)                                            |
| TANH          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#tanh(double)                                            |
| DEGREES       | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#toDegrees(double)                                       |
| RADIANS       | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#toRadians(double)                                       |
| ABS           | INT32 / INT64 / FLOAT / DOUBLE  | Same type as the input series | Math#abs(int) / Math#abs(long) /Math#abs(float) /Math#abs(double) |
| SIGN          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#signum(double)                                          |
| CEIL          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#ceil(double)                                            |
| FLOOR         | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#floor(double)                                           |
| ROUND         | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#rint(double)                                            |
| EXP           | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#exp(double)                                             |
| LN            | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#log(double)                                             |
| LOG10         | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#log10(double)                                           |
| SQRT          | INT32 / INT64 / FLOAT / DOUBLE  | DOUBLE                        | Math#sqrt(double)                                            |

Example:

```   sql
select s1, sin(s1), cos(s1), tan(s1) from root.sg1.d1 limit 5 offset 1000;
```

Result:

```
+-----------------------------+-------------------+-------------------+--------------------+-------------------+
|                         Time|     root.sg1.d1.s1|sin(root.sg1.d1.s1)| cos(root.sg1.d1.s1)|tan(root.sg1.d1.s1)|
+-----------------------------+-------------------+-------------------+--------------------+-------------------+
|2020-12-10T17:11:49.037+08:00|7360723084922759782| 0.8133527237573284|  0.5817708713544664| 1.3980636773094157|
|2020-12-10T17:11:49.038+08:00|4377791063319964531|-0.8938962705202537|  0.4482738644511651| -1.994085181866842|
|2020-12-10T17:11:49.039+08:00|7972485567734642915| 0.9627757585308978|-0.27030138509681073|-3.5618602479083545|
|2020-12-10T17:11:49.040+08:00|2508858212791964081|-0.6073417341629443| -0.7944406950452296| 0.7644897069734913|
|2020-12-10T17:11:49.041+08:00|2817297431185141819|-0.8419358900502509| -0.5395775727782725| 1.5603611649667768|
+-----------------------------+-------------------+-------------------+--------------------+-------------------+
Total line number = 5
It costs 0.008s
```

### String Processing Functions

Currently, IoTDB supports the following string processing functions:

| Function Name   | Allowed Input Series Data Types | Required Attributes                                          | Output Series Data Type | Description                                            |
| --------------- | ------------------------------- | ------------------------------------------------------------ | ----------------------- | ------------------------------------------------------ |
| STRING_CONTAINS | TEXT                            | `s`: the sequence to search for                              | BOOLEAN                 | Determine whether `s` is in the string                 |
| STRING_MATCHES  | TEXT                            | `regex`: the regular expression to which the string is to be matched | BOOLEAN                 | Determine whether the string can be matched by `regex` |

Example：

```   sql
select s1, string_contains(s1, 's'='warn'), string_matches(s1, 'regex'='[^\\s]+37229') from root.sg1.d4;
```

Result：

``` 
+-----------------------------+--------------+-------------------------------------------+------------------------------------------------------+
|                         Time|root.sg1.d4.s1|string_contains(root.sg1.d4.s1, "s"="warn")|string_matches(root.sg1.d4.s1, "regex"="[^\\s]+37229")|
+-----------------------------+--------------+-------------------------------------------+------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|    warn:-8721|                                       true|                                                 false|
|1970-01-01T08:00:00.002+08:00|  error:-37229|                                      false|                                                  true|
|1970-01-01T08:00:00.003+08:00|     warn:1731|                                       true|                                                 false|
+-----------------------------+--------------+-------------------------------------------+------------------------------------------------------+
Total line number = 3
It costs 0.007s
```

### Selector Functions

Currently, IoTDB supports the following selector functions:

| Function Name | Allowed Input Series Data Types       | Required Attributes                                          | Output Series Data Type       | Description                                                  |
| ------------- | ------------------------------------- | ------------------------------------------------------------ | ----------------------------- | ------------------------------------------------------------ |
| TOP_K         | INT32 / INT64 / FLOAT / DOUBLE / TEXT | `k`: the maximum number of selected data points, must be greater than 0 and less than or equal to 1000 | Same type as the input series | Returns `k` data points with the largest values in a time series. |
| BOTTOM_K      | INT32 / INT64 / FLOAT / DOUBLE / TEXT | `k`: the maximum number of selected data points, must be greater than 0 and less than or equal to 1000 | Same type as the input series | Returns `k` data points with the smallest values in a time series. |

Example：

```   sql
select s1, top_k(s1, 'k'='2'), bottom_k(s1, 'k'='2') from root.sg1.d2 where time > 2020-12-10T20:36:15.530+08:00;
```

Result：

``` 
+-----------------------------+--------------------+------------------------------+---------------------------------+
|                         Time|      root.sg1.d2.s1|top_k(root.sg1.d2.s1, "k"="2")|bottom_k(root.sg1.d2.s1, "k"="2")|
+-----------------------------+--------------------+------------------------------+---------------------------------+
|2020-12-10T20:36:15.531+08:00| 1531604122307244742|           1531604122307244742|                             null|
|2020-12-10T20:36:15.532+08:00|-7426070874923281101|                          null|                             null|
|2020-12-10T20:36:15.533+08:00|-7162825364312197604|          -7162825364312197604|                             null|
|2020-12-10T20:36:15.534+08:00|-8581625725655917595|                          null|             -8581625725655917595|
|2020-12-10T20:36:15.535+08:00|-7667364751255535391|                          null|             -7667364751255535391|
+-----------------------------+--------------------+------------------------------+---------------------------------+
Total line number = 5
It costs 0.006s
```

### Variation Trend Calculation Functions

Currently, IoTDB supports the following variation trend calculation functions:

| Function Name           | Allowed Input Series Data Types                 | Output Series Data Type       | Description                                                  |
| ----------------------- | ----------------------------------------------- | ----------------------------- | ------------------------------------------------------------ |
| TIME_DIFFERENCE         | INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | INT64                         | Calculates the difference between the time stamp of a data point and the time stamp of the previous data point. There is no corresponding output for the first data point. |
| DIFFERENCE              | INT32 / INT64 / FLOAT / DOUBLE                  | Same type as the input series | Calculates the difference between the value of a data point and the value of the previous data point. There is no corresponding output for the first data point. |
| NON_NEGATIVE_DIFFERENCE | INT32 / INT64 / FLOAT / DOUBLE                  | Same type as the input series | Calculates the absolute value of the difference between the value of a data point and the value of the previous data point. There is no corresponding output for the first data point. |
| DERIVATIVE              | INT32 / INT64 / FLOAT / DOUBLE                  | DOUBLE                        | Calculates the rate of change of a data point compared to the previous data point, the result is equals to DIFFERENCE / TIME_DIFFERENCE. There is no corresponding output for the first data point. |
| NON_NEGATIVE_DERIVATIVE | INT32 / INT64 / FLOAT / DOUBLE                  | DOUBLE                        | Calculates the absolute value of the rate of change of a data point compared to the previous data point, the result is equals to NON_NEGATIVE_DIFFERENCE / TIME_DIFFERENCE. There is no corresponding output for the first data point. |

Example:

```   sql
select s1, time_difference(s1), difference(s1), non_negative_difference(s1), derivative(s1), non_negative_derivative(s1) from root.sg1.d1 limit 5 offset 1000; 
```

Result:

``` 
+-----------------------------+-------------------+-------------------------------+--------------------------+---------------------------------------+--------------------------+---------------------------------------+
|                         Time|     root.sg1.d1.s1|time_difference(root.sg1.d1.s1)|difference(root.sg1.d1.s1)|non_negative_difference(root.sg1.d1.s1)|derivative(root.sg1.d1.s1)|non_negative_derivative(root.sg1.d1.s1)|
+-----------------------------+-------------------+-------------------------------+--------------------------+---------------------------------------+--------------------------+---------------------------------------+
|2020-12-10T17:11:49.037+08:00|7360723084922759782|                              1|      -8431715764844238876|                    8431715764844238876|    -8.4317157648442388E18|                  8.4317157648442388E18|
|2020-12-10T17:11:49.038+08:00|4377791063319964531|                              1|      -2982932021602795251|                    2982932021602795251|     -2.982932021602795E18|                   2.982932021602795E18|
|2020-12-10T17:11:49.039+08:00|7972485567734642915|                              1|       3594694504414678384|                    3594694504414678384|     3.5946945044146785E18|                  3.5946945044146785E18|
|2020-12-10T17:11:49.040+08:00|2508858212791964081|                              1|      -5463627354942678834|                    5463627354942678834|     -5.463627354942679E18|                   5.463627354942679E18|
|2020-12-10T17:11:49.041+08:00|2817297431185141819|                              1|        308439218393177738|                     308439218393177738|     3.0843921839317773E17|                  3.0843921839317773E17|
+-----------------------------+-------------------+-------------------------------+--------------------------+---------------------------------------+--------------------------+---------------------------------------+
Total line number = 5
It costs 0.014s
```

### Constant Timeseries Generating Functions

The constant timeseries generating function is used to generate a timeseries in which the values of all data points are the same.

The constant timeseries generating function accepts one or more timeseries inputs, and the timestamp set of the output data points is the union of the timestamp sets of the input timeseries.

Currently, IoTDB supports the following constant timeseries generating functions:

| Function Name | Required Attributes                                          | Output Series Data Type                      | Description                                                  |
| ------------- | ------------------------------------------------------------ | -------------------------------------------- | ------------------------------------------------------------ |
| CONST         | `value`: the value of the output data point <br />`type`: the type of the output data point, it can only be INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | Determined by the required attribute  `type` | Output the user-specified constant timeseries according to the  attributes `value` and `type`. |
| PI            | None                                                         | DOUBLE                                       | Data point value: a `double` value of  `π`, the ratio of the circumference of a circle to its diameter, which is equals to `Math.PI` in the *Java Standard Library*. |
| E             | None                                                         | DOUBLE                                       | Data point value: a `double` value of  `e`, the base of the natural logarithms, which is equals to `Math.E` in the *Java Standard Library*. |

Example:

```   sql
select s1, s2, const(s1, 'value'='1024', 'type'='INT64'), pi(s2), e(s1, s2) from root.sg1.d1; 
```

Result:

```
select s1, s2, const(s1, 'value'='1024', 'type'='INT64'), pi(s2), e(s1, s2) from root.sg1.d1; 
+-----------------------------+--------------+--------------+-----------------------------------------------------+------------------+---------------------------------+
|                         Time|root.sg1.d1.s1|root.sg1.d1.s2|const(root.sg1.d1.s1, "value"="1024", "type"="INT64")|pi(root.sg1.d1.s2)|e(root.sg1.d1.s1, root.sg1.d1.s2)|
+-----------------------------+--------------+--------------+-----------------------------------------------------+------------------+---------------------------------+
|1970-01-01T08:00:00.000+08:00|           0.0|           0.0|                                                 1024| 3.141592653589793|                2.718281828459045|
|1970-01-01T08:00:00.001+08:00|           1.0|          null|                                                 1024|              null|                2.718281828459045|
|1970-01-01T08:00:00.002+08:00|           2.0|          null|                                                 1024|              null|                2.718281828459045|
|1970-01-01T08:00:00.003+08:00|          null|           3.0|                                                 null| 3.141592653589793|                2.718281828459045|
|1970-01-01T08:00:00.004+08:00|          null|           4.0|                                                 null| 3.141592653589793|                2.718281828459045|
+-----------------------------+--------------+--------------+-----------------------------------------------------+------------------+---------------------------------+
Total line number = 5
It costs 0.005s
```
### Data Type Conversion Function
The IoTDB currently supports 6 data types, including INT32, INT64 ,FLOAT, DOUBLE, BOOLEAN, TEXT. When we query or evaluate data, we may need to convert data types, such as TEXT to INT32, or improve the accuracy of the data, such as FLOAT to DOUBLE. Therefore, IoTDB supports the use of cast functions to convert data types.

| Function Name | Required Attributes                                          | Output Series Data Type                      | Series Data Type  Description                               |
| ------------- | ------------------------------------------------------------ | -------------------------------------------- | ----------------------------------------------------------- |
| CAST          | `type`: the type of the output data point, it can only be INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | Determined by the required attribute  `type` | Converts data to the type specified by the `type` argument. |

#### Notes
1. The value of type BOOLEAN is `true`, when data is converted to BOOLEAN if INT32 and INT64 are not 0, FLOAT and DOUBLE are not 0.0, TEXT is not empty string or "false", otherwise `false`.    
2. The value of type INT32, INT64, FLOAT, DOUBLE are 1 or 1.0 and TEXT is "true", when BOOLEAN data is true, otherwise 0, 0.0 or "false".  
3. When TEXT is converted to INT32, INT64, or FLOAT, the TEXT is first converted to DOUBLE and then to the corresponding type, which may cause loss of precision. It will skip directly if the data can not be converted.

#### Syntax
Example data:
```
IoTDB> select text from root.test;
+-----------------------------+--------------+
|                         Time|root.test.text|
+-----------------------------+--------------+
|1970-01-01T08:00:00.001+08:00|           1.1|
|1970-01-01T08:00:00.002+08:00|             1|
|1970-01-01T08:00:00.003+08:00|   hello world|
|1970-01-01T08:00:00.004+08:00|         false|
+-----------------------------+--------------+
```
SQL:
```sql
select cast(text, 'type'='BOOLEAN'), cast(text, 'type'='INT32'), cast(text, 'type'='INT64'), cast(text, 'type'='FLOAT'), cast(text, 'type'='DOUBLE') from root.test;
```
Result:
```
+-----------------------------+--------------------------------------+------------------------------------+------------------------------------+------------------------------------+-------------------------------------+
|                         Time|cast(root.test.text, "type"="BOOLEAN")|cast(root.test.text, "type"="INT32")|cast(root.test.text, "type"="INT64")|cast(root.test.text, "type"="FLOAT")|cast(root.test.text, "type"="DOUBLE")|
+-----------------------------+--------------------------------------+------------------------------------+------------------------------------+------------------------------------+-------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                  true|                                   1|                                   1|                                 1.1|                                  1.1|
|1970-01-01T08:00:00.002+08:00|                                  true|                                   1|                                   1|                                 1.0|                                  1.0|
|1970-01-01T08:00:00.003+08:00|                                  true|                                null|                                null|                                null|                                 null|
|1970-01-01T08:00:00.004+08:00|                                 false|                                null|                                null|                                null|                                 null|
+-----------------------------+--------------------------------------+------------------------------------+------------------------------------+------------------------------------+-------------------------------------+
Total line number = 4
It costs 0.078s
```
### User Defined Timeseries Generating Functions

Please refer to [UDF (User Defined Function)](../Process-Data/UDF-User-Defined-Function.md).

Known Implementation UDF Libraries:

+ [IoTDB-Quality](https://thulab.github.io/iotdb-quality), a UDF library about data quality, including data profiling, data quality evalution and data repairing, etc.

## Nested Expressions

IoTDB supports the calculation of arbitrary nested expressions. Since time series query and aggregation query can not be used in a query statement at the same time, we divide nested expressions into two types, which are nested expressions with time series query and nested expressions with aggregation query. 

The following is the syntax definition of the `select` clause:

```sql
selectClause
    : SELECT influxResultColumn (',' influxResultColumn)*
    ;

influxResultColumn
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
|avg(root.sg1.a)|avg(root.sg1.b)|(((avg(root.sg1.a) + 1) * 3) / 2) - 1|(((avg(root.sg1.b) + 1) * 3) / 2) - 1|
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

##### Note

> Automated fill (`FILL`) and grouped by level (`GROUP BY LEVEL`) are not supported in an aggregation query with expression nested. They may be supported in future versions.
>
> The aggregation expression must be the lowest level input of one expression tree. Any kind expressions except timeseries are not valid as aggregation function parameters。
>
> In a word, the following queries are not valid.
>
> ```sql
> SELECT avg(s1+1) FROM root.sg.d1; -- The aggregation function has expression parameters.
> SELECT avg(s1) + avg(s2) FROM root.sg.* GROUP BY LEVEL=1; -- Grouped by level
> SELECT avg(s1) + avg(s2) FROM root.sg.d1 GROUP BY([0, 10000), 1s) FILL(previous); -- Automated fill
> ```

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