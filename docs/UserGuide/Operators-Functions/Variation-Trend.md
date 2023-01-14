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

# Variation Trend Calculation Functions

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

| Function Name | Allowed Input Series Data Types | Attributes                                                                                                                                                                                                                                                                                              | Output Series Data Type | Description                                                                                                                                                                        |
|---------------|---------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DIFF          | INT32 / INT64 / FLOAT / DOUBLE  | `ignoreNull`：optional，default is true. If is true, the previous data point is ignored when it is null and continues to find the first non-null value forwardly. If the value is false, previous data point is not ignored when it is null, the result is also null because null is used for subtraction | DOUBLE                  | Calculates the difference between the value of a data point and the value of the previous data point. There is no corresponding output for the first data point, so output is null |

### Example

#### RawData

``` 
+-----------------------------+------------+------------+
|                         Time|root.test.s1|root.test.s2|
+-----------------------------+------------+------------+
|1970-01-01T08:00:00.001+08:00|           1|         1.0|
|1970-01-01T08:00:00.002+08:00|           2|        null|
|1970-01-01T08:00:00.003+08:00|        null|         3.0|
|1970-01-01T08:00:00.004+08:00|           4|        null|
|1970-01-01T08:00:00.005+08:00|           5|         5.0|
|1970-01-01T08:00:00.006+08:00|        null|         6.0|
+-----------------------------+------------+------------+
```

#### Not use `ignoreNull` attribute (Ignore Null)

SQL:
```sql
SELECT DIFF(s1), DIFF(s2) from root.test;
```

Result:
```
+-----------------------------+------------------+------------------+
|                         Time|DIFF(root.test.s1)|DIFF(root.test.s2)|
+-----------------------------+------------------+------------------+
|1970-01-01T08:00:00.001+08:00|              null|              null|
|1970-01-01T08:00:00.002+08:00|               1.0|              null|
|1970-01-01T08:00:00.003+08:00|              null|               2.0|
|1970-01-01T08:00:00.004+08:00|               2.0|              null|
|1970-01-01T08:00:00.005+08:00|               1.0|               2.0|
|1970-01-01T08:00:00.006+08:00|              null|               1.0|
+-----------------------------+------------------+------------------+
```

#### Use `ignoreNull` attribute

SQL:
```sql
SELECT DIFF(s1, 'ignoreNull'='false'), DIFF(s2, 'ignoreNull'='false') from root.test;
```

Result:
```
+-----------------------------+----------------------------------------+----------------------------------------+
|                         Time|DIFF(root.test.s1, "ignoreNull"="false")|DIFF(root.test.s2, "ignoreNull"="false")|
+-----------------------------+----------------------------------------+----------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                    null|                                    null|
|1970-01-01T08:00:00.002+08:00|                                     1.0|                                    null|
|1970-01-01T08:00:00.003+08:00|                                    null|                                    null|
|1970-01-01T08:00:00.004+08:00|                                    null|                                    null|
|1970-01-01T08:00:00.005+08:00|                                     1.0|                                    null|
|1970-01-01T08:00:00.006+08:00|                                    null|                                     1.0|
+-----------------------------+----------------------------------------+----------------------------------------+
```