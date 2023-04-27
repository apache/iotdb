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

# Continuous Interval Functions

The continuous interval functions are used to query all continuous intervals that meet specified conditions.
They can be divided into two categories according to return value:
1. Returns the start timestamp and time span of the continuous interval that meets the conditions (a time span of 0 means that only the start time point meets the conditions)
2. Returns the start timestamp of the continuous interval that meets the condition and the number of points in the interval (a number of 1 means that only the start time point meets the conditions)

| Function Name     | Input TSDatatype                     | Parameters                                                                                    | Output TSDatatype | Function Description                                                                                                                                                         |
|-------------------|--------------------------------------|-----------------------------------------------------------------------------------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ZERO_DURATION     | INT32/ INT64/ FLOAT/ DOUBLE/ BOOLEAN | `min`:Optional with default value `0L`<br>`max`:Optional with default value `Long.MAX_VALUE` | Long              | Return intervals' start times and duration times in which the value is always 0(false), and the duration time `t` satisfy `t >= min && t <= max`. The unit of `t` is ms      |
| NON_ZERO_DURATION | INT32/ INT64/ FLOAT/ DOUBLE/ BOOLEAN | `min`:Optional with default value `0L`<br>`max`:Optional with default value `Long.MAX_VALUE` | Long              | Return intervals' start times and duration times in which the value is always not 0, and the duration time `t` satisfy `t >= min && t <= max`. The unit of `t` is ms         |
| ZERO_COUNT        | INT32/ INT64/ FLOAT/ DOUBLE/ BOOLEAN | `min`:Optional with default value `1L`<br>`max`:Optional with default value `Long.MAX_VALUE` | Long              | Return intervals' start times and the number of data points in the interval in which the value is always 0(false). Data points number `n` satisfy `n >= min && n <= max`     |
| NON_ZERO_COUNT    | INT32/ INT64/ FLOAT/ DOUBLE/ BOOLEAN | `min`:Optional with default value `1L`<br>`max`:Optional with default value `Long.MAX_VALUE` | Long              | Return intervals' start times and the number of data points in the interval in which the value is always not 0(false). Data points number `n` satisfy `n >= min && n <= max` |

##### Demonstrate
Example data:
```
IoTDB> select s1,s2,s3,s4,s5 from root.sg.d2;
+-----------------------------+-------------+-------------+-------------+-------------+-------------+
|                         Time|root.sg.d2.s1|root.sg.d2.s2|root.sg.d2.s3|root.sg.d2.s4|root.sg.d2.s5|
+-----------------------------+-------------+-------------+-------------+-------------+-------------+
|1970-01-01T08:00:00.000+08:00|            0|            0|          0.0|          0.0|        false|
|1970-01-01T08:00:00.001+08:00|            1|            1|          1.0|          1.0|         true|
|1970-01-01T08:00:00.002+08:00|            1|            1|          1.0|          1.0|         true|
|1970-01-01T08:00:00.003+08:00|            0|            0|          0.0|          0.0|        false|
|1970-01-01T08:00:00.004+08:00|            1|            1|          1.0|          1.0|         true|
|1970-01-01T08:00:00.005+08:00|            0|            0|          0.0|          0.0|        false|
|1970-01-01T08:00:00.006+08:00|            0|            0|          0.0|          0.0|        false|
|1970-01-01T08:00:00.007+08:00|            1|            1|          1.0|          1.0|         true|
+-----------------------------+-------------+-------------+-------------+-------------+-------------+
```

Sql:
```sql
select s1, zero_count(s1), non_zero_count(s2), zero_duration(s3), non_zero_duration(s4) from root.sg.d2;
```

Result:
```
+-----------------------------+-------------+-------------------------+-----------------------------+----------------------------+--------------------------------+
|                         Time|root.sg.d2.s1|zero_count(root.sg.d2.s1)|non_zero_count(root.sg.d2.s2)|zero_duration(root.sg.d2.s3)|non_zero_duration(root.sg.d2.s4)|
+-----------------------------+-------------+-------------------------+-----------------------------+----------------------------+--------------------------------+
|1970-01-01T08:00:00.000+08:00|            0|                        1|                         null|                           0|                            null|
|1970-01-01T08:00:00.001+08:00|            1|                     null|                            2|                        null|                               1|
|1970-01-01T08:00:00.002+08:00|            1|                     null|                         null|                        null|                            null|
|1970-01-01T08:00:00.003+08:00|            0|                        1|                         null|                           0|                            null|
|1970-01-01T08:00:00.004+08:00|            1|                     null|                            1|                        null|                               0|
|1970-01-01T08:00:00.005+08:00|            0|                        2|                         null|                           1|                            null|
|1970-01-01T08:00:00.006+08:00|            0|                     null|                         null|                        null|                            null|
|1970-01-01T08:00:00.007+08:00|            1|                     null|                            1|                        null|                               0|
+-----------------------------+-------------+-------------------------+-----------------------------+----------------------------+--------------------------------+
```