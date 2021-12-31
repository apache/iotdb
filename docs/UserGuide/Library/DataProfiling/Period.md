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
# Period

## Usage

The function is used to compute the period of a numeric time series.

**Name:** PERIOD

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is INT32. There is only one data point in the series, whose timestamp is 0 and value is the period.

## Examples

Input series:


```
+-----------------------------+---------------+
|                         Time|root.test.d3.s1|
+-----------------------------+---------------+
|1970-01-01T08:00:00.001+08:00|            1.0|
|1970-01-01T08:00:00.002+08:00|            2.0|
|1970-01-01T08:00:00.003+08:00|            3.0|
|1970-01-01T08:00:00.004+08:00|            1.0|
|1970-01-01T08:00:00.005+08:00|            2.0|
|1970-01-01T08:00:00.006+08:00|            3.0|
|1970-01-01T08:00:00.007+08:00|            1.0|
|1970-01-01T08:00:00.008+08:00|            2.0|
|1970-01-01T08:00:00.009+08:00|            3.0|
+-----------------------------+---------------+
```

SQL for query:

```sql
select period(s1) from root.test.d3
```

Output series:

```
+-----------------------------+-----------------------+
|                         Time|period(root.test.d3.s1)|
+-----------------------------+-----------------------+
|1970-01-01T08:00:00.000+08:00|                      3|
+-----------------------------+-----------------------+
```
### examples on zeppelin
link: <http://101.6.15.213:18181/#/notebook/2GEJBUSZ9>
