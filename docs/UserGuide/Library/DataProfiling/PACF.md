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
# PACF

## Usage

This function is used to calculate partial autocorrelation of input series by solving Yule-Walker equation. For some cases, the equation may not be solved, and NaN will be output.

**Name:** PACF

**Input Series:** Only support a single input series. The type is INT32 / INT64 / FLOAT / DOUBLE.

+ `lag`: Maximum lag of pacf to calculate. The default value is $\min(10\log_{10}n,n-1)$, where $n$ is the number of data points.

**Output Series:** Output a single series. The type is DOUBLE.

## Examples

### Assigning maximum lag

Input series:

```
+-----------------------------+------------+
|                         Time|root.test.s1|
+-----------------------------+------------+
|2019-12-27T00:00:00.000+08:00|         5.0|
|2019-12-27T00:05:00.000+08:00|         5.0|
|2019-12-27T00:10:00.000+08:00|         5.0|
|2019-12-27T00:15:00.000+08:00|         5.0|
|2019-12-27T00:20:00.000+08:00|         6.0|
|2019-12-27T00:25:00.000+08:00|         5.0|
|2019-12-27T00:30:00.000+08:00|         6.0|
|2019-12-27T00:35:00.000+08:00|         6.0|
|2019-12-27T00:40:00.000+08:00|         6.0|
|2019-12-27T00:45:00.000+08:00|         6.0|
|2019-12-27T00:50:00.000+08:00|         6.0|
|2019-12-27T00:55:00.000+08:00|    5.982609|
|2019-12-27T01:00:00.000+08:00|   5.9652176|
|2019-12-27T01:05:00.000+08:00|    5.947826|
|2019-12-27T01:10:00.000+08:00|   5.9304347|
|2019-12-27T01:15:00.000+08:00|   5.9130435|
|2019-12-27T01:20:00.000+08:00|   5.8956523|
|2019-12-27T01:25:00.000+08:00|    5.878261|
|2019-12-27T01:30:00.000+08:00|   5.8608694|
|2019-12-27T01:35:00.000+08:00|    5.843478|
............
Total line number = 18066
```

SQL for query: 

```sql
select pacf(s1, "lag"="5") from root.test
```

Output series:

```
+-----------------------------+-----------------------------+
|                         Time|pacf(root.test.s1, "lag"="5")|
+-----------------------------+-----------------------------+
|2019-12-27T00:00:00.000+08:00|                          1.0|
|2019-12-27T00:05:00.000+08:00|           0.3528915091942786|
|2019-12-27T00:10:00.000+08:00|           0.1761346122516304|
|2019-12-27T00:15:00.000+08:00|           0.1492391973294682|
|2019-12-27T00:20:00.000+08:00|          0.03560059645868398|
|2019-12-27T00:25:00.000+08:00|           0.0366222998995286|
+-----------------------------+-----------------------------+
```

