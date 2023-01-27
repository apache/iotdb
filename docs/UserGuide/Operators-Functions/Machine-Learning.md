<!--

​    Licensed to the Apache Software Foundation (ASF) under one
​    or more contributor license agreements.  See the NOTICE file
​    distributed with this work for additional information
​    regarding copyright ownership.  The ASF licenses this file
​    to you under the Apache License, Version 2.0 (the
​    "License"); you may not use this file except in compliance
​    with the License.  You may obtain a copy of the License at
​    
​        http://www.apache.org/licenses/LICENSE-2.0
​    
​    Unless required by applicable law or agreed to in writing,
​    software distributed under the License is distributed on an
​    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
​    KIND, either express or implied.  See the License for the
​    specific language governing permissions and limitations
​    under the License.

-->

# Machine Learning

## AR

### Usage

This function is used to learn the coefficients of the autoregressive models for a time series.

**Name:** AR

**Input Series:** Only support a single input numeric series. The type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:** 

- `p`: The order of the autoregressive model. Its default value is 1.

**Output Series:** Output a single series. The type is DOUBLE. The first line corresponds to the first order coefficient, and so on. 

**Note:** 

- Parameter `p` should be a positive integer. 
- Most points in the series should be sampled at a constant time interval. 
- Linear interpolation is applied for the missing points in the series.

### Examples

#### Assigning Model Order

Input Series: 

```
+-----------------------------+---------------+
|                         Time|root.test.d0.s0|
+-----------------------------+---------------+
|2020-01-01T00:00:01.000+08:00|           -4.0|
|2020-01-01T00:00:02.000+08:00|           -3.0|
|2020-01-01T00:00:03.000+08:00|           -2.0|
|2020-01-01T00:00:04.000+08:00|           -1.0|
|2020-01-01T00:00:05.000+08:00|            0.0|
|2020-01-01T00:00:06.000+08:00|            1.0|
|2020-01-01T00:00:07.000+08:00|            2.0|
|2020-01-01T00:00:08.000+08:00|            3.0|
|2020-01-01T00:00:09.000+08:00|            4.0|
+-----------------------------+---------------+
```

SQL for query: 

```sql
select ar(s0,"p"="2") from root.test.d0
```

Output Series:

```
+-----------------------------+---------------------------+
|                         Time|ar(root.test.d0.s0,"p"="2")|
+-----------------------------+---------------------------+
|1970-01-01T08:00:00.001+08:00|                     0.9429|
|1970-01-01T08:00:00.002+08:00|                    -0.2571|
+-----------------------------+---------------------------+
```

