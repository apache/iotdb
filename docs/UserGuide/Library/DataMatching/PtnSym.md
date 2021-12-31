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
# PtnSym

## Usage

This function is used to find all symmetric subseries in the input whose degree of symmetry is less than the threshold. 
The degree of symmetry is calculated by DTW. 
The smaller the degree, the more symmetrical the series is.

**Name:** PATTERNSYMMETRIC

**Input Series:** Only support a single input series. The type is INT32 / INT64 / FLOAT / DOUBLE

**Parameter:**

+ `window`: The length of the symmetric subseries. It's a positive integer and the default value is 10.
+ `threshold`: The threshold of the degree of symmetry. It's non-negative. Only the subseries whose degree of symmetry is below it will be output. By default, all subseries will be output. 


**Output Series:** Output a single series. The type is DOUBLE. Each data point in the output series corresponds to a symmetric subseries. The output timestamp is the starting timestamp of the subseries and the output value is the degree of symmetry.

## Example

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s4|
+-----------------------------+---------------+
|2021-01-01T12:00:00.000+08:00|            1.0|
|2021-01-01T12:00:01.000+08:00|            2.0|
|2021-01-01T12:00:02.000+08:00|            3.0|
|2021-01-01T12:00:03.000+08:00|            2.0|
|2021-01-01T12:00:04.000+08:00|            1.0|
|2021-01-01T12:00:05.000+08:00|            1.0|
|2021-01-01T12:00:06.000+08:00|            1.0|
|2021-01-01T12:00:07.000+08:00|            1.0|
|2021-01-01T12:00:08.000+08:00|            2.0|
|2021-01-01T12:00:09.000+08:00|            3.0|
|2021-01-01T12:00:10.000+08:00|            2.0|
|2021-01-01T12:00:11.000+08:00|            1.0|
+-----------------------------+---------------+
```

SQL for query:

```sql
select ptnsym(s4, 'window'='5', 'threshold'='0') from root.test.d1
```

Output series:

```
+-----------------------------+------------------------------------------------------+
|                         Time|ptnsym(root.test.d1.s4, "window"="5", "threshold"="0")|
+-----------------------------+------------------------------------------------------+
|2021-01-01T12:00:00.000+08:00|                                                   0.0|
|2021-01-01T12:00:07.000+08:00|                                                   0.0|
+-----------------------------+------------------------------------------------------+
```
