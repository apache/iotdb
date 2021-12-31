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
# Distinct

## Usage
This function returns all unique values in time series.

**Name:** DISTINCT

**Input Series:** Only support a single input series. The type is arbitrary.

**Output Series:** Output a single series. The type is the same as the input. 

**Note:** 

+ The timestamp of the output series is meaningless. The output order is arbitrary.
+ Missing points and null points in the input series will be ignored, but `NaN` will not.
+ Case Sensitive.


## Examples

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d2.s2|
+-----------------------------+---------------+
|2020-01-01T08:00:00.001+08:00|          Hello|
|2020-01-01T08:00:00.002+08:00|          hello|
|2020-01-01T08:00:00.003+08:00|          Hello|
|2020-01-01T08:00:00.004+08:00|          World|
|2020-01-01T08:00:00.005+08:00|          World|
+-----------------------------+---------------+
```

SQL for query:

```sql
select distinct(s2) from root.test.d2
```

Output series:

```
+-----------------------------+-------------------------+
|                         Time|distinct(root.test.d2.s2)|
+-----------------------------+-------------------------+
|1970-01-01T08:00:00.001+08:00|                    Hello|
|1970-01-01T08:00:00.002+08:00|                    hello|
|1970-01-01T08:00:00.003+08:00|                    World|
+-----------------------------+-------------------------+
```
