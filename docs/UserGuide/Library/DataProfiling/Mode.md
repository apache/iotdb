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
# Mode

## Usage
This function is used to calculate the mode of time series, that is, the value that occurs most frequently.

**Name:** MODE

**Input Series:** Only support a single input series. The type is arbitrary.

**Output Series:** Output a single series. The type is the same as the input. There is only one data point in the series, whose timestamp is 0 and value is the mode.

**Note:** 
+ If there are multiple values with the most occurrences, the arbitrary one will be output.
+ Missing points and null points in the input series will be ignored, but `NaN` will not.

## Examples

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d2.s2|
+-----------------------------+---------------+
|1970-01-01T08:00:00.001+08:00|          Hello|
|1970-01-01T08:00:00.002+08:00|          hello|
|1970-01-01T08:00:00.003+08:00|          Hello|
|1970-01-01T08:00:00.004+08:00|          World|
|1970-01-01T08:00:00.005+08:00|          World|
|1970-01-01T08:00:01.600+08:00|          World|
|1970-01-15T09:37:34.451+08:00|          Hello|
|1970-01-15T09:37:34.452+08:00|          hello|
|1970-01-15T09:37:34.453+08:00|          Hello|
|1970-01-15T09:37:34.454+08:00|          World|
|1970-01-15T09:37:34.455+08:00|          World|
+-----------------------------+---------------+
```

SQL for query:

```sql
select mode(s2) from root.test.d2
```

Output series:

```
+-----------------------------+---------------------+
|                         Time|mode(root.test.d2.s2)|
+-----------------------------+---------------------+
|1970-01-01T08:00:00.000+08:00|                World|
+-----------------------------+---------------------+
```