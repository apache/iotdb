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
# Deconv

## Usage
This function is used to calculate the deconvolution, i.e. polynomial division.

**Name:** DECONV

**Input:** Only support two input series. The types are both INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:** 

+ `result`: The result of deconvolution, which is 'quotient' or 'remainder'. By default, the quotient will be output. 

**Output:** Output a single series. The type is DOUBLE. It is the result of deconvolving the second series from the first series (dividing the first series by the second series) whose timestamps starting from 0 only indicate the order.

**Note:** `NaN` in the input series will be ignored. 

## Examples


### Calculate the quotient

When `result` is 'quotient' or the default, this function calculates the quotient of the deconvolution.

Input series:

```
+-----------------------------+---------------+---------------+
|                         Time|root.test.d2.s3|root.test.d2.s2|
+-----------------------------+---------------+---------------+
|1970-01-01T08:00:00.000+08:00|            8.0|            7.0|
|1970-01-01T08:00:00.001+08:00|            2.0|            2.0|
|1970-01-01T08:00:00.002+08:00|            7.0|           null|
|1970-01-01T08:00:00.003+08:00|            2.0|           null|
+-----------------------------+---------------+---------------+
```

SQL for query:

```sql
select deconv(s3,s2) from root.test.d2
```

Output series:

```
+-----------------------------+----------------------------------------+
|                         Time|deconv(root.test.d2.s3, root.test.d2.s2)|
+-----------------------------+----------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                     1.0|
|1970-01-01T08:00:00.001+08:00|                                     0.0|
|1970-01-01T08:00:00.002+08:00|                                     1.0|
+-----------------------------+----------------------------------------+
```

### Calculate the remainder

When `result` is 'remainder', this function calculates the remainder of the deconvolution. 

Input series is the same as above, the SQL for query is shown below:


```sql
select deconv(s3,s2,'result'='remainder') from root.test.d2
```

Output series:

```
+-----------------------------+--------------------------------------------------------------+
|                         Time|deconv(root.test.d2.s3, root.test.d2.s2, "result"="remainder")|
+-----------------------------+--------------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                                           1.0|
|1970-01-01T08:00:00.001+08:00|                                                           0.0|
|1970-01-01T08:00:00.002+08:00|                                                           0.0|
|1970-01-01T08:00:00.003+08:00|                                                           0.0|
+-----------------------------+--------------------------------------------------------------+
```
