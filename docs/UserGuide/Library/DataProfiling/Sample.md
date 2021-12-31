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
# Sample
## Usage
This function is used to sample the input series, 
that is, select a specified number of data points from the input series and output them.
Currently, two sampling methods are supported:
**Reservoir sampling** randomly selects data points. 
All of the points have the same probability of being sampled. 
**Isometric sampling** selects data points at equal index intervals.


**Name:** SAMPLE

**Input Series:** Only support a single input series. The type is arbitrary.

**Parameters:**

+ `method`: The method of sampling, which is 'reservoir' or 'isometric'. By default, reservoir sampling is used.
+ `k`: The number of sampling, which is a positive integer. By default, it's 1.

**Output Series:** Output a single series. The type is the same as the input. The length of the output series is `k`. Each data point in the output series comes from the input series.

**Note:** If `k` is greater than the length of input series, all data points in the input series will be output.

## Examples
### Reservoir Sampling

When `method` is 'reservoir' or the default, reservoir sampling is used. 
Due to the randomness of this method, the output series shown below is only a possible result.


Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2020-01-01T00:00:01.000+08:00|            1.0|
|2020-01-01T00:00:02.000+08:00|            2.0|
|2020-01-01T00:00:03.000+08:00|            3.0|
|2020-01-01T00:00:04.000+08:00|            4.0|
|2020-01-01T00:00:05.000+08:00|            5.0|
|2020-01-01T00:00:06.000+08:00|            6.0|
|2020-01-01T00:00:07.000+08:00|            7.0|
|2020-01-01T00:00:08.000+08:00|            8.0|
|2020-01-01T00:00:09.000+08:00|            9.0|
|2020-01-01T00:00:10.000+08:00|           10.0|
+-----------------------------+---------------+
```

SQL for query:

```sql
select sample(s1,'method'='reservoir','k'='5') from root.test.d1
```

Output series:

```
+-----------------------------+------------------------------------------------------+
|                         Time|sample(root.test.d1.s1, "method"="reservoir", "k"="5")|
+-----------------------------+------------------------------------------------------+
|2020-01-01T00:00:02.000+08:00|                                                   2.0|
|2020-01-01T00:00:03.000+08:00|                                                   3.0|
|2020-01-01T00:00:05.000+08:00|                                                   5.0|
|2020-01-01T00:00:08.000+08:00|                                                   8.0|
|2020-01-01T00:00:10.000+08:00|                                                  10.0|
+-----------------------------+------------------------------------------------------+
```



### Isometric Sampling

When `method` is 'isometric', isometric sampling is used.

Input series is the same as above, the SQL for query is shown below: 

```sql
select sample(s1,'method'='isometric','k'='5') from root.test.d1
```

Output series:

```
+-----------------------------+------------------------------------------------------+
|                         Time|sample(root.test.d1.s1, "method"="isometric", "k"="5")|
+-----------------------------+------------------------------------------------------+
|2020-01-01T00:00:01.000+08:00|                                                   1.0|
|2020-01-01T00:00:03.000+08:00|                                                   3.0|
|2020-01-01T00:00:05.000+08:00|                                                   5.0|
|2020-01-01T00:00:07.000+08:00|                                                   7.0|
|2020-01-01T00:00:09.000+08:00|                                                   9.0|
+-----------------------------+------------------------------------------------------+
```

