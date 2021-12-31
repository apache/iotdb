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
# DWT

## Usage

This function is used to calculate 1d discrete wavelet transform of a numerical series.

**Name:** DWT

**Input:** Only support a single input series. The type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:** 

+ `method`: The type of wavelet. May select 'Haar', 'DB4', 'DB6', 'DB8', where DB means Daubechies. User may offer coefficients of wavelet transform and ignore this parameter. Case ignored.
+ `coef`: Coefficients of wavelet transform. When providing this parameter, use comma ',' to split them, and leave no spaces or other punctuations.
+ `layer`: Times to transform. The number of output vectors equals $layer+1$. Default is 1.

**Output:** Output a single series. The type is DOUBLE. The length is the same as the input. 

**Note:** The length of input series must be an integer number power of 2.

## Examples


### Haar wavelet transform

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|1970-01-01T08:00:00.000+08:00|            0.0|
|1970-01-01T08:00:00.100+08:00|            0.2|
|1970-01-01T08:00:00.200+08:00|            1.5|
|1970-01-01T08:00:00.300+08:00|            1.2|
|1970-01-01T08:00:00.400+08:00|            0.6|
|1970-01-01T08:00:00.500+08:00|            1.7|
|1970-01-01T08:00:00.600+08:00|            0.8|
|1970-01-01T08:00:00.700+08:00|            2.0|
|1970-01-01T08:00:00.800+08:00|            2.5|
|1970-01-01T08:00:00.900+08:00|            2.1|
|1970-01-01T08:00:01.000+08:00|            0.0|
|1970-01-01T08:00:01.100+08:00|            2.0|
|1970-01-01T08:00:01.200+08:00|            1.8|
|1970-01-01T08:00:01.300+08:00|            1.2|
|1970-01-01T08:00:01.400+08:00|            1.0|
|1970-01-01T08:00:01.500+08:00|            1.6|
+-----------------------------+---------------+
```

SQL for query:

```sql
select dwt(s1,"method"="haar") from root.test.d1
```

Output series:

```
+-----------------------------+-------------------------------------+
|                         Time|dwt(root.test.d1.s1, "method"="haar")|
+-----------------------------+-------------------------------------+
|1970-01-01T08:00:00.000+08:00|                  0.14142135834465192|
|1970-01-01T08:00:00.100+08:00|                    1.909188342921157|
|1970-01-01T08:00:00.200+08:00|                   1.6263456473052773|
|1970-01-01T08:00:00.300+08:00|                   1.9798989957517026|
|1970-01-01T08:00:00.400+08:00|                    3.252691126023161|
|1970-01-01T08:00:00.500+08:00|                    1.414213562373095|
|1970-01-01T08:00:00.600+08:00|                   2.1213203435596424|
|1970-01-01T08:00:00.700+08:00|                   1.8384776479437628|
|1970-01-01T08:00:00.800+08:00|                 -0.14142135834465192|
|1970-01-01T08:00:00.900+08:00|                  0.21213200063848547|
|1970-01-01T08:00:01.000+08:00|                  -0.7778174761639416|
|1970-01-01T08:00:01.100+08:00|                  -0.8485281289944873|
|1970-01-01T08:00:01.200+08:00|                   0.2828427799095765|
|1970-01-01T08:00:01.300+08:00|                   -1.414213562373095|
|1970-01-01T08:00:01.400+08:00|                  0.42426400127697095|
|1970-01-01T08:00:01.500+08:00|                 -0.42426408557066786|
+-----------------------------+-------------------------------------+
```

