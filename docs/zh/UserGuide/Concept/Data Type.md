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

# 数据类型

IoTDB支持:
* BOOLEAN（布尔值）
* INT32（整型）
* INT64（长整型）
* FLOAT（单精度浮点数）
* DOUBLE（双精度浮点数）
* TEXT（字符串）

一共六种数据类型。

其中**FLOAT**与**DOUBLE**类型的序列，如果编码方式采用[RLE](../Concept/Encoding.md)或[TS_2DIFF](../Concept/Encoding.md)可以指定MAX_POINT_NUMBER，该项为浮点数的小数点后位数，具体指定方式请参见本文[第5.4节](../Operation%20Manual/SQL%20Reference.md)，若不指定则系统会根据配置文件`iotdb-engine.properties`文件中的[float_precision项](../Server/Config%20Manual.md)配置。

当系统中用户输入的数据类型与该时间序列的数据类型不对应时，系统会提醒类型错误，如下所示，二阶差分编码不支持布尔类型：

```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
error: encoding TS_2DIFF does not support BOOLEAN
```
