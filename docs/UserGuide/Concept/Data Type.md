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

# Data Type

IoTDB supports six data types in total:
* BOOLEAN (Boolean)
* INT32 (Integer)
* INT64 (Long Integer)
* FLOAT (Single Precision Floating Point)
* DOUBLE (Double Precision Floating Point)
* TEXT (String).


The time series of **FLOAT** and **DOUBLE** type can specify (MAX\_POINT\_NUMBER, see [this page](../Operation%20Manual/SQL%20Reference.md) for more information on how to specify), which is the number of digits after the decimal point of the floating point number, if the encoding method is [RLE](../Concept/Encoding.md) or [TS\_2DIFF](../Concept/Encoding.md) (Refer to [Create Timeseries Statement](../Operation%20Manual/SQL%20Reference.md) for more information on how to specify). If MAX\_POINT\_NUMBER is not specified, the system will use [float\_precision](../Server/Config%20Manual.md) in the configuration file `iotdb-engine.properties`.

* For Float data value, The data range is (-Integer.MAX_VALUE, Integer.MAX_VALUE), rather than Float.MAX_VALUE, and the max_point_number is 19, it is because of the limition of function Math.round(float) in Java.
* For Double data value, The data range is (-Long.MAX_VALUE, Long.MAX_VALUE), rather than Double.MAX_VALUE, and the max_point_number is 19, it is because of the limition of function Math.round(double) in Java (Long.MAX_VALUE=9.22E18).

When the data type of data input by the user in the system does not correspond to the data type of the time series, the system will report type errors. As shown below, the second-order difference encoding does not support the Boolean type:

```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
error: encoding TS_2DIFF does not support BOOLEAN
```
