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

# Deadband Process

## SDT

The Swinging Door Trending (SDT) algorithm is a deadband process algorithm.
SDT has low computational complexity and uses a linear trend to represent a quantity of data.

In IoTDB SDT compresses and discards data when flushing into the disk.

IoTDB allows you to specify the properties of SDT when creating a time series, and supports three properties:

* CompDev (Compression Deviation)

CompDev is the most important parameter in SDT that represents the maximum difference between the
current sample and the current linear trend. CompDev needs to be greater than 0 to perform compression.

* CompMinTime (Compression Minimum Time Interval)

CompMinTime is a parameter measures the time distance between two stored data points, which is used for noisy reduction.
If the time interval between the current point and the last stored point is less than or equal to its value,
current point will NOT be stored regardless of compression deviation.
The default value is 0 with time unit ms.

* CompMaxTime (Compression Maximum Time Interval)

CompMaxTime is a parameter measure the time distance between two stored data points.
If the time interval between the current point and the last stored point is greater than or equal to its value,
current point will be stored regardless of compression deviation.
The default value is 9,223,372,036,854,775,807 with time unit ms.

The specified syntax for SDT is detailed in [Create Timeseries Statement](../Reference/SQL-Reference.md).

Supported datatypes:

* INT32 (Integer)
* INT64 (Long Integer)
* FLOAT (Single Precision Floating Point)
* DOUBLE (Double Precision Floating Point)

The following is an example of using SDT compression.

```
IoTDB> CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=2
```

Prior to flushing and SDT compression, the results are shown below:

```
IoTDB> SELECT s0 FROM root.sg1.d0
+-----------------------------+--------------+
|                         Time|root.sg1.d0.s0|
+-----------------------------+--------------+
|2017-11-01T00:06:00.001+08:00|             1|
|2017-11-01T00:06:00.002+08:00|             1|
|2017-11-01T00:06:00.003+08:00|             1|
|2017-11-01T00:06:00.004+08:00|             1|
|2017-11-01T00:06:00.005+08:00|             1|
|2017-11-01T00:06:00.006+08:00|             1|
|2017-11-01T00:06:00.007+08:00|             1|
|2017-11-01T00:06:00.015+08:00|            10|
|2017-11-01T00:06:00.016+08:00|            20|
|2017-11-01T00:06:00.017+08:00|             1|
|2017-11-01T00:06:00.018+08:00|            30|
+-----------------------------+--------------+
Total line number = 11
It costs 0.008s
```

After flushing and SDT compression, the results are shown below:

```
IoTDB> FLUSH
IoTDB> SELECT s0 FROM root.sg1.d0
+-----------------------------+--------------+
|                         Time|root.sg1.d0.s0|
+-----------------------------+--------------+
|2017-11-01T00:06:00.001+08:00|             1|
|2017-11-01T00:06:00.007+08:00|             1|
|2017-11-01T00:06:00.015+08:00|            10|
|2017-11-01T00:06:00.016+08:00|            20|
|2017-11-01T00:06:00.017+08:00|             1|
+-----------------------------+--------------+
Total line number = 5
It costs 0.044s
```

SDT takes effect when flushing to the disk. The SDT algorithm always stores the first point and does not store the last point.

The data in [2017-11-01T00:06:00.001, 2017-11-01T00:06:00.007] is within the compression deviation thus discarded.
The data point at time 2017-11-01T00:06:00.007 is stored because the next data point at time 2017-11-01T00:06:00.015
exceeds compression deviation. When a data point exceeds the compression deviation, SDT stores the last read
point and updates the upper and lower boundaries. The last point at time 2017-11-01T00:06:00.018 is not stored.
