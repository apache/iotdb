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

# SDT

The Swinging Door Trending (SDT) algorithm is a lossy compression algorithm. 
SDT has low computational complexity and uses a linear trend to represent a quantity of data. 

IoTDB allows you to specify the properties of SDT when creating a time series, and supports three properties:

* CompDev (Compression Deviation)

CompDev is the most important parameter in SDT that represents the maximum difference between the current sample and the current linear trend.

* CompMin (Compression Minimum)

CompMin is used for noisy reduction. CompMin measures the time distance between two stored data points, 
if the current point's time to the last stored point's time distance is smaller than or equal to compMin, 
current point will NOT be stored regardless of compression deviation.

* CompMax (Compression Maximum)

CompMax is used to periodically check the time distance between the last stored point to the current point. 
It measures the time difference between stored points. If the current point time to the last 
stored point's time distance is greater than or equal to compMax, current point will be stored regardless of compression deviation.

The specified syntax for SDT is detailed in [Create Timeseries Statement](../Operation%20Manual/SQL%20Reference.md).
