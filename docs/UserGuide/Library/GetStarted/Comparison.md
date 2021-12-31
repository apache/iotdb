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
# Comparison

## InfluxDB v2.0

[InfluxDB](https://www.influxdata.com/products/influxdb/) is a popular time series database.
InfluxQL is its query language, some of whose universal functions are related to data profiling.
The comparison is shown below. *Native* means this function has been the native function of IoTDB and *Built-in UDF* means this function has been the built-in UDF of IoTDB. 

 

| Data profiling functions of IoTDB-Quality | Univeral functions from InfluxQL |
| :---------------------------------------: | :------------------------------: |
|                 *Native*                  |             COUNT()              |
|               **Distinct**                |            DISTINCT()            |
|               **Integral**                |            INTEGRAL()            |
|                 *Native*                  |              MEAN()              |
|                **Median**                 |             MEDIAN()             |
|                 **Mode**                  |              MODE()              |
|                **Spread**                 |             SPREAD()             |
|                **Stddev**                 |             STDDEV()             |
|                 *Native*                  |              SUM()               |
|              *Built-in UDF*               |             BOTTOM()             |
|                 *Native*                  |             FIRST()              |
|                 *Native*                  |              LAST()              |
|                 *Native*                  |              MAX()               |
|                 *Native*                  |              MIN()               |
|              **Percentile**               |           PERCENTILE()           |
|                **Sample**                 |             SAMPLE()             |
|              *Built-in UDF*               |              TOP()               |
|               **Histogram**               |           HISTOGRAM()            |
|                  **Mad**                  |                                  |
|                 **Skew**                  |              SKEW()              |
|            **TimeWeightedAVG**            |        TIMEWEIGHTEDAVG()         |
|            **SelfCorrelation**            |                                  |
|           **CrossCorrelation**            |                                  |

Kapacitor offers UDF to realize user-defined anomaly detection. Python scripts can be applied to Kapacitor, and no native function for anomaly detection is offered in [InfluxDB](https://www.influxdata.com/products/influxdb/).
