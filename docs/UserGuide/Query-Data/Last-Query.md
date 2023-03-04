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

# Last Query

The last query is a special query provided in the time series database Apache IoTDB. The last query returns the data point with the largest timestamp in the time series, that is, the latest state of a sequence. Users can specify the last query through `select last`. It is especially important in IoT data analysis scenarios as the latest point data characterizes the current state. In order to provide a millisecond-level return speed, Apache IoTDB optimizes the cache for the last query to meet users' performance requirements for real-time monitoring of devices.

The last query is to return the most recent data point of the given timeseries in a three column format.

The SQL statement is defined as:

```sql
select last <Path> [COMMA <Path>]* from < PrefixPath > [COMMA < PrefixPath >]* <WhereClause>
```

which means: Query and return the last data points of timeseries prefixPath.path.

Only time filter with '>' or '>=' is supported in \<WhereClause\>. Any other filters given in the \<WhereClause\> will give an exception.

The result will be returned in a four column table format.

```
| Time | timeseries | value | dataType |
```

**Note:** The `value` colum will always return the value as `string` and thus also has `TSDataType.TEXT`. Therefore the colum `dataType` is returned also which contains the _real_ type how the value should be interpreted.

**Example 1:** get the last point of root.ln.wf01.wt01.status:

```
IoTDB> select last status from root.ln.wf01.wt01
+-----------------------------+------------------------+-----+--------+
|                         Time|              timeseries|value|dataType|
+-----------------------------+------------------------+-----+--------+
|2017-11-07T23:59:00.000+08:00|root.ln.wf01.wt01.status|false| BOOLEAN|
+-----------------------------+------------------------+-----+--------+
Total line number = 1
It costs 0.000s
```

**Example 2:** get the last status and temperature points of root.ln.wf01.wt01, whose timestamp larger or equal to 2017-11-07T23:50:00。

```
IoTDB> select last status, temperature from root.ln.wf01.wt01 where time >= 2017-11-07T23:50:00
+-----------------------------+-----------------------------+---------+--------+
|                         Time|                   timeseries|    value|dataType|
+-----------------------------+-----------------------------+---------+--------+
|2017-11-07T23:59:00.000+08:00|     root.ln.wf01.wt01.status|    false| BOOLEAN|
|2017-11-07T23:59:00.000+08:00|root.ln.wf01.wt01.temperature|21.067368|  DOUBLE|
+-----------------------------+-----------------------------+---------+--------+
Total line number = 2
It costs 0.002s
```