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

The last query is a special type of query in Apache IoTDB. It returns the data point with the largest timestamp of the specified time series. In other word, it returns the latest state of a time series. This feature is especially important in IoT data analysis scenarios. To meet the performance requirement of real-time device monitoring systems, Apache IoTDB caches the latest values of all time series to achieve microsecond read latency.

The last query is to return the most recent data point of the given timeseries in a three column format.

The SQL syntax is defined as:

```sql
select last <Path> [COMMA <Path>]* from < PrefixPath > [COMMA < PrefixPath >]* <WhereClause> [ORDER BY TIMESERIES (DESC | ASC)?]
```

which means: Query and return the last data points of timeseries prefixPath.path.

- Only time filter is supported in \<WhereClause\>. Any other filters given in the \<WhereClause\> will give an exception. When the cached most recent data point does not satisfy the criterion specified by the filter, IoTDB will have to get the result from the external storage, which may cause a decrease in performance.

- The result will be returned in a four column table format.

    ```
    | Time | timeseries | value | dataType |
    ```

    **Note:** The `value` colum will always return the value as `string` and thus also has `TSDataType.TEXT`. Therefore, the column `dataType` is returned also which contains the _real_ type how the value should be interpreted.

- We can use `TIME/TIMESERIES/VALUE/DATATYPE (DESC | ASC)` to specify that the result set is sorted in descending/ascending order based on a particular column. When the value column contains multiple types of data, the sorting is based on the string representation of the values.

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

**Example 3:** get the last points of all sensor in root.ln.wf01.wt01, and order the result by the timeseries column in descending order

```
IoTDB> select last * from root.ln.wf01.wt01 order by timeseries desc;
+-----------------------------+-----------------------------+---------+--------+
|                         Time|                   timeseries|    value|dataType|
+-----------------------------+-----------------------------+---------+--------+
|2017-11-07T23:59:00.000+08:00|root.ln.wf01.wt01.temperature|21.067368|  DOUBLE|
|2017-11-07T23:59:00.000+08:00|     root.ln.wf01.wt01.status|    false| BOOLEAN|
+-----------------------------+-----------------------------+---------+--------+
Total line number = 2
It costs 0.002s
```

**Example 4：** get the last points of all sensor in root.ln.wf01.wt01, and order the result by the dataType column in descending order

```
IoTDB> select last * from root.ln.wf01.wt01 order by dataType desc;
+-----------------------------+-----------------------------+---------+--------+
|                         Time|                   timeseries|    value|dataType|
+-----------------------------+-----------------------------+---------+--------+
|2017-11-07T23:59:00.000+08:00|root.ln.wf01.wt01.temperature|21.067368|  DOUBLE|
|2017-11-07T23:59:00.000+08:00|     root.ln.wf01.wt01.status|    false| BOOLEAN|
+-----------------------------+-----------------------------+---------+--------+
Total line number = 2
It costs 0.002s
```
