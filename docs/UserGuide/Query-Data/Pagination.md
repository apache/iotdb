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

# Pagination with LIMIT & OFFSET

When the query result set has a large amount of data, it is not conducive to display on one page. You can use the `LIMIT/SLIMIT` clause and the `OFFSET/SOFFSET` clause to control paging.

- The `LIMIT` and `SLIMIT` clauses are used to control the number of rows and columns of query results.
- The `OFFSET` and `SOFFSET` clauses are used to control the starting position of the result display.

## Row Control over Query Results

By using LIMIT and OFFSET clauses, users control the query results in a row-related manner. We demonstrate how to use LIMIT and OFFSET clauses through the following examples.

* Example 1: basic LIMIT clause

The SQL statement is:

```sql
select status, temperature from root.ln.wf01.wt01 limit 10
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires the first 10 rows of the query result.

The result is shown below:

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:00:00.000+08:00|                    true|                        25.96|
|2017-11-01T00:01:00.000+08:00|                    true|                        24.36|
|2017-11-01T00:02:00.000+08:00|                   false|                        20.09|
|2017-11-01T00:03:00.000+08:00|                   false|                        20.18|
|2017-11-01T00:04:00.000+08:00|                   false|                        21.13|
|2017-11-01T00:05:00.000+08:00|                   false|                        22.72|
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
|2017-11-01T00:08:00.000+08:00|                   false|                        22.58|
|2017-11-01T00:09:00.000+08:00|                   false|                        20.98|
+-----------------------------+------------------------+-----------------------------+
Total line number = 10
It costs 0.000s
```

* Example 2: LIMIT clause with OFFSET

The SQL statement is:

```sql
select status, temperature from root.ln.wf01.wt01 limit 5 offset 3
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires rows 3 to 7 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:03:00.000+08:00|                   false|                        20.18|
|2017-11-01T00:04:00.000+08:00|                   false|                        21.13|
|2017-11-01T00:05:00.000+08:00|                   false|                        22.72|
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
+-----------------------------+------------------------+-----------------------------+
Total line number = 5
It costs 0.342s
```

* Example 3: LIMIT clause combined with WHERE clause

The SQL statement is:

```sql
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time< 2017-11-01T00:12:00.000 limit 2 offset 3
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires rows 3 to 4 of  the status and temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" (with the first row numbered as row 0).

The result is shown below:

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:03:00.000+08:00|                   false|                        20.18|
|2017-11-01T00:04:00.000+08:00|                   false|                        21.13|
|2017-11-01T00:05:00.000+08:00|                   false|                        22.72|
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
+-----------------------------+------------------------+-----------------------------+
Total line number = 5
It costs 0.000s
```

* Example 4: LIMIT clause combined with GROUP BY clause

The SQL statement is:

```sql
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d) limit 5 offset 3
```
which means:

The SQL statement clause requires rows 3 to 7 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

```
+-----------------------------+-------------------------------+----------------------------------------+
|                         Time|count(root.ln.wf01.wt01.status)|max_value(root.ln.wf01.wt01.temperature)|
+-----------------------------+-------------------------------+----------------------------------------+
|2017-11-04T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-05T00:00:00.000+08:00|                           1440|                                    26.0|
|2017-11-06T00:00:00.000+08:00|                           1440|                                   25.99|
|2017-11-07T00:00:00.000+08:00|                           1380|                                    26.0|
+-----------------------------+-------------------------------+----------------------------------------+
Total line number = 4
It costs 0.016s
```

It is worth noting that because the current FILL clause can only fill in the missing value of timeseries at a certain time point, that is to say, the execution result of FILL clause is exactly one line, so LIMIT and OFFSET are not expected to be used in combination with FILL clause, otherwise errors will be prompted. For example, executing the following SQL statement:

```sql
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(previous, 1m) limit 10
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 401: line 1:107 mismatched input 'limit' expecting {<EOF>, ';'}
```

## Column Control over Query Results

By using SLIMIT and SOFFSET clauses, users can control the query results in a column-related manner. We will demonstrate how to use SLIMIT and SOFFSET clauses through the following examples.

* Example 1: basic SLIMIT clause

The SQL statement is:

```sql
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the first column under this device, i.e., the power supply status. The SQL statement requires the status sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The result is shown below:

```
+-----------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+
|2017-11-01T00:06:00.000+08:00|                        20.71|
|2017-11-01T00:07:00.000+08:00|                        21.45|
|2017-11-01T00:08:00.000+08:00|                        22.58|
|2017-11-01T00:09:00.000+08:00|                        20.98|
|2017-11-01T00:10:00.000+08:00|                        25.52|
|2017-11-01T00:11:00.000+08:00|                        22.91|
+-----------------------------+-----------------------------+
Total line number = 6
It costs 0.000s
```

* Example 2: SLIMIT clause with SOFFSET

The SQL statement is:

```sql
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the second column under this device, i.e., the temperature. The SQL statement requires the temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The result is shown below:

```
+-----------------------------+------------------------+
|                         Time|root.ln.wf01.wt01.status|
+-----------------------------+------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|
|2017-11-01T00:07:00.000+08:00|                   false|
|2017-11-01T00:08:00.000+08:00|                   false|
|2017-11-01T00:09:00.000+08:00|                   false|
|2017-11-01T00:10:00.000+08:00|                    true|
|2017-11-01T00:11:00.000+08:00|                   false|
+-----------------------------+------------------------+
Total line number = 6
It costs 0.003s
```

* Example 3: SLIMIT clause combined with GROUP BY clause

The SQL statement is:

```sql
select max_value(*) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d) slimit 1 soffset 1
```

The result is shown below:

```
+-----------------------------+-----------------------------------+
|                         Time|max_value(root.ln.wf01.wt01.status)|
+-----------------------------+-----------------------------------+
|2017-11-01T00:00:00.000+08:00|                               true|
|2017-11-02T00:00:00.000+08:00|                               true|
|2017-11-03T00:00:00.000+08:00|                               true|
|2017-11-04T00:00:00.000+08:00|                               true|
|2017-11-05T00:00:00.000+08:00|                               true|
|2017-11-06T00:00:00.000+08:00|                               true|
|2017-11-07T00:00:00.000+08:00|                               true|
+-----------------------------+-----------------------------------+
Total line number = 7
It costs 0.000s
```

* Example 4: SLIMIT clause combined with FILL clause

The SQL statement is:

```sql
select * from root.sgcc.wf03.wt01 where time = 2017-11-01T16:35:00 fill(previous, 1m) slimit 1 soffset 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the second column under this device, i.e., the temperature.

The result is shown below:

```
+-----------------------------+--------------------------+
|                         Time|root.sgcc.wf03.wt01.status|
+-----------------------------+--------------------------+
|2017-11-01T16:35:00.000+08:00|                      true|
+-----------------------------+--------------------------+
Total line number = 1
It costs 0.007s
```

## Row and Column Control over Query Results

In addition to row or column control over query results, IoTDB allows users to control both rows and columns of query results. Here is a complete example with both LIMIT clauses and SLIMIT clauses.

The SQL statement is:

```sql
select * from root.ln.wf01.wt01 limit 10 offset 100 slimit 2 soffset 0
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is columns 0 to 1 under this device (with the first column numbered as column 0). The SQL statement clause requires rows 100 to 109 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

```
+-----------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+-----------------------------+-----------------------------+------------------------+
|2017-11-01T01:40:00.000+08:00|                        21.19|                   false|
|2017-11-01T01:41:00.000+08:00|                        22.79|                   false|
|2017-11-01T01:42:00.000+08:00|                        22.98|                   false|
|2017-11-01T01:43:00.000+08:00|                        21.52|                   false|
|2017-11-01T01:44:00.000+08:00|                        23.45|                    true|
|2017-11-01T01:45:00.000+08:00|                        24.06|                    true|
|2017-11-01T01:46:00.000+08:00|                         22.6|                   false|
|2017-11-01T01:47:00.000+08:00|                        23.78|                    true|
|2017-11-01T01:48:00.000+08:00|                        24.72|                    true|
|2017-11-01T01:49:00.000+08:00|                        24.68|                    true|
+-----------------------------+-----------------------------+------------------------+
Total line number = 10
It costs 0.009s
```

## Error Handling

If the parameter N/SN of LIMIT/SLIMIT exceeds the size of the result set, IoTDB returns all the results as expected. For example, the query result of the original SQL statement consists of six rows, and we select the first 100 rows through the LIMIT clause:

```sql
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 100
```

The result is shown below:

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
|2017-11-01T00:08:00.000+08:00|                   false|                        22.58|
|2017-11-01T00:09:00.000+08:00|                   false|                        20.98|
|2017-11-01T00:10:00.000+08:00|                    true|                        25.52|
|2017-11-01T00:11:00.000+08:00|                   false|                        22.91|
+-----------------------------+------------------------+-----------------------------+
Total line number = 6
It costs 0.005s
```

If the parameter N/SN of LIMIT/SLIMIT clause exceeds the allowable maximum value (N/SN is of type int32), the system prompts errors. For example, executing the following SQL statement:

```sql
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 1234567890123456789
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 303: check metadata error: Out of range. LIMIT <N>: N should be Int32.
```

If the parameter N/SN of LIMIT/SLIMIT clause is not a positive intege, the system prompts errors. For example, executing the following SQL statement:

```sql
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 13.1
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 401: line 1:129 mismatched input '.' expecting {<EOF>, ';'}
```

If the parameter OFFSET of LIMIT clause exceeds the size of the result set, IoTDB will return an empty result set. For example, executing the following SQL statement:

```sql
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 2 offset 6
```

The result is shown below:

```
+----+------------------------+-----------------------------+
|Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+----+------------------------+-----------------------------+
+----+------------------------+-----------------------------+
Empty set.
It costs 0.005s
```

If the parameter SOFFSET of SLIMIT clause is not smaller than the number of available timeseries, the system prompts errors. For example, executing the following SQL statement:

```sql
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 2
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 411: Meet error in query process: The value of SOFFSET (2) is equal to or exceeds the number of sequences (2) that can actually be returned.
```
