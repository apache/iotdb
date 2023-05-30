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

# Order By

## Order by in ALIGN BY TIME mode

The result set of IoTDB is in ALIGN BY TIME mode by default and `ORDER BY TIME` clause can also be used to specify the ordering of timestamp. The SQL statement is:
```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 order by time desc;
```
Results：

```
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf02.wt02.hardware|root.ln.wf02.wt02.status|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
|2017-11-01T00:01:00.000+08:00|                        v2|                    true|                        24.36|                    true|
|2017-11-01T00:00:00.000+08:00|                        v2|                    true|                        25.96|                    true|
|1970-01-01T08:00:00.002+08:00|                        v2|                   false|                         null|                    null|
|1970-01-01T08:00:00.001+08:00|                        v1|                    true|                         null|                    null|
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
```

## Order by in ALIGN BY DEVICE mode
When querying in ALIGN BY DEVICE mode, `ORDER BY` clause can be used to specify the ordering of result set.

ALIGN BY DEVICE mode supports four kinds of clauses with two sort keys which are `Device` and `Time`.

1. ``ORDER BY DEVICE``: sort by the alphabetical order of the device name. The devices with the same column names will be clustered in a group view.

2. ``ORDER BY TIME``: sort by the timestamp, the data points from different devices will be shuffled according to the timestamp.

3. ``ORDER BY DEVICE,TIME``: sort by the alphabetical order of the device name. The data points with the same device name will be sorted by timestamp.

4. ``ORDER BY TIME,DEVICE``: sort by timestamp. The data points with the same time will be sorted by the alphabetical order of the device name.

> To make the result set more legible, when `ORDER BY` clause is not used, default settings will be provided.
> The default ordering clause is `ORDER BY DEVICE,TIME` and the default ordering is `ASC`.

When `Device` is the main sort key, the result set is sorted by device name first, then by timestamp in the group with the same device name, the SQL statement is:
```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 order by device desc,time asc align by device;
```
The result shows below:

```
+-----------------------------+-----------------+--------+------+-----------+
|                         Time|           Device|hardware|status|temperature|
+-----------------------------+-----------------+--------+------+-----------+
|1970-01-01T08:00:00.001+08:00|root.ln.wf02.wt02|      v1|  true|       null|
|1970-01-01T08:00:00.002+08:00|root.ln.wf02.wt02|      v2| false|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf01.wt01|    null|  true|      25.96|
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|    null|  true|      24.36|
+-----------------------------+-----------------+--------+------+-----------+
```
When `Time` is the main sort key, the result set is sorted by timestamp first, then by device name in data points with the same timestamp. The SQL statement is:
```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 order by time asc,device desc align by device;
```
The result shows below:
```
+-----------------------------+-----------------+--------+------+-----------+
|                         Time|           Device|hardware|status|temperature|
+-----------------------------+-----------------+--------+------+-----------+
|1970-01-01T08:00:00.001+08:00|root.ln.wf02.wt02|      v1|  true|       null|
|1970-01-01T08:00:00.002+08:00|root.ln.wf02.wt02|      v2| false|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf01.wt01|    null|  true|      25.96|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|    null|  true|      24.36|
+-----------------------------+-----------------+--------+------+-----------+
```
When `ORDER BY` clause is not used, sort in default way, the SQL statement is：
```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 align by device;
```
The result below indicates `ORDER BY DEVICE ASC,TIME ASC` is the clause in default situation.
`ASC` can be omitted because it's the default ordering.
```
+-----------------------------+-----------------+--------+------+-----------+
|                         Time|           Device|hardware|status|temperature|
+-----------------------------+-----------------+--------+------+-----------+
|2017-11-01T00:00:00.000+08:00|root.ln.wf01.wt01|    null|  true|      25.96|
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|    null|  true|      24.36|
|1970-01-01T08:00:00.001+08:00|root.ln.wf02.wt02|      v1|  true|       null|
|1970-01-01T08:00:00.002+08:00|root.ln.wf02.wt02|      v2| false|       null|
|2017-11-01T00:00:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|      v2|  true|       null|
+-----------------------------+-----------------+--------+------+-----------+
```
Besides，`ALIGN BY DEVICE` and `ORDER BY` clauses can be used with aggregate query，the SQL statement is：
```sql
select count(*) from root.ln.** group by ((2017-11-01T00:00:00.000+08:00,2017-11-01T00:03:00.000+08:00],1m) order by device asc,time asc align by device
```
The result shows below:
```
+-----------------------------+-----------------+---------------+-------------+------------------+
|                         Time|           Device|count(hardware)|count(status)|count(temperature)|
+-----------------------------+-----------------+---------------+-------------+------------------+
|2017-11-01T00:01:00.000+08:00|root.ln.wf01.wt01|           null|            1|                 1|
|2017-11-01T00:02:00.000+08:00|root.ln.wf01.wt01|           null|            0|                 0|
|2017-11-01T00:03:00.000+08:00|root.ln.wf01.wt01|           null|            0|                 0|
|2017-11-01T00:01:00.000+08:00|root.ln.wf02.wt02|              1|            1|              null|
|2017-11-01T00:02:00.000+08:00|root.ln.wf02.wt02|              0|            0|              null|
|2017-11-01T00:03:00.000+08:00|root.ln.wf02.wt02|              0|            0|              null|
+-----------------------------+-----------------+---------------+-------------+------------------+
```
## Order by arbitrary expressions

In addition to the predefined keywords "Time" and "Device" in IoTDB, `ORDER BY` can also be used to sort by any expressions.

When sorting, `ASC` or `DESC` can be used to specify the sorting order, and `NULLS` syntax is supported to specify the priority of NULL values in the sorting. By default, `NULLS FIRST` places NULL values at the top of the result, and `NULLS LAST` ensures that NULL values appear at the end of the result. If not specified in the clause, the default order is ASC with NULLS LAST.

Here are several examples of queries for sorting arbitrary expressions using the following data:
```
+-----------------------------+-------------+-------+-------+--------+-------+
|                         Time|       Device|   base|  score|   bonus|  total|    
+-----------------------------+-------------+-------+-------+--------+-------+
|1970-01-01T08:00:00.000+08:00|     root.one|     12|   50.0|    45.0|  107.0|  
|1970-01-02T08:00:00.000+08:00|     root.one|     10|   50.0|    45.0|  105.0|
|1970-01-03T08:00:00.000+08:00|     root.one|      8|   50.0|    45.0|  103.0|       
|1970-01-01T08:00:00.010+08:00|     root.two|      9|   50.0|    15.0|   74.0|   
|1970-01-01T08:00:00.020+08:00|     root.two|      8|   10.0|    15.0|   33.0|  
|1970-01-01T08:00:00.010+08:00|   root.three|      9|   null|    24.0|   33.0|    
|1970-01-01T08:00:00.020+08:00|   root.three|      8|   null|    22.5|   30.5|   
|1970-01-01T08:00:00.030+08:00|   root.three|      7|   null|    23.5|   30.5|   
|1970-01-01T08:00:00.010+08:00|    root.four|      9|   32.0|    45.0|   86.0|  
|1970-01-01T08:00:00.020+08:00|    root.four|      8|   32.0|    45.0|   85.0|   
|1970-01-01T08:00:00.030+08:00|    root.five|      7|   53.0|    44.0|  104.0|
|1970-01-01T08:00:00.040+08:00|    root.five|      6|   54.0|    42.0|  102.0|     
+-----------------------------+-------------+-------+-------+--------+-------+
```
When you need to sort the results based on the base score score, you can use the following SQL:
```Sql
select score from root.** order by score desc align by device
```
This will give you the following results:

```
+-----------------------------+---------+-----+
|                         Time|   Device|score|
+-----------------------------+---------+-----+
|1970-01-01T08:00:00.040+08:00|root.five| 54.0|
|1970-01-01T08:00:00.030+08:00|root.five| 53.0|
|1970-01-01T08:00:00.000+08:00| root.one| 50.0|
|1970-01-02T08:00:00.000+08:00| root.one| 50.0|
|1970-01-03T08:00:00.000+08:00| root.one| 50.0|
|1970-01-01T08:00:00.000+08:00| root.two| 50.0|
|1970-01-01T08:00:00.010+08:00| root.two| 50.0|
|1970-01-01T08:00:00.010+08:00|root.four| 32.0|
|1970-01-01T08:00:00.020+08:00|root.four| 32.0|
|1970-01-01T08:00:00.020+08:00| root.two| 10.0|
+-----------------------------+---------+-----+
```
If you want to sort the results based on the total score, you can use an expression in the `ORDER BY` clause to perform the calculation:
```Sql
select score,total from root.one order by base+score+bonus desc
```
This SQL is equivalent to:
```Sql
select score,total from root.one order by total desc
```
Here are the results:
```
+-----------------------------+--------------+--------------+
|                         Time|root.one.score|root.one.total|
+-----------------------------+--------------+--------------+
|1970-01-01T08:00:00.000+08:00|          50.0|         107.0|
|1970-01-02T08:00:00.000+08:00|          50.0|         105.0|
|1970-01-03T08:00:00.000+08:00|          50.0|         103.0|
+-----------------------------+--------------+--------------+
```
If you want to sort the results based on the total score and, in case of tied scores, sort by score, base, bonus, and submission time in descending order, you can specify multiple layers of sorting using multiple expressions:

```Sql
select base, score, bonus, total from root.** order by total desc NULLS Last,
                                  score desc NULLS Last,
                                  bonus desc NULLS Last,
                                  time desc align by device
```
Here are the results:
```
+-----------------------------+----------+----+-----+-----+-----+
|                         Time|    Device|base|score|bonus|total|
+-----------------------------+----------+----+-----+-----+-----+
|1970-01-01T08:00:00.000+08:00|  root.one|  12| 50.0| 45.0|107.0|
|1970-01-02T08:00:00.000+08:00|  root.one|  10| 50.0| 45.0|105.0|
|1970-01-01T08:00:00.030+08:00| root.five|   7| 53.0| 44.0|104.0|
|1970-01-03T08:00:00.000+08:00|  root.one|   8| 50.0| 45.0|103.0|
|1970-01-01T08:00:00.040+08:00| root.five|   6| 54.0| 42.0|102.0|
|1970-01-01T08:00:00.010+08:00| root.four|   9| 32.0| 45.0| 86.0|
|1970-01-01T08:00:00.020+08:00| root.four|   8| 32.0| 45.0| 85.0|
|1970-01-01T08:00:00.010+08:00|  root.two|   9| 50.0| 15.0| 74.0|
|1970-01-01T08:00:00.000+08:00|  root.two|   9| 50.0| 15.0| 74.0|
|1970-01-01T08:00:00.020+08:00|  root.two|   8| 10.0| 15.0| 33.0|
|1970-01-01T08:00:00.010+08:00|root.three|   9| null| 24.0| 33.0|
|1970-01-01T08:00:00.030+08:00|root.three|   7| null| 23.5| 30.5|
|1970-01-01T08:00:00.020+08:00|root.three|   8| null| 22.5| 30.5|
+-----------------------------+----------+----+-----+-----+-----+
```
In the `ORDER BY` clause, you can also use aggregate query expressions. For example:
```Sql
select min_value(total) from root.** order by min_value(total) asc align by device
```
This will give you the following results:
```
+----------+----------------+
|    Device|min_value(total)|
+----------+----------------+
|root.three|            30.5|
|  root.two|            33.0|
| root.four|            85.0|
| root.five|           102.0|
|  root.one|           103.0|
+----------+----------------+
```
When specifying multiple columns in the query, the unsorted columns will change order along with the rows and sorted columns. The order of rows when the sorting columns are the same may vary depending on the specific implementation (no fixed order). For example:
```Sql
select min_value(total),max_value(base) from root.** order by max_value(total) desc align by device
```
This will give you the following results:
·
```
+----------+----------------+---------------+
|    Device|min_value(total)|max_value(base)|
+----------+----------------+---------------+
|  root.one|           103.0|             12|
| root.five|           102.0|              7|
| root.four|            85.0|              9|
|  root.two|            33.0|              9|
|root.three|            30.5|              9|
+----------+----------------+---------------+
```

You can use both `ORDER BY DEVICE,TIME` and `ORDER BY EXPRESSION` together. For example:
```Sql
select score from root.** order by device asc, score desc, time asc align by device
```
This will give you the following results:
```
+-----------------------------+---------+-----+
|                         Time|   Device|score|
+-----------------------------+---------+-----+
|1970-01-01T08:00:00.040+08:00|root.five| 54.0|
|1970-01-01T08:00:00.030+08:00|root.five| 53.0|
|1970-01-01T08:00:00.010+08:00|root.four| 32.0|
|1970-01-01T08:00:00.020+08:00|root.four| 32.0|
|1970-01-01T08:00:00.000+08:00| root.one| 50.0|
|1970-01-02T08:00:00.000+08:00| root.one| 50.0|
|1970-01-03T08:00:00.000+08:00| root.one| 50.0|
|1970-01-01T08:00:00.000+08:00| root.two| 50.0|
|1970-01-01T08:00:00.010+08:00| root.two| 50.0|
|1970-01-01T08:00:00.020+08:00| root.two| 10.0|
+-----------------------------+---------+-----+
```
