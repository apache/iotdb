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


# Aggregate Result Filtering

If you want to filter the results of aggregate queries, 
you can use the `HAVING` clause after the `GROUP BY` clause.

> NOTE：
>
> 1.The expression in HAVING clause must consist of aggregate values; the original sequence cannot appear alone.
> The following usages are incorrect：
> ```sql
> select count(s1) from root.** group by ([1,3),1ms) having sum(s1) > s1
> select count(s1) from root.** group by ([1,3),1ms) having s1 > 1
> ```
> 2.When filtering the `GROUP BY LEVEL` result, the PATH in `SELECT` and `HAVING` can only have one node.
> The following usages are incorrect：
> ```sql
> select count(s1) from root.** group by ([1,3),1ms), level=1 having sum(d1.s1) > 1
> select count(d1.s1) from root.** group by ([1,3),1ms), level=1 having sum(s1) > 1
> ```

Here are a few examples of using the 'HAVING' clause to filter aggregate results.

Aggregation result 1：

```
+-----------------------------+---------------------+---------------------+
|                         Time|count(root.test.*.s1)|count(root.test.*.s2)|
+-----------------------------+---------------------+---------------------+
|1970-01-01T08:00:00.001+08:00|                    4|                    4|
|1970-01-01T08:00:00.003+08:00|                    1|                    0|
|1970-01-01T08:00:00.005+08:00|                    2|                    4|
|1970-01-01T08:00:00.007+08:00|                    3|                    2|
|1970-01-01T08:00:00.009+08:00|                    4|                    4|
+-----------------------------+---------------------+---------------------+
```

Aggregation result filtering query 1：

```sql
 select count(s1) from root.** group by ([1,11),2ms), level=1 having count(s2) > 1
```

Filtering result 1：

```
+-----------------------------+---------------------+
|                         Time|count(root.test.*.s1)|
+-----------------------------+---------------------+
|1970-01-01T08:00:00.001+08:00|                    4|
|1970-01-01T08:00:00.005+08:00|                    2|
|1970-01-01T08:00:00.009+08:00|                    4|
+-----------------------------+---------------------+
```

Aggregation result 2：

```
+-----------------------------+-------------+---------+---------+
|                         Time|       Device|count(s1)|count(s2)|
+-----------------------------+-------------+---------+---------+
|1970-01-01T08:00:00.001+08:00|root.test.sg1|        1|        2|
|1970-01-01T08:00:00.003+08:00|root.test.sg1|        1|        0|
|1970-01-01T08:00:00.005+08:00|root.test.sg1|        1|        2|
|1970-01-01T08:00:00.007+08:00|root.test.sg1|        2|        1|
|1970-01-01T08:00:00.009+08:00|root.test.sg1|        2|        2|
|1970-01-01T08:00:00.001+08:00|root.test.sg2|        2|        2|
|1970-01-01T08:00:00.003+08:00|root.test.sg2|        0|        0|
|1970-01-01T08:00:00.005+08:00|root.test.sg2|        1|        2|
|1970-01-01T08:00:00.007+08:00|root.test.sg2|        1|        1|
|1970-01-01T08:00:00.009+08:00|root.test.sg2|        2|        2|
+-----------------------------+-------------+---------+---------+
```

Aggregation result filtering query 2：

```sql
 select count(s1), count(s2) from root.** group by ([1,11),2ms) having count(s2) > 1 align by device
```

Filtering result 2：

```
+-----------------------------+-------------+---------+---------+
|                         Time|       Device|count(s1)|count(s2)|
+-----------------------------+-------------+---------+---------+
|1970-01-01T08:00:00.001+08:00|root.test.sg1|        1|        2|
|1970-01-01T08:00:00.005+08:00|root.test.sg1|        1|        2|
|1970-01-01T08:00:00.009+08:00|root.test.sg1|        2|        2|
|1970-01-01T08:00:00.001+08:00|root.test.sg2|        2|        2|
|1970-01-01T08:00:00.005+08:00|root.test.sg2|        1|        2|
|1970-01-01T08:00:00.009+08:00|root.test.sg2|        2|        2|
+-----------------------------+-------------+---------+---------+
```