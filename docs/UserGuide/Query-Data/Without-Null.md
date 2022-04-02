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

# Null Value Filter

In practical application, users may want to filter some rows with null values in the query result set. In IoTDB, the `WITHOUT NULL` clause can be used to filter null values in the result set. There are two filtering strategies: `WITHOUT NULL ANY`和`WITHOUT NULL ALL`. In addition, the `WITHOUT NULL` clause supports specifying the corresponding columns for filtering.

> WITHOUT NULL ANY: if one of the columns in the specified column set is null, the conditions are met for filtering.
> 
> WITHOUT NULL ALL: if all columns in the specified column set are null, the conditions are met for filtering.

## Don't specify columns

> By default, it is effective for all columns in the result set. That is the specified column set includes all columns in the result set.

1. In the following query, if any column of one row in the result set is null, the row will be filtered out. That is the result set obtained does not contain any null values.

```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 WITHOUT NULL ANY
```

2. In the following query, if all columns of one row in the result set are null, the row will be filtered out.

```sql
select * from root.ln.** where group by ([1,10), 2ms) WITHOUT NULL ALL
```

## Specify columns

> take effect only for the specified column set

Use the `WITHOUT NULL` clause to filter the null value of the specified column in the result set. The following are some examples and descriptions:

> Note that if the specified column does not exist in the current metadata, it will be filtered directly, which is consistent with the output result of the current query.
> If the specified column exists but does not match the column name output from the result set, an error will be reported: `The without null columns don't match the columns queried.If has alias, please use the alias.`

For examples:

1. In `without null` specified column set, `root.test.sg1.s1` column exists in the current metadata,`root.test.sg1.usag` column does not exist in the current metadata. The function of the `without null` clause in the following query is equivalent to without null all(s1).

```sql
select * from root.test.sg1 without null all (s1, usag)
```

2. In `without null` specified column set, `root.test.sg1.s2` column exists in the current metadata, but doesn't exist in the result set of the query. So it will report an error: `The without null columns don't match the columns queried.If has alias, please use the alias.`

```sql
select s1 + s2, s1 - s2, s1 * s2, s1 / s2, s1 % s2 from root.test.sg1 without null all (s1+s2, s2)
```

### Raw data query

1. If the column `root.ln.sg1.s1` of one row in the result set of the query is null, the row will be filtered out.

```sql
select * from root.ln.sg1 WITHOUT NULL ANY(s1)
```

2. If at least one column in `root.ln.sg1.s1` and `root.ln.sg1.s2` of one row is null in the result set of the query, the row will be filtered out.

```sql
select * from root.ln.sg1 WITHOUT NULL ANY(s1, s2)
```

3. If both `root.ln.sg1.s1` and `root.ln.sg1.s2` columns of one row are null in the result set of the query, the row will be filtered out.

```sql
select * from root.ln.sg1 WITHOUT NULL ALL(s1, s2)
```

### With expression query

specified columns can be expression

1. If both `s2+s4` and `s2` columns of one row are null in the result set of the query, the row will be filtered out.

```sql
select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null all (s2+s4, s2)
``` 

2. If at least one column in `s2+s4` and `s2` of one row is null in the result set of the query, the row will be filtered out.

```sql
select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null any (s2+s4, s2)
``` 

### With alias query

specified columns can be alias

1. If both `t2` and `t1` aliases of one row are null in the result set of the query, the row will be filtered out.

```sql
select s2 as t1, - s2, s4, + s4, s2 + s4 as t2, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null all (t2, t1)
```

```sql
select s1, sin(s2) + cos(s2) as t1, cos(sin(s2 + s4) + s2) as t2 from root.test.sg1 without null all (t1, t2)
```

2. When you specify aliases in the column set that queried, if you use the original name of the column with an alias in the column set specified without null, an error will be reported：`The without null columns don't match the columns queried.If has alias, please use the alias.` For example, `tan(s1)` and `t` columns are used at the same time in the following query.

```sql
select s1 as d, sin(s1), cos(s1), tan(s1) as t, s2 from root.test.sg1 without null all(d,  tan(s1), t) limit 5
```

### With function query

```sql
select s1, sin(s2) + cos(s2), cos(sin(s2 + s4) + s2) from root.test.sg1 without null all (sin(s2) + cos(s2), cos(sin(s2 + s4) + s2))
```

### Align by device query

```sql
select last_value(*) from root.test.sg1 group by([1,10), 2ms) without null all(last_value(s2), last_value(s3)) align by device
```

Examples of results are as follows：

```
IoTDB> select last_value(*) from root.sg1.* group by([1,10), 2ms) without null all(last_value(s2), last_value(s3)) align by device
+-----------------------------+-----------+--------------+--------------+--------------+
|                         Time|     Device|last_value(s1)|last_value(s2)|last_value(s3)|
+-----------------------------+-----------+--------------+--------------+--------------+
|1970-01-01T08:00:00.001+08:00|root.sg1.d1|           1.0|           2.0|          null|
|1970-01-01T08:00:00.003+08:00|root.sg1.d1|           3.0|           4.0|          null|
|1970-01-01T08:00:00.001+08:00|root.sg1.d2|           1.0|           1.0|           1.0|
+-----------------------------+-----------+--------------+--------------+--------------+
Total line number = 3
It costs 0.007s
```

The specified column name corresponds to the column name of the output result. At present, the `without null` clause doesn't support specifying a column of a device. If you do, an error will be reported: `The without null columns don't match the columns queried.If has alias, please use the alias.` For example, in the following query example, it is not supported to filter the row with column `last_value(root.sg1.d1.s3)` that is null.

```sql
select last_value(*) from root.test.sg1 group by([1,10), 2ms) without null all(last_value(`root.sg1.d1.s3`)) align by device
```

### Aggregation query

```sql
select avg(s4), sum(s2) from root.test.sg1 group by ([1,10), 2ms) without null all(sum(s2))
```

```sql
select avg(s4), sum(s2), count(s3) from root.test.sg1 group by ([1,10), 2ms) without null all(avg(s4), sum(s2))
```

### Specify full path columns

Assuming that the output results of the following query are listed as `root.test.sg1.s2`, `root.test.sg1.s3`, `root.test.sg2.s2` and `root.test.sg2.s3`, you can specify the corresponding columns with full pathname for filtering, such as the following example:

1. If both `root.test.sg1.s2` and `root.test.sg2.s3` columns of one row are null in the result set of the query, the row will be filtered out.

```sql
select s2, s3 from root.test.** without null all(`root.test.sg1.s2`, `root.test.sg2.s3`)
```

2. If `root.test.sg1.s2`, `root.test.sg1.s3` and `root.test.sg2.s3` columns of one row are null in the result set of the query, the row will be filtered out.

```sql
select s2, s3 from root.test.** without null all(`root.test.sg1.s2`, s3)
```

### Aligned Timeseries Query

1. You can specify the `without null` column name as the aligned timeseries column name.

```sql
CREATE ALIGNED TIMESERIES root.test.sg3(s5 INT32, s6 BOOLEAN, s7 DOUBLE, s8 INT32)
select sg1.s1, sg1.s2, sg2.s3, sg3.* from root.test without null all (sg3.s5, sg3.s6, sg2.s3)
```