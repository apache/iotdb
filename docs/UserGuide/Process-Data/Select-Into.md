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



# Query Write-back (SELECT INTO)

The `SELECT ... INTO ...` statement copies data from query result set into target time series.



## SQL

### Syntax

**The following is the syntax definition of the `select` statement:**

```sql
selectClause 
intoClause? 
fromClause 
whereClause? 
specialClause?
```

If the `intoClause` is removed, then the `select` statement is a simple query statement.

The `intoClause` is the mark clause for query write-back.



**The following is the definition of the `intoClause`:**

```sql
intoClause
  : INTO ALIGNED? intoPath (COMMA intoPath)*
  ;

intoPath
  : fullPath
  | nodeNameWithoutStar (DOT nodeNameWithoutStar)*
  ;
```

There are 2 ways to specify target paths (`intoPath`).

*  Full target series name starting with `root`.

  * Example：

    ```sql
    select s1, s1 
    into root.sg.d1.t1, root.sg.d1.t2 
    from root.sg.d1
    ```

* Suffix path does not start with `root`. In this case, the target series name equals to the series prefix path in the `from` clause  +   the suffix path.

  * Example:

    ```sql
    select s1, s1 
    into t1, t2 
    from root.sg.d1
    ```

    which equals to:

    ```sql
    select s1, s1 
    into root.sg.d1.t1, root.sg.d1.t2 
    from root.sg.d1
    ```



**In `intoPath`, you can also use `${i}` to represent part of the prefix path in the `from` clause. **

For example, for the path `root.sg1.d1.v1`,  `${1}` means `sg1`,  `${2}` means `d1`, and `${3}` means `v1`.


  * Example:

    ```sql
    select s1, s1, s1
    into ${1}_t1, ${2}, root.${2}.${1}.t2
    from root.sg.d1
    ```

    which equals to:

    ```sql
    select s1, s1, s1
    into root.sg.d1.sg_t1, root.sg.d1.d1, root.d1.sg.t2
    from root.sg.d1
    ```



**You can specify whether the target timeseries are aligned via the keyword `ALIGNED`. **

When the target aligned timeseries are existed, you need to ensure that the types of the source and target time series match.

When the target aligned timeseries are not existed, the system will automatically create the target aligned time series.


   * Example:

     ```sql
     select s1, s2, s3
     into aligned root.sg.d2.t1, root.sg.d2.t2, root.sg.d2.t3
     from root.sg.d1
     ````



### Supported Query Types

**Note that except for the following types of queries, other types of queries (such as `LAST` queries and raw aggregation queries) are not supported. **

* Raw time series query

  ```sql
  select s1, s1 
  into t1, t2 
  from root.sg.d1
  ```

* Time series generating function query（including UDF query）

  ```sql
  select s1, sin(s2) 
  into t1, t2 
  from root.sg.d1
  ```

* Arithmetic query

  ```sql
  select s1, sin(s2), s1 + s3 
  into t1, t2, t3 
  from root.sg.d1
  ```

* Nested query

  ```sql
  select -s1, sin(cos(tan(s1 + s2 * s3))) + cos(s3), top_k(s1 + s3, 'k'='1') 
  into t1, t2, t3 
  from root.sg.d1
  ```
  
* Fill query

  ```sql
  select s1 
  into fill_s1 
  from root.sg.d1 
  where time = 10 
  fill(float [linear, 1ms, 1ms])
  ```

* Group-by query

  ```sql
  select count(s1) 
  into group_by_s1 
  from root.sg.d1 
  group by ([1, 5), 1ms)
  ```

* Group-by-fill query

  ```sql
  select last_value(s1) 
  into group_by_fill_s1 
  from root.sg.d1 
  group by ([1, 10),1ms) 
  fill (float[PREVIOUS])
  ```



### Special Cluases Supported in Queries

**Note that except for the following clauses, other query clauses (such as `DESC`, `SOFFSET`, etc.) are not supported. **

* Value filter

  ```sql
  select s1, s1 
  into t1, t2 
  from root.sg.d1
  where s1 > 0 and s2 < 0
  ```

* Time filter

  ```sql
  select s1, s1 
  into t1, t2 
  from root.sg.d1
  where time > 0
  ```

* LIMIT / OFFSET

  ```sql
  select s1, s1 
  into t1, t2 
  from root.sg.d1
  limit 5 offset 1000
  ```



### Other Restrictions

* The number of source series in the `select` clause and the number of target series in the `into` clause must be the same.
* The `select *` and `select **` clause are not supported.
* The target series in the `into` clause do not need to be created in advance. When the target series in the `into` clause already exist, you need to ensure that the source series in the `select` clause and the target series in the `into` clause have the same data types.
* The target series in the `into` clause must be different from each other.
* Only one prefix path of a series is allowed in the `from` clause.
* `*` and `**` are not allowed in the `from` clause.
* Aligned Timeseries has not been supported in Time series generating function query（including UDF query）/ Arithmetic query / Nested query yet. An error message is expected if you use these types of query with Aligned Timeseries selected in the `select` clause.



## User Permission Management

The user must have the following permissions to execute a query write-back statement:

* All `READ_TIMESERIES` permissions for the source series in the `select` clause
* All `INSERT_TIMESERIES` permissions for the target series in the `into` clause

For more user permissions related content, please refer to [Account Management Statements](../Administration-Management/Administration.md).



## Configurable Properties

* `select_into_insert_tablet_plan_row_limit`: The maximum number of rows can be processed in one insert-tablet-plan when executing select-into statements. 10000 by default.

