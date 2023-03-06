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

The `SELECT INTO` statement copies data from query result set into target time series.

The application scenarios are as follows:
- **Implement IoTDB internal ETL**: ETL the original data and write a new time series.
- **Query result storage**: Persistently store the query results, which acts like a materialized view.
- **Non-aligned time series to aligned time series**:  Rewrite non-aligned time series into another aligned time series.

## SQL Syntax

### Syntax Definition

**The following is the syntax definition of the `select` statement:**

```sql
selectIntoStatement
: SELECT
      resultColumn [, resultColumn] ...
        INTO intoItem [, intoItem] ...
        FROM prefixPath [, prefixPath] ...
        [WHERE whereCondition]
      [GROUP BY groupByTimeClause, groupByLevelClause]
      [FILL {PREVIOUS | LINEAR | constant}]
      [LIMIT rowLimit OFFSET rowOffset]
      [ALIGN BY DEVICE]
;

intoItem
: [ALIGNED] intoDevicePath '(' intoMeasurementName [',' intoMeasurementName]* ')'
    ;
```

### `INTO` Clause

The `INTO` clause consists of several `intoItem`.

Each `intoItem` consists of a target device and a list of target measurements (similar to the `INTO` clause in an `INSERT` statement).

Each target measurement and device form a target time series, and an `intoItem` contains a series of time series. For example: `root.sg_copy.d1(s1, s2)` specifies two target time series `root.sg_copy.d1.s1` and `root.sg_copy.d1.s2`.

The target time series specified by the `INTO` clause must correspond one-to-one with the columns of the query result set. The specific rules are as follows:

- **Align by time** (default): The number of target time series contained in all `intoItem` must be consistent with the number of columns in the query result set (except the time column) and correspond one-to-one in the order from left to right in the header.
- **Align by device** (using `ALIGN BY DEVICE`): the number of target devices specified in all `intoItem` is the same as the number of devices queried (i.e., the number of devices matched by the path pattern in the `FROM` clause), and One-to-one correspondence according to the output order of the result set device.
  <br>The number of measurements specified for each target device should be consistent with the number of columns in the query result set (except for the time and device columns). It should be in one-to-one correspondence from left to right in the header.

For examples:

- **Example 1** (aligned by time)
```shell
IoTDB> select s1, s2 into root.sg_copy.d1(t1), root.sg_copy.d2(t1, t2), root.sg_copy.d1(t2) from root.sg.d1, root.sg.d2;
+--------------+-------------------+--------+
| source column|  target timeseries| written|
+--------------+-------------------+--------+
| root.sg.d1.s1| root.sg_copy.d1.t1|    8000|
+--------------+-------------------+--------+
| root.sg.d2.s1| root.sg_copy.d2.t1|   10000|
+--------------+-------------------+--------+
| root.sg.d1.s2| root.sg_copy.d2.t2|   12000|
+--------------+-------------------+--------+
| root.sg.d2.s2| root.sg_copy.d1.t2|   10000|
+--------------+-------------------+--------+
Total line number = 4
It costs 0.725s
```
This statement writes the query results of the four time series under the `root.sg` database to the four specified time series under the `root.sg_copy` database. Note that `root.sg_copy.d2(t1, t2)` can also be written as `root.sg_copy.d2(t1), root.sg_copy.d2(t2)`.

We can see that the writing of the `INTO` clause is very flexible as long as the combined target time series is not repeated and corresponds to the query result column one-to-one.

> In the result set displayed by `CLI`, the meaning of each column is as follows:
> - The `source column` column represents the column name of the query result.
> - `target timeseries` represents the target time series for the corresponding column to write.
> - `written` indicates the amount of data expected to be written.


- **Example 2** (aligned by time)
```shell
IoTDB> select count(s1 + s2), last_value(s2) into root.agg.count(s1_add_s2), root.agg.last_value(s2) from root.sg.d1 group by ([0, 100), 10ms);
+--------------------------------------+-------------------------+--------+
|                         source column|        target timeseries| written|
+--------------------------------------+-------------------------+--------+
|  count(root.sg.d1.s1 + root.sg.d1.s2)| root.agg.count.s1_add_s2|      10|
+--------------------------------------+-------------------------+--------+
|             last_value(root.sg.d1.s2)|   root.agg.last_value.s2|      10|
+--------------------------------------+-------------------------+--------+
Total line number = 2
It costs 0.375s
```

This statement stores the results of an aggregated query into the specified time series.

- **Example 3** (aligned by device)
```shell
IoTDB> select s1, s2 into root.sg_copy.d1(t1, t2), root.sg_copy.d2(t1, t2) from root.sg.d1, root.sg.d2 align by device;
+--------------+--------------+-------------------+--------+
| source device| source column|  target timeseries| written|
+--------------+--------------+-------------------+--------+
|    root.sg.d1|            s1| root.sg_copy.d1.t1|    8000|
+--------------+--------------+-------------------+--------+
|    root.sg.d1|            s2| root.sg_copy.d1.t2|   11000|
+--------------+--------------+-------------------+--------+
|    root.sg.d2|            s1| root.sg_copy.d2.t1|   12000|
+--------------+--------------+-------------------+--------+
|    root.sg.d2|            s2| root.sg_copy.d2.t2|    9000|
+--------------+--------------+-------------------+--------+
Total line number = 4
It costs 0.625s
```
This statement also writes the query results of the four time series under the `root.sg` database to the four specified time series under the `root.sg_copy` database. However, in ALIGN BY DEVICE, the number of `intoItem` must be the same as the number of queried devices, and each queried device corresponds to one `intoItem`.

> When aligning the query by device, the result set displayed by `CLI` has one more column, the `source device` column indicating the queried device.

- **Example 4** (aligned by device)
```shell
IoTDB> select s1 + s2 into root.expr.add(d1s1_d1s2), root.expr.add(d2s1_d2s2) from root.sg.d1, root.sg.d2 align by device;
+--------------+--------------+------------------------+--------+
| source device| source column|       target timeseries| written|
+--------------+--------------+------------------------+--------+
|    root.sg.d1|       s1 + s2| root.expr.add.d1s1_d1s2|   10000|
+--------------+--------------+------------------------+--------+
|    root.sg.d2|       s1 + s2| root.expr.add.d2s1_d2s2|   10000|
+--------------+--------------+------------------------+--------+
Total line number = 2
It costs 0.532s
```
This statement stores the result of evaluating an expression into the specified time series.

### Using variable placeholders

In particular, We can use variable placeholders to describe the correspondence between the target and query time series, simplifying the statement. The following two variable placeholders are currently supported:

- Suffix duplication character `::`: Copy the suffix (or measurement) of the query device, indicating that from this layer to the last layer (or measurement) of the device, the node name (or measurement) of the target device corresponds to the queried device The node name (or measurement) is the same.
- Single-level node matcher `${i}`: Indicates that the current level node name of the target sequence is the same as the i-th level node name of the query sequence. For example, for the path `root.sg1.d1.s1`, `${1}` means `sg1`, `${2}` means `d1`, and `${3}` means `s1`.

When using variable placeholders, there must be no ambiguity in the correspondence between `intoItem` and the columns of the query result set. The specific cases are classified as follows:

#### ALIGN BY TIME (default)

> Note: The variable placeholder **can only describe the correspondence between time series**. If the query includes aggregation and expression calculation, the columns in the query result cannot correspond to a time series, so neither the target device nor the measurement can use variable placeholders.

##### (1) The target device does not use variable placeholders & the target measurement list uses variable placeholders

**Limitations:**
1. In each `intoItem`, the length of the list of physical quantities must be 1. <br> (If the length can be greater than 1, e.g. `root.sg1.d1(::, s1)`, it is not possible to determine which columns match `::`)
2. The number of `intoItem` is 1, or the same as the number of columns in the query result set. <br>(When the length of each target measurement list is 1, if there is only one `intoItem`, it means that all the query sequences are written to the same device; if the number of `intoItem` is consistent with the query sequence, it is expressed as each query time series specifies a target device; if `intoItem` is greater than one and less than the number of query sequences, it cannot be a one-to-one correspondence with the query sequence)

**Matching method:** Each query time series specifies the target device, and the target measurement is generated from the variable placeholder.

**Example:**

```sql
select s1, s2
into root.sg_copy.d1(::), root.sg_copy.d2(s1), root.sg_copy.d1(${3}), root.sg_copy.d2(::)
from root.sg.d1, root.sg.d2;
````
This statement is equivalent to:
```sql
select s1, s2
into root.sg_copy.d1(s1), root.sg_copy.d2(s1), root.sg_copy.d1(s2), root.sg_copy.d2(s2)
from root.sg.d1, root.sg.d2;
````
As you can see, the statement is not very simplified in this case.

##### (2) The target device uses variable placeholders & the target measurement list does not use variable placeholders

**Limitations:** The number of target measurements in all `intoItem` is the same as the number of columns in the query result set.

**Matching method:** The target measurement is specified for each query time series, and the target device is generated according to the target device placeholder of the `intoItem` where the corresponding target measurement is located.

**Example:**
```sql
select d1.s1, d1.s2, d2.s3, d3.s4
into ::(s1_1, s2_2), root.sg.d2_2(s3_3), root.${2}_copy.::(s4)
from root.sg;
````
##### (3) The target device uses variable placeholders & the target measurement list uses variable placeholders

**Limitations:** There is only one `intoItem`, and the length of the list of measurement list is 1.

**Matching method:** Each query time series can get a target time series according to the variable placeholder.

**Example:**
```sql
select * into root.sg_bk.::(::) from root.sg.**;
````
Write the query results of all time series under `root.sg` to `root.sg_bk`, the device name suffix and measurement remain unchanged.

#### ALIGN BY DEVICE

> Note: The variable placeholder **can only describe the correspondence between time series**. If the query includes aggregation and expression calculation, the columns in the query result cannot correspond to a specific physical quantity, so the target measurement cannot use variable placeholders.

##### (1) The target device does not use variable placeholders & the target measurement list uses variable placeholders

**Limitations:** In each `intoItem`, if the list of measurement uses variable placeholders, the length of the list must be 1.

**Matching method:** Each query time series specifies the target device, and the target measurement is generated from the variable placeholder.

**Example:**
```sql
select s1, s2, s3, s4
into root.backup_sg.d1(s1, s2, s3, s4), root.backup_sg.d2(::), root.sg.d3(backup_${4})
from root.sg.d1, root.sg.d2, root.sg.d3
align by device;
````

##### (2) The target device uses variable placeholders & the target measurement list does not use variable placeholders

**Limitations:** There is only one `intoItem`. (If there are multiple `intoItem` with placeholders, we will not know which source devices each `intoItem` needs to match)

**Matching method:** Each query device obtains a target device according to the variable placeholder, and the target measurement written in each column of the result set under each device is specified by the target measurement list.

**Example:**
```sql
select avg(s1), sum(s2) + sum(s3), count(s4)
into root.agg_${2}.::(avg_s1, sum_s2_add_s3, count_s4)
from root.**
align by device;
````

##### (3) The target device uses variable placeholders & the target measurement list uses variable placeholders

**Limitations:** There is only one `intoItem` and the length of the target measurement list is 1.

**Matching method:** Each query time series can get a target time series according to the variable placeholder.

**Example:**
```sql
select * into ::(backup_${4}) from root.sg.** align by device;
````
Write the query result of each time series in `root.sg` to the same device, and add `backup_` before the measurement.

### Specify the target time series as the aligned time series

We can use the `ALIGNED` keyword to specify the target device for writing to be aligned, and each `intoItem` can be set independently.

**Example:**
```sql
select s1, s2 into root.sg_copy.d1(t1, t2), aligned root.sg_copy.d2(t1, t2) from root.sg.d1, root.sg.d2 align by device;
```
This statement specifies that `root.sg_copy.d1` is an unaligned device and `root.sg_copy.d2` is an aligned device.

### Unsupported query clauses

- `SLIMIT`, `SOFFSET`: The query columns are uncertain, so they are not supported.
- `LAST`, `GROUP BY TAGS`, `DISABLE ALIGN`: The table structure is inconsistent with the writing structure, so it is not supported.

### Other points to note

- For general aggregation queries, the timestamp is meaningless, and the convention is to use 0 to store.
- When the target time-series exists, the data type of the source column and the target time-series must be compatible. About data type compatibility, see the document [Data Type](../Data-Concept/Data-Type.md#Data Type Compatibility).
- When the target time series does not exist, the system automatically creates it (including the database).
- When the queried time series does not exist, or the queried sequence does not have data, the target time series will not be created automatically.

## Application examples

### Implement IoTDB internal ETL
ETL the original data and write a new time series.
```shell
IOTDB > SELECT preprocess_udf(s1, s2) INTO ::(preprocessed_s1, preprocessed_s2) FROM root.sg.* ALIGN BY DEIVCE;
+--------------+-------------------+---------------------------+--------+
| source device|      source column|          target timeseries| written|
+--------------+-------------------+---------------------------+--------+
|    root.sg.d1| preprocess_udf(s1)| root.sg.d1.preprocessed_s1|    8000|
+--------------+-------------------+---------------------------+--------+
|    root.sg.d1| preprocess_udf(s2)| root.sg.d1.preprocessed_s2|   10000|
+--------------+-------------------+---------------------------+--------+
|    root.sg.d2| preprocess_udf(s1)| root.sg.d2.preprocessed_s1|   11000|
+--------------+-------------------+---------------------------+--------+
|    root.sg.d2| preprocess_udf(s2)| root.sg.d2.preprocessed_s2|    9000|
+--------------+-------------------+---------------------------+--------+
```

### Query result storage
Persistently store the query results, which acts like a materialized view.
```shell
IOTDB > SELECT count(s1), last_value(s1) INTO root.sg.agg_${2}(count_s1, last_value_s1) FROM root.sg1.d1 GROUP BY ([0, 10000), 10ms);
+--------------------------+-----------------------------+--------+
|             source column|            target timeseries| written|
+--------------------------+-----------------------------+--------+
|      count(root.sg.d1.s1)|      root.sg.agg_d1.count_s1|    1000|
+--------------------------+-----------------------------+--------+
| last_value(root.sg.d1.s2)| root.sg.agg_d1.last_value_s2|    1000|
+--------------------------+-----------------------------+--------+
Total line number = 2
It costs 0.115s
```

### Non-aligned time series to aligned time series
Rewrite non-aligned time series into another aligned time series.

**Note:** It is recommended to use the `LIMIT & OFFSET` clause or the `WHERE` clause (time filter) to batch data to prevent excessive data volume in a single operation.

```shell
IOTDB > SELECT s1, s2 INTO ALIGNED root.sg1.aligned_d(s1, s2) FROM root.sg1.non_aligned_d WHERE time >= 0 and time < 10000;
+--------------------------+----------------------+--------+
|             source column|     target timeseries| written|
+--------------------------+----------------------+--------+
| root.sg1.non_aligned_d.s1| root.sg1.aligned_d.s1|   10000|
+--------------------------+----------------------+--------+
| root.sg1.non_aligned_d.s2| root.sg1.aligned_d.s2|   10000|
+--------------------------+----------------------+--------+
Total line number = 2
It costs 0.375s
```

## User Permission Management

The user must have the following permissions to execute a query write-back statement:

* All `READ_TIMESERIES` permissions for the source series in the `select` clause.
* All `INSERT_TIMESERIES` permissions for the target series in the `into` clause.

For more user permissions related content, please refer to [Account Management Statements](../Administration-Management/Administration.md).

## Configurable Properties

* `select_into_insert_tablet_plan_row_limit`: The maximum number of rows can be processed in one insert-tablet-plan when executing select-into statements. 10000 by default.

