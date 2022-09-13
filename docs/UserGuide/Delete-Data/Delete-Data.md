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

# DELETE

Users can delete data that meet the deletion condition in the specified timeseries by using the [DELETE statement](../Reference/SQL-Reference.md). When deleting data, users can select one or more timeseries paths, prefix paths, or paths with star  to delete data within a certain time interval.

In a JAVA programming environment, you can use the [Java JDBC](../API/Programming-JDBC.md) to execute single or batch UPDATE statements.

## Delete Single Timeseries
Taking ln Group as an example, there exists such a usage scenario:

The wf02 plant's wt02 device has many segments of errors in its power supply status before 2017-11-01 16:26:00, and the data cannot be analyzed correctly. The erroneous data affected the correlation analysis with other devices. At this point, the data before this time point needs to be deleted. The SQL statement for this operation is

```sql
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

In case we hope to merely delete the data before 2017-11-01 16:26:00 in the year of 2017, The SQL statement is:
```sql
delete from root.ln.wf02.wt02.status where time>=2017-01-01T00:00:00 and time<=2017-11-01T16:26:00;
```

IoTDB supports to delete a range of timeseries points. Users can write SQL expressions as follows to specify the delete interval:

```sql
delete from root.ln.wf02.wt02.status where time < 10
delete from root.ln.wf02.wt02.status where time <= 10
delete from root.ln.wf02.wt02.status where time < 20 and time > 10
delete from root.ln.wf02.wt02.status where time <= 20 and time >= 10
delete from root.ln.wf02.wt02.status where time > 20
delete from root.ln.wf02.wt02.status where time >= 20
delete from root.ln.wf02.wt02.status where time = 20
```

Please pay attention that multiple intervals connected by "OR" expression are not supported in delete statement:

```
delete from root.ln.wf02.wt02.status where time > 4 or time < 0
Msg: 303: Check metadata error: For delete statement, where clause can only contain atomic
expressions like : time > XXX, time <= XXX, or two atomic expressions connected by 'AND'
```

If no "where" clause specified in a delete statement, all the data in a timeseries will be deleted.

```sql
delete from root.ln.wf02.status
```


## Delete Multiple Timeseries
If both the power supply status and hardware version of the ln group wf02 plant wt02 device before 2017-11-01 16:26:00 need to be deleted, [the prefix path with broader meaning or the path with star](../Data-Concept/Data-Model-and-Terminology.md) can be used to delete the data. The SQL statement for this operation is:

```sql
delete from root.ln.wf02.wt02 where time <= 2017-11-01T16:26:00;
```
or

```sql
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```
It should be noted that when the deleted path does not exist, IoTDB will not prompt that the path does not exist, but that the execution is successful, because SQL is a declarative programming method. Unless it is a syntax error, insufficient permissions and so on, it is not considered an error, as shown below:
```
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
Msg: The statement is executed successfully.
```

## Delete Time Partition (experimental)
You may delete all data in a time partition of a storage group using the following grammar:

```sql
DELETE PARTITION root.ln 0,1,2
```

The `0,1,2` above is the id of the partition that is to be deleted, you can find it from the IoTDB
data folders or convert a timestamp manually to an id using `timestamp / partitionInterval
` (flooring), and the `partitionInterval` should be in your config (if time-partitioning is
supported in your version).

Please notice that this function is experimental and mainly for development, please use it with care.
