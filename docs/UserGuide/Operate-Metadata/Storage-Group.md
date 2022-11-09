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

# Database Management

Storage Group can be regarded as Database in the relational database.

## Create Database

According to the storage model we can set up the corresponding database. Two SQL statements are supported for creating databases, as follows:

```
IoTDB > create database root.ln
IoTDB > create database root.sgcc
```

We can thus create two storage groups using the above two SQL statements.

It is worth noting that when the path itself or the parent/child layer of the path is already created as database, the path is then not allowed to be created as database. For example, it is not feasible to create `root.ln.wf01` as database when two databases `root.ln` and `root.sgcc` exist. The system gives the corresponding error prompt as shown below:

```
IoTDB> CREATE DATABASE root.ln.wf01
Msg: 300: root.ln has already been created as database.
IoTDB> create database root.ln.wf01
Msg: 300: root.ln has already been created as database.
```
The LayerName of database can only be characters, numbers, underscores. If you want to set it to pure numbers or contain other characters, you need to enclose the database name with backticks (``). 
 
Besides, if deploy on Windows system, the LayerName is case-insensitive, which means it's not allowed to create databases `root.ln` and `root.LN` at the same time.

## SHOW DATABASES

After creating the database, we can use the [SHOW DATABASES](../Reference/SQL-Reference.md) statement and [SHOW DATABASES \<PathPattern>](../Reference/SQL-Reference.md) to view the storage groups. The SQL statements are as follows:

```
IoTDB> SHOW DATABASES
IoTDB> SHOW DATABASES root.**
```

The result is as follows:

```
+-------------+----+-------------------------+-----------------------+-----------------------+
|storage group| ttl|schema_replication_factor|data_replication_factor|time_partition_interval|
+-------------+----+-------------------------+-----------------------+-----------------------+
|    root.sgcc|null|                        2|                      2|                 604800|
|      root.ln|null|                        2|                      2|                 604800|
+-------------+----+-------------------------+-----------------------+-----------------------+
Total line number = 2
It costs 0.060s
```

## Delete Storage Group

User can use the `DELETE STORAGE GROUP <PathPattern>` statement to delete all storage groups matching the pathPattern. Please note the data in the storage group will also be deleted. 

```
IoTDB > DELETE STORAGE GROUP root.ln
IoTDB > DELETE STORAGE GROUP root.sgcc
// delete all data, all timeseries and all storage groups
IoTDB > DELETE STORAGE GROUP root.**
```

## Count Storage Group

User can use the `COUNT STORAGE GROUP <PathPattern>` statement to count the number of storage groups. It is allowed to specify `PathPattern` to count the number of storage groups matching the `PathPattern`.

SQL statement is as follows:

```
IoTDB> SHOW DATABASES
IoTDB> count storage group
IoTDB> count storage group root.*
IoTDB> count storage group root.sgcc.*
IoTDB> count storage group root.sgcc
```

The result is as follows:

```
+-------------+
|     database|
+-------------+
|    root.sgcc|
| root.turbine|
|      root.ln|
+-------------+
Total line number = 3
It costs 0.003s

+-------------+
|     database|
+-------------+
|            3|
+-------------+
Total line number = 1
It costs 0.003s

+-------------+
|     database|
+-------------+
|            3|
+-------------+
Total line number = 1
It costs 0.002s

+-------------+
|     database|
+-------------+
|            0|
+-------------+
Total line number = 1
It costs 0.002s

+-------------+
|     database|
+-------------+
|            1|
+-------------+
Total line number = 1
It costs 0.002s
```
