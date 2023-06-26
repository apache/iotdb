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

## Create Database

According to the storage model we can set up the corresponding database. Two SQL statements are supported for creating databases, as follows:

```
IoTDB > create database root.ln
IoTDB > create database root.sgcc
```

We can thus create two databases using the above two SQL statements.

It is worth noting that when the path itself or the parent/child layer of the path is already created as database, the path is then not allowed to be created as database. For example, it is not feasible to create `root.ln.wf01` as database when two databases `root.ln` and `root.sgcc` exist. The system gives the corresponding error prompt as shown below:

```
IoTDB> CREATE DATABASE root.ln.wf01
Msg: 300: root.ln has already been created as database.
IoTDB> create database root.ln.wf01
Msg: 300: root.ln has already been created as database.
```
The LayerName of database can only be characters, numbers, underscores. If you want to set it to pure numbers or contain other characters, you need to enclose the database name with backticks (``). 
 
Besides, if deploy on Windows system, the LayerName is case-insensitive, which means it's not allowed to create databases `root.ln` and `root.LN` at the same time.

## Show Databases

After creating the database, we can use the [SHOW DATABASES](../Reference/SQL-Reference.md) statement and [SHOW DATABASES \<PathPattern>](../Reference/SQL-Reference.md) to view the databases. The SQL statements are as follows:

```
IoTDB> SHOW DATABASES
IoTDB> SHOW DATABASES root.**
```

The result is as follows:

```
+-------------+----+-------------------------+-----------------------+-----------------------+
|database| ttl|schema_replication_factor|data_replication_factor|time_partition_interval|
+-------------+----+-------------------------+-----------------------+-----------------------+
|    root.sgcc|null|                        2|                      2|                 604800|
|      root.ln|null|                        2|                      2|                 604800|
+-------------+----+-------------------------+-----------------------+-----------------------+
Total line number = 2
It costs 0.060s
```

## Delete Database

User can use the `DELETE DATABASE <PathPattern>` statement to delete all databases matching the pathPattern. Please note the data in the database will also be deleted. 

```
IoTDB > DELETE DATABASE root.ln
IoTDB > DELETE DATABASE root.sgcc
// delete all data, all timeseries and all databases
IoTDB > DELETE DATABASE root.**
```

## Count Databases

User can use the `COUNT DATABASE <PathPattern>` statement to count the number of databases. It is allowed to specify `PathPattern` to count the number of databases matching the `PathPattern`.

SQL statement is as follows:

```
IoTDB> count databases
IoTDB> count databases root.*
IoTDB> count databases root.sgcc.*
IoTDB> count databases root.sgcc
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

### Setting up heterogeneous databases (Advanced operations)

Under the premise of familiar with IoTDB metadata modeling, 
users can set up heterogeneous databases in IoTDB to cope with different production needs.

Currently, the following database heterogeneous parameters are supported:

| Parameter                 | Type    | Description                                   |
|---------------------------|---------|-----------------------------------------------|
| TTL                       | Long    | TTL of the Database                           |
| SCHEMA_REPLICATION_FACTOR | Integer | The schema replication number of the Database |
| DATA_REPLICATION_FACTOR   | Integer | The data replication number of the Database   |
| SCHEMA_REGION_GROUP_NUM   | Integer | The SchemaRegionGroup number of the Database  |
| DATA_REGION_GROUP_NUM     | Integer | The DataRegionGroup number of the Database    |

Note the following when configuring heterogeneous parameters:
+ TTL and TIME_PARTITION_INTERVAL must be positive integers.
+ SCHEMA_REPLICATION_FACTOR and DATA_REPLICATION_FACTOR must be smaller than or equal to the number of deployed DataNodes.
+ The function of SCHEMA_REGION_GROUP_NUM and DATA_REGION_GROUP_NUM are related to the parameter `schema_region_group_extension_policy` and `data_region_group_extension_policy` in iotdb-common.properties configuration file. Take DATA_REGION_GROUP_NUM as an example:
If `data_region_group_extension_policy=CUSTOM` is set, DATA_REGION_GROUP_NUM serves as the number of DataRegionGroups owned by the Database.
If `data_region_group_extension_policy=AUTO`, DATA_REGION_GROUP_NUM is used as the lower bound of the DataRegionGroup quota owned by the Database. That is, when the Database starts writing data, it will have at least this number of DataRegionGroups.

Users can set any heterogeneous parameters when creating a Database, or adjust some heterogeneous parameters during a stand-alone/distributed IoTDB run.

#### Set heterogeneous parameters when creating a Database

The user can set any of the above heterogeneous parameters when creating a Database. The SQL statement is as follows:

```
CREATE DATABASE prefixPath (WITH databaseAttributeClause (COMMA? databaseAttributeClause)*)?
```

For example:
```
CREATE DATABASE root.db WITH SCHEMA_REPLICATION_FACTOR=1, DATA_REPLICATION_FACTOR=3, SCHEMA_REGION_GROUP_NUM=1, DATA_REGION_GROUP_NUM=2;
```

#### Adjust heterogeneous parameters at run time

Users can adjust some heterogeneous parameters during the IoTDB runtime, as shown in the following SQL statement:

```
ALTER DATABASE prefixPath WITH databaseAttributeClause (COMMA? databaseAttributeClause)*
```

For example:
```
ALTER DATABASE root.db WITH SCHEMA_REGION_GROUP_NUM=1, DATA_REGION_GROUP_NUM=2;
```

Note that only the following heterogeneous parameters can be adjusted at runtime:
+ SCHEMA_REGION_GROUP_NUM
+ DATA_REGION_GROUP_NUM

#### Show heterogeneous databases

The user can query the specific heterogeneous configuration of each Database, and the SQL statement is as follows:

```
SHOW DATABASES DETAILS prefixPath?
```

For example:

```
IoTDB> SHOW DATABASES DETAILS
+--------+--------+-----------------------+---------------------+---------------------+--------------------+-----------------------+-----------------------+------------------+---------------------+---------------------+
|Database|     TTL|SchemaReplicationFactor|DataReplicationFactor|TimePartitionInterval|SchemaRegionGroupNum|MinSchemaRegionGroupNum|MaxSchemaRegionGroupNum|DataRegionGroupNum|MinDataRegionGroupNum|MaxDataRegionGroupNum|
+--------+--------+-----------------------+---------------------+---------------------+--------------------+-----------------------+-----------------------+------------------+---------------------+---------------------+
|root.db1|    null|                      1|                    3|            604800000|                   0|                      1|                      1|                 0|                    2|                    2|
|root.db2|86400000|                      1|                    1|            604800000|                   0|                      1|                      1|                 0|                    2|                    2|
|root.db3|    null|                      1|                    1|            604800000|                   0|                      1|                      1|                 0|                    2|                    2|
+--------+--------+-----------------------+---------------------+---------------------+--------------------+-----------------------+-----------------------+------------------+---------------------+---------------------+
Total line number = 3
It costs 0.058s
```

The query results in each column are as follows:
+ The name of the Database
+ The TTL of the Database
+ The schema replication number of the Database
+ The data replication number of the Database
+ The time partition interval of the Database
+ The current SchemaRegionGroup number of the Database
+ The required minimum SchemaRegionGroup number of the Database
+ The permitted maximum SchemaRegionGroup number of the Database
+ The current DataRegionGroup number of the Database
+ The required minimum DataRegionGroup number of the Database
+ The permitted maximum DataRegionGroup number of the Database