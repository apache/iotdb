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

# Schema Template

IoTDB supports the schema template function, enabling different entities of the same type to share metadata, reduce the memory usage of metadata, and simplify the management of numerous entities and measurements.

Note: The `schema` keyword in the following statements can be omitted.

## Create Schema Template

The SQL syntax for creating a metadata template is as follows:

```sql
CREATE SCHEMA TEMPLATE <templateName> ALIGNED? '(' <measurementId> <attributeClauses> [',' <measurementId> <attributeClauses>]+ ')'
```

**Example 1:** Create a template containing two non-aligned timeseires

```shell
IoTDB> create schema template t1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

**Example 2:** Create a template containing a group of aligned timeseires

```shell
IoTDB> create schema template t2 aligned (lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla)
```

The` lat` and `lon` measurements are aligned.

## Set Schema Template

After a schema template is created, it should be set to specific path before creating related timeseries or insert data.

**It should be ensured that the related database has been set before setting template.**

**It is recommended to set schema template to database path. It is not suggested to set schema template to some path above database**


The SQL Statement for setting schema template is as follow:

```shell
IoTDB> set schema template t1 to root.sg1.d1
```

## Activate Schema Template

After setting the schema template, with the system enabled to auto create schema, you can insert data into the timeseries. For example, suppose there's a database root.sg1 and t1 has been set to root.sg1.d1, then timeseries like root.sg1.d1.temperature and root.sg1.d1.status are available and data points can be inserted.


**Attention**: Before inserting data or the system not enabled to auto create schema, timeseries defined by the schema template will not be created. You can use the following SQL statement to create the timeseries or activate the schema template, act before inserting data:

```shell
IoTDB> create timeseries using schema template on root.sg1.d1
```

**Example:** Execute the following statement
```shell
IoTDB> set schema template t1 to root.sg1.d1
IoTDB> set schema template t2 to root.sg1.d2
IoTDB> create timeseries using schema template on root.sg1.d1
IoTDB> create timeseries using schema template on root.sg1.d2
```

Show the time series:
```sql
show timeseries root.sg1.**
````

```shell
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+--------+-------------------+
|             timeseries|alias|     database|dataType|encoding|compression|tags|attributes|deadband|deadband parameters|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+--------+-------------------+
|root.sg1.d1.temperature| null|     root.sg1|   FLOAT|     RLE|     SNAPPY|null|      null|    null|               null|
|     root.sg1.d1.status| null|     root.sg1| BOOLEAN|   PLAIN|     SNAPPY|null|      null|    null|               null|
|        root.sg1.d2.lon| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|    null|               null|
|        root.sg1.d2.lat| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|    null|               null|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+--------+-------------------+
```

Show the devices:
```sql
show devices root.sg1.**
````

```shell
+---------------+---------+
|        devices|isAligned|
+---------------+---------+
|    root.sg1.d1|    false|
|    root.sg1.d2|     true|
+---------------+---------+
````

## Show Schema Template

- Show all schema templates

The SQL statement looks like this:

```shell
IoTDB> show schema templates
```

The execution result is as follows:
```shell
+-------------+
|template name|
+-------------+
|           t2|
|           t1|
+-------------+
```

- Show nodes under in schema template

The SQL statement looks like this:

```shell
IoTDB> show nodes in schema template t1
```

The execution result is as follows:
```shell
+-----------+--------+--------+-----------+
|child nodes|dataType|encoding|compression|
+-----------+--------+--------+-----------+
|temperature|   FLOAT|     RLE|     SNAPPY|
|     status| BOOLEAN|   PLAIN|     SNAPPY|
+-----------+--------+--------+-----------+
```

- Show the path prefix where a schema template is set

```shell
IoTDB> show paths set schema template t1
```

The execution result is as follows:
```shell
+-----------+
|child paths|
+-----------+
|root.sg1.d1|
+-----------+
```

- Show the path prefix where a schema template is used (i.e. the time series has been created)

```shell
IoTDB> show paths using schema template t1
```

The execution result is as follows:
```shell
+-----------+
|child paths|
+-----------+
|root.sg1.d1|
+-----------+
```

## Deactivate Schema Template

To delete a group of timeseries represented by schema template, namely deactivate the schema template, use the following SQL statement:

```shell
IoTDB> delete timeseries of schema template t1 from root.sg1.d1
```

or

```shell
IoTDB> deactivate schema template t1 from root.sg1.d1
```

The deactivation supports batch process. 

```shell
IoTDB> delete timeseries of schema template t1 from root.sg1.*, root.sg2.*
```

or

```shell
IoTDB> deactivate schema template t1 from root.sg1.*, root.sg2.*
```

If the template name is not provided in sql, all template activation on paths matched by given path pattern will be removed.

## Unset Schema Template

The SQL Statement for unsetting schema template is as follow:

```shell
IoTDB> unset schema template t1 from root.sg1.d1
```

**Attention**: It should be guaranteed that none of the timeseries represented by the target schema template exists, before unset it. It can be achieved by deactivation operation.

## Drop Schema Template

The SQL Statement for dropping schema template is as follow:

```shell
IoTDB> drop schema template t1
```

**Attention**: Dropping an already set template is not supported.