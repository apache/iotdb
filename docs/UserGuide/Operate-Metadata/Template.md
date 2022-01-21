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
CREATE SCHEMA? TEMPLATE <templateName> ALIGNED '(' templateMeasurementClause [',' templateMeasurementClause]+ ')'

templateMeasurementClause
    : <measurementId> <attributeClauses> # single measurement
    | <deviceId> ALIGNED '(' <measurementId> <attributeClauses> [',' <measurementId> <attributeClauses>]+ ')' # a group of aligned measurements
    ;
```

**Example 1:** Create a template containing two non-aligned timeseires

```shell
IoTDB> create schema template temp1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

**Example 2:** Create a template containing a group of aligned timeseires

```shell
IoTDB> create schema template temp2 aligned (lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla)
```

**Example 3:** Creating a template that mixes aligned and non-aligned timeseires

```shell
IoTDB> create schema template temp3 (GPS aligned (lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla compression=SNAPPY), status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

The` lat` and `lon` measurements under the `GPS` device are aligned.

## Set Schema Template

The SQL Statement for setting schema template is as follow:

```shell
IoTDB> set schema template temp1 to root.ln.wf01
```

After setting the schema template, you can insert data into the timeseries. For example, suppose there's a storage group root.ln and temp1 has been set to root.ln.wf01, then timeseries like root.ln.wf01.GPS.lat and root.ln.wf01.status are available and data points can be inserted.

**Attention**: Before inserting data, timeseries defined by the schema template will not be created. You can use the following SQL statement to create the timeseries before inserting data:

```shell
IoTDB> create timeseries of schema template on root.ln.wf01
```

**Example:** Execute the following statement
```shell
set schema template temp1 to root.sg1.d1
set schema template temp2 to root.sg1.d2
set schema template temp3 to root.sg1.d3
create timeseries of schema template on root.sg1.d1
create timeseries of schema template on root.sg1.d2
create timeseries of schema template on root.sg1.d3
````

Show the time series:
```sql
show timeseries root.sg1.**
````

```shell
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+
|             timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.sg1.d1.temperature| null|     root.sg1|   FLOAT|     RLE|     SNAPPY|null|      null|
|     root.sg1.d1.status| null|     root.sg1| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
|        root.sg1.d2.lon| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|
|        root.sg1.d2.lat| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|
|    root.sg1.d3.GPS.lon| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|
|    root.sg1.d3.GPS.lat| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|
|     root.sg1.d3.status| null|     root.sg1| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+
````

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
|    root.sg1.d3|    false|
|root.sg1.d3.GPS|     true|
+---------------+---------+
````

## Show Schema Template

- Show all schema templates

The SQL statement looks like this:

```shell
IoTDB> show schema templates on root.ln.wf01
````

The execution result is as follows:
```shell
+-------------+
|template name|
+-------------+
|        temp2|
|        temp3|
|        temp1|
+-------------+
````

- Show nodes under in schema template

The SQL statement looks like this:

```shell
IoTDB> show nodes in schema template temp3
````

The execution result is as follows:
```shell
+-----------+--------+--------+-----------+
|child nodes|dataType|encoding|compression|
+-----------+--------+--------+-----------+
|    GPS.lon|   FLOAT| GORILLA|     SNAPPY|
|    GPS.lat|   FLOAT| GORILLA|     SNAPPY|
|     status| BOOLEAN|   PLAIN|     SNAPPY|
+-----------+--------+--------+-----------+
````

- Show the path prefix where a schema template is set

```shell
IoTDB> show paths set schema template temp1
````

The execution result is as follows:
```shell
+------------+
| child paths|
+------------+
|root.ln.wf01|
+------------+
````

- Show the path prefix where a schema template is used (i.e. the time series has been created)

```shell
IoTDB> show paths using schema template temp1
````

The execution result is as follows:
```shell
+------------+
| child paths|
+------------+
|root.ln.wf01|
+------------+
````

## Uset Schema Template

The SQL Statement for unsetting schema template is as follow:

```shell
IoTDB> unset schema template temp1 from root.beijing
```

**Attention**: Unsetting the template from entities, which have already inserted records using the template, is not supported.

## Drop Schema Template

The SQL Statement for dropping schema template is as follow:

```shell
IoTDB> drop schema template temp1
```

**Attention**: Dropping an already set template is not supported.