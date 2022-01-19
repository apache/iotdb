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
CREATE SCHEMA? TEMPLATE <templateName> '(' templateMeasurementClause [',' templateMeasurementClause]+ ')'

templateMeasurementClause
    : <measurementId> <attributeClauses> # non-aligned measurement
    | <deviceId> '(' <measurementId> <attributeClauses> [',' <measurementId> <attributeClauses>]+ ')' # a group of aligned measurements
    ;
```

For example:

```shell
IoTDB> create schema template temp1(GPS (lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla compression=SNAPPY), status BOOLEAN encoding=PLAIN compression=SNAPPY)
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
|temp1|
+-------------+
````

- Show nodes under in schema template

The SQL statement looks like this:

```shell
IoTDB> show nodes in schema template templ1
````

The execution result is as follows:
```shell
+------------+
|child nodes|
+------------+
| status|
|GPS.lat|
| GPS.lon|
+------------+
````

- Show the path prefix where a schema template is set

```shell
IoTDB> show paths set schema template templ1
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
IoTDB> show paths using schema template templ1
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