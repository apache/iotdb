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

## Schema Template

IoTDB supports the schema template function, enabling different entities of the same type to share metadata, reduce the memory usage of metadata, and simplify the management of numerous entities and measurements.

### Create Schema Template

The SQL Statement for creating schema template is as follow:

```
IoTDB> create schema template temp1(GPS(lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla compression=SNAPPY), status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

The` lat` and `lon` measurements under the `GPS` device are aligned.

### Set Schema Template

The SQL Statement for setting schema template is as follow:

```
IoTDB> set schema template temp1 to root.ln.wf01
```

After setting the schema template, you can insert data into the timeseries. For example, suppose there's a storage group root.ln and temp1 has been set to root.ln.wf01, then timeseries like root.ln.wf01.GPS.lat and root.ln.wf01.status are available and data points can be inserted.

**Attention**: Before inserting data, timeseries defined by the schema template will not be created. You can use the following SQL statement to create the timeseries before inserting data:

```
IoTDB> create timeseries of schema template on root.ln.wf01
```

### Uset Schema Template

The SQL Statement for unsetting schema template is as follow:

```
IoTDB> unset schema template temp1 from root.beijing
```

**Attention**: Unsetting the template from entities, which have already inserted records using the template, is not supported.
