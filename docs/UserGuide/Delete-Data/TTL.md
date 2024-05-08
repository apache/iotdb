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

# TTL

IoTDB supports storage-level TTL settings, which means it is able to delete old data automatically and periodically. The benefit of using TTL is that hopefully you can control the total disk space usage and prevent the machine from running out of disks. Moreover, the query performance may downgrade as the total number of files goes up and the memory usage also increase as there are more files. Timely removing such files helps to keep at a high query performance level and reduce memory usage.

The default unit of TTL is milliseconds. If the time precision in the configuration file changes to another, the TTL is still set to milliseconds.

## Set TTL

The SQL Statement for setting TTL is as follow:

```
IoTDB> set ttl to root.ln 3600000
```

This example means that for data in `root.ln`, only 3600000 ms, that is, the latest 1 hour will remain, the older one is removed or made invisible.

```
IoTDB> set ttl to root.sgcc.** 3600000
```
It supports setting TTL for databases in a path. This example represents setting TTL for all databases in the `root.sgcc` path.
```
IoTDB> set ttl to root.** 3600000
```
This example represents setting TTL for all databases.

## Unset TTL

To unset TTL, we can use follwing SQL statement:

```
IoTDB> unset ttl to root.ln
```

After unset TTL, all data will be accepted in `root.ln`.
```
IoTDB> unset ttl to root.sgcc.**
```

Unset the TTL setting for all databases in the `root.sgcc` path.
```
IoTDB> unset ttl to root.**
```

Unset the TTL setting for all databases.

## Show TTL

To Show TTL, we can use following SQL statement:

```
IoTDB> SHOW ALL TTL
IoTDB> SHOW TTL ON StorageGroupNames
```

The SHOW ALL TTL example gives the TTL for all databases.
The SHOW TTL ON root.ln,root.sgcc,root.DB example shows the TTL for the three storage 
groups specified.
Note: the TTL for databases that do not have a TTL set will display as null.

```
IoTDB> show all ttl
+----------+-------+
| database|ttl(ms)|
+---------+-------+
|  root.ln|3600000|
|root.sgcc|   null|
|  root.DB|3600000|
+----------+-------+
```