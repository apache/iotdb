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

# Storage Group Management
## Create Storage Group

According to the storage model we can set up the corresponding storage group. Two SQL statements are supported for creating storage groups, as follows:

```
IoTDB > set storage group to root.ln
IoTDB > create storage group root.sgcc
```

We can thus create two storage groups using the above two SQL statements.

It is worth noting that when the path itself or the parent/child layer of the path is already set as a storage group, the path is then not allowed to be set as a storage group. For example, it is not feasible to set `root.ln.wf01` as a storage group when two storage groups `root.ln` and `root.sgcc` exist. The system gives the corresponding error prompt as shown below:

```
IoTDB> set storage group to root.ln.wf01
Msg: 300: root.ln has already been set to storage group.
IoTDB> create storage group root.ln.wf01
Msg: 300: root.ln has already been set to storage group.
```
The LayerName of storage group can only be characters, numbers, underscores and hyphens. 
 
Besides, if deploy on Windows system, the LayerName is case-insensitive, which means it's not allowed to set storage groups `root.ln` and `root.LN` at the same time.

## Show Storage Group

After creating the storage group, we can use the [SHOW STORAGE GROUP](../Reference/SQL-Reference.md) statement and [SHOW STORAGE GROUP \<PathPattern>](../Reference/SQL-Reference.md) to view the storage groups. The SQL statements are as follows:

```
IoTDB> show storage group
IoTDB> show storage group root.**
```

The result is as follows:

```
+-------------+
|storage group|
+-------------+
|    root.sgcc|
|      root.ln|
+-------------+
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
