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

# Background

The database is specified by the user display.
Use the statement "CREATE DATABASE" to create the database.
Each database has a corresponding StorageGroupProcessor.

To ensure eventually consistency, a insert lock (exclusive lock) is used to synchronize each insert request in each database.
So the server side parallelism of data ingestion is equal to the number of database.

# Problem

From background, we can infer that the parallelism of data ingestion of IoTDB is max(num of client, server side parallelism), which equals to max(num of client, num of database)

The concept of database usually is related to real world entity such as factory, location, country and so on.
The number of databases may be small which makes the parallelism of data ingestion of IoTDB insufficient. We can't jump out of this dilemma even we start hundreds of client for ingestion.

# Solution

Our idea is to group devices into buckets and change the granularity of synchronization from database level to device buckets level.

In detail, we use hash to group different devices into buckets called data region. 
For example, one device called "root.sg.d"(assume it's database is "root.sg") is belonged to data region "root.sg.[hash("root.sg.d") mod num_of_data_region]"

# Usage

To use data region, you can set this config below:

```
data_region_num
```

Recommended value is [data region number] = [CPU core number] / [user-defined database number]

For more information, you can refer to [this page](../Reference/DataNode-Config-Manual.md).