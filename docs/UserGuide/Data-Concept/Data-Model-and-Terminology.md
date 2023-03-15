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

# Data Model

A wind power IoT scenario is taken as an example to illustrate how to creat a correct data model in IoTDB.

According to the enterprise organization structure and equipment entity hierarchy, it is expressed as an attribute hierarchy structure, as shown below. The hierarchical from top to bottom is: power group layer - power plant layer - entity layer - measurement layer. ROOT is the root node, and each node of measurement layer is a leaf node. In the process of using IoTDB, the attributes on the path from ROOT node is directly connected to each leaf node with ".", thus forming the name of a timeseries in IoTDB. For example, The left-most path in Figure 2.1 can generate a timeseries named `root.ln.wf01.wt01.status`.

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/122668849-b1c69280-d1ec-11eb-83cb-3b73c40bdf72.png"></center>

Here are the basic concepts of the model involved in IoTDB. 

## Measurement, Entity, Database, Path

### Measurement (Also called field)

It is information measured by detection equipment in an actual scene and can transform the sensed information into an electrical signal or other desired form of information output and send it to IoTDB.  In IoTDB, all data and paths stored are organized in units of measurements.

### Entity (Also called device)

**An entity** is an equipped with measurements in real scenarios. In IoTDB, all measurements should have their corresponding entities.

### Database

**A group of entities.** Users can create any prefix path as a database. Provided that there are four timeseries `root.ln.wf01.wt01.status`, `root.ln.wf01.wt01.temperature`, `root.ln.wf02.wt02.hardware`, `root.ln.wf02.wt02.status`, two devices `wt01`, `wt02` under the path `root.ln` may belong to the same owner or the same manufacturer, so d1 and d2 are closely related. At this point, the prefix path root.vehicle can be designated as a database, which will enable IoTDB to store all devices under it in the same folder. Newly added devices under `root.ln` will also belong to this database.

> Note1: A full path (`root.ln.wf01.wt01.status` as in the above example) is not allowed to be set as a database.
>
> Note2: The prefix of a timeseries must belong to a database. Before creating a timeseries, users must set which database the series belongs to. Only timeseries whose database is set can be persisted to disk.
> 
> Note3: The number of character in the path as database, including `root.`, shall not exceed 64.

Once a prefix path is set as a database, the database settings cannot be changed.

After a database is set, the ancestral layers, children and descendant layers of the corresponding prefix path are not allowed to be set up again (for example, after `root.ln` is set as the database, the root layer and `root.ln.wf01` are not allowed to be created as database).

The Layer Name of database can only consist of characters, numbers, and underscores, like `root.storagegroup_1`.

### Path

A `path` is an expression that conforms to the following constraints:

```sql
path       
    : nodeName ('.' nodeName)*
    ;
    
nodeName
    : wildcard? identifier wildcard?
    | wildcard
    ;
    
wildcard 
    : '*' 
    | '**'
    ;
```

We call the part of a path divided by `'.'` as a  `node` or `nodeName`. For example: `root.a.b.c` is a path with 4 nodes.

The following are the constraints on the `nodeName`:

* `root` is a reserved character, and it is only allowed to appear at the beginning layer of the time series mentioned below. If `root` appears in other layers, it cannot be parsed and an error will be reported.
* Except for the beginning layer (`root`) of the time series, the characters supported in other layers are as follows:

  * [ 0-9 a-z A-Z _ ] （letters, numbers, underscore)
  * ['\u2E80'..'\u9FFF'] （Chinese characters）
* In particular, if the system is deployed on a Windows machine, the database layer name will be case-insensitive. For example, creating both `root.ln` and `root.LN` at the same time is not allowed.
* If you want to use special characters in `nodeName`, you can quote it with back quote, detailed information can be found from charpter Syntax-Conventions,click here: [Syntax-Conventions](https://iotdb.apache.org/UserGuide/Master/Syntax-Conventions/Literal-Values.html).

### Path Pattern

In order to make it easier and faster to express multiple timeseries paths, IoTDB provides users with the path pattern. Users can construct a path pattern by using wildcard `*` and `**`. Wildcard can appear in any node of the path. 

`*` represents one node. For example, `root.vehicle.*.sensor1` represents a 4-node path which is prefixed with `root.vehicle` and suffixed with `sensor1`.

`**` represents (`*`)+, which is one or more nodes of `*`. For example, `root.vehicle.device1.**` represents all paths prefixed by `root.vehicle.device1` with nodes num greater than or equal to 4, like `root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`, etc; `root.vehicle.**.sensor1` represents a path which is prefixed with `root.vehicle` and suffixed with `sensor1` and has at least 4 nodes.

> Note1: Wildcard `*` and `**` cannot be placed at the beginning of the path.


## Timeseries

### Timestamp

The timestamp is the time point at which data is produced. It includes absolute timestamps and relative timestamps. For detailed description, please go to [Data Type doc](./Data-Type.md).

### Data point

**A "time-value" pair**.

### Timeseries

**The record of a measurement of an entity on the time axis.** Timeseries is a series of data points.

A measurement of an entity corresponds to a timeseries. 

Also called meter, timeline, and tag, parameter in real time database.

For example, if entity wt01 in power plant wf01 of power group ln has a measurement named status, its timeseries  can be expressed as: `root.ln.wf01.wt01.status`.

### Aligned timeseries

There is a situation that multiple measurements of an entity are sampled simultaneously in practical applications, forming multiple timeseries with the same time column. Such a group of timeseries can be modeled as aligned timeseries in Apache IoTDB.

The timestamp columns of a group of aligned timeseries need to be stored only once in memory and disk when inserting data, instead of once per timeseries.

It would be best if you created a group of aligned timeseries at the same time.

You cannot create non-aligned timeseries under the entity to which the aligned timeseries belong, nor can you create aligned timeseries under the entity to which the non-aligned timeseries belong.

When querying, you can query each timeseries separately.

When inserting data, it is allowed to insert null value in the aligned timeseries.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/114125919-f4850800-9929-11eb-8211-81d4c04af1ec.png">

In the following chapters of data definition language, data operation language and Java Native Interface, various operations related to aligned timeseries will be introduced one by one.

## Schema Template

In the actual scenario, many entities collect the same measurements, that is, they have the same measurements name and type. A **schema template** can be declared to define the collectable measurements set. Schema template helps save memory by implementing schema sharing. For detailed description, please refer to [Schema Template doc](./Schema-Template.md).

In the following chapters of, data definition language, data operation language and Java Native Interface, various operations related to schema template will be introduced one by one.
