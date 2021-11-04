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
# Data Concept
## Data Model

A wind power IoT scenario is taken as an example to illustrate how to creat a correct data model in IoTDB.

According to the enterprise organization structure and equipment entity hierarchy, it is expressed as an attribute hierarchy structure, as shown below. The hierarchical from top to bottom is: power group layer - power plant layer - entity layer - measurement layer. ROOT is the root node, and each node of measurement layer is a leaf node. In the process of using IoTDB, the attributes on the path from ROOT node is directly connected to each leaf node with ".", thus forming the name of a timeseries in IoTDB. For example, The left-most path in Figure 2.1 can generate a timeseries named `root.ln.wf01.wt01.status`.

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/122668849-b1c69280-d1ec-11eb-83cb-3b73c40bdf72.png"></center>

Here are the basic concepts of the model involved in IoTDB. 



### Measurement, Entity, Storage Group, Path

#### Measurement (Also called field)

**Univariable or multi-variable measurement**. It is information measured by a detection equipment in an actual scene, and can transform the sensed information into an electrical signal or other desired form of information output and send it to IoTDB.  In IoTDB, all data and paths stored are organized in units of measuements.

#### Sub-measurement

In multi-variable measurements, there are many sub-measurement. For example, GPS is a multi-variable measurements, including three sub-measurement: longitude, dimension and altitude. Multi-variable measurements are usually collected at the same time and share time series.

The univariable measurement overlaps the sub-measurement name with the measurement name. For example, temperature is a univariable measurement.

#### Entity (Also called device)

**An entity** is an equipped with measurements in real scenarios. In IoTDB, all measurements should have their corresponding entities.

#### Storage Group

**A group of entities.** Users can set any prefix path as a storage group. Provided that there are four timeseries `root.ln.wf01.wt01.status`, `root.ln.wf01.wt01.temperature`, `root.ln.wf02.wt02.hardware`, `root.ln.wf02.wt02.status`, two devices `wt01`, `wt02` under the path `root.ln` may belong to the same owner or the same manufacturer, so d1 and d2 are closely related. At this point, the prefix path root.vehicle can be designated as a storage group, which will enable IoTDB to store all devices under it in the same folder. Newly added devices under `root.ln` will also belong to this storage group.

> Note1: A full path (`root.ln.wf01.wt01.status` as in the above example) is not allowed to be set as a storage group.
>
> Note2: The prefix of a timeseries must belong to a storage group. Before creating a timeseries, users must set which storage group the series belongs to. Only timeseries whose storage group is set can be persisted to disk.

Once a prefix path is set as a storage group, the storage group settings cannot be changed.

After a storage group is set, the ancestral layers, children and descendant layers of the corresponding prefix path are not allowed to be set up again (for example, after `root.ln` is set as the storage group, the root layer and `root.ln.wf01` are not allowed to be set as storage groups).

The Layer Name of storage group can only consist of characters, numbers, underscores and hyphen, like `root.storagegroup_1-sg1`.

#### Path

A `path` is an expression that conforms to the following constraints:

```sql
path       
    : layer_name ('.' layer_name)*
    ;
layer_name
    : wildcard? id wildcard?
    | wildcard
    ;
wildcard 
    : '*' 
    | '**'
    ;
```

You can refer to the definition of `id` in [Syntax-Conventions](../IoTDB-SQL-Language/Syntax-Conventions.md).

We call the part of a path divided by `'.'` as a layer (`layer_name`). For example: `root.a.b.c` is a path with 4 layers.

The following are the constraints on the layer (`layer_name`):

* `root` is a reserved character, and it is only allowed to appear at the beginning layer of the time series mentioned below. If `root` appears in other layers, it cannot be parsed and an error will be reported.

* Except for the beginning layer (`root`) of the time series, the characters supported in other layers are as follows:

  * Chinese characters:  `"\u2E80"` to `"\u9FFF"`
  * `"_"，"@"，"#"，"$"`
  * `"A"` to `"Z"`, `"a"` to `"z"`, `"0"` to `"9"`

* In addition to the beginning layer (`root`) of the time series and the storage group layer, other layers also support the use of special strings referenced by \` or `" ` as its name. It should be noted that the quoted string cannot contain `.` characters. Here are some legal examples:

  * root.sg."select"."+-from="."where""where"""."\$", which contains 6 layers: root, sg, select, +-from, where"where", \$
  * root.sg.\`\`\`\`.\`select\`.\`+="from"\`.\`\$\`, which contains 6 layers: root, sg, \`, select, +-"from", \$

* Layer (`layer_name`) cannot start with a digit unless the layer(`layer_name`) is quoted with \` or `"`.

* In particular, if the system is deployed on a Windows machine, the storage group layer name will be case-insensitive. For example, creating both `root.ln` and `root.LN` at the same time is not allowed.

#### Path Pattern

In order to make it easier and faster to express multiple timeseries paths, IoTDB provides users with the path pattern. Users can construct a path pattern by using wildcard `*` and `**`. Wildcard can appear in any layer of the path. 

`*` represents one layer. For example, `root.vehicle.*.sensor1` represents a 4-layer path which is prefixed with `root.vehicle` and suffixed with `sensor1`.

`**` represents (`*`)+, which is one or more layers of `*`. For example, `root.vehicle.device1.*` represents all paths prefixed by `root.vehicle.device1` with layers greater than or equal to 4, like `root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`, etc; `root.vehicle.**.sensor1` represents a path which is prefixed with `root.vehicle` and suffixed with `sensor1` and has at least 4 layers.

> Note1: Wildcard `*` and `**` cannot be placed at the beginning of the path.


### Timeseries

#### Data point

**A "time-value" pair**.

#### Timeseries (A measurement of an entity corresponds to a timeseries. Also called meter, timeline, and tag, parameter in real time database)

**The record of a measurement of an entity on the time axis.** Timeseries is a series of data points.

For example, if entity wt01 in power plant wf01 of power group ln has a measurement named status, its timeseries  can be expressed as: `root.ln.wf01.wt01.status`.

#### Multi-variable timeseries (Also called aligned timeseries, from v0.13)

A multi-variable measurements of an entity corresponds to a multi-variable timeseries. These timeseries are called **multi-variable timeseries**, also called **aligned timeseries**.

Multi-variable timeseries need to be created, inserted and deleted at the same time. However, when querying, you can query each sub-measurement separately.

By using multi-variable timeseries, the timestamp columns of a group of multi-variable timeseries need to be stored only once in memory and disk when inserting data, instead of once per timeseries:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/114125919-f4850800-9929-11eb-8211-81d4c04af1ec.png">

In the following chapters of data definition language, data operation language and Java Native Interface, various operations related to multi-variable timeseries will be introduced one by one.

#### Timestamp

The timestamp is the time point at which data is produced. It includes absolute timestamps and relative timestamps. For detailed description, please go to Data Type doc.



### Measurement Template

#### Measurement template (From v0.13)

In the actual scenario, many entities collect the same measurements, that is, they have the same measurements name and type. A **measurement template** can be declared to define the collectable measurements set. Measurement template helps save memory by implementing schema sharing. For detailed description, please refer to Measurement Template doc.

In the following chapters of, data definition language, data operation language and Java Native Interface, various operations related to measurement template will be introduced one by one.
