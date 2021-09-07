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

Here are the basic concepts of the model involved in IoTDB:


**Attribute hierarchy organization structure**


**Univariable or multi-variable measurement**. It is information measured by a detection equipment in an actual scene, and can transform the sensed information into an electrical signal or other desired form of information output and send it to IoTDB.  In IoTDB, all data and paths stored are organized in units of measuements.

* Sub-measurement

In multi-variable measurements, there are many sub-measurement. For example, GPS is a multi-variable measurements, including three sub-measurement: longitude, dimension and altitude. Multi-variable measurements are usually collected at the same time and share time series.

The univariable measurement overlaps the sub-measurement name with the measurement name. For example, temperature is a univariable measurement.

* Entity (Also called device)


A sensor is a detection equipment in an actual scene, which can sense the information to be measured, and can transform the sensed information into an electrical signal or other desired form of information output and send it to IoTDB.  In IoTDB, all stored data and paths are organized as sensors.


* Storage Group

**A group of entities.** Users can set any prefix path as a storage group. Provided that there are four timeseries `root.ln.wf01.wt01.status`, `root.ln.wf01.wt01.temperature`, `root.ln.wf02.wt02.hardware`, `root.ln.wf02.wt02.status`, two devices `wt01`, `wt02` under the path `root.ln` may belong to the same owner or the same manufacturer, so d1 and d2 are closely related. At this point, the prefix path root.vehicle can be designated as a storage group, which will enable IoTDB to store all devices under it in the same folder. Newly added devices under `root.ln` will also belong to this storage group.

> Note1: A full path (`root.ln.wf01.wt01.status` as in the above example) is not allowed to be set as a storage group.
>
> Note2: The prefix of a timeseries must belong to a storage group. Before creating a timeseries, users must set which storage group the series belongs to. Only timeseries whose storage group is set can be persisted to disk.

Once a prefix path is set as a storage group, the storage group settings cannot be changed.


Setting a reasonable number of storage groups can lead to performance gains: there will be nither too many storage files (folders) causing frequent swiching of IO to slow down the system (which will also take up a lot of memory and result in frequent memory-file switching), nor too few storage floders (which reduces concurrency) causing write commonds to block.


The Layer Name of storage group can only consist of characters, numbers, underscores and hyphen, like `root.storagegroup_1-sg1`.

* Data point

**A "time-value" pair**.

* Timeseries (A measurement of an entity corresponds to a timeseries. Also called meter, timeline, and tag, parameter in real time database)

**The record of a measurement of an entity on the time axis.** Timeseries is a series of data points.

For example, if entity wt01 in power plant wf01 of power group ln has a measurement named status, its timeseries  can be expressed as: `root.ln.wf01.wt01.status`.


* Multi-variable timeseries (Also called aligned timeseries, from v0.13)

A multi-variable measurements of an entity corresponds to a multi-variable timeseries. These timeseries are called **multi-variable timeseries**, also called **aligned timeseries**.

Multi-variable timeseries need to be created, inserted and deleted at the same time. However, when querying, you can query each sub-measurement separately.

By using multi-variable timeseries, the timestamp columns of a group of multi-variable timeseries need to be stored only once in memory and disk when inserting data, instead of once per timeseries:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/114125919-f4850800-9929-11eb-8211-81d4c04af1ec.png">

In the following chapters of data definition language, data operation language and Java Native Interface, various operations related to multi-variable timeseries will be introduced one by one.


* Measurement template (From v0.13)

In the actual scenario, many entities collect the same measurements, that is, they have the same measurements name and type. A **measurement template** can be declared to define the collectable measurements set. Measurement template is hung on any node of the tree data pattern, which means that all entities under the node have the same measurements set.

Currently you can only set one **measurement template** on a specific path. An entity will use it's own measurement template or nearest ancestor's measurement template.

In the following chapters of data definition language, data operation language and Java Native Interface, various operations related to measurement template will be introduced one by one.

* Path

In IoTDB, a path is an expression that conforms to the following constraints:

```
path: LayerName (DOT LayerName)+
LayerName: Identifier | STAR
```

Among them, STAR is "*" and DOT is ".".

We call the middle part of a path between two "." as a layer, and thus `root.A.B.C` is a path with four layers. 

It is worth noting that in the path, root is a reserved character, which is only allowed to appear at the beginning of the time series mentioned below. If root appears in other layers, it cannot be parsed and an error is reported.

Single quotes are not allowed in the path. If you want to use special characters such as "." in LayerName, use double quotes. For example, `root.sg."d.1"."s.1"`. Users can use escape characters nest double quotation marks, for example, root.sg.d1."s.\"t\"1". 

The characters supported in LayerName without double quotes are as below:

* Chinese characters '\u2E80' to '\u9FFF'
* '+', '&', '%', '$', '#', '@', '/', '_', '-', ':'
* 'A' to 'Z', 'a' to 'z', '0' to '9'
* '[', ']' (eg. 's[1', 's[1]', s[ab]')

'-' and ':' cannot be the first character. '+' cannot use alone.

> Note: the LayerName of storage group can only be characters, numbers, underscores and hyphen. 
> 
> Besides, if deploy on Windows system, the LayerName is case-insensitive, which means it's not allowed to set storage groups `root.ln` and `root.LN` at the same time.


* Prefix Path

The prefix path refers to the path where the prefix of a timeseries path is located. A prefix path contains all timeseries paths prefixed by the path. For example, suppose that we have three sensors: `root.vehicle.device1.sensor1`, `root.vehicle.device1.sensor2`, `root.vehicle.device2.sensor1`, the prefix path `root.vehicle.device1` contains two timeseries paths `root.vehicle.device1.sensor1` and `root.vehicle.device1.sensor2` while `root.vehicle.device2.sensor1` is excluded.

* Path With Star

In order to make it easier and faster to express multiple timeseries paths or prefix paths, IoTDB provides users with the path pith star. `*` can appear in any layer of the path. According to the position where `*` appears, the path with star can be divided into two types:

`*` appears at the end of the path;

`*` appears in the middle of the path;

When `*` appears at the end of the path, it represents (`*`)+, which is one or more layers of `*`. For example, `root.vehicle.device1.*` represents all paths prefixed by `root.vehicle.device1` with layers greater than or equal to 4, like `root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`, etc.

When `*` appears in the middle of the path, it represents `*` itself, i.e., a layer. For example, `root.vehicle.*.sensor1` represents a 4-layer path which is prefixed with `root.vehicle` and suffixed with `sensor1`.   

> Note1: `*` cannot be placed at the beginning of the path.

> Note2: A path with `*` at the end has the same meaning as a prefix path, e.g., `root.vehicle.*` and `root.vehicle` is the same.

> Note3: When * create created, the following path cannot contain * at the same time.

* Timestamp

The timestamp is the time point at which data is produced. It includes absolute timestamps and relative timestamps

* Absolute timestamp

Absolute timestamps in IoTDB are divided into two types: LONG and DATETIME (including DATETIME-INPUT and DATETIME-DISPLAY). When a user inputs a timestamp, he can use a LONG type timestamp or a DATETIME-INPUT type timestamp, and the supported formats of the DATETIME-INPUT type timestamp are shown in the table below:

<center>**Supported formats of DATETIME-INPUT type timestamp**


|Format|
|:---:|
|yyyy-MM-dd HH:mm:ss|
|yyyy/MM/dd HH:mm:ss|
|yyyy.MM.dd HH:mm:ss|
|yyyy-MM-dd'T'HH:mm:ss|
|yyyy/MM/dd'T'HH:mm:ss|
|yyyy.MM.dd'T'HH:mm:ss|
|yyyy-MM-dd HH:mm:ssZZ|
|yyyy/MM/dd HH:mm:ssZZ|
|yyyy.MM.dd HH:mm:ssZZ|
|yyyy-MM-dd'T'HH:mm:ssZZ|
|yyyy/MM/dd'T'HH:mm:ssZZ|
|yyyy.MM.dd'T'HH:mm:ssZZ|
|yyyy/MM/dd HH:mm:ss.SSS|
|yyyy-MM-dd HH:mm:ss.SSS|
|yyyy.MM.dd HH:mm:ss.SSS|
|yyyy/MM/dd'T'HH:mm:ss.SSS|
|yyyy-MM-dd'T'HH:mm:ss.SSS|
|yyyy.MM.dd'T'HH:mm:ss.SSS|
|yyyy-MM-dd HH:mm:ss.SSSZZ|
|yyyy/MM/dd HH:mm:ss.SSSZZ|
|yyyy.MM.dd HH:mm:ss.SSSZZ|
|yyyy-MM-dd'T'HH:mm:ss.SSSZZ|
|yyyy/MM/dd'T'HH:mm:ss.SSSZZ|
|yyyy.MM.dd'T'HH:mm:ss.SSSZZ|
|ISO8601 standard time format|

</center>


IoTDB can support LONG types and DATETIME-DISPLAY types when displaying timestamps. The DATETIME-DISPLAY type can support user-defined time formats. The syntax of the custom time format is shown in the table below:

<center>**The syntax of the custom time format**

|Symbol|Meaning|Presentation|Examples|
|:---:|:---:|:---:|:---:|
|G|era|era|era|
|C|century of era (>=0)|	number|	20|
| Y	|year of era (>=0)|	year|	1996|
|||||
| x	|weekyear|	year|	1996|
| w	|week of weekyear|	number	|27|
| e	|day of week	|number|	2|
| E	|day of week	|text	|Tuesday; Tue|
|||||
| y|	year|	year|	1996|
| D	|day of year	|number|	189|
| M	|month of year	|month|	July; Jul; 07|
| d	|day of month	|number|	10|
|||||
| a	|halfday of day	|text	|PM|
| K	|hour of halfday (0~11)	|number|	0|
| h	|clockhour of halfday (1~12)	|number|	12|
|||||
| H	|hour of day (0~23)|	number|	0|
| k	|clockhour of day (1~24)	|number|	24|
| m	|minute of hour|	number|	30|
| s	|second of minute|	number|	55|
| S	|fraction of second	|millis|	978|
|||||
| z	|time zone	|text	|Pacific Standard Time; PST|
| Z	|time zone offset/id|	zone|	-0800; -08:00; America/Los_Angeles|
|||||
| '|	escape for text	|delimiter|	　|
| ''|	single quote|	literal	|'|

</center>

* Relative timestamp

Relative time refers to the time relative to the server time ```now()``` and ```DATETIME``` time.

 Syntax:
 ```
  Duration = (Digit+ ('Y'|'MO'|'W'|'D'|'H'|'M'|'S'|'MS'|'US'|'NS'))+
  RelativeTime = (now() | DATETIME) ((+|-) Duration)+
        
 ```

  <center>**The syntax of the duration unit**

|Symbol|Meaning|Presentation|Examples|
|:---:|:---:|:---:|:---:|
|y|year|1y=365 days|1y|
|mo|month|1mo=30 days|1mo|
|w|week|1w=7 days|1w|
|d|day|1d=1 day|1d|
|||||
|h|hour|1h=3600 seconds|1h|
|m|minute|1m=60 seconds|1m|
|s|second|1s=1 second|1s|
|||||
|ms|millisecond|1ms=1000_000 nanoseconds|1ms|
|us|microsecond|1us=1000 nanoseconds|1us|
|ns|nanosecond|1ns=1 nanosecond|1ns|

  </center>

  eg：
  ```
  now() - 1d2h //1 day and 2 hours earlier than the current server time
  now() - 1w //1 week earlier than the current server time
  ```
  > Note：There must be spaces on the left and right of '+' and '-'.
