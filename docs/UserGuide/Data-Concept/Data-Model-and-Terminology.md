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

In this section, a power scenario is taken as an example to illustrate how to creat a correct data model in IoTDB. For convenience, a sample data file is attached for you to practise IoTDB.

Download the attachment: [IoTDB-SampleData.txt](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt).

According to the data attribute layers, it is expressed as an attribute hierarchy structure based on the coverage of attributes and the subordinate relationship between them, as shown below. The hierarchical from top to bottom is: power group layer - power plant layer - device layer - sensor layer. ROOT is the root node, and each node of sensor layer is a leaf node. In the process of using IoTDB, the attributes on the path from ROOT node is directly connected to each leaf node with ".", thus forming the name of a timeseries in IoTDB. For example, The left-most path in Figure 2.1 can generate a timeseries named `root.ln.wf01.wt01.status`.

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577327-7aa50780-1ef4-11e9-9d75-cadabb62444e.jpg"></center>

**Attribute hierarchy structure**

After getting the name of the timeseries, we need to set up the storage group according to the actual scenario and scale of the data. Because in the scenario of this chapter data is usually arrived in the unit of groups (i.e., data may be across electric fields and devices), in order to avoid frequent switch of IO when writing data, and meet the user's requirement of physical isolation of data in the unit of groups, storage group is set at the group layer.

Here are the basic concepts of the model involved in IoTDB:

* Device

A device is an installation equipped with sensors in real scenarios. In IoTDB, all sensors should have their corresponding devices.

* Sensor

A sensor is a detection equipment in an actual scene, which can sense the information to be measured, and can transform the sensed information into an electrical signal or other desired form of information output and send it to IoTDB. In IoTDB, all data and paths stored are organized in units of sensors.

* Storage Group

Storage groups are used to let users define how to organize and isolate different time series data on disk. Time series belonging to the same storage group is continuously written to the same file in the corresponding folder. The file may be closed due to user commands or system policies, and hence the data coming next from these sensors will be stored in a new file in the same folder. Time series belonging to different storage groups are stored in different folders.

Users can set any prefix path as a storage group. Provided that there are four time series `root.vehicle.d1.s1`, `root.vehicle.d1.s2`, `root.vehicle.d2.s1`, `root.vehicle.d2.s2`, two devices `d1` and `d2` under the path `root.vehicle` may belong to the same owner or the same manufacturer, so d1 and d2 are closely related. At this point, the prefix path root.vehicle can be designated as a storage group, which will enable IoTDB to store all devices under it in the same folder. Newly added devices under `root.vehicle` will also belong to this storage group.

> Note: A full path (`root.vehicle.d1.s1` as in the above example) is not allowed to be set as a storage group.

Setting a reasonable number of storage groups can lead to performance gains: there is neither the slowdown of the system due to frequent switching of IO (which will also take up a lot of memory and result in frequent memory-file switching) caused by too many storage files (or folders), nor the block of write commands caused by too few storage files (or folders) (which reduces concurrency).

Users should balance the storage group settings of storage files according to their own data size and usage scenarios to achieve better system performance. (There will be officially provided storage group scale and performance test reports in the future).

> Note: The prefix of a time series must belong to a storage group. Before creating a time series, the user must set which storage group the series belongs to. Only the time series whose storage group is set can be persisted to disk.

Once a prefix path is set as a storage group, the storage group settings cannot be changed.

After a storage group is set, all parent and child layers of the corresponding prefix path are not allowed to be set up again (for example, after `root.ln` is set as the storage group, the root layer and `root.ln.wf01` are not allowed to be set as storage groups).

The Layer Name of storage group can only consist of characters, numbers, underscores and hyphen, like `root.storagegroup_1-sg1`.

* Path

In IoTDB, a path is an expression that conforms to the following constraints:

```
path: LayerName (DOT LayerName)+
LayerName: Identifier | STAR
```

Among them, STAR is "*" and DOT is ".".

We call the middle part of a path between two "." as a layer, and thus `root.A.B.C` is a path with four layers. 

It is worth noting that in the path, root is a reserved character, which is only allowed to appear at the beginning of the time series mentioned below. If root appears in other layers, it cannot be parsed and an error is reported.

Single quotes are not allowed in the path. If you want to use special characters such as "." in LayerName, use double quotes. For example, `root.sg."d.1"."s.1"`. 

The characters supported in LayerName without double quotes are as below:

* Chinese characters '\u2E80' to '\u9FFF'
* '+', '&', '%', '$', '#', '@', '/', '_', '-', ':'
* 'A' to 'Z', 'a' to 'z', '0' to '9'
* '[', ']' (eg. 's[1', 's[1]', s[ab]')

'-' and ':' cannot be the first character. '+' cannot use alone.

> Note: the LayerName of storage group can only be characters, numbers, underscores and hyphen. 
> 
> Besides, if deploy on Windows system, the LayerName is case-insensitive, which means it's not allowed to set storage groups `root.ln` and `root.LN` at the same time.

* Timeseries Path

The timeseries path is the core concept in IoTDB. A timeseries path can be thought of as the complete path of a sensor that produces the time series data. All timeseries paths in IoTDB must start with root and end with the sensor. A timeseries path can also be called a full path.

For example, if device1 of the vehicle type has a sensor named sensor1, its timeseries path can be expressed as: `root.vehicle.device1.sensor1`. Double quotes can be nested with escape characters, e.g. `root.sg.d1."s.\"t\"1"`.

> Note: The layer of timeseries paths supported by the current IoTDB must be greater than or equal to four (it will be changed to two in the future).


* Aligned timeseries (From v0.13)

When a group of sensors detects data at the same time, multiple timeseries with the same timestamp will be produced, which are called **aligned timeseries** in IoTDB (and are also called **multivariate timeseries** academically. It contains multiple unary timeseries as components, and the sampling time of each unary timeseries is the same.)

Aligned timeseries can be created, inserted values, and deleted at the same time. However, when querying, each sensor can be queried separately.

By using aligned timeseries, the timestamp column could be stored only once in memory and disk when inserting data, instead of stored as many times as the number of timeseries:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/114125919-f4850800-9929-11eb-8211-81d4c04af1ec.png">

In the following chapters of data definition language, data operation language and Java Native Interface, various operations related to aligned timeseries will be introduced one by one.


* Device template (From v0.13)

In the actual scenario, there are many devices with the same model, that is, they have the same working condition name and type. To save system resources, you can declare a **device template** for the same type of device, mount it to any node in the path.

Currently you can only set one **device template** on a specific path. Device will use it's own device template or nearest ancestor's device template.

In the following chapters of data definition language, data operation language and Java Native Interface, various operations related to device template will be introduced one by one.


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
