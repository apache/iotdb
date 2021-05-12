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

# 数据模式

## 数据模型

本节，我们以电力场景为例，说明如何在IoTDB中创建一个正确的数据模型。

根据属性层级，属性涵盖范围以及数据之间的从属关系，我们可将其数据模型表示为如下图所示的属性层级组织结构，即电力集团层-电厂层-设备层-传感器层。其中ROOT为根节点，传感器层的每一个节点为叶子节点。IoTDB的语法规定，ROOT节点到叶子节点的路径以“.”连接，以此完整路径命名IoTDB中的一个时间序列。例如，下图最左侧路径对应的时间序列名称为`ROOT.ln.wf01.wt01.status`。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577327-7aa50780-1ef4-11e9-9d75-cadabb62444e.jpg">

**属性层级组织结构**

得到时间序列的名称之后，我们需要根据数据的实际场景和规模设置存储组。由于在本文所述场景中，每次到达的数据通常以集团为单位（即数据可能为跨电场、跨设备的），为了写入数据时避免频繁切换IO降低系统速度，且满足用户以集团为单位进行物理隔离数据的要求，我们将存储组设置在集团层。

根据模型结构，IoTDB中涉及如下基本概念：

* 设备

设备指的是在实际场景中拥有传感器的装置。在IoTDB当中，所有的传感器都应有其对应的归属的设备。

* 传感器

传感器是指在实际场景中的一种检测装置，它能感受到被测量的信息，并能将感受到的信息按一定规律变换成为电信号或其他所需形式的信息输出并发送给IoTDB。在IoTDB当中，存储的所有的数据及路径，都是以传感器为单位进行组织。

* 存储组

用户可以将任意前缀路径设置成存储组。如有4条时间序列`root.vehicle.d1.s1`, `root.vehicle.d1.s2`, `root.vehicle.d2.s1`, `root.vehicle.d2.s2`，路径`root.vehicle`下的两个设备d1,d2可能属于同一个业主，或者同一个厂商，因此关系紧密。这时候就可以将前缀路径`root.vehicle`指定为一个存储组，这将使得IoTDB将其下的所有设备的数据存储在同一个文件夹下。未来`root.vehicle`下增加了新的设备，也将属于该存储组。

> 注意：不允许将一个完整路径(如上例的`root.vehicle.d1.s1`)设置成存储组。

设置合理数量的存储组可以带来性能的提升：既不会因为产生过多的存储文件（夹）导致频繁切换IO降低系统速度（并且会占用大量内存且出现频繁的内存-文件切换），也不会因为过少的存储文件夹（降低了并发度从而）导致写入命令阻塞。

用户应根据自己的数据规模和使用场景，平衡存储文件的存储组设置，以达到更好的系统性能。（未来会有官方提供的存储组规模与性能测试报告）

> 注意：一个时间序列其前缀必须属于某个存储组。在创建时间序列之前，用户必须设定该序列属于哪个存储组（Storage Group）。只有设置了存储组的时间序列才可以被持久化在磁盘上。

一个前缀路径一旦被设定成存储组后就不可以再更改这个存储组的设置。

一个存储组设定后，其对应的前缀路径的所有父层级与子层级也不允许再设置存储组（如，`root.ln`设置存储组后，root层级与`root.ln.wf01`不允许被设置为存储组）。

存储组节点名只支持中英文字符、数字、下划线和中划线的组合。例如`root.存储组_1-组1` 。


* 路径

在IoTDB中，路径是指符合以下约束的表达式：

```
path: LayerName (DOT LayerName)+
LayerName: Identifier | STAR
```

其中STAR为“*”，DOT为“.”。

我们称一个路径中在两个“.”中间的部分叫做一个层级，则`root.a.b.c`为一个层级为4的路径。

值得说明的是，在路径中，root为一个保留字符，它只允许出现在下文提到的时间序列的开头，若其他层级出现root，则无法解析，提示报错。

在路径中，不允许使用单引号。如果你想在LayerName中使用`.`等特殊字符，请使用双引号。例如，`root.sg."d.1"."s.1"`。双引号内支持使用转义符进行双引号的嵌套，如 `root.sg.d1."s.\"t\"1"`。

除了storage group 存储组，其他的LayerName中不用加双引号就支持的字符如下：

* 中文字符"\u2E80"到"\u9FFF"
* "+"，"&"，"%"，"$"，"#"，"@"，"/"，"_"，"-"，":"
* "A"到"Z"，"a"到"z"，"0"到"9"

其中'-' 和 ':' 不能放置在第一位，不能使用单个 '+'。

> 注意: storage group中的LayerName只支持数字，字母，汉字，下划线和中划线。另外，如果在Windows系统上部署，存储组层级名称是大小写不敏感的。例如同时创建`root.ln` 和 `root.LN` 是不被允许的。

* 时间序列

时间序列是IoTDB中的核心概念。时间序列可以被看作产生时序数据的传感器的所在完整路径，在IoTDB中所有的时间序列必须以root开始、以传感器作为结尾。一个时间序列也可称为一个全路径。

例如，vehicle种类的device1有名为sensor1的传感器，则它的时间序列可以表示为：`root.vehicle.device1.sensor1`。 

> 注意：当前IoTDB支持的时间序列必须大于等于四层（之后会更改为两层）。

* 对齐时间序列（v0.13 起支持）

在同一个时间戳有多个传感器同时采样，会形成具有相同时间戳的多条时间序列，在 IoTDB 中，这些时间序列成为**对齐时间序列**（在学术上也称为**多元时间序列**，即包含多个一元时间序列作为分量， 各个一元时间序列的采样时间点相同）。

对齐时间序列可以被同时创建，同时插入值，删除时也必须同时删除。不过在查询的时候，可以对于每一个传感器单独查询。

通过使用对齐的时间序列，在插入数据时，一组对齐序列的时间戳列在内存和磁盘中仅需存储一次，而不是时间序列的条数次：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/114125919-f4850800-9929-11eb-8211-81d4c04af1ec.png">

在后续数据定义语言、数据操作语言和 Java 原生接口章节，将对涉及到对齐时间序列的各种操作进行逐一介绍。


* 设备模板（v0.13 起支持）

实际场景中有许多设备型号相同，即具有相同的工况名称和类型，为了节省系统资源，可以声明一个**设备模板**表征同一类型的设备，设备模版可以挂在到路径的任意节点上。

目前每一个路径节点仅允许挂载一个设备模板，具体的设备将使用其自身或最近祖先的设备模板作为有效模板。

在后续数据定义语言、数据操作语言和 Java 原生接口章节，将对涉及到设备模板的各种操作进行逐一介绍。

* 前缀路径

前缀路径是指一个时间序列的前缀所在的路径，一个前缀路径包含以该路径为前缀的所有时间序列。例如当前我们有`root.vehicle.device1.sensor1`, `root.vehicle.device1.sensor2`, `root.vehicle.device2.sensor1`三个传感器，则`root.vehicle.device1`前缀路径包含`root.vehicle.device1.sensor1`、`root.vehicle.device1.sensor2`两个时间序列，而不包含`root.vehicle.device2.sensor1`。

* 带`*`路径
为了使得在表达多个时间序列或表达前缀路径的时候更加方便快捷，IoTDB为用户提供带`*`路径。`*`可以出现在路径中的任何层。按照`*`出现的位置，带`*`路径可以分为两种：

`*`出现在路径的结尾；

`*`出现在路径的中间；

当`*`出现在路径的结尾时，其代表的是（`*`）+，即为一层或多层`*`。例如`root.vehicle.device1.*`代表的是`root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`等所有以`root.vehicle.device1`为前缀路径的大于等于4层的路径。

当`*`出现在路径的中间，其代表的是`*`本身，即为一层。例如`root.vehicle.*.sensor1`代表的是以`root.vehicle`为前缀，以`sensor1`为后缀，层次等于4层的路径。

> 注意：`*`不能放在路径开头。

> 注意：`*`放在末尾时与前缀路径表意相同，例如`root.vehicle.*`与`root.vehicle`为相同含义。

> 注意：`*`create创建时，后面的路径同时不能含有`*`。 

* 时间戳

时间戳是一个数据到来的时间点，其中包括绝对时间戳和相对时间戳。

* 绝对时间戳

IOTDB中绝对时间戳分为二种，一种为LONG类型，一种为DATETIME类型（包含DATETIME-INPUT, DATETIME-DISPLAY两个小类）。

在用户在输入时间戳时，可以使用LONG类型的时间戳或DATETIME-INPUT类型的时间戳，其中DATETIME-INPUT类型的时间戳支持格式如表所示：

<center>**DATETIME-INPUT类型支持格式**

|format|
|:---|
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

IoTDB在显示时间戳时可以支持LONG类型以及DATETIME-DISPLAY类型，其中DATETIME-DISPLAY类型可以支持用户自定义时间格式。自定义时间格式的语法如表所示：

<center>**DATETIME-DISPLAY自定义时间格式的语法**

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

* 相对时间戳

  相对时间是指与服务器时间```now()```和```DATETIME```类型时间相差一定时间间隔的时间。
  形式化定义为：

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

  例子：
  ```
  now() - 1d2h //比服务器时间早1天2小时的时间
  now() - 1w //比服务器时间早1周的时间
  ```
  > 注意：'+'和'-'的左右两边必须有空格 
