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

我们以风电场物联网场景为例，说明如何在 IoTDB 中创建一个正确的数据模型。

根据企业组织结构和设备实体层次结构，我们将其物联网数据模型表示为如下图所示的属性层级组织结构，即电力集团层-风电场层-实体层-物理量层。其中 ROOT 为根节点，物理量层的每一个节点为叶子节点。IoTDB 采用树形结构定义数据模式，以从 ROOT 节点到叶子节点的路径来命名一个时间序列，层次间以“.”连接。例如，下图最左侧路径对应的时间序列名称为`ROOT.ln.wf01.wt01.status`。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/123542457-5f511d00-d77c-11eb-8006-562d83069baa.png">

IoTDB 模型结构涉及的基本概念在下文将做详细叙述。

### 物理量、实体、存储组、路径

* 物理量（Measurement，也称工况、字段 field）

**一元或多元物理量**，是在实际场景中检测装置所记录的测量信息，且可以按一定规律变换成为电信号或其他所需形式的信息输出并发送给 IoTDB。在 IoTDB 当中，存储的所有数据及路径，都是以物理量为单位进行组织。

* 物理分量（SubMeasurement、分量）

在多元物理量中，包括多个分量。如 GPS 是一个多元物理量，包含 3 个分量：经度、维度、海拔。多元物理量通常被同时采集，共享时间列。

一元物理量则将分量名和物理量名字重合。如温度是一个一元物理量。

* 实体（Entity，也称设备，device）

**一个物理实体**，是在实际场景中拥有物理量的设备或装置。在 IoTDB 当中，所有的物理量都有其对应的归属实体。

* 存储组（Storage group）

**一组物理实体**，用户可以将任意前缀路径设置成存储组。如有 4 条时间序列`root.ln.wf01.wt01.status`, `root.ln.wf01.wt01.temperature`, `root.ln.wf02.wt02.hardware`, `root.ln.wf02.wt02.status`，路径`root.ln`下的两个实体 `wt01`, `wt02`可能属于同一个业主，或者同一个制造商，这时候就可以将前缀路径`root.ln`指定为一个存储组。未来`root.ln`下增加了新的实体，也将属于该存储组。

一个存储组中的所有实体的数据会存储在同一个文件夹下，不同存储组的实体数据会存储在磁盘的不同文件夹下，从而实现物理隔离。

> 注意 1：不允许将一个完整路径（如上例的`root.ln.wf01.wt01.status`) 设置成存储组。
>
> 注意 2：一个时间序列其前缀必须属于某个存储组。在创建时间序列之前，用户必须设定该序列属于哪个存储组（Storage Group）。只有设置了存储组的时间序列才可以被持久化在磁盘上。

一个前缀路径一旦被设定成存储组后就不可以再更改这个存储组的设定。

一个存储组设定后，其对应的前缀路径的祖先层级与孩子及后裔层级也不允许再设置存储组（如，`root.ln`设置存储组后，root 层级与`root.ln.wf01`不允许被设置为存储组）。

存储组节点名只支持中英文字符、数字、下划线和中划线的组合。例如`root. 存储组_1-组1` 。

* 路径（Path）

在 IoTDB 中，路径是指符合以下约束的表达式：

```
path: LayerName (DOT LayerName)+
LayerName: Identifier | STAR
```

其中 STAR 为 `*` 或 `**`，DOT 为 `.`。

我们称一个路径中在两个“.”中间的部分叫做一个层级，则`root.a.b.c`为一个层级为 4 的路径。

值得说明的是，在路径中，root 为一个保留字符，它只允许出现在下文提到的时间序列的开头，若其他层级出现 root，则无法解析，提示报错。

在路径中，不允许使用单引号。如果你想在 LayerName 中使用`.`等特殊字符，请使用双引号。例如，`root.sg."d.1"."s.1"`。双引号内支持使用转义符进行双引号的嵌套，如 `root.sg.d1."s.\"t\"1"`。

除了 storage group 存储组，其他的 LayerName 中不用加双引号就支持的字符如下：

* 中文字符"\u2E80"到"\u9FFF"
* "+"，"&"，"%"，"$"，"#"，"@"，"/"，"_"，"-"，":"
* "A"到"Z"，"a"到"z"，"0"到"9"

其中'-' 和 ':' 不能放置在第一位，不能使用单个 '+'。

> 注意：storage group 中的 LayerName 只支持数字，字母，汉字，下划线和中划线。另外，如果在 Windows 系统上部署，存储组层级名称是大小写不敏感的。例如同时创建`root.ln` 和 `root.LN` 是不被允许的。

* 路径模式（Path Pattern）
  
为了使得在表达多个时间序列的时候更加方便快捷，IoTDB 为用户提供带通配符`*`或`**`的路径。用户可以利用两种通配符构造出期望的路径模式。通配符可以出现在路径中的任何层。

`*`在路径中表示一层。例如`root.vehicle.*.sensor1`代表的是以`root.vehicle`为前缀，以`sensor1`为后缀，层次等于 4 层的路径。

`**`在路径中表示是（`*`）+，即为一层或多层`*`。例如`root.vehicle.device1.**`代表的是`root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`等所有以`root.vehicle.device1`为前缀路径的大于等于 4 层的路径；`root.vehicle.**.sensor1`代表的是以`root.vehicle`为前缀，以`sensor1`为后缀，层次大于等于 4 层的路径。

> 注意：`*`和`**`不能放在路径开头。


### 一元、多元时间序列

* 数据点（Data point）

**一个“时间-值”对**。

* 时间序列（一个实体的某个物理量对应一个时间序列，Timeseries，也称测点 meter、时间线 timeline，实时数据库中常被称作标签 tag、参数 parameter）

**一个物理实体的某个物理量在时间轴上的记录**，是数据点的序列。

例如，ln 电力集团、wf01 风电场的实体 wt01 有名为 status 的物理量，则它的时间序列可以表示为：`root.ln.wf01.wt01.status`。 

* 一元时间序列（single-variable timeseries 或 timeseries，v0.1 起支持）

一个实体的一个一元物理量对应一个一元时间序列。实体+物理量=时间序列

* 多元时间序列（Multi-variable timeseries 或 Aligned timeseries，v0.13 起支持）

一个实体的一个多元物理量对应一个多元时间序列。这些时间序列称为**多元时间序列**，也叫**对齐时间序列**。

多元时间序列需要被同时创建、同时插入值，删除时也必须同时删除。不过在查询的时候，可以对于每一个分量单独查询。

通过使用对齐的时间序列，在插入数据时，一组对齐序列的时间戳列在内存和磁盘中仅需存储一次，而不是每个时间序列存储一次：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/123542458-62e4a400-d77c-11eb-8c45-ca516f1b7eba.png">

在后续数据定义语言、数据操作语言和 Java 原生接口章节，将对涉及到对齐时间序列的各种操作进行逐一介绍。

* 时间戳类型

时间戳是一个数据到来的时间点，其中包括绝对时间戳和相对时间戳，详细描述参见数据类型文档。


### 物理量模板

* 物理量模板（Measurement template，v0.13 起支持）

实际应用中有许多实体所采集的物理量相同，即具有相同的工况名称和类型，可以声明一个**物理量模板**来定义可采集的物理量集合。在实践中，物理量模板的使用可帮助减少元数据的资源占用，详细内容参见物理量模板文档。