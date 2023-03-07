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

# 数据模型

我们以风电场物联网场景为例，说明如何在 IoTDB 中创建一个正确的数据模型。

根据企业组织结构和设备实体层次结构，我们将其物联网数据模型表示为如下图所示的属性层级组织结构，即电力集团层-风电场层-实体层-物理量层。其中 ROOT 为根节点，物理量层的每一个节点为叶子节点。IoTDB 采用树形结构定义数据模式，以从 ROOT 节点到叶子节点的路径来命名一个时间序列，层次间以“.”连接。例如，下图最左侧路径对应的时间序列名称为`ROOT.ln.wf01.wt01.status`。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/123542457-5f511d00-d77c-11eb-8006-562d83069baa.png">

在上图所描述的实际场景中，有许多实体所采集的物理量相同，即具有相同的工况名称和类型，因此，可以声明一个**元数据模板**来定义可采集的物理量集合。在实践中，元数据模板的使用可帮助减少元数据的资源占用，详细内容参见 [元数据模板文档](./Schema-Template.md)。

IoTDB 模型结构涉及的基本概念在下文将做详细叙述。

## 物理量、实体、数据库、路径

### 物理量（Measurement）

**物理量**，也称工况或字段（field），是在实际场景中检测装置所记录的测量信息，且可以按一定规律变换成为电信号或其他所需形式的信息输出并发送给 IoTDB。在 IoTDB 当中，存储的所有数据及路径，都是以物理量为单位进行组织。

### 实体（Entity）

**一个物理实体**，也称设备（device），是在实际场景中拥有物理量的设备或装置。在 IoTDB 当中，所有的物理量都有其对应的归属实体。

### 数据库（Database）

用户可以将任意前缀路径设置成数据库。如有 4 条时间序列`root.ln.wf01.wt01.status`, `root.ln.wf01.wt01.temperature`, `root.ln.wf02.wt02.hardware`, `root.ln.wf02.wt02.status`，路径`root.ln`下的两个实体 `wt01`, `wt02`可能属于同一个业主，或者同一个制造商，这时候就可以将前缀路径`root.ln`指定为一个数据库。未来`root.ln`下增加了新的实体，也将属于该数据库。

一个 database 中的所有数据会存储在同一批文件夹下，不同 database 的数据会存储在磁盘的不同文件夹下，从而实现物理隔离。

> 注意 1：不允许将一个完整路径（如上例的`root.ln.wf01.wt01.status`) 设置成 database。
>
> 注意 2：一个时间序列其前缀必须属于某个 database。在创建时间序列之前，用户必须设定该序列属于哪个database。只有设置了 database 的时间序列才可以被持久化在磁盘上。
> 
> 注意 3：被设置为数据库的路径总字符数不能超过64，包括路径开头的`root.`这5个字符。

一个前缀路径一旦被设定成 database 后就不可以再更改这个 database 的设定。

一个 database 设定后，其对应的前缀路径的祖先层级与孩子及后裔层级也不允许再设置 database（如，`root.ln`设置 database 后，root 层级与`root.ln.wf01`不允许被设置为 database）。

Database 节点名只支持中英文字符、数字和下划线的组合。例如`root.数据库_1` 。

### 路径（Path）

路径（`path`）是指符合以下约束的表达式：

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

我们称一个路径中由 `'.'` 分割的部分叫做路径结点名（`nodeName`）。例如：`root.a.b.c`为一个层级为 4 的路径。

下面是对路径结点名（`nodeName`）的约束：

* `root` 作为一个保留字符，它只允许出现在下文提到的时间序列的开头，若其他层级出现 `root`，则无法解析，提示报错。
* 除了时间序列的开头的层级（`root`）外，其他的层级支持的字符如下：
  * [ 0-9 a-z A-Z _ ] （字母，数字，下划线）
  * ['\u2E80'..'\u9FFF'] （UNICODE 中文字符）
* 特别地，如果系统在 Windows 系统上部署，那么 database 路径结点名是大小写不敏感的。例如，同时创建`root.ln` 和 `root.LN` 是不被允许的。
* 如果需要在路径结点名中用特殊字符，可以用反引号引用路径结点名，具体使用方法可以参考[语法约定](../Syntax-Conventions/Literal-Values.md)。

### 路径模式（Path Pattern）

为了使得在表达多个时间序列的时候更加方便快捷，IoTDB 为用户提供带通配符`*`或`**`的路径。用户可以利用两种通配符构造出期望的路径模式。通配符可以出现在路径中的任何层。

`*`在路径中表示一层。例如`root.vehicle.*.sensor1`代表的是以`root.vehicle`为前缀，以`sensor1`为后缀，层次等于 4 层的路径。

`**`在路径中表示是（`*`）+，即为一层或多层`*`。例如`root.vehicle.device1.**`代表的是`root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`等所有以`root.vehicle.device1`为前缀路径的大于等于 4 层的路径；`root.vehicle.**.sensor1`代表的是以`root.vehicle`为前缀，以`sensor1`为后缀，层次大于等于 4 层的路径。

> 注意：`*`和`**`不能放在路径开头。


## 时间序列

### 时间戳 (Timestamp)

时间戳是一个数据到来的时间点，其中包括绝对时间戳和相对时间戳，详细描述参见 [数据类型文档](./Data-Type.md)。

### 数据点（Data Point）

**一个“时间戳-值”对**。

### 时间序列（Timeseries）

**一个物理实体的某个物理量在时间轴上的记录**，是数据点的序列。

一个实体的一个物理量对应一个时间序列，即实体+物理量=时间序列。

时间序列也被称测点（meter）、时间线（timeline）。实时数据库中常被称作标签（tag）、参数（parameter）。

例如，ln 电力集团、wf01 风电场的实体 wt01 有名为 status 的物理量，则它的时间序列可以表示为：`root.ln.wf01.wt01.status`。 

### 对齐时间序列（Aligned Timeseries）

在实际应用中，存在某些实体的多个物理量**同时采样**，形成一组时间列相同的时间序列，这样的一组时间序列在Apache IoTDB中可以建模为对齐时间序列。

在插入数据时，一组对齐序列的时间戳列在内存和磁盘中仅需存储一次，而不是每个时间序列存储一次。

对齐的一组时间序列最好同时创建。

不可以在对齐序列所属的实体下创建非对齐的序列，不可以在非对齐序列所属的实体下创建对齐序列。

查询数据时，可以对于每一条时间序列单独查询。

插入数据时，对齐的时间序列中某列的某些行允许有空值。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/114125919-f4850800-9929-11eb-8211-81d4c04af1ec.png">

在后续数据定义语言、数据操作语言和 Java 原生接口章节，将对涉及到对齐时间序列的各种操作进行逐一介绍。
