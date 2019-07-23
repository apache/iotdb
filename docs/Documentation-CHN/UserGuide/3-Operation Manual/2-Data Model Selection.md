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

# 第3章 IoTDB操作指南

## 数据模型选用与创建

在向IoTDB导入数据之前，首先要根据[样例数据](/#/Documents/0.8.0/chap3/sec1)选择合适的数据存储模型，然后使用[SET STORAGE GROUP](/#/Documents/0.8.0/chap5/sec1)语句和[CREATE TIMESERIES](/#/Documents/0.8.0/chap5/sec1)语句设置存储组，并创建时间序列。

### 选用存储模型

根据本文描述的[数据](/#/Documents/0.8.0/chap3/sec1)属性层级，按照属性涵盖范围以及它们之间的从属关系，我们可将其表示为如下图3.1的属性层级组织结构，其层级关系为：集团层-电场层-设备层-传感器层。其中ROOT为根节点，传感器层的每一个节点称为叶子节点。在使用IoTDB的过程中，您可以直接将由ROOT节点到每一个叶子节点路径上的属性用“.”连接，将其作为一个IoTDB的时间序列的名称。图3.1中最左侧的路径可以生成一个名为`ROOT.ln.wf01.wt01.status`的时间序列。

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577327-7aa50780-1ef4-11e9-9d75-cadabb62444e.jpg">

**图3.1 属性层级组织结构**</center>

得到时间序列的名称之后，我们需要根据数据的实际场景和规模设置存储组。由于在本文所述场景中，每次到达的数据通常以集团为单位（即数据可能为跨电场、跨设备的），为了写入数据时避免频繁切换IO降低系统速度，且满足用户以集团为单位进行物理隔离数据的要求，我们将存储组设置在集团层。

### 创建存储组

存储模型选用后，我们可以根据存储模型建立相应的存储组。创建存储组的SQL语句如下所示：

```
IoTDB > set storage group to root.ln
IoTDB > set storage group to root.sgcc
```

根据以上两条SQL语句，我们可以创建出两个存储组。

需要注意的是，当系统中已经存在某个存储组或存储组的父亲节点或者孩子节点被设置为存储组的情况下，用户不可创建存储组。例如在已经有`root.ln`和`root.sgcc`这两个存储组的情况下，创建`root.ln.wf01`存储组是不可行的。系统将给出相应的错误提示，如下所示：

```
IoTDB> set storage group to root.ln.wf01
Msg: org.apache.iotdb.exception.MetadataErrorException: org.apache.iotdb.exception.PathErrorException: The prefix of root.ln.wf01 has been set to the storage group.
```

### 查看存储组

在存储组创建后，我们可以使用[SHOW STORAGE GROUP](/#/Documents/0.8.0/chap5/sec1)语句来查看所有的存储组，SQL语句如下所示：

```
IoTDB> show storage group
```

执行结果为：
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577338-84c70600-1ef4-11e9-9dab-605b32c02836.jpg"></center>

### 创建时间序列

根据建立的数据模型，我们可以分别在两个存储组中创建相应的时间序列。创建时间序列的SQL语句如下所示：

```
IoTDB > create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE
IoTDB > create timeseries root.ln.wf02.wt02.hardware with datatype=TEXT,encoding=PLAIN
IoTDB > create timeseries root.ln.wf02.wt02.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.temperature with datatype=FLOAT,encoding=RLE
```

需要注意的是，当创建时间序列时指定的编码方式与数据类型不对应时，系统会给出相应的错误提示，如下所示：
```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
error: encoding TS_2DIFF does not support BOOLEAN
```

详细的数据类型与编码方式的对应列表请参见[编码方式](/#/Documents/0.8.0/chap2/sec3)。

### 查看时间序列

目前，IoTDB支持两种查看时间序列的方式：

* SHOW TIMESERIES语句以JSON形式展示系统中所有的时间序列信息

* SHOW TIMESERIES <`Path`>语句以表格的形式返回给定路径的下的所有时间序列信息及时间序列总数。时间序列信息具体包括：时间序列路径名，数据类型，编码类型。其中，`Path`需要为一个前缀路径、带星路径或时间序列路径。例如，分别查看`root`路径和`root.ln`路径下的时间序列，SQL语句如下所示：

```
IoTDB> show timeseries root
IoTDB> show timeseries root.ln
```

执行结果分别为：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577347-8db7d780-1ef4-11e9-91d6-764e58c10e94.jpg"></center>
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577359-97413f80-1ef4-11e9-8c10-53b291fc10a5.jpg"></center>

需要注意的是，当查询路径不存在时，系统会返回0条时间序列。

### 注意事项

0.7.0版本对用户操作的数据规模进行一些限制：

限制1：假设运行时IoTDB分配到的JVM内存大小为p，用户自定义的每次将内存中的数据写入到磁盘时的大小（[group_size_in_byte](/#/Documents/0.8.0/chap4/sec2)）为q。存储组的数量不能超过p/q。

限制2：时间序列的数量不超过运行时IoTDB分配到的JVM内存与20KB的比值。
