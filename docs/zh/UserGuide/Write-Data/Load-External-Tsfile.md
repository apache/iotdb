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

# 加载 TsFile

## 介绍
加载外部 tsfile 文件工具允许用户向正在运行中的 Apache IoTDB 中加载、删除或移出 tsfile 文件。

## 使用方式
用户通过 Cli 工具或 JDBC 向 Apache IoTDB 系统发送指定命令实现文件加载的功能。

### 加载 tsfile 文件

加载 tsfile 文件的指令为：`load '<path/dir>' [sglevel=int][verify=true/false][onSuccess=delete/none]`

该指令有两种用法：

1. 通过指定文件路径(绝对路径)加载单 tsfile 文件。

第一个参数表示待加载的 tsfile 文件的路径。load 命令有三个可选项，分别是 sglevel，值域为整数，verify，值域为 true/false，onSuccess，值域为delete/none。不同选项之间用空格隔开，选项之间无顺序要求。

SGLEVEL 选项，当 tsfile 对应的存储组不存在时，用户可以通过 sglevel 参数的值来制定存储组的级别，默认为`iotdb-datanode.properties`中设置的级别。例如当设置 level 参数为1时表明此 tsfile 中所有时间序列中层级为1的前缀路径是存储组，即若存在设备 root.sg.d1.s1，此时 root.sg 被指定为存储组。

VERIFY 选项表示是否对载入的 tsfile 中的所有时间序列进行元数据检查，默认为 true。开启时，若载入的 tsfile 中的时间序列在当前 iotdb 中也存在，则会比较该时间序列的所有 Measurement 的数据类型是否一致，如果出现不一致将会导致载入失败，关闭该选项会跳过检查，载入更快。

ONSUCCESS选项表示对于成功载入的tsfile的处置方式，默认为delete，即tsfile成功加载后将被删除，如果是none表明tsfile成功加载之后依然被保留在源文件夹。

若待加载的 tsfile 文件对应的`.resource`文件存在，会被一并加载至 Apache IoTDB 数据文件的目录和引擎中，否则将通过 tsfile 文件重新生成对应的`.resource`文件，即加载的 tsfile 文件所对应的`.resource`文件不是必要的。

示例：

* `load '/Users/Desktop/data/1575028885956-101-0.tsfile'`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' onSuccess=delete`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true onSuccess=none`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false sglevel=1 onSuccess=delete`


2. 通过指定文件夹路径(绝对路径)批量加载文件。

第一个参数表示待加载的 tsfile 文件夹的路径。选项意义与加载单个 tsfile 文件相同。

示例：

* `load '/Users/Desktop/data'`
* `load '/Users/Desktop/data' verify=false`
* `load '/Users/Desktop/data' verify=true`
* `load '/Users/Desktop/data' verify=true sglevel=1`
* `load '/Users/Desktop/data' verify=false sglevel=1 onSuccess=delete`

**注意**，如果`$IOTDB_HOME$/conf/iotdb-datanode.properties`中`enable_auto_create_schema=true`时会在加载tsfile的时候自动创建tsfile中的元数据，否则不会自动创建。

### 删除 tsfile 文件

删除 tsfile 文件的指令为：`remove '<path>'`

该指令通过指定文件路径删除 tsfile 文件，具体做法是将该 tsfile 和其对应的`.resource`和`.modification`文件全部删除。

示例：

* `remove '/Users/Desktop/data/data/root.vehicle/0/0/1575028885956-101-0.tsfile'`

### 卸载 tsfile 文件至指定目录

卸载 tsfile 文件的指令为：`unload '<path>' '<dir>'`

该指令将指定路径的 tsfile 文件卸载并移动至目标文件夹（绝对路径）中，具体做法是在引擎中卸载该 tsfile，并将该 tsfile 文件和其对应的`.resource`文件移动到目标文件夹下

示例：

* `unload '/Users/Desktop/data/data/root.vehicle/0/0/1575028885956-101-0.tsfile' '/data/data/tmp'`
