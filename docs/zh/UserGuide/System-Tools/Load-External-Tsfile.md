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

# 系统工具

## 加载 TsFile

### 介绍
加载外部tsfile文件工具允许用户向正在运行中的Apache IoTDB中加载、删除或移出tsfile文件。

### 使用方式
用户通过Cli工具或JDBC向Apache IoTDB系统发送指定命令实现文件加载的功能。

#### 加载tsfile文件
加载tsfile文件的指令为：`load "<path/dir>" [autoregister=true/false][,sglevel=int][,verify=true/false]`

该指令有两种用法：
1. 通过指定文件路径(绝对路径)加载单tsfile文件。

第二个参数表示待加载的tsfile文件的路径，其中文件名称需要符合tsfile的命名规范，即`{systemTime}-{versionNum}-{mergeNum}.tsfile`或者`{systemTime}-{versionNum}-{mergeNum}-{unsequenceMergeNum}.tsfile`。load命令有三个可选项，分别是schema，值域为true/false，level，值域为正整数，metadatat，值域为true/false。不同选项之间用逗号连接，选项之间无顺序要求。

AUTOREGISTER选项表示当待加载的tsfile文件中时间序列对应的元数据不存在时，用户可以选择是否自动创建schema，参数为true表示自动创建schema，相反false表示不创建，缺省时默认创建schema。

SGLEVEL选项，当tsfile对应的存储组不存在时，用户可以通过level参数的值来制定存储组的级别，默认为`iotdb-engine.properties`中设置的级别。例如当设置level参数为1时表明此tsfile中所有时间序列中层级为1的前缀路径是存储组，即若存在设备root.sg.d1.s1，此时root.sg被指定为存储组。

VERIFY选项表示是否对载入的tsfile中的所有时间序列进行元数据检查，默认为true。开启时，若载入的tsfile中的时间序列在当前iotdb中也存在，则会比较该时间序列的所有Measurement的数据类型是否一致，如果出现不一致将会导致载入失败，关闭该选项会跳过检查，载入更快。

若待加载的tsfile文件对应的`.resource`文件存在，会被一并加载至Apache IoTDB数据文件的目录和引擎中，否则将通过tsfile文件重新生成对应的`.resource`文件，即加载的tsfile文件所对应的`.resource`文件不是必要的。

示例：

* `load "/Users/Desktop/data/1575028885956-101-0.tsfile"`
* `load "/Users/Desktop/data/1575028885956-101-0.tsfile" autoregister=false`
* `load "/Users/Desktop/data/1575028885956-101-0.tsfile" autoregister=true`
* `load "/Users/Desktop/data/1575028885956-101-0.tsfile" sglevel=1`
* `load "/Users/Desktop/data/1575028885956-101-0.tsfile" verify=true`
* `load "/Users/Desktop/data/1575028885956-101-0.tsfile" autoregister=true,sglevel=1`
* `load "/Users/Desktop/data/1575028885956-101-0.tsfile" verify=false,sglevel=1`
* `load "/Users/Desktop/data/1575028885956-101-0.tsfile" autoregister=false,verify=true`
* `load "/Users/Desktop/data/1575028885956-101-0.tsfile" autoregister=false,sglevel=1,verify=true`


2. 通过指定文件夹路径(绝对路径)批量加载文件。

第二个参数表示待加载的tsfile文件夹的路径，其中文件夹内所有文件名称需要符合tsfile的命名规范，即`{systemTime}-{versionNum}-{mergeNum}.tsfile`或者`{systemTime}-{versionNum}-{mergeNum}-{unsequenceMergeNum}.tsfile`。选项意义与加载单个tsfile文件相同。

示例：

* `load "/Users/Desktop/data"`
* `load "/Users/Desktop/data" autoregister=false`
* `load "/Users/Desktop/data" autoregister=true`
* `load "/Users/Desktop/data" autoregister=true,sglevel=1`
* `load "/Users/Desktop/data" autoregister=false,sglevel=1,verify=true`

#### 删除tsfile文件

删除tsfile文件的指令为：`remove "<path>"`

该指令通过指定文件路径删除tsfile文件，具体做法是将该tsfile和其对应的`.resource`和`.modification`文件全部删除。

示例：

* `remove "root.vehicle/1575028885956-101-0.tsfile"`
* `remove "1575028885956-101-0.tsfile"`

#### 移出tsfile文件至指定目录

移出tsfile文件的指令为：`move "<path>" "<dir>"`

该指令将指定路径的tsfile文件移动至目标文件夹(绝对路径)中，具体做法是在引擎中移出该tsfile，并将该tsfile文件和其对应的`.resource`文件移动到目标文件夹下

示例：

* `move "root.vehicle/1575029224130-101-0.tsfile" "/data/data/tmp"`
* `move "1575029224130-101-0.tsfile" "/data/data/tmp"`
