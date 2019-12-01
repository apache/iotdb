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

# 第4章 系统部署与管理


## 数据管理

本节将介绍IoTDB的数据存储方式，便于您对IoTDB的数据管理有一个直观的了解。

IoTDB需要存储的数据分为三类，分别为数据文件、系统文件以及写前日志文件。

### 数据文件

数据文件存储了用户写入IoTDB系统的所有数据。包含TsFile文件和其他文件，其中，TsFile文件可以通过配置项`tsfile_dir`配置存储路径（详情见[tsfile_dir配置项](/#/Documents/0.8.2/chap4/sec2)），其他文件可通过[data_dir配置项](/#/Documents/0.8.2/chap4/sec2)进行配置。

为了更好的支持用户对于磁盘空间扩展等存储需求，IoTDB为TsFile的存储配置增加了多文件目录的存储方式，用户可自主配置多个存储路径作为数据的持久化位置（详情见[tsfile_dir配置项](/#/Documents/0.8.2/chap4/sec2)），并可以指定或自定义目录选择策略（详情见[mult_dir_strategy配置项](/#/Documents/0.8.2/chap4/sec2)）。

### 系统文件

系统文件包括Restore文件和Schema文件，存储了数据文件的元数据信息。可通过sys_dir配置项进行配置（详情见[sys_dir配置项](/#/Documents/0.8.2/chap4/sec2)）。

### 写前日志文件

写前日志文件存储了系统的写前日志。可通过`wal_dir`配置项进行配置（详情见[wal_dir配置项](/#/Documents/0.8.2/chap4/sec2)）。

### 数据存储目录设置举例

接下来我们将举一个数据目录配置的例子，来具体说明如何配置数据的存储目录。

IoTDB涉及到的所有数据目录路径有：data_dir, tsfile_dir, mult_dir_strategy, sys_dir和wal_dir，它们分别涉及的是IoTDB的数据文件、系统文件以及写前日志文件。您可以选择输入路径自行配置，也可以不进行任何操作使用系统默认的配置项（关于各个配置项的具体内容见本文第5.2.2.2.2节）。

以下我们给出一个用户对五个目录都进行自行配置的例子。

```
data_dir = D:\\iotdb\\data\\data  
tsfile_dir = E:\\iotdb\\data\\data1, data\\data2, F:\\data3  mult_dir_strategy = MaxDiskUsableSpaceFirstStrategy sys_dir = data\\system wal_dir = data

```
按照上述配置，系统会：

* 将除TsFile之外的数据文件存储在D:\iotdb\data\data
* 将TsFile存储在路径E:\iotdb\data\data1、路径%IOTDB_HOME%\data\data2和路径F:\data3中。且对这三个路径的选择策略是：`优先选择磁盘剩余空间最大的目录`，即在每次数据持久化到磁盘时系统会自动选择磁盘剩余空间最大的一个目录将数据进行写入
* 将TsFile之外的其他数据文件存储在路径D:\\iotdb\\data\\data中
* 将系统文件存储在%IOTDB_HOME%\data\system
* 将写前日志文件存储在%IOTDB_HOME%\data

> 如果对上述TsFile的目录进行了修改，那么需要将旧目录中的所有TsFile移动到新的目录中，并且要严格按照目录的顺序。
> 如果您在TsFile的目录中增加了新路径，那么IoTDB会直接将该目录纳入到多目录选择的范围中，用户无需有其他的操作。

例如，将tsfile_dir修改为
```
D:\\data4, E:\\data5, F:\\data6
```

那么就需要将E:\iotdb\data\data1中的文件移动到D:\data4，%IOTDB_HOME%\data\data2的文件移动到E:\data5，F:\data3的文件移动到F:\data6。系统即可继续正常运行。
