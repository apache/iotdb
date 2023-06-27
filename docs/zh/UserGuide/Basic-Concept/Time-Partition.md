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

## 时间分区

### 主要功能

时间分区按照时间分割数据，一个时间分区用于保存某个时间范围内的所有数据。时间分区编号使用自然数表示，0 表示 1970 年 1 月 1 日，每隔 partition_interval 毫秒后加一。数据通过计算 timestamp / partition_interval 得到自己所在的时间分区编号，主要配置项如下所示：

* time\_partition\_interval

|名字| time\_partition\_interval              |
 |:---------------------------------------------------:|:----------------------------------------|
|描述| Database 分区的时间段长度，用户指定的 database 下会使用该时间段进行分区，单位：毫秒 |
|类型|                        Int64                        |
|默认值|                      604800000                      |
|改后生效方式|                   仅允许在第一次启动服务前修改                    |

### 配置示例

开启时间分区功能，并设置 partition_interval 为 86400000（一天），则数据的分布情况如下图所示：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Data-Concept/Time-Partition/time_partition_example.png?raw=true" alt="time partition example">

* 插入一条时间戳为 0 的数据，计算 0 / 86400000 = 0，则该数据会被存储到 0 号文件夹下的TsFile中
  
* 插入一条时间戳为 1609459200010 的数据，计算 1609459200010 / 86400000 = 18628，则该数据会被存储到 18628 号文件夹下的TsFile中

### 使用建议

使用时间分区功能时，建议同时打开 Memtable 的定时刷盘功能，共 6 个相关配置参数（详情见 [timed_flush配置项](../Reference/DataNode-Config-Manual.md)）。

* enable_timed_flush_unseq_memtable: 是否开启乱序 Memtable 的定时刷盘，默认打开。

* enable_timed_flush_seq_memtable: 是否开启顺序 Memtable 的定时刷盘，默认关闭。应当在开启时间分区后打开，定时刷盘非活跃时间分区下的 Memtable，为定时关闭 TsFileProcessor 作准备。