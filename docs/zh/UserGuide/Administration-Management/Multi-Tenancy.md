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

# 多租户

IoTDB 提供了多租户操作，主要是对数据库，或者用户在使用的过程中，对数据库进行一定的资源限制。

## 空间限额

### 基本概念

空间限额是指对某个数据库的使用空间的限制，该限制主要包括以下类型：

| 类型           | 解释                             | 单位                                         |
| -------------- | -------------------------------- | -------------------------------------------- |
| Device num     | 对某个数据库下设备数量的限制     | 个                                           |
| TimeSeries num | 对某个数据库下时序数量的限制     | 个                                           |
| disk           | 对某个数据库下空间大小的使用限制 | M（兆字节）、G（千兆字节）、T（TB）、P（PB） |

### 限额开启

如果需要使用限额，则需要在根目录的 conf 文件夹下 的 iotdb-commons.properties 中开启以下配置项：

```
quota_enable=true
```

### 设置空间限额

我们可以通过给数据库设置空间限额来限制这个数据库的可用空间大小。

示例：限制数据库 root.sg1 内的设备个数为5，时序个数为 10，可使用的空间大小为 100g。

```SQL
set space quota devices=5,timeseries=10,disk=100g on root.sg1;
```

可以支持同时给多个数据库设置相同的限额。

```SQL
set space quota devices=5,timeseries=10,disk=100g on root.sg1, root.sg2;
```

如果我们想取消某个限额，我们可以将该限额设置为 unlimited，例如取消数据库 root.sg1 的时序数量的限额：

```SQL
set space quota timeseries=unlimited on root.sg1;
```

### 限额信息查看

- 查看全部数据库的空间限额信息

```SQL
IoTDB> set space quota devices=5,timeseries=10,disk=100g on root.sg1, root.sg2;
Msg: The statement is executed successfully.
IoTDB> show space quota;
+--------+-------------+-------+----+
|database|    quotaType|  limit|used|
+--------+-------------+-------+----+
|root.sg1|     diskSize|102400M|  0M|
|root.sg1|    deviceNum|      5|   0|
|root.sg1|timeSeriesNum|     10|   0|
|root.sg2|     diskSize|102400M|  0M|
|root.sg2|    deviceNum|      5|   0|
|root.sg2|timeSeriesNum|     10|   0|
+--------+-------------+-------+----+
Total line number = 6
It costs 0.067s
```

- 查看指定数据库的空间限额信息

```SQL
IoTDB> show space quota root.sg1;
+--------+-------------+-------+----+
|database|    quotaType|  limit|used|
+--------+-------------+-------+----+
|root.sg1|     diskSize|102400M|  0M|
|root.sg1|    deviceNum|      5|   0|
|root.sg1|timeSeriesNum|     10|   0|
+--------+-------------+-------+----+
Total line number = 3
It costs 0.007s
```

