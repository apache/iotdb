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

# 概述

IoT-benchmark 是基于 Java 和大数据环境开发的时序数据库基准测试工具，由清华大学软件学院研发并开源。它使用方便，支持多种写入以及查询方式，支持存储测试信息和结果以供进一步查询或分析，支持与 Tableau 集成以可视化测试结果。

下图1-1囊括了测试基准流程及其他扩展功能。这些流程可以由IoT-benchmark 统一来完成。IoT Benchmark 支持多种工作负载，包括**纯写入、纯查询、写入查询混合**等，支持**软硬件系统监控、测试指标度量**等监控功能，还实现了**初始化数据库自动化、测试数据分析及系统参数优化**等功能。

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=ODBlZTU1ZDZhNjNlYTQ4ZjA4NGQ3NTA0MDRhNjAyODlfdFluUXA4NTQ3Zk5UMVF1MzhaUG1wenVRUFBiQWlOUzRfVG9rZW46Ym94Y252TXZBdlpoT0VRSEVXdkxJZDZtSzNmXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

图1-1

借鉴 YCSB 测试工具将工作负载生成、性能指标测量和数据库接口三个组件分离的设计思想，IoT-benchmark 的模块化设计如图1-2所示。与基于 YCSB 的测试工具系统不同的是，IoT-benchmark 增加了系统监控模块，支持测试数据和系统指标监控数据的持久化。此外也增加了一些特别针对时序数据场景的特殊负载测试功能，如支持物联网场景的批量写入和多种乱序数据写入模式。

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=MjU1ZjAzNjg0OWVhMzYxZjY5OWNhMTkyMjgxNzFmNzFfc2I0UUJpcTVteHN3eWV5N0p3VWFvNEdqTGJhUG1TQTZfVG9rZW46Ym94Y251Z3NvT29HSWNDVVh6OWJOWnRNY2ljXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

图1-2



目前 IoT-benchmark 支持如下时间序列数据库、版本和连接方式：

| 数据库          | DB-Engines排名 | 版本    | 连接方式                                                 |
| --------------- | -------------- | ------- | -------------------------------------------------------- |
| InfluxDB        | 1              | v1.x    | SDK                                                      |
| v2.0            |                |         |                                                          |
| TimescaleDB     | 5              | --      | jdbc                                                     |
| OpenTSDB        | 8              | --      | Http Request                                             |
| QuestDB         | 10             | v6.0.7  | jdbc                                                     |
| TDengine        | 13             | 2.2.0.2 | jdbc                                                     |
| VictoriaMetrics | 16             | v1.64.0 | Http Request                                             |
| KairosDB        | 17             | --      | Http Request                                             |
| IoTDB           | 19             | v1.0    | jdbc、sessionByTablet、sessionByRecord、sessionByRecords |
| v0.13           |                |         |                                                          |

表1-1大数据测试基准对比

##  

## 软件安装与环境搭建

### IoT Benchmark 运行的前置条件

1. Java 8
2. Maven 3.6+
3. 对应的合适版本的数据库，如 Apache IoTDB 1.0



### IoT Benchmark 的获取方式

- **获取二进制包**：进入https://github.com/thulab/iot-benchmark/releases 下载需要的安装包。下载下来为一个压缩文件，选择文件夹解压即可使用。
- 源代码编译（可用户 Apache IoTDB 1.0 的测试）：
  - 第一步（编译 IoTDB Session 最新包）：进入官网 https://github.com/apache/iotdb/tree/rel/1.0 下载 IoTDB 源码，在根目录下运行命令 mvn clean package install -pl session -am -DskipTests 编译 IoTDB Session 的最新包。
  - 第二步（编译 IoTDB Benchmark 测试包）：进入官网 https://github.com/thulab/iot-benchmark 下载源码，在根目录下运行 mvn clean package install -pl iotdb-1.0 -am -DskipTests 编译测试 Apache IoTDB 1.0版本的测试包，测试包位置与根目录的相对路径为 ./iotdb-1.0/target/iotdb-1.0-0.0.1/iotdb-1.0-0.0.1



### IoT Benchmark 的测试包结构

测试包的目录结构如下图1-3所示。其中测试配置文件为conf/config.properties，测试启动脚本为benchmark.sh (Linux & MacOS) 和 benchmark.bat (Windows)，详细文件用途见表1-2所示。

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=ZWVjNTA3M2JjYTc5MTljMjUyMTgxNTE4YzZkN2Q4ZGZfb0EydkFBWk92ZklRV3J3WUYxNXRYRTFPa1lnTDd4MXlfVG9rZW46Ym94Y25zejVyT2RHWEhTVnY5emM5QUQ1VXNmXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

图1-3文件和文件夹列表

| 名称             | 子文件            | 用途                      |
| ---------------- | ----------------- | ------------------------- |
| benchmark.bat    | -                 | Windows环境运行启动脚本   |
| benchmark.sh     | -                 | Linux/Mac环境运行启动脚本 |
| conf             | config.properties | 测试场景配置文件          |
| logback.xml      | 日志输出配置文件  |                           |
| lib              | -                 | 依赖库文件                |
| LICENSE          | -                 | 许可文件                  |
| bin              | startup.sh        | 初始化脚本文件夹          |
| ser-benchmark.sh | -                 | 监控模式启动脚本          |

表1-2文件和文件夹列表用途

### IoT Benchmark 执行测试

1. 按照测试需求修改配置文件，主要参数介绍见 1.2 节，对应配置文件为conf/config.properties，**比如测试****Apache** **IoTDB 1.0，则需要修改 DB_SWITCH=IoTDB-100-SESSION_BY_TABLET**
2. 启动被测时间序列数据库
3. 通过运行
4. 启动IoT-benchmark执行测试。执行中观测被测时间序列数据库和IoT-benchmark状态，执行完毕后查看结果和分析测试过程。

### IoT Benchmark 结果说明

测试的所有日志文件被存放于 logs 文件夹下，测试的结果在测试完成后被存放到 data/csvOutput 文件夹下，例如测试后我们得到了如下的结果矩阵：

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=MzI4OTkxNjVjNmY4ZDg5YzRkMzYwYzI4NWIxYWVmNjJfeEk0Y1VGOE9OR0xqUE1jZjhKRERDdlVZSjdsRm4waXNfVG9rZW46Ym94Y25pcWI3VDltV1BMUHJLSGxKQ0VacGxnXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

- Result Matrix
  - OkOperation：成功的对应操作次数
  - OkPoint：对于写入操作，是成功写入的点数；对于查询操作，是成功查询到的点数。
  - FailOperation：失败的对应操作次数
  - FailPoint：对于写入操作是写入失败的点数
- Latency(mx) Matrix
  - AVG：操作平均耗时
  - MIN：操作最小耗时
  - Pn：操作整体分布的对应分位值，比如P25是下四分位数



## 主要参数介绍

本节重点解释说明了主要参数的用途和配置方法。

### 工作模式和操作比例

- 工作模式参数“BENCHMARK_WORK_MODE”可选项为“默认模式”和“服务器监控”；其中“服务器监控”模式可直接通过执行ser-benchmark.sh脚本启动，脚本会自动修改该参数。“默认模式”为常用测试模式，结合配置OPERATION_PROPORTION参数达到“纯写入”、“纯查询”和“读写混合”的测试操作比例定义

-   当运行ServerMode来执行被测时序数据库运行环境监控时IoT-benchmark依赖sysstat软件相关命令；如果需要持久化测试过程数据时选择MySQL或IoTDB，则需要安装该类数据库；ServerMode和CSV的记录模式只能在Linux系统中使用，记录测试过程中的相关系统信息。因此我们建议使用MacOs或Linux系统，本文以Linux（Centos7）系统为例，如果使用Windows系统，可以使用conf文件夹下的benchmark.bat脚本启动IoT-benchmark。

 表1-3测试模式

| 模式名称               | BENCHMARK_WORK_MODE | 模式内容                                                     |
| ---------------------- | ------------------- | ------------------------------------------------------------ |
| 常规测试模式           | testWithDefaultPath | 支持多种读和写操作的混合负载                                 |
| 服务器资源使用监控模式 | serverMODE          | 服务器资源使用监控模式（该模式下运行通过ser-benchmark.sh脚本启动，无需手动配置该参数 |

### 服务器连接信息

工作模式指定后，被测时序数据库的信息要如何告知IoT-benchmark呢？当前通过“DB_SWITCH”告知被测时序数据库类型；通过“HOST”告知被测时序数据库网络地址；通过“PORT”告知被测时序数据库网络端口；通过“USERNAME”告知被测时序数据库登录用户名；通过“PASSWORD”告知被测时序数据库登录用户的密码；通过“DB_NAME”告知被测时序数据库名称；通过“TOKEN”告知被测时序数据库连接认证Token（InfluxDB 2.0使用）；

### 写入场景构建参数

表1-4写入场景构建参数

| 参数名称                   | 类型   | 示例                      | 系统描述                                                     |
| -------------------------- | ------ | ------------------------- | ------------------------------------------------------------ |
| CLIENT_NUMBER              | 整数   | 100                       | 客户端总数                                                   |
| GROUP_NUMBER               | 整数   | 20                        | 存储组的数量；仅针对IoTDB。                                  |
| DEVICE_NUMBER              | 整数   | 100                       | 设备总数                                                     |
| SENSOR_NUMBER              | 整数   | 300                       | 每个设备的传感器总数                                         |
| INSERT_DATATYPE_PROPORTION | 字符串 | 1:1:1:1:1:1               | 设备的数据类型比例，BOOLEAN:INT32:INT64:FLOAT:DOUBLE:TEXT    |
| POINT_STEP                 | 整数   | 1000                      | 数据间时间戳间隔，即生成的数据两个时间戳之间的固定长度。     |
| OP_MIN_INTERVAL            | 整数   | 0                         | 操作最小执行间隔：若操作耗时大于该值则立即执行下一个，否则等待 (OP_MIN_INTERVAL-实际执行时间) ms；如果为0，则参数不生效；如果为-1，则其值和POINT_STEP一致 |
| IS_OUT_OF_ORDER            | 布尔   | false                     | 是否乱序写入                                                 |
| OUT_OF_ORDER_RATIO         | 浮点数 | 0.3                       | 乱序写入的数据比例                                           |
| BATCH_SIZE_PER_WRITE       | 整数   | 1                         | 批写入数据行数（一次写入多少行数据）                         |
| START_TIME                 | 时间   | 2022-10-30T00:00:00+08:00 | 写入数据的开始时间戳；以该时间戳为起点开始模拟创建数据时间戳。 |
| LOOP                       | 整数   | 86400                     | 总操作次数：具体每种类型操作会按OPERATION_PROPORTION定义的比例划分 |
| OPERATION_PROPORTION       | 字符   | 1:0:0:0:0:0:0:0:0:0:0     | # 各操作的比例,按照顺序为 写入:Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10, 请注意使用英文冒号。比例中的每一项是整数。 |

按照表1-4配置参数启动可描述测试场景为：向被测时序数据库压力写入30000个（100个设备，每个设备300个传感器）时间序列2022年10月30日一天的顺序数据，总计25.92亿个数据点。其中每个设备的300个传感器数据类型分别为50个布尔、50个整数、50个长整数、50个浮点、50个双精度、50个字符。如果我们将表格中IS_OUT_OF_ORDER的值改为true，那么他表示的场景为：向被测时序数据库压力写入30000个时间序列2022年10月30日一天的数据，其中存在30%的乱序数据（到达时序数据库时间晚于其他生成时间晚于自身的数据点）。

### 查询场景构建参数

表1-5查询场景构建参数

| 参数名称             | 类型 | 示例                  | 系统描述                                                     |
| -------------------- | ---- | --------------------- | ------------------------------------------------------------ |
| QUERY_DEVICE_NUM     | 整数 | 2                     | 每条查询语句中查询涉及到的设备数量                           |
| QUERY_SENSOR_NUM     | 整数 | 2                     | 每条查询语句中查询涉及到的传感器数量                         |
| QUERY_AGGREGATE_FUN  | 字符 | count                 | 在聚集查询中使用的聚集函数，比如count、avg、sum、max_time等  |
| STEP_SIZE            | 整数 | 1                     | 时间过滤条件的时间起点变化步长，若设为0则每个查询的时间过滤条件是一样的，单位：POINT_STEP |
| QUERY_INTERVAL       | 整数 | 250000                | 起止时间的查询中开始时间与结束时间之间的时间间隔，和Group By中的时间间隔 |
| QUERY_LOWER_VALUE    | 整数 | -5                    | 条件查询子句时的参数，where xxx > QUERY_LOWER_VALUE          |
| GROUP_BY_TIME_UNIT   | 整数 | 20000                 | Group by语句中的组的大小                                     |
| LOOP                 | 整数 | 10                    | 总操作次数：具体每种类型操作会按OPERATION_PROPORTION定义的比例划分 |
| OPERATION_PROPORTION | 字符 | 0:0:0:0:0:0:0:0:0:0:1 | 写入:Q1:Q2:Q3:Q4:Q5:Q6:Q7:Q8:Q9:Q10                          |

表1-6查询类型及示例 SQL

| 编号 | 查询类型                     | IoTDB 示例 SQL                                               |
| ---- | ---------------------------- | ------------------------------------------------------------ |
| Q1   | 精确点查询                   | select v1 from root.db.d1 where time = ?                     |
| Q2   | 时间范围查询                 | select v1 from root.db.d1 where time > ? and time < ?        |
| Q3   | 带值过滤的时间范围查询       | select v1 from root.db.d1 where time > ? and time < ? and v1 > ? |
| Q4   | 时间范围聚合查询             | select count(v1) from root.db.d1 where and time > ? and time < ? |
| Q5   | 带值过滤的全时间范围聚合查询 | select count(v1) from root.db.d1 where v1 > ?                |
| Q6   | 带值过滤的时间范围聚合查询   | select count(v1) from root.db.d1 where v1 > ? and time > ? and time < ? |
| Q7   | 时间分组聚合查询             | select count(v1) from root.db.d1 group by ([?, ?), ?, ?)     |
| Q8   | 最新点查询                   | select last v1 from root.db.d1                               |
| Q9   | 倒序范围查询                 | select v1 from root.sg.d1 where time > ? and time < ? order by time desc |
| Q10  | 倒序带值过滤的范围查询       | select v1 from root.sg.d1 where time > ? and time < ? and v1 > ? order by time desc |





按照表1-5配置参数启动可描述测试场景为：从被测时序数据库执行10次2个设备2个传感器的倒序带值过滤的范围查询，SQL语句为：select s_0,s_31from data where time >2022-10-30T00:00:00+08:00 and time < 2022-10-30T00:04:10+08:00 and s_0 > -5 and device in d_21,d_46 order by time desc。

### 测试过程和测试结果持久化

IoT-benchmark目前支持通过配置参数“TEST_DATA_PERSISTENCE”将测试过程和测试结果持久化到IoTDB、MySQL和CSV；其中写入到MySQL和CSV可以定义分库分表的行数上限，例如“RECORD_SPLIT=true、RECORD_SPLIT_MAX_LINE=10000000”表示每个数据库表或CSV文件按照总行数为1千万切分存放；如果记录到MySQL或IoTDB需要提供数据库链接信息，分别包括“TEST_DATA_STORE_IP”数据库的IP地址、“TEST_DATA_STORE_PORT”数据库的端口号、“TEST_DATA_STORE_DB”数据库的名称、“TEST_DATA_STORE_USER”数据库用户名、“TEST_DATA_STORE_PW”数据库用户密码。

如果我们设置“TEST_DATA_PERSISTENCE=CSV”，测试执行时和执行完毕后我们可以在IoT-benchmark根目录下看到新生成的data文件夹，其下包含csv文件夹记录测试过程；csvOutput文件夹记录测试结果。如果我们设置“TEST_DATA_PERSISTENCE=MySQL”，它会在测试开始前在指定的MySQL数据库中创建命名如“testWithDefaultPath_被测数据库名称_备注_测试启动时间”的数据表记录测试过程；会在名为“CONFIG”的数据表（如果不存在则创建该表），写入本次测试的配置信息；当测试完成时会在名为“FINAL_RESULT”的数据表（如果不存在则创建该表）中写入本次测试结果。

# 实际案例

我们以中车青岛四方车辆研究所有限公司应用为例，参考《ＡｐａｃｈｅＩｏＴＤＢ在智能运维平台存储中的应用》中描述的场景进行实际操作说明。

测试目标：模拟中车青岛四方所场景因切换时间序列数据库实际需求，对比预期使用的IoTDB和原有系统使用的KairosDB性能。

测试环境：为了保证在实验过程中消除其他无关服务与进程对数据库性能的影响，以及不同数据库之间的相互影响，本实验中的本地数据库均部署并运行在资源配置相同的多个独立的虚拟机上。因此，本实验搭建了 4 台 Linux( CentOS7 /x86) 虚拟机，并分别在上面部署了IoT-benchmark、 IoTDB数据库、KairosDB数据库、MySQL数据库。每一台虚拟机的具体资源配置如表2-1所示。每一台虚拟机的具体用途如表2-2所示。

表2-1虚拟机配置信息

| 硬件配置信息 | 系统描述 |
| ------------ | -------- |
| OS System    | CentOS7  |
| CPU核数      | 16       |
| 内存         | 32G      |
| 硬盘         | 200G     |
| 网卡         | 千兆     |



表2-2虚拟机用途

| IP         | 用途          |
| ---------- | ------------- |
| 172.21.4.2 | IoT-benchmark |
| 172.21.4.3 | Apache-iotdb  |
| 172.21.4.4 | KaiosDB       |
| 172.21.4.5 | MySQL         |

##  写入测试

场景描述：创建100个客户端来模拟100列车、每列车3000个传感器、数据类型为DOUBLE类型、数据时间间隔为500ms（2Hz）、顺序发送。参考以上需求我们需要修改IoT-benchmark配置参数如表2-3中所列。

表2-3配置参数信息

| 参数名称                   | IoTDB值                     | KairosDB值 |
| -------------------------- | --------------------------- | ---------- |
| DB_SWITCH                  | IoTDB-013-SESSION_BY_TABLET | KairosDB   |
| HOST                       | 172.21.4.3                  | 172.21.4.4 |
| PORT                       | 6667                        | 8080       |
| BENCHMARK_WORK_MODE        | testWithDefaultPath         |            |
| OPERATION_PROPORTION       | 1:0:0:0:0:0:0:0:0:0:0       |            |
| CLIENT_NUMBER              | 100                         |            |
| GROUP_NUMBER               | 10                          |            |
| DEVICE_NUMBER              | 100                         |            |
| SENSOR_NUMBER              | 3000                        |            |
| INSERT_DATATYPE_PROPORTION | 0:0:0:0:1:0                 |            |
| POINT_STEP                 | 500                         |            |
| OP_MIN_INTERVAL            | 0                           |            |
| IS_OUT_OF_ORDER            | false                       |            |
| BATCH_SIZE_PER_WRITE       | 1                           |            |
| LOOP                       | 10000                       |            |
| TEST_DATA_PERSISTENCE      | MySQL                       |            |
| TEST_DATA_STORE_IP         | 172.21.4.5                  |            |
| TEST_DATA_STORE_PORT       | 3306                        |            |
| TEST_DATA_STORE_DB         | demo                        |            |
| TEST_DATA_STORE_USER       | root                        |            |
| TEST_DATA_STORE_PW         | admin                       |            |
| REMARK                     | demo                        |            |

首先在172.21.4.3和172.21.4.4上分别启动被测时间序列数据库Apache-IoTDB和KairosDB，之后在172.21.4.2、172.21.4.3和172.21.4.4上通过ser-benchamrk.sh脚本启动服务器资源监控（图2-1）。然后按照表2-3在172.21.4.2分别修改iotdb-0.13-0.0.1和kairosdb-0.0.1文件夹内的conf/config.properties文件满足测试需求。先后使用benchmark.sh启动对Apache-IoTDB和KairosDB的写入测试。

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=YmRjNjEwMzdjOGIwODczNGZjNzQ3MzA0ZGJlMzUwODFfbWNyY014WERmOFNUSVZtSmZVQ0tiTU9qVUI4aWpCQ3dfVG9rZW46Ym94Y256QkhCdDhBYUdjR1lyWkx4VXA4VDNiXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

图2-1服务器监控任务

​    例如我们首先启动对KairosDB的测试，IoT-benchmark会在MySQL数据库中创建CONFIG数据表存放本次测试配置信息（图2-2），测试执行中会有日志输出当前测试进度（图2-3）。测试完成时会输出本次测试结果（图2-3），同时将结果写入FINAL_RESULT数据表中（图2-4）。

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=MDc4MTIxZjMwZGM4OWQxNzZhMTZjNTg4NGVlZGViM2Nfd2dYRzZJM3U5dm9lQmxrUEtBcG15QWQxa1ZxMkc0c0VfVG9rZW46Ym94Y240azkwb0FOMGx0VVFKYzFZV2s4Q2ZnXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

图2-2测试配置信息表

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=NjY5NGExMmVjYmRkNTNiZTk5YjQ3NzJhZjBkM2I0ODRfTEY5TXZKampNa3FOQ0Y2UzVNT0ZiWmFRNkxNTEdZQ0pfVG9rZW46Ym94Y25laEJielRheFpBdGxpdWtWOEFhQ25mXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

 



![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=OTExMWRiNGExYmE5NGU4NzBlMzFiMDBhYzM2ZGJhZTdfU2lsTlU3elVGb3RlUmZvWDlETmVnb3FYeE1VQzJQZEZfVG9rZW46Ym94Y24zdjJ0U2NVNjU1M2pxNkpCSU15VHNoXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)



![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=ODY5Mzk3ZmMxMzliNWU4YjNhYTk4MjcyMmUyYjRhY2RfWjVZb0tZZDVJcllJVkF1UU9mZXk1VlUyV2tWcmZGM2tfVG9rZW46Ym94Y25lYXY2OWVqRVRJdHp0V2NsUHNaR1hiXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

 

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=MmIzNjc5ZjE2NmEzNDFkZjVmODFmM2IyMThjZTYwNmVfaURYZmpHdnlqTjFtbkk5aEk5alVieEhQcmNEYzBzR3lfVG9rZW46Ym94Y245OEZEY0ozS1RHV2djdmFTTjJMcVBlXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)



图2-3测试进度和结果

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=MGZkNmEzMWU5ZmVhMTg0MzQ1NzlmM2IyZDY3OWUzY2RfdWUySUF5UmI3SFMwOGNuWk5aSzMxRktJdkJxNnY1VDVfVG9rZW46Ym94Y24yaVRVQVZ1dDROeGUwQlVwMkRoNjJiXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

图2-4测试结果表

之后我们再启动对Apache-IoTDB的测试，同样的IoT-benchmark会在MySQL数据库CONFIG数据表中写入本次测试配置信息，测试执行中会有日志输出当前测试进度。测试完成时会输出本次测试结果，同时将结果写入FINAL_RESULT数据表中。

依照测试结果信息我们知道同样的配置写入Apache-IoTDB和KairosDB写入延时时间分别为：55.98ms和1324.45ms；写入吞吐分别为：5,125,600.86点/秒和224,819.01点/秒；测试分别执行了585.30秒和11777.99秒。并且KairosDB有写入失败出现，排查后发现是数据磁盘使用率已达到100%，无磁盘空间继续接收数据。而Apache-IoTDB无写入失败现象，全部数据写入完毕后占用磁盘空间仅为4.7G（如图2-5所示）；从写入吞吐和磁盘占用情况上看Apache-IoTDB均优于KairosDB。当然后续还有其他测试来从多方面观察和对比，比如查询性能、文件压缩比、数据安全性等。

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=ODMxYWFkNGQ5OTIzYjQyODY5NjkzMjAwYWIyZTc1Y2FfeFJPU01rTzI3bXJtNFNWalBOYmlCS2h2SWNpdzd0c2NfVG9rZW46Ym94Y25JdU5xbmlFZGF3MzljbzZXMDRpYVc5XzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

图2-5磁盘使用情况

那么测试过程中各个服务器资源使用情况如何呢？每个写操作具体的表现如何呢？这个时候我们就可以通过安装和使用Tableau来可视化服务器监控表和测试过程记录表内的数据了。Tableau的使用本文不展开介绍，通过它连接测试数据持久化的数据表后具体结果下如图（以Apache-IoTDB为例）：

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=ODlhZmVjMzEyYmM2ODYwNzExMGU4Yzc3ZGMwZmFiMWVfTGJhWHczMU5qaks0MzBwSWpha1JRZ3QxZFhtdzBNRVFfVG9rZW46Ym94Y25NNGN0dVVGaFV6SHA2YmppWGlkYjlkXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=NGFjOTAzMjdjMTg3NDg2ZGNjOWRiNzNmNWVlMDAxZjBfRUVBeko2NWhudjUxSnZoWlV0ZURPNEJwd2E5aDM3VnFfVG9rZW46Ym94Y25DTWVLMFpnUXdUVFRXSTZZSVp6M1FjXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

图2-6Tableau可视化测试过程



## 查询测试

场景描述：在写入测试场景下模拟10个客户端对时序数据库Apache-IoTDB内存放的数据进行全类型查询任务。配置如下：

表2-4配置参数信息

| 参数名称             | 示例                  |
| -------------------- | --------------------- |
| CLIENT_NUMBER        | 100                   |
| QUERY_DEVICE_NUM     | 2                     |
| QUERY_SENSOR_NUM     | 2                     |
| QUERY_AGGREGATE_FUN  | count                 |
| STEP_SIZE            | 1                     |
| QUERY_INTERVAL       | 250000                |
| QUERY_LOWER_VALUE    | -5                    |
| GROUP_BY_TIME_UNIT   | 20000                 |
| LOOP                 | 30                    |
| OPERATION_PROPORTION | 0:1:1:1:1:1:1:1:1:1:1 |

执行结果：

![img](https://apache-iotdb.feishu.cn/space/api/box/stream/download/asynccode/?code=MzJjYTA1OGVjMDdjZDA3YTc0OTNkZTFiNTI5ODNlOGJfRllWNFhrMUhPVjRHV1ZudTR0T1NVSmNWUlQ2bE54UnZfVG9rZW46Ym94Y25TTVVDMjFVc3FDdERXVnZmRkdacTJkXzE2ODU1MTQ5MTM6MTY4NTUxODUxM19WNA)

图2-7查询测试结果

## 其他参数说明

之前章节中针对Apache-IoTDB和KairosDB进行写入性能对比，但是用户如果要执行模拟真实写入速率测试该如何配置？测试时间过长该如何控制呢？生成的模拟数据有哪些规律吗？如果IoT-Benchmark服务器配置较低，可以使用多台机器模拟压力输出吗？

表2-5配置参数信息

| 场景                                                         | 参数                       | 值                                                           | 说明                              |
| ------------------------------------------------------------ | -------------------------- | ------------------------------------------------------------ | --------------------------------- |
| 模拟真实写入速率                                             | OP_INTERVAL                | -1                                                           | 也可输入整数控制操作间隔          |
| 指定测试时长（1小时）                                        | TEST_MAX_TIME              | 3600000                                                      | 单位 ms；需要LOOP执行时间大于该值 |
| 定义模拟数据规律：支持全部数据类型，数量平均分类；支持五种数据分布，数量平均分布；字符串长度为10；小数位数为2 | INSERT_DATATYPE_PROPORTION | 1:1:1:1:1:1                                                  | 数据类型分布比率                  |
| LINE_RATIO                                                   | 1                          | 线性                                                         |                                   |
| SIN_RATIO                                                    | 1                          | 傅里叶函数                                                   |                                   |
| SQUARE_RATIO                                                 | 1                          | 方波                                                         |                                   |
| RANDOM_RATIO                                                 | 1                          | 随机数                                                       |                                   |
| CONSTANT_RATIO                                               | 1                          | 常数                                                         |                                   |
| STRING_LENGTH                                                | 10                         | 字符串长度                                                   |                                   |
| DOUBLE_LENGTH                                                | 2                          | 小数位数                                                     |                                   |
| 三台机器模拟300台设备数据写入                                | BENCHMARK_CLUSTER          | true                                                         | 开启多benchmark模式               |
| BENCHMARK_INDEX                                              | 0、1、3                    | 以2.3章节写入参数为例：0号负责设备编号0~99数据写入；1号负责设备编号100~199数据写入；2号负责设备点好200~299数据写入； |                                   |