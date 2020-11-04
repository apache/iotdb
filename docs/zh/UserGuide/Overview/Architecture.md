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

# 架构

IoTDB套件由若干个组件构成，共同形成“数据收集-数据写入-数据存储-数据查询-数据可视化-数据分析”等一系列功能。

图1.1展示了使用IoTDB套件全部组件后形成的整体应用架构。下文称所有组件形成IoTDB套件，而IoTDB特指其中的时间序列数据库组件。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/25913899/67943956-39c1e800-fc16-11e9-8da2-a662f8246816.png">

在图1.1中，用户可以通过JDBC将来自设备上传感器采集的时序数据、服务器负载和CPU内存等系统状态数据、消息队列中的时序数据、应用程序的时序数据或者其他数据库中的时序数据导入到本地或者远程的IoTDB中。用户还可以将上述数据直接写成本地（或位于HDFS上）的TsFile文件。

可以将TsFile文件写入到HDFS上，进而实现在Hadoop或Spark的数据处理平台上的诸如异常检测、机器学习等数据处理任务。

对于写入到HDFS或者本地的TsFile文件，可以利用TsFile-Hadoop或TsFile-Spark连接器允许Hadoop或Spark进行数据处理。

对于分析的结果，可以写回成TsFile文件。

IoTDB和TsFile还提供了相应的客户端工具，满足用户查看和写入数据的SQL形式、脚本形式和图形化形式等多种需求。
