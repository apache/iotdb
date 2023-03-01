---
title: 主要功能特点
order: 20
---
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

## 主要功能特点

IoTDB 具有以下特点：

* 灵活的部署方式
  * 云端一键部署
  * 终端解压即用
  * 终端-云端无缝连接（数据云端同步工具）
* 低硬件成本的存储解决方案
  *	高压缩比的磁盘存储（10 亿数据点硬盘成本低于 1.4 元）
* 目录结构的时间序列组织管理方式
  *	支持复杂结构的智能网联设备的时间序列组织
  *	支持大量同类物联网设备的时间序列组织
  *	可用模糊方式对海量复杂的时间序列目录结构进行检索
* 高通量的时间序列数据读写
  *	支持百万级低功耗强连接设备数据接入（海量）
  *	支持智能网联设备数据高速读写（高速）
  *	以及同时具备上述特点的混合负载
* 面向时间序列的丰富查询语义
  *	跨设备、跨传感器的时间序列时间对齐
  *	面向时序数据特征的计算
  *	提供面向时间维度的丰富聚合函数支持
* 极低的学习门槛
  *	支持类 SQL 的数据操作
  *	提供 JDBC 的编程接口
  *	完善的导入导出工具
* 完美对接开源生态环境
  *	支持开源数据分析生态系统：Hadoop、Spark
  *	支持开源可视化工具对接：Grafana
