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

# 数据查询

数据查询有如下几种类型

* 原始数据查询
* 聚合查询
* 降采样查询
* 单点补空值查询
* 最新数据查询

为了实现以上几种查询，IoTDB 查询引擎中设计了针对单个时间序列的基础查询组件，在此基础上，实现了多种查询功能。

## 相关文档

* [基础查询组件](/#/SystemDesign/progress/chap5/sec2)
* [原始数据查询](/#/SystemDesign/progress/chap5/sec3)
* [聚合查询](/#/SystemDesign/progress/chap5/sec4)
* [降采样查询](/#/SystemDesign/progress/chap5/sec5)
* [最近时间戳查询](/#/SystemDesign/progress/chap5/sec6)
