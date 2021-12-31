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

## 什么是IoTDB-Quality-UDF

对基于时序数据的应用而言，数据质量至关重要。**IoTDB-Library-UDF**基于IoTDB用户自定义函数(UDF)，实现了一系列关于数据质量的函数，包括数据画像、数据质量评估与修复等，有效满足了工业领域对数据质量的需求。

## 快速开始
1. 下载包含全部依赖的jar包和注册脚本；
2. 将jar包复制到IoTDB程序目录的`ext\udf`目录下；
3. 运行`sbin\start-server.bat`（在Windows下）或`sbin\start-server.sh`（在Linux或MacOS下）以启动IoTDB服务器；
4. 将注册脚本复制到IoTDB的程序目录下（与`sbin`目录同级的根目录下），修改脚本中的参数（如果需要）并运行注册脚本以注册UDF。

## 联系我们

+ Email: iotdb-quality@protonmail.com