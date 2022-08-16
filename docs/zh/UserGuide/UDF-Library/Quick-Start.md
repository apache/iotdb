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
# UDF 函数库
## 快速开始

### 什么是 UDF 函数库

对基于时序数据的应用而言，数据质量至关重要。**UDF 函数库** 基于 IoTDB 用户自定义函数 (UDF)，实现了一系列关于数据质量的函数，包括数据画像、数据质量评估与修复等，有效满足了工业领域对数据质量的需求。

### 快速开始

1. 下载包含全部依赖的 jar 包和注册脚本；
2. 将 jar 包复制到 IoTDB 程序目录的`ext\udf`目录下；
3. 运行`sbin\start-server.bat`（在 Windows 下）或`sbin\start-server.sh`（在 Linux 或 MacOS 下）以启动 IoTDB 服务器；
4. 将注册脚本复制到 IoTDB 的程序目录下（与`sbin`目录同级的根目录下），修改脚本中的参数（如果需要）并运行注册脚本以注册 UDF。


### 下载

由于我们的代码正在审核中，Apache仓库中目前还没有代码。在审核完成之前，您可以前往我们的[旧网页](https://thulab.github.io/iotdb-quality/zh/Download.html)下载上述文件。


