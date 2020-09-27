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

# 其他语言

## Python API

### 1. 介绍

这是一个如何使用thrift rpc接口通过python连接到IoTDB的示例。 在Linux或Windows上情况会有所不同，我们将介绍如何分别在两个系统上进行操作。

### 2. 先决条件

首选python3.7或更高版本。

您必须安装Thrift（0.11.0或更高版本）才能将我们的Thrift文件编译为python代码。 

下面是官方安装教程：

```
http://thrift.apache.org/docs/install/
```

### 3. 如何获取Python库

#### 方案1: pip install

您可以在https://pypi.org/project/apache-iotdb/上找到Apache IoTDB Python客户端API软件包。

下载命令为：

```
pip install apache-iotdb
```

#### 方案2: 使用我们提供的编译脚本

如果你在路径中添加了Thrift可执行文件，则可以运行`client-py/compile.sh`或
  `client-py \ compile.bat`，或者你必须对其进行修改以将变量`THRIFT_EXE`设置为指向你的可执行文件。 这将在`target`文件夹下生成节俭源，你可以将其添加到你的`PYTHONPATH`，以便你可以在代码中使用该库。 请注意，脚本通过相对路径找到节俭的源文件，因此，如果将脚本移动到其他位置，它们将不再有效。

#### 方案3：thrift的基本用法

或者，如果您了解thrift的基本用法，则只能在以下位置下载Thrift源文件：
`thrift\src\main\thrift\rpc.thrift`，并且只需使用`thrift -gen py -out ./target/iotdb rpc.thrift`生成python库。

### 4. 示例代码

我们在`client-py / src/ client_example.py`中提供了一个示例，说明如何使用Thrift库连接到IoTDB，请先仔细阅读，然后再编写自己的代码。
