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

# 编程-其他语言

## Python API

### 1. 介绍

这是一个如何使用thrift rpc接口通过python连接到IoTDB的示例。 在Linux或Windows上情况会有所不同，我们将介绍如何分别在两个系统上进行操作。

### 2. Prerequisites

首选python3.7或更高版本。

您必须安装Thrift（0.11.0或更高版本）才能将我们的Thrift文件编译为python代码。 以下是安装的官方教程：

```
http://thrift.apache.org/docs/install/
```

### 3. 如何获取Python库

#### 选项1：pip安装

您可以在https://pypi.org/project/apache-iotdb/上找到Apache IoTDB Python客户端API软件包。
​    下载命令为：

```
pip install apache-iotdb
```

#### 选项2：使用我们提供的编译脚本

如果您在路径中添加了Thrift可执行文件，则可以只运行`client-py / compile.sh`或`client-py \ compile.bat`，或者必须对其进行修改以将变量`THRIFT_EXE`设置为指向 您的可执行文件。 这将在文件夹“`target`下生成节俭的源代码，您可以将其添加到`PYTHONPATH`中，以便可以在代码中使用该库。 请注意，这些脚本按相对路径查找节俭源文件，因此，如果将脚本移动到其他位置，它们将不再有效。

#### 选项3：节俭的基本用法

或者，如果您了解Thrift的基本用法，则只能将Thrift源文件下载到`service-rpc \ src \ main \ thrift \ rpc.thrift`中，而只需使用`thrift -gen py -out ./target/  iotdb rpc.thrift`生成python库。

### 4. 使用范例

我们在`client-py / src /client_example.py`中提供了一个示例，说明如何使用Thrift库连接到IoTDB，在编写自己的代码之前，请仔细阅读。
