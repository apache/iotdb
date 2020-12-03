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

# 下载

IoTDB为您提供了两种安装方式，您可以参考下面的建议，任选其中一种：

第一种，从官网下载安装包。这是我们推荐使用的安装方式，通过该方式，您将得到一个可以立即使用的、打包好的二进制可执行文件。

第二种，使用源码编译。若您需要自行修改代码，可以使用该安装方式。

## 安装环境要求

安装前请保证您的电脑上配有JDK>=1.8的运行环境，并配置好JAVA_HOME环境变量。

如果您需要从源码进行编译，还需要安装：

1. Maven>=3.1的运行环境，具体安装方法可以参考以下链接：[https://maven.apache.org/install.html](https://maven.apache.org/install.html)。

> 注： 也可以选择不安装，使用我们提供的'mvnw.sh' 或 'mvnw.cmd' 工具。使用时请用'mvnw.sh' 或 'mvnw.cmd'命令代替下文的'mvn'命令。

## 从官网下载二进制可执行文件

您可以从[http://iotdb.apache.org/Download/](http://iotdb.apache.org/Download/)上下载已经编译好的可执行程序iotdb-xxx.zip，该压缩包包含了IoTDB系统运行所需的所有必要组件。


下载后，您可使用以下操作对IoTDB的压缩包进行解压: 

```
Shell > uzip iotdb-<version>.zip
```


## 使用源码编译

您可以获取已发布的源码https://iotdb.apache.org/Download/，或者从git仓库获取https://github.com/apache/iotdb/tree/master

源码克隆后，进入到源码文件夹目录下，使用以下命令进行编译：

```
> mvn clean package -pl server -am -Dmaven.test.skip=true
```

编译后，IoTDB 服务器会在 "server/target/iotdb-server-{project.version}" 文件夹下，包含以下内容：

```
+- sbin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
|
+- tools/      <-- system tools
```

## 通过Docker安装 (Dockerfile)

你可以通过[这份指南](../Server/Docker%20Image.md)编译并运行一个IoTDB docker image。
