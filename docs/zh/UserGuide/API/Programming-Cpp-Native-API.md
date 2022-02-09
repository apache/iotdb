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

## C++ 原生接口

### 依赖

- Java 8+
- Maven 3.5+
- Flex
- Bison 2.7+
- Boost 1.56+
- OpenSSL 1.0+
- GCC 5.5.0+


### 安装方法

编译 C++客户端之前首先需要本地编译 Thrift 库，compile-tools 模块负责编译 Thrift，之后再编译 client-cpp。

#### 在 Mac 上编译 Thrift

- Bison

Mac 环境下预安装了 Bison 2.3 版本，但该版本过低不能够用来编译 Thrift。使用 Bison 2.3 版本是会报以下错误：

  ```invalid directive: '%code'```

使用下面 brew 命令更新 bison 版本    

```     shell
brew install bison     
brew link bison --force        
```

 添加环境变量：

```            shell
echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.bash_profile     
```

- Boost

确保安装较新的 Boost 版本：

```shell
brew install boost
brew link boost
```

- OpenSSL

确保 openssl 库已安装，默认的 openssl 头文件路径为"/usr/local/opt/openssl/include"
如果在编译 Thrift 过程中出现找不到 openssl 的错误，尝试添加

`-Dopenssl.include.dir=""`

#### 在 Linux 上编译 Thrift

Linux 下需要确保 g++已被安装。

Ubuntu 20:

一条命令安装所有依赖库：  

```shell
sudo apt-get install gcc-9 g++-9 libstdc++-9-dev bison flex libboost-all-dev libssl-dev zlib1g-dev
```

CentOS 7.x:

在centos 7.x里，可用yum命令安装部分依赖。  

```shell
sudo yum install bison flex openssl-devel
```

使用yum安装的GCC、boost版本过低，在编译时会报错，需自行安装或升级。  

#### 在 Windows 上编译 Thrift

- 编译构建环境

保证你的 Windows 系统已经搭建好了完整的 C/C++的编译构建环境。可以是 MSVC，MinGW 等。

如使用 MS Visual Studio，在安装时需要勾选 Visual Studio C/C++ IDE and compiler(supporting CMake, Clang, MinGW)。

- CMake

CMake 官网下载地址 https://cmake.org/download/

CMake 需要根据不同编译平台使用不同的生成器。CMake 支持的生成器列表如下 (`cmake --help`的结果）：

```
  Visual Studio 16 2019        = Generates Visual Studio 2019 project files.
                                 Use -A option to specify architecture.
  Visual Studio 15 2017 [arch] = Generates Visual Studio 2017 project files.
                                 Optional [arch] can be "Win64" or "ARM".
  Visual Studio 14 2015 [arch] = Generates Visual Studio 2015 project files.
                                 Optional [arch] can be "Win64" or "ARM".
  Visual Studio 12 2013 [arch] = Generates Visual Studio 2013 project files.
                                 Optional [arch] can be "Win64" or "ARM".
  Visual Studio 11 2012 [arch] = Generates Visual Studio 2012 project files.
                                 Optional [arch] can be "Win64" or "ARM".
  Visual Studio 10 2010 [arch] = Generates Visual Studio 2010 project files.
                                 Optional [arch] can be "Win64" or "IA64".
  Visual Studio 9 2008 [arch]  = Generates Visual Studio 2008 project files.
                                 Optional [arch] can be "Win64" or "IA64".
  Borland Makefiles            = Generates Borland makefiles.
* NMake Makefiles              = Generates NMake makefiles.
  NMake Makefiles JOM          = Generates JOM makefiles.
  MSYS Makefiles               = Generates MSYS makefiles.
  MinGW Makefiles              = Generates a make file for use with
                                 mingw32-make.
  Unix Makefiles               = Generates standard UNIX makefiles.
  Green Hills MULTI            = Generates Green Hills MULTI files
                                 (experimental, work-in-progress).
  Ninja                        = Generates build.ninja files.
  Ninja Multi-Config           = Generates build-<Config>.ninja files.
  Watcom WMake                 = Generates Watcom WMake makefiles.
  CodeBlocks - MinGW Makefiles = Generates CodeBlocks project files.
  CodeBlocks - NMake Makefiles = Generates CodeBlocks project files.
```

编译 client-cpp 时的 mvn 命令中添加 -Dcmake.generator="" 选项来指定使用的生成器名称。

 `mvn package -Dcmake.generator="Visual Studio 15 2017 [arch]"`

- Flex 和 Bison

Windows 版的 Flex 和 Bison 可以从 SourceForge 下载：https://sourceforge.net/projects/winflexbison/

下载后需要将可执行文件重命名为 flex.exe 和 bison.exe 以保证编译时能够被找到，添加可执行文件的目录到 PATH 环境变量中。

- Boost

Boost 官网下载地址 https://www.boost.org/users/download/

依次执行 bootstrap.bat 和 b2.exe，本地编译 boost

```shell
bootstrap.bat
b2.exe
```

为了帮助 CMake 本地安装好的 Boost，在编译 client-cpp 的 mvn 命令中需添加： 

`-Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder}`

- openssl

openssl 官网源码下载地址 https://www.openssl.org/source/

二进制文件下载地址 http://slproweb.com/products/Win32OpenSSL.html

- 增加环境变量

在编译前，需要确定cmake,flex,bison,openssl都加入了PATH。

#### 编译及测试

Maven 命令中添加"-P client-cpp" 选项编译 client-cpp 模块。client-cpp 需要依赖编译好的 thrift，即 compile-tools 模块。

- Mac , Linux 下，编译C++客户端完整命令如下：

`mvn package -P compile-cpp -pl example/client-cpp-example -am -DskipTest`

- Windows下，编译C++客户端：

编译前需额外添加Boost相关的参数：

`-Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder}` 

需指定cmake的generator,例如：

`-Dcmake.generator="Visual Studio 15 2017 [arch]"`

完整编译C++客户端命令如下：

```shell
mvn package -P compile-cpp -pl client-cpp,server,example/client-cpp-example -am -Dcmake.generator="your cmake generator" -Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder} -DskipTests
```

编译成功后，打包好的 zip 文件将位于："client-cpp/target/client-cpp-${project.version}-cpp-${os}.zip"

解压后的目录结构如下图所示 (Mac)：

```shell
.
+-- client
|   +-- include
|       +-- Session.h
|       +-- TSIService.h
|       +-- rpc_types.h
|       +-- rpc_constants.h
|       +-- thrift
|           +-- thrift_headers...
|   +-- lib
|       +-- libiotdb_session.dylib
```

### Q&A

#### Mac 相关问题

本地 Maven 编译 Thrift 时如出现以下链接的问题，可以尝试将 xcode-commandline 版本从 12 降低到 11.5

https://stackoverflow.com/questions/63592445/ld-unsupported-tapi-file-type-tapi-tbd-in-yaml-file/65518087#65518087

#### Windows 相关问题

Maven 编译 Thrift 时需要使用 wget 下载远端文件，可能出现以下报错：

```
Failed to delete cached file C:\Users\Administrator\.m2\repository\.cache\download-maven-plugin\index.ser
```

解决方法：

- 尝试删除 ".m2\repository\\.cache\" 目录并重试。
- 在添加 pom 文件对应的 download-maven-plugin 中添加 "\<skipCache>true\</skipCache>"
