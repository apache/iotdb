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


## Python 原生接口

### 依赖

首选python3.7或更高版本。

您必须安装Thrift（0.11.0或更高版本）才能将我们的Thrift文件编译为python代码。 

下面是官方安装教程：

```
http://thrift.apache.org/docs/install/
```

### 安装方法

 * 方案1: pip install

您可以在https://pypi.org/project/apache-iotdb/上找到Apache IoTDB Python客户端API软件包。

下载命令为：

```
pip install apache-iotdb
```

 * 方案2：thrift的基本用法

或者，如果您了解thrift的基本用法，则可以在以下位置查看thrift源文件：
`thrift\src\main\thrift\rpc.thrift`，使用`thrift -gen py -out ./target/iotdb rpc.thrift`生成Python库。

### 示例代码

我们在`client-py/src/SessionExample.py`中提供了一个示例，说明如何使用Thrift库连接到IoTDB，请先仔细阅读，然后再编写自己的代码。


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

编译C++客户端之前首先需要本地编译Thrift库，compile-tools模块负责编译Thrift，之后再编译client-cpp。

#### 在Mac上编译Thrift

- Bison

Mac 环境下预安装了Bison 2.3版本，但该版本过低不能够用来编译Thrift。使用Bison 2.3版本是会报以下错误：

  ```invalid directive: '%code'```      
        
使用下面brew 命令更新bison版本    

```     
brew install bison     
brew link bison --force        
```

 添加环境变量：

```            
echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.bash_profile     
```



- Boost

确保安装较新的Boost版本：

```
brew install boost
brew link boost
```


- OpenSSL

确保openssl库已安装，默认的openssl头文件路径为"/usr/local/opt/openssl/include"
如果在编译Thrift过程中出现找不到openssl的错误，尝试添加

`-Dopenssl.include.dir=""`



#### 在Linux上编译Thrift

Linux下需要确保g++已被安装。

Ubuntu 20:

一条命令安装所有依赖库：  

```
sudo apt-get install gcc-9 g++-9 libstdc++-9-dev bison flex libboost-all-dev libssl-dev zlib1g-dev
```

CentOS 7.x:  

在centos 7.x里，可用yum命令安装部分依赖。  

```
yum install bison flex openssl-devel
```

使用yum安装的GCC、boost版本过低，在编译时会报错，需自行安装或升级。  


#### 在Windows上编译Thrift

保证你的Windows系统已经搭建好了完整的C/C++的编译构建环境。可以是MSVC，MinGW等。

如使用MS Visual Studio，在安装时需要勾选 Visual Studio C/C++ IDE and compiler(supporting CMake, Clang, MinGW)。



- Flex 和 Bison

Windows版的 Flex 和 Bison 可以从 SourceForge下载: https://sourceforge.net/projects/winflexbison/

下载后需要将可执行文件重命名为flex.exe和bison.exe以保证编译时能够被找到，添加可执行文件的目录到PATH环境变量中。

- Boost

Boost官网下载新版本Boost: https://www.boost.org/users/download/

依次执行bootstrap.bat 和 b2.exe，本地编译boost

```
bootstrap.bat
.\b2.exe
```

为了帮助CMake本地安装好的Boost，在编译client-cpp的mvn命令中需添加： 

`-Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder}`

#### CMake 生成器

CMake需要根据不同编译平台使用不同的生成器。CMake支持的生成器列表如下(`cmake --help`的结果)：


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
  CodeBlocks - NMake Makefiles = Generates CodeBlocks project fi

```

编译client-cpp 时的mvn命令中添加 -Dcmake.generator="" 选项来指定使用的生成器名称。
 `mvn package -Dcmake.generator="Visual Studio 15 2017 [arch]"`



#### 编译C++ 客户端



Maven 命令中添加"-P client-cpp" 选项编译client-cpp模块。client-cpp需要依赖编译好的thrift，即compile-tools模块。


#### 编译及测试

完整的C++客户端命令如下：

`mvn  package -P compile-cpp  -pl example/client-cpp-example -am -DskipTest`

注意在Windows下需提前安装好Boost，并添加以下Maven 编译选项:
```
-Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder}` 
```

例如：

```
mvn package -P compile-cpp -pl client-cpp,server,example/client-cpp-example -am 
-D"boost.include.dir"="D:\boost_1_75_0" -D"boost.library.dir"="D:\boost_1_75_0\stage\lib" -DskipTests
```



编译成功后，打包好的.zip文件将位于："client-cpp/target/client-cpp-${project.version}-cpp-${os}.zip"

解压后的目录结构如下图所示(Mac)：

```
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

#### Mac相关问题

本地Maven编译Thrift时如出现以下链接的问题，可以尝试将xcode-commandline版本从12降低到11.5

https://stackoverflow.com/questions/63592445/ld-unsupported-tapi-file-type-tapi-tbd-in-yaml-file/65518087#65518087

#### Windows相关问题

Maven编译Thrift时需要使用wget下载远端文件，可能出现以下报错：

```
Failed to delete cached file C:\Users\Administrator\.m2\repository\.cache\download-maven-plugin\index.ser
```

解决方法:

- 尝试删除 ".m2\repository\\.cache\" 目录并重试。
- 在添加 pom文件对应的 download-maven-plugin 中添加 "\<skipCache>true\</skipCache>"



## Go 原生接口

### 依赖

 * golang >= 1.13
 * make   >= 3.0
 * curl   >= 7.1.1
 * thrift 0.13.x
 * Linux、Macos或其他类unix系统
 * Windows+bash(WSL、cygwin、Git Bash)


### 安装方法

 * 通过go mod

```sh
export GO111MODULE=on
export GOPROXY=https://goproxy.io

mkdir session_example && cd session_example

curl -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/example/session_example.go

go mod init session_example
go run session_example.go
```

* 通过GOPATH

```sh
# get thrift 0.13.0
go get github.com/apache/thrift
cd $GOPATH/src/github.com/apache/thrift
git checkout 0.13.0

mkdir -p $GOPATH/src/iotdb-client-go-example/session_example
cd $GOPATH/src/iotdb-client-go-example/session_example
curl -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/example/session_example.go
go run session_example.go
```

