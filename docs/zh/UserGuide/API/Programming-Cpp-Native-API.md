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

# C++ 原生接口

## 依赖

- Java 8+
- Maven 3.5+
- Flex
- Bison 2.7+
- Boost 1.56+
- OpenSSL 1.0+
- GCC 5.5.0+


## 安装

### 安装相关依赖

- **MAC**

  1. 安装 Bison ：Mac 环境下预安装了 Bison 2.3 版本，但该版本过低不能够用来编译 Thrift。
  
     使用 Bison 2.3 版本会报以下错误：
     ```invalid directive: '%code'```
  
     使用下面 brew 命令更新 bison 版本：
     ```shell
     brew install bison
     brew link bison --force
     ```
     
     添加环境变量：
     ```shell
     echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.bash_profile
     ```
     
  2. 安装 Boost ：确保安装较新的 Boost 版本。
  
     ```shell
     brew install boost
     brew link boost
     ```
     
  3. 检查 OpenSSL ：确保 openssl 库已安装，默认的 openssl 头文件路径为"/usr/local/opt/openssl/include"

     如果在编译过程中出现找不到 openssl 的错误，尝试添加`-Dopenssl.include.dir=""`


- **Ubuntu 20**

    使用以下命令安装所有依赖：

    ```shell
    sudo apt-get install gcc-9 g++-9 libstdc++-9-dev bison flex libboost-all-dev libssl-dev zlib1g-dev
    ```


- **CentOS 7.x**

    使用 yum 命令安装部分依赖。

    ```shell
    sudo yum install bison flex openssl-devel
    ```

    使用 yum 安装的 GCC、Boost 版本过低，在编译时会报错，需自行安装或升级。


- **Windows**

    1. 构建编译环境
       * 安装 MS Visual Studio（推荐安装 2019 版本）：安装时需要勾选 Visual Studio C/C++ IDE and compiler(supporting CMake, Clang, MinGW)。
       * 下载安装 [CMake](https://cmake.org/download/) 。

    2. 下载安装 Flex、Bison
       * 下载 [Win_Flex_Bison](https://sourceforge.net/projects/winflexbison/) 。
       * 下载后需要将可执行文件重命名为 flex.exe 和 bison.exe 以保证编译时能够被找到，添加可执行文件的目录到 PATH 环境变量中。

    3. 安装 Boost
       * 下载 [Boost](https://www.boost.org/users/download/) 。
       * 本地编译 Boost ：依次执行 bootstrap.bat 和 b2.exe 。
       
    4. 安装 OpenSSL 
       * 下载安装 [OpenSSL](http://slproweb.com/products/Win32OpenSSL.html) 。


### 执行编译

从 git 克隆源代码:
```shell
git clone https://github.com/apache/iotdb.git
```

默认的主分支是master分支，如果你想使用某个发布版本，请切换分支 (如 0.13 版本):
```shell
git checkout rel/0.13
```

在 IoTDB 根目录下执行 maven 编译:

- Mac & Linux
    ```shell
    mvn package -P compile-cpp -pl example/client-cpp-example -am -DskipTest
    ```
  
- Windows
    ```shell
    mvn package -P compile-cpp -pl client-cpp,server,example/client-cpp-example -am -Dcmake.generator="your cmake generator" -Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder} -DskipTests
    ```
     - CMake 根据不同编译平台使用不同的生成器，需添加`-Dcmake.generator=""`选项来指定使用的生成器名称，例如： `-Dcmake.generator="Visual Studio 16 2019"`。（通过`cmake --help`命令可以查看 CMake 支持的生成器列表）
     - 为了帮助 CMake 找到本地安装好的 Boost，在编译命令中需添加相关参数，例如：`-DboostIncludeDir="C:\Program Files (x86)\boost_1_78_0" -DboostLibraryDir="C:\Program Files (x86)\boost_1_78_0\stage\lib"`

编译成功后，打包好的 zip 文件位于 `client-cpp/target/client-cpp-${project.version}-cpp-${os}.zip`


## 基本接口说明

下面将给出 Session 接口的简要介绍和原型定义：

### 初始化

- 开启 Session
```cpp
void open(); 
```

- 开启 Session，并决定是否开启 RPC 压缩
```cpp
void open(bool enableRPCCompression); 
```
  注意: 客户端的 RPC 压缩开启状态需和服务端一致。

- 关闭 Session
```cpp
void close(); 
```

### 数据定义接口（DDL）

#### Database 管理

- 设置 database
```cpp
void setStorageGroup(const std::string &storageGroupId);
```

- 删除单个或多个 database
```cpp
void deleteStorageGroup(const std::string &storageGroup);
void deleteStorageGroups(const std::vector<std::string> &storageGroups);
```

#### 时间序列管理

- 创建单个或多个非对齐时间序列
```cpp
void createTimeseries(const std::string &path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                          CompressionType::CompressionType compressor);
                          
void createMultiTimeseries(const std::vector<std::string> &paths,
                           const std::vector<TSDataType::TSDataType> &dataTypes,
                           const std::vector<TSEncoding::TSEncoding> &encodings,
                           const std::vector<CompressionType::CompressionType> &compressors,
                           std::vector<std::map<std::string, std::string>> *propsList,
                           std::vector<std::map<std::string, std::string>> *tagsList,
                           std::vector<std::map<std::string, std::string>> *attributesList,
                           std::vector<std::string> *measurementAliasList);
```

- 创建对齐时间序列
```cpp
void createAlignedTimeseries(const std::string &deviceId,
                             const std::vector<std::string> &measurements,
                             const std::vector<TSDataType::TSDataType> &dataTypes,
                             const std::vector<TSEncoding::TSEncoding> &encodings,
                             const std::vector<CompressionType::CompressionType> &compressors);
```

- 删除一个或多个时间序列
```cpp
void deleteTimeseries(const std::string &path);
void deleteTimeseries(const std::vector<std::string> &paths);
```

- 检查时间序列是否存在
```cpp
bool checkTimeseriesExists(const std::string &path);
```

#### 元数据模版

- 创建元数据模板
```cpp
void createSchemaTemplate(const Template &templ);
```

- 挂载元数据模板
```cpp
void setSchemaTemplate(const std::string &template_name, const std::string &prefix_path);
```
请注意，如果一个子树中有多个孩子节点需要使用模板，可以在其共同父母节点上使用 setSchemaTemplate 。而只有在已有数据点插入模板对应的物理量时，模板才会被设置为激活状态，进而被 show timeseries 等查询检测到。

- 卸载元数据模板
```cpp
void unsetSchemaTemplate(const std::string &prefix_path, const std::string &template_name);
```
注意：目前不支持从曾经在`prefixPath`路径及其后代节点使用模板插入数据后（即使数据已被删除）卸载模板。

- 在创建概念元数据模板以后，还可以通过以下接口增加或删除模板内的物理量。请注意，已经挂载的模板不能删除内部的物理量。
```cpp
// 为指定模板新增一组对齐的物理量，若其父节点在模板中已经存在，且不要求对齐，则报错
void addAlignedMeasurementsInTemplate(const std::string &template_name,
                                      const std::vector<std::string> &measurements,
                                      const std::vector<TSDataType::TSDataType> &dataTypes,
                                      const std::vector<TSEncoding::TSEncoding> &encodings,
                                      const std::vector<CompressionType::CompressionType> &compressors);

// 为指定模板新增一个对齐物理量, 若其父节点在模板中已经存在，且不要求对齐，则报错
void addAlignedMeasurementsInTemplate(const std::string &template_name,
                                      const std::string &measurement,
                                      TSDataType::TSDataType dataType,
                                      TSEncoding::TSEncoding encoding,
                                      CompressionType::CompressionType compressor);

// 为指定模板新增一个不对齐物理量, 若其父节在模板中已经存在，且要求对齐，则报错
void addUnalignedMeasurementsInTemplate(const std::string &template_name,
                                        const std::vector<std::string> &measurements,
                                        const std::vector<TSDataType::TSDataType> &dataTypes,
                                        const std::vector<TSEncoding::TSEncoding> &encodings,
                                        const std::vector<CompressionType::CompressionType> &compressors);

// 为指定模板新增一组不对齐的物理量, 若其父节在模板中已经存在，且要求对齐，则报错
void addUnalignedMeasurementsInTemplate(const std::string &template_name,
                                        const std::string &measurement,
                                        TSDataType::TSDataType dataType,
                                        TSEncoding::TSEncoding encoding,
                                        CompressionType::CompressionType compressor);

// 从指定模板中删除一个节点及其子树
void deleteNodeInTemplate(const std::string &template_name, const std::string &path);
```

- 对于已经创建的元数据模板，还可以通过以下接口查询模板信息：
```cpp
// 查询返回目前模板中所有物理量的数量
int countMeasurementsInTemplate(const std::string &template_name);

// 检查模板内指定路径是否为物理量
bool isMeasurementInTemplate(const std::string &template_name, const std::string &path);

// 检查在指定模板内是否存在某路径
bool isPathExistInTemplate(const std::string &template_name, const std::string &path);

// 返回指定模板内所有物理量的路径
std::vector<std::string> showMeasurementsInTemplate(const std::string &template_name);

// 返回指定模板内某前缀路径下的所有物理量的路径
std::vector<std::string> showMeasurementsInTemplate(const std::string &template_name, const std::string &pattern);
```


### 数据操作接口（DML）

#### 数据写入

> 推荐使用 insertTablet 帮助提高写入效率。

- 插入一个 Tablet，Tablet 是一个设备若干行数据块，每一行的列都相同。
  - 写入效率高。 
  - 支持写入空值：空值处可以填入任意值，然后通过 BitMap 标记空值。
```cpp
void insertTablet(Tablet &tablet);
```

- 插入多个 Tablet
```cpp
void insertTablets(std::unordered_map<std::string, Tablet *> &tablets);
```

- 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据
```cpp
void insertRecord(const std::string &deviceId, int64_t time, const std::vector<std::string> &measurements,
                  const std::vector<TSDataType::TSDataType> &types, const std::vector<char *> &values);
```

- 插入多个 Record
```cpp
void insertRecords(const std::vector<std::string> &deviceIds,
                   const std::vector<int64_t> &times,
                   const std::vector<std::vector<std::string>> &measurementsList,
                   const std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                   const std::vector<std::vector<char *>> &valuesList);
```

- 插入同属于一个 device 的多个 Record
```cpp
void insertRecordsOfOneDevice(const std::string &deviceId,
                              std::vector<int64_t> &times,
                              std::vector<std::vector<std::string>> &measurementsList,
                              std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                              std::vector<std::vector<char *>> &valuesList);
```

#### 带有类型推断的写入

服务器需要做类型推断，可能会有额外耗时，速度较无需类型推断的写入慢。

```cpp
void insertRecord(const std::string &deviceId, int64_t time, const std::vector<std::string> &measurements,
                  const std::vector<std::string> &values);


void insertRecords(const std::vector<std::string> &deviceIds,
                   const std::vector<int64_t> &times,
                   const std::vector<std::vector<std::string>> &measurementsList,
                   const std::vector<std::vector<std::string>> &valuesList);


void insertRecordsOfOneDevice(const std::string &deviceId,
                              std::vector<int64_t> &times,
                              std::vector<std::vector<std::string>> &measurementsList,
                              const std::vector<std::vector<std::string>> &valuesList);
```

#### 对齐时间序列写入

对齐时间序列的写入使用 insertAlignedXXX 接口，其余与上述接口类似：

- insertAlignedRecord 
- insertAlignedRecords 
- insertAlignedRecordsOfOneDevice 
- insertAlignedTablet 
- insertAlignedTablets

#### 数据删除

- 删除一个或多个时间序列在某个时间点前或这个时间点的数据
```cpp
void deleteData(const std::string &path, int64_t time);
void deleteData(const std::vector<std::string> &deviceId, int64_t time);
```

### IoTDB-SQL 接口

- 执行查询语句
```cpp
void executeNonQueryStatement(const std::string &sql);
```

- 执行非查询语句
```cpp
void executeNonQueryStatement(const std::string &sql);
```


## 示例代码

示例工程源代码：

- `example/client-cpp-example/src/SessionExample.cpp`
- `example/client-cpp-example/src/AlignedTimeseriesSessionExample.cpp` （使用对齐时间序列）

编译成功后，示例代码工程位于 `example/client-cpp-example/target`

## FAQ

### Thrift 编译相关问题

1. MAC：本地 Maven 编译 Thrift 时如出现以下链接的问题，可以尝试将 xcode-commandline 版本从 12 降低到 11.5
   https://stackoverflow.com/questions/63592445/ld-unsupported-tapi-file-type-tapi-tbd-in-yaml-file/65518087#65518087


2. Windows：Maven 编译 Thrift 时需要使用 wget 下载远端文件，可能出现以下报错：
   ```
   Failed to delete cached file C:\Users\Administrator\.m2\repository\.cache\download-maven-plugin\index.ser
   ```
   
    解决方法：
   - 尝试删除 ".m2\repository\\.cache\" 目录并重试。
   - 在添加 pom 文件对应的 download-maven-plugin 中添加 "\<skipCache>true\</skipCache>"
