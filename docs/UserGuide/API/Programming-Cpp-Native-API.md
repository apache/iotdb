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

# C++ Native API

## Dependencies
- Java 8+
- Maven 3.5+
- Flex
- Bison 2.7+
- Boost 1.56+
- OpenSSL 1.0+
- GCC 5.5.0+


## Installation From Source Code

### Install CPP Dependencies

- **MAC**

    1. Install Bison ：Bison 2.3 is preinstalled on OSX, but this version is too low.

       When building Thrift with Bison 2.3, the following error would pop out:
       ```invalid directive: '%code'```

       For such case, please update `Bison`:
       ```shell
       brew install bison
       brew link bison --force
       ```
       
       Then, you need to tell the OS where the new bison is.
    
          For Bash users:
          ```shell
          echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.bash_profile
          ```

          For zsh users:
          ```shell
          echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.zshrc
          ```

    2. Install Boost ：Please make sure a relative new version of Boost is ready on your machine.
       If no Boost available, install the latest version of Boost:
          ```shell
          brew install boost
          brew link boost
          ```

    3. OpenSSL ：Make sure the Openssl libraries has been install on your Mac. The default Openssl include file search path is "/usr/local/opt/openssl/include".

       If Openssl header files can not be found when building Thrift, please add option`-Dopenssl.include.dir=""`.


- **Ubuntu 20**

  To install all dependencies, run:

    ```shell
    sudo apt-get install gcc-9 g++-9 libstdc++-9-dev bison flex libboost-all-dev libssl-dev zlib1g-dev
    ```


- **CentOS 7.x**

  Some packages can be installed using Yum:

    ```shell
    sudo yum install bison flex openssl-devel
    ```

  The version of gcc and boost installed by yum is too low, therefore you should compile or download these binary packages by yourself.


- **Windows**

    1. Building environment
        * Install `MS Visual Studio`(recommend 2019 version): remember to install Visual Studio C/C++ IDE and compiler(supporting CMake, Clang, MinGW).
        * Download and install [CMake](https://cmake.org/download/) .

    2. Download and install `Flex` & `Bison`
        * Download [Win_Flex_Bison](https://sourceforge.net/projects/winflexbison/) .
        * After downloaded, please rename the executables to `flex.exe` and `bison.exe` and add them to "PATH" environment variables.

    3. Install `Boost`
        * Download [Boost](https://www.boost.org/users/download/) .
        * Then build `Boost` by executing bootstrap.bat and b2.exe.

    4. Install `OpenSSL`
        * Download and install [OpenSSL](http://slproweb.com/products/Win32OpenSSL.html) .


### Compile

You can download the source code from:
```shell
git clone https://github.com/apache/iotdb.git
```

The default dev branch is the master branch, If you want to use a released version (eg. `rel/0.13`):
```shell
git checkout rel/0.13
```

Under the root path of iotdb:

- Mac & Linux
    ```shell
    mvn package -P compile-cpp -pl example/client-cpp-example -am -DskipTest
    ```

- Windows
    ```shell
    mvn package -P compile-cpp -pl client-cpp,server,example/client-cpp-example -am -Dcmake.generator="your cmake generator" -Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder} -DskipTests
    ```
    - When building client-cpp project, use `-Dcmake.generator=""` option to specify a Cmake generator. E.g. `-Dcmake.generator="Visual Studio 16 2019"` (`cmake --help` shows a long list of supported Cmake generators.)
    - To help CMake find your Boost libraries on windows, you should set `-DboostIncludeDir="C:\Program Files (x86)\boost_1_78_0" -DboostLibraryDir="C:\Program Files (x86)\boost_1_78_0\stage\lib"` to your mvn build command.
    ``

If the compilation finishes successfully, the packaged zip file will be placed under `client-cpp/target/client-cpp-1.1.0-SNAPSHOT-cpp-${os}.zip`


## Native APIs

Here we show the commonly used interfaces and their parameters in the Native API:

### Initialization

- Open a Session
```cpp
void open(); 
```

- Open a session, with a parameter to specify whether to enable RPC compression
```cpp
void open(bool enableRPCCompression); 
```
Notice: this RPC compression status of client must comply with that of IoTDB server

- Close a Session
```cpp
void close(); 
```

### Data Definition Interface (DDL)

#### Database Management

- CREATE DATABASE
```cpp
void setStorageGroup(const std::string &storageGroupId);
```

- Delete one or several databases
```cpp
void deleteStorageGroup(const std::string &storageGroup);
void deleteStorageGroups(const std::vector<std::string> &storageGroups);
```

#### Timeseries Management

- Create one or multiple timeseries
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

- Create aligned timeseries
```cpp
void createAlignedTimeseries(const std::string &deviceId,
                             const std::vector<std::string> &measurements,
                             const std::vector<TSDataType::TSDataType> &dataTypes,
                             const std::vector<TSEncoding::TSEncoding> &encodings,
                             const std::vector<CompressionType::CompressionType> &compressors);
```

- Delete one or several timeseries
```cpp
void deleteTimeseries(const std::string &path);
void deleteTimeseries(const std::vector<std::string> &paths);
```

- Check whether the specific timeseries exists.
```cpp
bool checkTimeseriesExists(const std::string &path);
```

#### Schema Template

- Create a schema template
```cpp
void createSchemaTemplate(const Template &templ);
```

- Set the schema template named `templateName` at path `prefixPath`.
```cpp
void setSchemaTemplate(const std::string &template_name, const std::string &prefix_path);
```

- Unset the schema template
```cpp
void unsetSchemaTemplate(const std::string &prefix_path, const std::string &template_name);
```

- After measurement template created, you can edit the template with belowed APIs.
```cpp
// Add aligned measurements to a template
void addAlignedMeasurementsInTemplate(const std::string &template_name,
                                      const std::vector<std::string> &measurements,
                                      const std::vector<TSDataType::TSDataType> &dataTypes,
                                      const std::vector<TSEncoding::TSEncoding> &encodings,
                                      const std::vector<CompressionType::CompressionType> &compressors);

// Add one aligned measurement to a template
void addAlignedMeasurementsInTemplate(const std::string &template_name,
                                      const std::string &measurement,
                                      TSDataType::TSDataType dataType,
                                      TSEncoding::TSEncoding encoding,
                                      CompressionType::CompressionType compressor);

// Add unaligned measurements to a template
void addUnalignedMeasurementsInTemplate(const std::string &template_name,
                                        const std::vector<std::string> &measurements,
                                        const std::vector<TSDataType::TSDataType> &dataTypes,
                                        const std::vector<TSEncoding::TSEncoding> &encodings,
                                        const std::vector<CompressionType::CompressionType> &compressors);

// Add one unaligned measurement to a template
void addUnalignedMeasurementsInTemplate(const std::string &template_name,
                                        const std::string &measurement,
                                        TSDataType::TSDataType dataType,
                                        TSEncoding::TSEncoding encoding,
                                        CompressionType::CompressionType compressor);

// Delete a node in template and its children
void deleteNodeInTemplate(const std::string &template_name, const std::string &path);
```

- You can query measurement templates with these APIS:
```cpp
// Return the amount of measurements inside a template
int countMeasurementsInTemplate(const std::string &template_name);

// Return true if path points to a measurement, otherwise returne false
bool isMeasurementInTemplate(const std::string &template_name, const std::string &path);

// Return true if path exists in template, otherwise return false
bool isPathExistInTemplate(const std::string &template_name, const std::string &path);

// Return all measurements paths inside template
std::vector<std::string> showMeasurementsInTemplate(const std::string &template_name);

// Return all measurements paths under the designated patter inside template
std::vector<std::string> showMeasurementsInTemplate(const std::string &template_name, const std::string &pattern);
```


### Data Manipulation Interface (DML)

#### Insert

> It is recommended to use insertTablet to help improve write efficiency.

- Insert a Tablet，which is multiple rows of a device, each row has the same measurements
    - Better Write Performance
    - Support null values: fill the null value with any value, and then mark the null value via BitMap
```cpp
void insertTablet(Tablet &tablet);
```

- Insert multiple Tablets
```cpp
void insertTablets(std::unordered_map<std::string, Tablet *> &tablets);
```

- Insert a Record, which contains multiple measurement value of a device at a timestamp
```cpp
void insertRecord(const std::string &deviceId, int64_t time, const std::vector<std::string> &measurements,
                  const std::vector<TSDataType::TSDataType> &types, const std::vector<char *> &values);
```

- Insert multiple Records
```cpp
void insertRecords(const std::vector<std::string> &deviceIds,
                   const std::vector<int64_t> &times,
                   const std::vector<std::vector<std::string>> &measurementsList,
                   const std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                   const std::vector<std::vector<char *>> &valuesList);
```

- Insert multiple Records that belong to the same device. With type info the server has no need to do type inference, which leads a better performance
```cpp
void insertRecordsOfOneDevice(const std::string &deviceId,
                              std::vector<int64_t> &times,
                              std::vector<std::vector<std::string>> &measurementsList,
                              std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                              std::vector<std::vector<char *>> &valuesList);
```

#### Insert with type inference

Without type information, server has to do type inference, which may cost some time.

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

#### Insert data into Aligned Timeseries

The Insert of aligned timeseries uses interfaces like `insertAlignedXXX`, and others are similar to the above interfaces:

- insertAlignedRecord
- insertAlignedRecords
- insertAlignedRecordsOfOneDevice
- insertAlignedTablet
- insertAlignedTablets

#### Delete

- Delete data before or equal to a timestamp of one or several timeseries
```cpp
void deleteData(const std::string &path, int64_t time);
void deleteData(const std::vector<std::string> &deviceId, int64_t time);
```

### IoTDB-SQL Interface

- Execute query statement
```cpp
void executeNonQueryStatement(const std::string &sql);
```

- Execute non query statement
```cpp
void executeNonQueryStatement(const std::string &sql);
```


## Examples

The sample code of using these interfaces is in:

- `example/client-cpp-example/src/SessionExample.cpp`
- `example/client-cpp-example/src/AlignedTimeseriesSessionExample.cpp` （使用对齐时间序列）

If the compilation finishes successfully, the example project will be placed under `example/client-cpp-example/target`

## FAQ

### on Mac

If errors occur when compiling thrift source code, try to downgrade your xcode-commandline from 12 to 11.5

see https://stackoverflow.com/questions/63592445/ld-unsupported-tapi-file-type-tapi-tbd-in-yaml-file/65518087#65518087


### on Windows

When Building Thrift and downloading packages via "wget", a possible annoying issue may occur with
error message looks like:
```shell
Failed to delete cached file C:\Users\Administrator\.m2\repository\.cache\download-maven-plugin\index.ser
```
Possible fixes:
- Try to delete the ".m2\repository\\.cache\" directory and try again.
- Add "\<skipCache>true\</skipCache>" configuration to the download-maven-plugin maven phase that complains this error.

