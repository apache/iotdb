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
# Building C++ Client

To compile cpp client, add "-P compile-cpp" option to maven build command.

The compiling requires the module "compile-tools" to be built first.
For more information, please refer to "compile-tools/README.md".


## Compile and Test:

`mvn integration-test -P compile-cpp -pl client-cpp,server -am -Diotdb.test.skip=true -Dtsfile.test.skip=true -Djdbc.test.skip=true`

To compile on Windows, please install Boost first and add following Maven settings:
```
-Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder}` 
```

e.g.,
```
mvn integration-test -P compile-cpp -pl client-cpp,server,example/client-cpp-example -am 
-D"iotdb.test.skip"=true -D"tsfile.test.skip"=true -D"jdbc.test.skip"=true 
-D"boost.include.dir"="D:\boost_1_75_0" -D"boost.library.dir"="D:\boost_1_75_0\stage\lib"
```

## 

If the compilation finishes successfully, the packaged zip file will be placed under
"client-cpp/target/client-cpp-${project.version}-cpp-${os}.zip". 

On Mac machines, the hierarchy of the package should look like this:
```
.
+-- client
|   +-- include
|       +-- Session.h
|       +-- IClientRPCService.h
|       +-- client_types.h
|       +-- common_types.h
|       +-- thrift
|           +-- thrift_headers...
|   +-- lib
|       +-- libiotdb_session.dylib
```

## Using C++ Client:
```
1. Put the zip file "client-cpp-${project.version}-cpp-${os}.zip" wherever you want

2. Unzip the archive using the following command, and then you can get the two directories mentioned above, the header file and the dynamic library
    unzip client-cpp-${project.version}-cpp-${os}.zip

3. Write C++ code to call the operation interface of cpp-client to operate IOTDB,
    for detail interface information, please refer to the link: https://iotdb.apache.org/zh/UserGuide/Master/API/Programming-Cpp-Native-API.html

   E.g:
    #include "include/Session.h"
    #include <memory>
    #include <iostream>

    int main() {
        std::cout << "open session" << std::endl;
        std::shared_ptr<Session> session(new Session("127.0.0.1", 6667, "root", "root"));
        session->open(false);

        std::cout << "setStorageGroup: root.test01" << std::endl;
        session->setStorageGroup("root.test01");

        if (!session->checkTimeseriesExists("root.test01.d0.s0")) {
            session->createTimeseries("root.test01.d0.s0", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
            std::cout << "create Timeseries: root.test01.d0.s0" << std::endl;
        }

        std::cout << "session close" << std::endl;
        session->close();
    }

4. Compile and execute
    clang++ -O2 user-cpp-code.cpp -liotdb_session -L/user-unzip-absolute-path/lib -Wl,-rpath /user-unzip-absolute-path/lib -std=c++11
    ./a.out
```
