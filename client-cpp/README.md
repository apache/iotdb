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

To compile cpp client, add "-P client-cpp" option to maven build command.

The compiling requires the module "compile-tools" to be built first.
For more information, please refer to "compile-tools/README.md".


## Compile and Test:

`mvn integration-test -P client-cpp -pl client-cpp,server -am -Diotdb.test.skip=true -Dtsfile.test.skip=true -Djdbc.test.skip=true`

To compile on Windows, please install Boost first and add following Maven settings:
```
-Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder}` 
```

e.g.,
```
mvn integration-test -P client-cpp -pl client-cpp,server,example/client-cpp-example -am 
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
|       +-- TSIService.h
|       +-- rpc_types.h
|       +-- rpc_constants.h
|       +-- thrift
|           +-- thrift_headers...
|   +-- lib
|       +-- libiotdb_session.dylib
```
