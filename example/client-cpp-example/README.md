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

# How to get a complete CPP client demo project

## get a project

using maven to build this example project:

* cd the root path of the whole project
* run `mvn package -DskipTests -P compile-cpp -pl example/client-cpp-example -am`
* cd example/client-cpp-example/target

You can find some files to form a complete project:
```
+-- client
|   +-- include
|       +-- Session.h
|       +-- IClientRPCService.h
|       +-- rpc_types.h
|       +-- rpc_constants.h
|       +-- thrift
|           +-- thrift_headers...
|   +-- lib
|       +-- libiotdb_session.dylib
+-- CMakeLists.txt
+-- SessionExample.cpp
```


