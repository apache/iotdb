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

# How to get a complete C client demo project (SessionC)

Pure **C** examples for the IoTDB Session **C API** (`SessionC.h`): **tree model** (`tree_example`) and **table model** (`table_example`). Default connection: `127.0.0.1:6667`, user `root` / password `root` (edit the macros at the top of each `.c` file).

## Get a project

Using Maven to build this example project (requires **Boost**, same as the C++ client; on Windows see [`iotdb-client/client-cpp/README.md`](../../iotdb-client/client-cpp/README.md)):

* `cd` the root path of the whole project
* run  
  `mvn package -DskipTests -P with-cpp -pl example/client-c-example -am`  
  (use `mvn clean package ...` when no other process holds files under `iotdb-client/client-cpp/target`; on Windows add Boost paths, e.g.  
  `-D"boost.include.dir=C:\local\boost_1_87_0" -D"boost.library.dir=C:\local\boost_1_87_0\lib64-msvc-14.2"`)
* `cd example/client-c-example/target`

After a successful build you should have:

* Unpacked C++ client headers and libraries under `client/` and Thrift under `thrift/` (same layout as [client-cpp-example](../client-cpp-example/README.md))
* CMake-generated binaries, for example on Windows MSVC:  
  `Release/tree_example.exe`, `Release/table_example.exe`  
  (exact path depends on the CMake generator)

## Run

Start IoTDB first, then:

```text
# Windows (example)
Release\tree_example.exe
Release\table_example.exe
```

On failure, the programs print to `stderr` and you can inspect `ts_get_last_error()`.

## Source layout

```
example/client-c-example/
+-- README.md
+-- pom.xml
+-- src/
|   +-- CMakeLists.txt
|   +-- tree_example.c
|   +-- table_example.c
```

The Maven build copies `src/*.c` and `src/CMakeLists.txt` into `target/` next to the unpacked `client/` and `thrift/` trees, then runs CMake to compile the two executables.
