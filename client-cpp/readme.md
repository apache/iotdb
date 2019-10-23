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

# introduction
    This module provides an example of how to connect to IoTDB with c++. You need to write your own program referring to src/SessionExample.cpp

## Requirement
    Using this module requires you to install thrift(0.11.0 or later). Below is the official tutorial of installation:
```
http://thrift.apache.org/docs/install/
```
## Compile
    You need to mimic src/SessionExample.cpp to write a program to use the interface you need. After you have written it, run compile.sh to generate the executable.
    Use the following command to generate an executable file:
    ./compile.sh executable_file_name cpp_program_file_name
e.g.

    ./compile.sh Example src/SessionExample.cpp

