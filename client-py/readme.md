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

# Python connection
## Introduction
This is an example of how to connect to IoTDB with python, using the thrift rpc interfaces. Things 
are almost the same on Windows or Linux, but pay attention to the difference like path separator.

## Prerequisites
python3.7 or later is preferred.

You have to install Thrift (0.11.0 or later) to compile our thrift file into python code. Below is the official
tutorial of installation, eventually, you should have a thrift executable.
```
http://thrift.apache.org/docs/install/
```

## Compile the thrift library
If you have added Thrift executable into your path, you may just run `client-py/compile.sh` or
 `client-py/compile.bat`, or you will have to modify it to set variable `THRIFT_EXE` to point to
your executable. This will generate thrift sources under folder `target`, you can add it to your
`PYTHONPATH` so that you would be able to use the library in your code. Notice that the scripts
locate the thrift source file by relative path, so if you move the scripts else where, they are
no longer valid.

Optionally, if you know the basic usage of thrift, you can only download the thrift source file in
`service-rpc\src\main\thrift\rpc.thrift`, and simply use `thrift -gen py -out ./target rpc.thrift` 
to generate the python library.

## Example
We provided an example of how to use the thrift library to connect to IoTDB in `client-py/src
/client_example.py`, please read it carefully before you write your own code.