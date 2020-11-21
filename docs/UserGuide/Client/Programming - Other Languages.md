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

# Programming - Other Languages

## Python API

### 1. Introduction

This is an example of how to connect to IoTDB with python, using the thrift rpc interfaces. Things will be a bit different
on Linux or Windows, we will introduce how to operate on the two systems separately.

### 2. Prerequisites

python3.7 or later is preferred.

You have to install Thrift (0.11.0 or later) to compile our thrift file into python code. Below is the official
tutorial of installation: 

```
http://thrift.apache.org/docs/install/
```

### 3. How to Get the Python Library

#### Option 1: pip install

You can find the Apache IoTDB Python Client API package on https://pypi.org/project/apache-iotdb/.

The download command is:

```
pip install apache-iotdb
```

#### Option 2: use the compile script we provided

If you have added Thrift executable into your path, you may just run `client-py/compile.sh` or
 `client-py\compile.bat`, otherwise, modify it to set variable `THRIFT_EXE` to point to
your executable. This will generate thrift sources under folder `target`, you can add it to your
`PYTHONPATH` so that you will be able to use the library in your code. Note that the scripts
locate the thrift source file by relative path, so if you move the scripts else where, they are
no longer valid.

#### Option 3: basic usage of thrift

Optionally, if you know the basic usage of thrift, download the thrift source file in
`thrift\src\main\thrift\rpc.thrift`, and simply use `thrift -gen py -out ./target/iotdb rpc.thrift` 
to generate the python library.

### 4. Use Example

We provided an example of how to use the thrift library to connect to IoTDB in `client-py/src
/client_example.py`, please read it carefully before you write your own code.
