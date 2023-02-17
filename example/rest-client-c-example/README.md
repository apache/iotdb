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

# a complete C rest client demo project

## project structure

- base64.c/base64.h: a util for base64.encode/decode

- main.c: call the rest api for IoTDB

## How to build

dependency: libcurl

- example: Ubuntu 16.04 STL (Ubuntu 20.04 may have some problems)

```shell script
sudo apt-get install libcurl4-openssl-dev
```

Before build, you must install the libcurl.

build steps:

```shell script
mkdir build
cd build
cmake ..
make 
```

Eventually, you will get an executable program `c_rest_iotdb` in `build` directory.

## cross compilation

- os : Ubuntu 16.04 STL

1. choose and install cross compiler according to the target host

arm-linux-gnueabihf-gcc as an example.

- arm-linux-gnueabihf-gcc

```shell script
sudo apt-get install gcc-arm-linux-gnueabihf
```

2. cross compile libcurl

- download the *.tar.gz in https://curl.haxx.se/download/, and uncompress it.

- compile and install

```shell script
./configure --host=arm-linux/arm-linux-gnueabihf/others CC=arm-linux-gnueabihf-gcc --prefix=`install dir` --enable-static --with-wolfssl
make
make install
```

3. cross compile the example program

```shell script
arm-linux-gnueabihf-gcc main.c base64.c -o c_rest -L `(libcurl install dir)/lib/` -l curl
```

if can't find `curl/curl.h`, you can just simply change to the absolutely path or specify the include path.

Reference Materials

libcurl usage:

https://blog.csdn.net/myvest/article/details/82899788

compilation:

https://blog.csdn.net/u011641885/article/details/46900771

https://www.cnblogs.com/flyinggod/p/10148228.html

https://blog.csdn.net/fangye945a/article/details/86500817

https://www.cnblogs.com/yxh-l-0824/p/13254891.html

https://www.cnblogs.com/tansuoxinweilai/p/11602830.html

https://www.cnblogs.com/pied/p/8805883.html






