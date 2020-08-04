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
# IoTDB C++ Client

## Requirement
* Java 8+
* Maven 3.5+
* Flex
* Bison 2.7+
* OpenSSL 1.0+

### Mac

Bison 2.3 is preinstalled on OSX, but this version is too low. When building Thrift with Bison 2.3, the following error would pop out:

```invalid directive: '%code'```

For such case, please update `Bison`:

```
    brew install bison
    brew link bison --force
    echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.bash_profile
```

Make sure the Openssl libraries has been install on your Mac.
The default Openssl include file search path is "/usr/local/opt/openssl/include".
If Openssl header files can not be found when building Thrift, please add option 
```-Dopenssl.include.dir=""``` to specify the correct directory on your Mac.

### Windows

#### Flex and Bison
For Flex and Bison, they could be downloaded from SourceForge: https://sourceforge.net/projects/winflexbison/

After downloaded, please rename the executables to flex.exe and bison.exe and add them to "PATH" environment variables.


#### Cmake generator on Windows

There is a long list of supported Cmake generators on Windows environment. 
You can use "cmake --help" to see all the generator names and decide which one to use in terms of the build environment.

```
Visual Studio 16 2019
Visual Studio 15 2017
Visual Studio 14 2015
MinGW Makefiles
...
```
When building client-cpp project, use -Dcmake.generator="" option to specify a Cmake generator.
