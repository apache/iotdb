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
## C++ Native API

### Dependencies
- Java 8+
- Maven 3.5+
- Flex
- Bison 2.7+
- Boost 1.56+
- OpenSSL 1.0+
- GCC 5.5.0+


### Installation

The compilation of CPP client requires the module "compile-tools" to be built first. 
"compile-tools" is mainly responsible for building Thrift libraries locally.



#### Build Thrift on MacOS

- Bison

Bison 2.3 is preinstalled on OSX, but this version is too low.

When building Thrift with Bison 2.3, the following error would pop out:


```shell
invalid directive: '%code'
```

For such case, please update `Bison`:
```		
brew install bison		
brew link bison --force		
```

Then, you need to tell the OS where the new bison is.

For Bash users:		

```    		shell
echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.bash_profile		
```

For zsh users:		
```    		shell
echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.zshrc
```

- Boost

Please make sure a relative new version of Boost is ready on your machine.

If no Boost available, install the latest version of Boost:

```shell
brew install boost
brew link boost
```


- OpenSSL

Make sure the Openssl libraries has been install on your Mac.
The default Openssl include file search path is "/usr/local/opt/openssl/include".
If Openssl header files can not be found when building Thrift, please add option 

```-Dopenssl.include.dir=""``` 

to specify the OpenSSL installation directory on your Mac.



#### Build Thrift on Linux

Ubuntu 20:  

To install all dependencies, run:  

```shell
sudo apt-get install gcc-9 g++-9 libstdc++-9-dev bison flex libboost-all-dev libssl-dev zlib1g-dev
```

CentOS 7.x:  

In CentOS 7.x, Some packages can be installed using Yum:

```shell
sudo yum install bison flex openssl-devel
```

The version of gcc and boost installed by yum is too low, therefore you should compile or download these binary packages by yourself. 



#### Build Thrift on Windows

- building environment

Make sure a complete Windows C++ building environment is prepared on your machine. 
MSVC, MinGW... are supported.

If you are using MS Visual Studio, remember to install Visual Studio C/C++ IDE and compiler(supporting CMake, Clang, MinGW).

- CMake

For CMake, please download from the official website: https://cmake.org/download/

There is a long list of supported Cmake generators on Windows environment. 

```
  Visual Studio 16 2019        = Generates Visual Studio 2019 project files.
                                 Use -A option to specify architecture.
  Visual Studio 15 2017 [arch] = Generates Visual Studio 2017 project files.
                                 Optional [arch] can be "Win64" or "ARM".
  Visual Studio 14 2015 [arch] = Generates Visual Studio 2015 project files.
                                 Optional [arch] can be "Win64" or "ARM".
  Visual Studio 12 2013 [arch] = Generates Visual Studio 2013 project files.
                                 Optional [arch] can be "Win64" or "ARM".
  Visual Studio 11 2012 [arch] = Generates Visual Studio 2012 project files.
                                 Optional [arch] can be "Win64" or "ARM".
  Visual Studio 10 2010 [arch] = Generates Visual Studio 2010 project files.
                                 Optional [arch] can be "Win64" or "IA64".
  Visual Studio 9 2008 [arch]  = Generates Visual Studio 2008 project files.
                                 Optional [arch] can be "Win64" or "IA64".
  Borland Makefiles            = Generates Borland makefiles.
* NMake Makefiles              = Generates NMake makefiles.
  NMake Makefiles JOM          = Generates JOM makefiles.
  MSYS Makefiles               = Generates MSYS makefiles.
  MinGW Makefiles              = Generates a make file for use with
                                 mingw32-make.
  Unix Makefiles               = Generates standard UNIX makefiles.
  Green Hills MULTI            = Generates Green Hills MULTI files
                                 (experimental, work-in-progress).
  Ninja                        = Generates build.ninja files.
  Ninja Multi-Config           = Generates build-<Config>.ninja files.
  Watcom WMake                 = Generates Watcom WMake makefiles.
  CodeBlocks - MinGW Makefiles = Generates CodeBlocks project files.
  CodeBlocks - NMake Makefiles = Generates CodeBlocks project files.
```
the list is available via command: `cmake --help`

When building client-cpp project, use -Dcmake.generator="" option to specify a Cmake generator.
E.g., `mvn package -Dcmake.generator="Visual Studio 15 2017 [arch]"`

- Flex and Bison

Windows Flex and Bison could be downloaded from SourceForge: https://sourceforge.net/projects/winflexbison/

After downloaded, please rename the executables to flex.exe and bison.exe and add them to "PATH" environment variables.

- Boost

For Boost, please download from the official website: https://www.boost.org/users/download/

Then build Boost by executing bootstrap.bat and b2.exe.
```powershell
bootstrap.bat
.\b2.exe
```

To help CMake find your Boost libraries on windows, you should set `-Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder}`
to your mvn build command.

- openssl

For openssl , download Source code from the official website: https://www.openssl.org/source/

download binary files from http://slproweb.com/products/Win32OpenSSL.html

- Add Path

Before compile，make sure cmake,flex,bison and openssl already add to "PATH" environment variables.

#### Compile and Test

To compile cpp client, add "-P compile-cpp" option to maven build command.

The compiling requires the module "compile-tools" to be built first.

- On Mac and Linux, the command to compile cpp-client is as follows：

`mvn package -P compile-cpp  -pl example/client-cpp-example -am -DskipTest`

- On Windwos , Compile cpp-client

please install Boost first and add following Maven settings before compile on Windows:
```shell
-Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder}` 
```
Add generator for cmake: 
`-Dcmake.generator="Visual Studio 15 2017 [arch]"`

The complete command:
```shell
mvn package -P compile-cpp -pl client-cpp,server,example/client-cpp-example -am 
-D"boost.include.dir"="D:\boost_1_75_0" -D"boost.library.dir"="D:\boost_1_75_0\stage\lib" -DskipTests
```

If the compilation finishes successfully, the packaged zip file will be placed under
"client-cpp/target/client-cpp-${project.version}-cpp-${os}.zip". 

On Mac machines, the hierarchy of the package should look like this:

```shell
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



### Q&A

#### on Mac

If errors occur when compiling thrift source code, try to downgrade your xcode-commandline from 12 to 11.5

see https://stackoverflow.com/questions/63592445/ld-unsupported-tapi-file-type-tapi-tbd-in-yaml-file/65518087#65518087



#### on Windows

When Building Thrift and downloading packages via "wget", a possible annoying issue may occur with
error message looks like:
```shell
Failed to delete cached file C:\Users\Administrator\.m2\repository\.cache\download-maven-plugin\index.ser
```
Possible fixes:
- Try to delete the ".m2\repository\\.cache\" directory and try again.
- Add "\<skipCache>true\</skipCache>" configuration to the download-maven-plugin maven phase that complains this error.

