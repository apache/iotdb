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
# Building compile tools


## Requirements
* Java 8+
* Maven 3.5+
* Flex
* Bison 2.7+
* Boost
* OpenSSL 1.0+


### Mac

- Bison

 Bison 2.3 is preinstalled on OSX, but this version is too low.
 When building Thrift with Bison 2.3, the following error would pop out:

 
  ```invalid directive: '%code'```		
		
 For such case, please update `Bison`:		
		
```		
     brew install bison		
     brew link bison --force		
```
		
 Then, you need to tell the OS where the new bison is.		
		
 For Bash users:		
 ```    		
     echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.bash_profile		
 ```
		
 For zsh users:		
 ```    		
     echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.zshrc
 ```

- Boost

Please make sure a relative new version of Boost is ready on your machine.
If no Boost available, install the latest version of Boost:

```
    brew install boost
    brew link boost
```


- OpenSSL

Make sure the Openssl libraries has been install on your Mac.
The default Openssl include file search path is "/usr/local/opt/openssl/include".
If Openssl header files can not be found when building Thrift, please add option 
```-Dopenssl.include.dir=""``` to specify the correct directory on your Mac.

### Linux

#### Additional requrirements

* g++

To install all dependencies, run:

Debian/Ubuntu:

```
sudo apt-get install gcc g++ bison flex libboost-all-dev
```

CentOS:
```
yum install gcc g++ bison flex boost-devel
```

### Windows

Make sure a complete Windows C++ building environment is prepared on your machine. 
MSVC, MinGW... are supported.

If you are using MS Visual Studio, remember to install Visual Studio C/C++ IDE and compiler(supporting CMake, Clang, MinGW).

#### Flex and Bison
Windows Flex and Bison could be downloaded from SourceForge: https://sourceforge.net/projects/winflexbison/

After downloaded, please rename the executables to flex.exe and bison.exe and add them to "PATH" environment variables.

#### Boost
For Boost, please download from the official website: https://www.boost.org/users/download/

Then build Boost by executing bootstrap.bat and b2.exe.
```
bootstrap.bat
.\b2.exe
```

To help CMake find your Boost libraries on windows, you should set `-Dboost.include.dir=${your boost header folder} -Dboost.library.dir=${your boost lib (stage) folder}`
to your mvn build command.

#### Cmake generator on Windows

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
  CodeBlocks - NMake Makefiles = Generates CodeBlocks project fi

```
(the list is gotten by `cmake --help`.)

When building client-cpp project, use -Dcmake.generator="" option to specify a Cmake generator.
E.g., `mvn package -Dcmake.generator="Visual Studio 15 2017 [arch]"`


### FAQ

#### on Mac

if you occur some errors when compiling thrift source code, try to downgrade your xcode-commandline from 12 to 11.5

see https://stackoverflow.com/questions/63592445/ld-unsupported-tapi-file-type-tapi-tbd-in-yaml-file/65518087#65518087

#### on Windows

When Building Thrift and downloading packages via "wget", a possible annoying issue may occur with
error message looks like:
```
Failed to delete cached file C:\Users\Administrator\.m2\repository\.cache\download-maven-plugin\index.ser
```
Possible fix:
- Try to delete the ".m2\repository\\.cache\" directory and try again.
- Add "\<skipCache>true\</skipCache>" configuration to the download-maven-plugin maven phase that complains this error.
