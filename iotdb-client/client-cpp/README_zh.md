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

# Apache IoTDB C++ Client

[English](README.md)

本目录用于构建和打包 Apache IoTDB C++ Session SDK。如果你已经拿到了
`iotdb-session-cpp-<version>-<classifier>.zip`，在自己的 C/C++ 项目中使用
时不需要 Maven、Thrift、Boost，也不需要本源码目录。解压 zip 后，让项目引用
其中的 `include/` 和 `lib/`，并在运行时把 IoTDB 动态库随可执行文件一起部署
即可。

## 在项目中使用已打好的 SDK 包

### 1. 选择并解压正确的包

请按应用运行环境选择对应 classifier：

| 目标环境 | Zip classifier 后缀 |
|----------|---------------------|
| Linux x86_64，glibc >= 2.28 | `linux-x86_64-glibc2.28` |
| Linux aarch64，glibc >= 2.28 | `linux-aarch64-glibc2.28` |
| macOS x86_64 | `macos-x86_64` |
| macOS arm64 | `macos-aarch64` |
| Windows + Visual Studio 2017 | `windows-x86_64-msvc14.1` |
| Windows + Visual Studio 2019 | `windows-x86_64-msvc14.2` |
| Windows + Visual Studio 2022 | `windows-x86_64-msvc14.3` |
| Windows + Visual Studio 2026 | `windows-x86_64-msvc14.4` |

示例：

```bash
unzip iotdb-session-cpp-2.0.7-SNAPSHOT-linux-x86_64-glibc2.28.zip
export IOTDB_SESSION_HOME=$PWD/iotdb-session-cpp-2.0.7-SNAPSHOT-linux-x86_64-glibc2.28
```

解压后的 SDK 主要包含：

```
include/                         C/C++ 公开 API 头文件
lib/                             libiotdb_session.so/.dylib 或 iotdb_session.dll + .lib
cmake/iotdb-session-config.cmake CMake package config
pkgconfig/iotdb-session.pc       pkg-config 元数据
examples/                        示例源码和示例 CMakeLists.txt
third_party/DEPENDENCIES.md      内置第三方依赖说明
```

Thrift 和 Boost 已封装进 `iotdb_session`，普通应用接入时不需要额外安装
Thrift 或 Boost 的头文件/库。

### 2. 用 CMake 链接

项目中的 `CMakeLists.txt` 可这样写：

```cmake
cmake_minimum_required(VERSION 3.15)
project(my_iotdb_app LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(iotdb-session REQUIRED CONFIG)

add_executable(my_iotdb_app main.cpp)
target_link_libraries(my_iotdb_app PRIVATE IoTDB::iotdb_session)
```

配置时传入 SDK 解压目录：

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH="$IOTDB_SESSION_HOME"
cmake --build build
```

Windows 上请使用与 SDK 包一致的 Visual Studio 代际。例如
`windows-x86_64-msvc14.3` 对应 Visual Studio 2022。

Visual Studio 用户可以直接打开项目目录，并在 CMake 缓存设置里把 SDK 解压目录
加入 `CMAKE_PREFIX_PATH`；也可以在 Developer Command Prompt 中执行：

```bat
cmake -S . -B build -G "Visual Studio 17 2022" -A x64 ^
  -DCMAKE_PREFIX_PATH="%IOTDB_SESSION_HOME%"
cmake --build build --config Release
copy "%IOTDB_SESSION_HOME%\lib\iotdb_session.dll" build\Release\
```

### 3. 在 Linux/macOS 上用 pkg-config 链接

```bash
export PKG_CONFIG_PATH="$IOTDB_SESSION_HOME/pkgconfig:$PKG_CONFIG_PATH"
c++ -std=c++11 main.cpp $(pkg-config --cflags --libs iotdb-session) -o my_iotdb_app
```

运行时如果系统找不到动态库，可以把动态库复制到可执行文件同目录，或设置库搜索
路径：

```bash
cp -P "$IOTDB_SESSION_HOME"/lib/libiotdb_session.so* .
./my_iotdb_app

# 或者：
LD_LIBRARY_PATH="$IOTDB_SESSION_HOME/lib:$LD_LIBRARY_PATH" ./my_iotdb_app
```

### 4. 手动指定头文件和库路径

Linux：

```bash
c++ -std=c++11 main.cpp \
  -I"$IOTDB_SESSION_HOME/include" \
  -L"$IOTDB_SESSION_HOME/lib" \
  -liotdb_session -pthread \
  -Wl,-rpath,"$IOTDB_SESSION_HOME/lib" \
  -o my_iotdb_app
```

macOS：

```bash
c++ -std=c++11 main.cpp \
  -I"$IOTDB_SESSION_HOME/include" \
  -L"$IOTDB_SESSION_HOME/lib" \
  -liotdb_session \
  -Wl,-rpath,"$IOTDB_SESSION_HOME/lib" \
  -o my_iotdb_app
```

Windows + MSVC：

```bat
cl /std:c++14 /EHsc main.cpp /I "%IOTDB_SESSION_HOME%\include" ^
  /link /LIBPATH:"%IOTDB_SESSION_HOME%\lib" iotdb_session.lib
copy "%IOTDB_SESSION_HOME%\lib\iotdb_session.dll" .
```

### 5. 编译包内 examples

SDK 包内 `examples/` 目录包含示例源码。进入 SDK 解压根目录后执行：

```bash
cmake -S examples -B examples-build -DCMAKE_BUILD_TYPE=Release
cmake --build examples-build
```

Windows：

```bat
cmake -S examples -B examples-build -G "Visual Studio 17 2022" -A x64
cmake --build examples-build --config Release
```

examples 构建会把 IoTDB 运行时库复制到每个示例可执行文件旁边。也可以执行
`cmake --build examples-build --target example-dist` 生成包含示例和运行时库的
`dist/` 目录。

### 6. 最小 C++ 示例

```cpp
#include "Session.h"

#include <iostream>
#include <memory>

int main() {
  auto session = std::make_shared<Session>("127.0.0.1", 6667, "root", "root");
  session->open(false);

  session->setStorageGroup("root.test");
  if (!session->checkTimeseriesExists("root.test.d0.s0")) {
    session->createTimeseries(
        "root.test.d0.s0", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
  }

  session->close();
  std::cout << "IoTDB C++ session is ready." << std::endl;
  return 0;
}
```

包内 `examples/` 下的示例默认连接 `127.0.0.1:6667`，用户名和密码均为
`root`。运行示例前请先启动 IoTDB 服务。

### 运行时部署注意事项

- Linux 发版包在 `manylinux_2_28` 容器中使用 GCC 14 构建，目标机器需要 glibc 2.28 或更新
  版本。
- Windows 包使用 MSVC 动态运行时（`/MD`）。目标机器如果没有对应运行时，请安装
  与 Visual Studio 代际匹配的 Microsoft Visual C++ Redistributable。
- 将 `libiotdb_session.so`、`libiotdb_session.dylib` 或 `iotdb_session.dll`
  放在可执行文件同目录，或配置平台对应的动态库搜索路径。Linux 手动复制时请保留
  `libiotdb_session.so*` 的符号链接链。

## 从源码构建 SDK

C++ client 使用本目录下单一的 `CMakeLists.txt` 构建。外层 Maven POM 只是调用
CMake 的包装；没有 Maven 时也可以直接使用 CMake。

### 常用构建命令

| 目标 | 命令 |
|------|------|
| 只构建库（Linux/macOS） | `mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests package` |
| 构建 Debug 库（Linux/macOS） | `mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests -Dcmake.build.type=Debug package` |
| 只构建库（Windows / MSVC） | `mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests "-Dboost.include.dir=C:\boost_1_88_0" package` |
| 构建 Debug 库（Windows / MSVC） | `mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests "-Dcmake.build.type=Debug" "-Dboost.include.dir=C:\boost_1_88_0" package` |
| 直接使用 CMake | `cmake -S iotdb-client/client-cpp -B build && cmake --build build --target install` |

Maven 构建会把 SDK 安装到 `target/install/`，并生成
`target/iotdb-session-cpp-<version>-<classifier>.zip` 及对应 `.sha512`。
可通过 `-Dclient.cpp.package.classifier=...` 指定 classifier。

### CMake 选项

通过 Maven 构建时，常用 CMake 选项对应的 Maven 属性如下：

| CMake 变量 | Maven 属性 |
|------------|------------|
| `WITH_SSL` | `with.ssl`（默认 `ON`，关闭用 `-Dwith.ssl=OFF`） |
| `IOTDB_OFFLINE` | `iotdb.offline` |
| `BUILD_TESTING` | `build.tests` |
| `IOTDB_DEPS_DIR` | `iotdb.deps.dir` |
| `BOOST_INCLUDEDIR` | `boost.include.dir` |
| `CMAKE_BUILD_TYPE` | `cmake.build.type`，例如 `-Dcmake.build.type=Debug` |

SSL 默认开启（`WITH_SSL=ON`）。所捆绑的 Apache Thrift 0.23 同时支持 OpenSSL 1.x
与 3.x，因此直接使用系统的 OpenSSL（任意版本）。CMake 通过 `find_package(OpenSSL)`
解析系统 OpenSSL，找不到时回退到从源码构建 OpenSSL 3.5.0；并会把所用的 OpenSSL
动态库一并复制到产物 `lib/` 目录。Windows 可用 `choco install openssl` 安装。
直接使用 CMake 时传入 `-DWITH_SSL=OFF`、`-DIOTDB_OFFLINE=ON` 等即可。
Debug 构建请在配置阶段传入 `-DCMAKE_BUILD_TYPE=Debug`。Windows 使用 Visual
Studio 生成器时也需要传入该选项，以便内置 Thrift 静态库使用 Debug MSVC 运行时；
随后用 `cmake --build build --config Debug --target install` 构建安装。

```bash
# Linux/macOS
cmake -S iotdb-client/client-cpp -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --target install

# Windows / Visual Studio
cmake -S iotdb-client/client-cpp -B build -G "Visual Studio 17 2022" -A x64 -DCMAKE_BUILD_TYPE=Debug
cmake --build build --config Debug --target install
```

### 离线构建

离线构建需要先把依赖源码包放入 `third-party/<platform>/`，然后执行：

```bash
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
  -Diotdb.offline=ON package
```

或直接使用 CMake：

```bash
cmake -S iotdb-client/client-cpp -B build -DIOTDB_OFFLINE=ON
cmake --build build --config Release --target install
```

完整依赖清单、SSL、测试和格式化说明请参考英文版 [README.md](README.md)。
