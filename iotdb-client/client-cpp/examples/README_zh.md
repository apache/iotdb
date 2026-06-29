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

# IoTDB C++ 客户端示例

[English](README.md)

本目录提供链接 **IoTDB C++ Session SDK**（`iotdb_session`）的示例程序。
应用侧编译 **不需要** 单独安装 Thrift 或 Boost 头文件/库，它们已封装在 SDK
共享库内部。

所有示例默认连接本机 IoTDB（`127.0.0.1:6667`，用户 `root` / 密码 `root`）。

| 示例 | 说明 |
|------|------|
| `SessionExample` | 树模型：建库建序列、写入、查询、删除 |
| `AlignedTimeseriesSessionExample` | 对齐时间序列与模板 |
| `TableModelSessionExample` | 表模型（关系型） |
| `MultiSvrNodeClient` | 多节点写入/查询循环 |
| `tree_example` | C Session API（树模型） |
| `table_example` | C Session API（表模型） |

## 选择哪个 SDK 压缩包

CI 发版（[client-cpp-package.yml](../../.github/workflows/client-cpp-package.yml)）
会按平台/工具链打出多份 zip，文件名形如
`iotdb-session-cpp-<version>-<classifier>.zip`（解压后根目录即为 `include/` 与 `lib/`）。请按目标环境选择：

| 目标环境 | classifier 后缀 |
|----------|-----------------|
| Linux x86_64，glibc >= 2.28 | `linux-x86_64-glibc2.28` |
| Linux aarch64，glibc >= 2.28 | `linux-aarch64-glibc2.28` |
| macOS x86_64 | `macos-x86_64` |
| macOS arm64 | `macos-aarch64` |
| Windows + 与工程相同的 VS 版本 | `windows-x86_64-msvc14.1` ... `msvc14.4` |

当前 CMake 构建在配置阶段从源码编译 Thrift 0.23，**不再**通过
`-Diotdb-tools-thrift.version=0.14.1.1-gcc4-SNAPSHOT` 等旧参数控制 glibc；
Linux 发版包在 `manylinux_2_28` 容器中构建，部署机需要 glibc 2.28 或更新版本。
详见 [client-cpp README](../../iotdb-client/client-cpp/README.md)。

## SDK 目录结构（解压后）

`client-cpp` 打出的 SDK 压缩包只包含 **公开头文件** 和 **一个共享库**：

```
client/
├── include/
│   ├── Session.h
│   ├── Export.h
│   └── ...          （17 个公开头；无 thrift/、boost/）
└── lib/
    ├── iotdb_session.dll + iotdb_session.lib   （Windows）
    ├── libiotdb_session.so                     （Linux）
    └── libiotdb_session.dylib                  （macOS）
```

## 编译示例

### 方式 A：Maven（本仓库推荐）

在仓库根目录执行：

```bash
mvn clean verify -P with-cpp -pl iotdb-client/client-cpp -am
```

Maven 会构建 C++ client，在 verify 阶段启动本地 IoTDB，并通过 CTest 运行 C++
测试和可自动运行的 examples。示例二进制在
`iotdb-client/client-cpp/target/build/examples/` 下（Windows + Visual Studio
通常在 `target/build/examples/Release/`）。

### 方式 B：仅 CMake（手动准备 SDK）

1. 自行编译或下载 SDK，解压后保证存在 `client/include` 与 `client/lib`（见
   上文目录结构）。
2. 使用这个 `examples/` 目录作为源码目录，并把 `client/` 放在旁边；也可以传入
   `-DIOTDB_SDK_ROOT=/path/to/iotdb-session-cpp-...`。
3. 配置并编译：

```bash
cmake -S iotdb-client/client-cpp/examples -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DIOTDB_SDK_ROOT=/path/to/iotdb-session-cpp-...
cmake --build build
```

Windows（Visual Studio 生成器）：

```powershell
cmake -S . -B build -A x64
cmake --build build --config Release
```

编译完成后，IoTDB 运行时库会通过 POST_BUILD 自动复制到 **与可执行文件相同
的目录**。Linux/macOS 可执行文件还设置了 `$ORIGIN` rpath，可在同目录加载
`.so` / `.dylib`。

可选：打部署包目录

```bash
cmake --build build --target example-dist
# 生成 build/dist/，内含全部示例二进制 + libiotdb_session.{so,dll,dylib}
```

## 在「干净机器」上运行（无需编译器、无需 SDK 头文件）

目标机器只需：

1. 可访问的 **IoTDB 服务**（已启动）。
2. **示例可执行文件** 与 **IoTDB 运行时库** 放在 **同一目录**（或位于系统
   库搜索路径中）。

可从 `build/.../Release/`（Windows）或 `build/`（Ninja/Make）复制，也可在
执行 `example-dist` 后直接使用 `build/dist/`。

### Windows

**需要拷贝的文件**

```
SessionExample.exe
iotdb_session.dll
```

（其他示例同理，可执行文件与 `iotdb_session.dll` 成对拷贝。）

**目标机器前置条件**

- **64 位 Windows**（示例为 x64 构建）。
- 安装 **[Microsoft Visual C++ 2015–2022 可再发行组件包（x64）](https://learn.microsoft.com/zh-cn/cpp/windows/latest-supported-vc-redist)**。  
  SDK 与示例均使用 **`/MD`**（动态 CRT），该安装包提供 `vcruntime140.dll`、
  `msvcp140.dll` 等运行时。  
  **仅安装此 Redistributable 即可**，目标机 **不需要** Visual Studio，也
  **不需要** IoTDB SDK 头文件或 Thrift/Boost。

**运行**

```powershell
.\SessionExample.exe
```

若提示缺少 `VCRUNTIME140.dll`，请安装上述 VC++ 可再发行包。

Thrift、Boost 已包含在 `iotdb_session.dll` 内，无需单独部署。

### Linux

**需要拷贝的文件**

```
SessionExample
libiotdb_session.so
chmod +x SessionExample
```

**目标机器前置条件**

- 目标机的 **glibc 版本必须 ≥ 编译 SDK 时的 glibc 版本**（仅向后兼容：
  新系统可跑旧库要求，旧系统不能跑需要更高 glibc 的二进制）。

在 **编译机** 上记录版本（建议写入发布说明）：

```bash
ldd --version | head -1
# 例如：ldd (Ubuntu GLIBC 2.35-0ubuntu3) 2.35
```

在 **目标机** 上检查：

```bash
ldd --version | head -1
# 版本号应 >= 编译机（同主版本次版本或更新）
```

查看二进制依赖的最高 `GLIBC_` 符号：

```bash
objdump -T SessionExample | grep GLIBC_ | sed 's/.*GLIBC_/GLIBC_/' | sort -Vu | tail -5
objdump -T libiotdb_session.so | grep GLIBC_ | sed 's/.*GLIBC_/GLIBC_/' | sort -Vu | tail -5
```

若目标 glibc 过旧，运行时会报错，例如
`version 'GLIBC_2.34' not found`。可在更旧的发行版（或旧版容器）上重新编译
SDK，以扩大兼容范围。

**运行**（`.so` 与可执行文件同目录）：

```bash
./SessionExample
```

若找不到共享库：

```bash
export LD_LIBRARY_PATH=.
./SessionExample
```

无需单独安装 Thrift。

### macOS

将示例可执行文件与 `libiotdb_session.dylib` 放在同一目录。目标 macOS 版本
应 **≥ 编译 SDK 时设置的 deployment target**。可用以下命令检查依赖：

```bash
otool -L SessionExample
```

## 开发说明

- **Windows**：应用与 SDK 均使用 **`/MD`**，与 Visual Studio 默认工程一致；
  链接 `iotdb_session.lib`，部署时携带 `iotdb_session.dll`。
- **Linux**：直接链接 `libiotdb_session.so`；建议与可执行文件同目录发布，或
  设置 `RPATH=$ORIGIN`。
- 示例默认连接 `127.0.0.1:6667`；如需修改地址/端口，请编辑对应源码。

## 本模块目录结构

```
client-cpp/examples/
├── README.md            # 英文说明
├── README_zh.md         # 中文说明（本文件）
├── CMakeLists.txt
├── SessionExample.cpp
├── AlignedTimeseriesSessionExample.cpp
├── TableModelSessionExample.cpp
├── MultiSvrNodeClient.cpp
├── tree_example.c
└── table_example.c
```

执行 `mvn verify -P with-cpp -pl iotdb-client/client-cpp -am` 后，可在
`iotdb-client/client-cpp/target/build/examples/` 下找到构建产物。
