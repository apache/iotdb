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

# IoTDB C++ client examples

[中文说明](README_zh.md)

Sample programs that link against the pre-built **IoTDB C++ Session SDK**
(`iotdb_session`). Thrift and Boost are **not** required at application compile
time; they are embedded inside the SDK shared library.

All examples connect to a running IoTDB instance (default `127.0.0.1:6667`,
user `root` / `root`).

| Example | Description |
|---------|-------------|
| `SessionExample` | Tree model: DDL, insert, query, delete |
| `AlignedTimeseriesSessionExample` | Aligned time series and templates |
| `TableModelSessionExample` | Table (relational) model |
| `MultiSvrNodeClient` | Multi-node insert/query loop |
| `tree_example` | C Session API (tree model) |
| `table_example` | C Session API (table model) |

## Which SDK zip to use

Release CI ([client-cpp-package.yml](../../.github/workflows/client-cpp-package.yml))
publishes one zip per platform/toolchain:
`iotdb-session-cpp-<version>-<classifier>.zip` (package root contains `include/` and `lib/`).

| Deployment target | Classifier suffix |
|-------------------|-------------------|
| Linux x86_64, glibc >= 2.28 | `linux-x86_64-glibc2.28` |
| Linux aarch64, glibc >= 2.28 | `linux-aarch64-glibc2.28` |
| macOS x86_64 | `macos-x86_64` |
| macOS arm64 | `macos-aarch64` |
| Windows (match your Visual Studio version) | `windows-x86_64-msvc14.1` ... `msvc14.4` |

The current build compiles Thrift 0.23 from source at CMake configure time.
Legacy `-Diotdb-tools-thrift.version=...` flags applied to the **old**
pre-built Thrift workflow only. Linux release packages are built in the
`manylinux_2_28` container and require glibc 2.28 or newer. See
[client-cpp README](../../iotdb-client/client-cpp/README.md).

## SDK layout (after unpack)

The SDK zip produced by `client-cpp` contains **public headers only** and one
shared library:

```
client/
├── include/
│   ├── Session.h
│   ├── Export.h
│   └── ...
└── lib/
    ├── iotdb_session.dll + iotdb_session.lib   (Windows)
    ├── libiotdb_session.so                     (Linux)
    └── libiotdb_session.dylib                  (macOS)
```

## Build the examples

### Option A – Maven (recommended in this repo)

From the repository root:

```bash
mvn clean verify -P with-cpp -pl iotdb-client/client-cpp -am
```

Maven builds the C++ client, starts a local IoTDB server for the verify phase,
and runs the C++ tests plus runnable examples through CTest. Example binaries
are under `iotdb-client/client-cpp/target/build/examples/` (or
`target/build/examples/Release/` with Visual Studio).

### Option B – CMake only (manual SDK)

1. Build or download the SDK and unpack it so `client/include` and
   `client/lib` exist (see layout above).
2. Use this `examples/` directory as the source tree and place `client/` beside
   it, or pass `-DIOTDB_SDK_ROOT=/path/to/iotdb-session-cpp-...`.
3. Configure and build:

```bash
cmake -S iotdb-client/client-cpp/examples -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DIOTDB_SDK_ROOT=/path/to/iotdb-session-cpp-...
cmake --build build
```

Windows (Visual Studio generator):

```powershell
cmake -S . -B build -A x64
cmake --build build --config Release
```

Each executable is built with the IoTDB runtime library copied **next to the
`.exe` / binary** (POST_BUILD step). Linux/macOS binaries use `$ORIGIN` rpath
so they resolve the `.so` / `.dylib` in the same directory.

Optional staging folder for deployment:

```bash
cmake --build build --target example-dist
# -> build/dist/ contains all example binaries + libiotdb_session.{so,dll,dylib}
```

## Run on a clean machine (no compiler, no IoTDB SDK headers)

You only need:

1. A running IoTDB server reachable from the machine.
2. The **example executable(s)** and the **IoTDB runtime library** in the
   **same directory** (or on the system library path).

Copy either from `build/.../Release/` (Windows) / `build/` (Ninja/Make) or from
`build/dist/` after `example-dist`.

### Windows

**Files to copy**

```
SessionExample.exe
iotdb_session.dll
```

(Repeat for the other example names if needed.)

**Prerequisites on the target PC**

- **64-bit Windows** (examples are built x64).
- **[Microsoft Visual C++ Redistributable for Visual Studio 2015–2022](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)**  
  (x64). The SDK and examples are built with **`/MD`**; the redistributable
  supplies `vcruntime140.dll`, `msvcp140.dll`, etc.  
  Installing this package is enough—you do **not** need Visual Studio or the
  IoTDB SDK on the target machine.

**Run**

```powershell
.\SessionExample.exe
```

If you see “The code execution cannot proceed because VCRUNRuntime140.dll was
missing”, install the VC++ redistributable above.

You do **not** need a separate Thrift or Boost runtime; they are inside
`iotdb_session.dll`.

### Linux

**Files to copy**

```
SessionExample
libiotdb_session.so
chmod +x SessionExample
```

**Prerequisites on the target machine**

- **glibc** on the target must be **≥ the glibc version on the machine that
  built the SDK** (backward compatible only in that direction).

Check **build machine** (record in release notes):

```bash
ldd --version | head -1
# e.g. ldd (Ubuntu GLIBC 2.35-0ubuntu3) 2.35
```

Check **target machine**:

```bash
ldd --version | head -1
# must be >= build glibc (same major.minor or newer)
```

See which `GLIBC_` symbols the binary needs:

```bash
objdump -T SessionExample | grep GLIBC_ | sed 's/.*GLIBC_/GLIBC_/' | sort -Vu | tail -5
objdump -T libiotdb_session.so | grep GLIBC_ | sed 's/.*GLIBC_/GLIBC_/' | sort -Vu | tail -5
```

If the target glibc is too old, you'll get errors like
`version 'GLIBC_2.34' not found` at runtime. Rebuild the SDK on an older distro
(or in an older container) to widen compatibility.

**Run** (with `.so` beside the binary):

```bash
./SessionExample
```

If the shared library is not found:

```bash
export LD_LIBRARY_PATH=.
./SessionExample
```

No separate Thrift install is required.

### macOS

Copy the example binary and `libiotdb_session.dylib` together. The target macOS
version should be **≥ the deployment target used to build the SDK**. Check with:

```bash
otool -L SessionExample
```

## Development notes

- **Windows**: Application and SDK both use **`/MD`** (dynamic CRT). This
  matches a default Visual Studio project; link `iotdb_session.lib`, ship
  `iotdb_session.dll`.
- **Linux**: SDK is `libiotdb_session.so`; link it directly. Prefer shipping
  the `.so` next to your binary or setting `RPATH` to `$ORIGIN`.
- Examples assume IoTDB is listening on `127.0.0.1:6667`; change host/port in
  the source if needed.

## Project layout in this module

```
client-cpp/examples/
├── README.md
├── README_zh.md
├── CMakeLists.txt
├── SessionExample.cpp
├── AlignedTimeseriesSessionExample.cpp
├── TableModelSessionExample.cpp
├── MultiSvrNodeClient.cpp
├── tree_example.c
└── table_example.c
```

After `mvn package`, the runnable tree is under `target/` (sources, `client/`,
and CMake build output).
