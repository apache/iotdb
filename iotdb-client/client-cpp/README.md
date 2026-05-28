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

The C++ client is built by a single top-level `CMakeLists.txt` in this
directory. The outer Maven POM is a thin wrapper that invokes CMake; you can
also build the client standalone with just `cmake` if you don't have Maven
available.

## Build layout at a glance

```
iotdb-client/client-cpp/
├── CMakeLists.txt            # single entry point - manages everything
├── cmake/                    # helpers (FetchBoost / FetchThrift / ...)
├── third-party/              # local tarball cache (one sub-dir per OS)
│   ├── linux/  mac/  windows/
├── src/include/              # public API headers (installed to include/)
├── src/session/              # Session / Table / C API implementation (.cpp)
├── src/rpc/                  # Thrift RPC layer (private, not installed)
├── test/                     # Catch2-based integration tests
└── pom.xml                   # Maven wrapper (cmake-maven-plugin)
```

During configure CMake will, in order:

1. Resolve Boost headers (`find_package` → local `third-party/<os>/` tarball →
   download from `archives.boost.io` when not in offline mode).
2. On Linux/macOS, ensure `m4` / `flex` / `bison` are available; if not,
   build them from local tarballs into `build/tools/bin` (no `sudo`
   required).
3. Build a static Apache Thrift from source (tarball cache → download fallback).
4. Run the produced `thrift` compiler on
   `iotdb-protocol/thrift-{commons,datanode}/src/main/thrift/*.thrift`.
5. Compile `iotdb_session` (the C/C++ session library) and, optionally,
   the Catch2 integration test binaries.
6. `cmake --install` lays out the SDK under `target/install/{include,lib}`,
   which Maven's assembly step packages into a zip.

## Build matrix

| Goal                          | Command                                                                                                |
|-------------------------------|--------------------------------------------------------------------------------------------------------|
| Library only (Linux/macOS)    | `mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests package`                                  |
| Library only (Windows / MSVC) | `mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests "-Dboost.include.dir=C:\boost_1_88_0" package` |
| Library + ITs (Linux/macOS)   | `mvn clean install -P with-cpp -pl distribution,iotdb-client/client-cpp -am` then `mvn -P with-cpp -pl iotdb-client/client-cpp -am verify` |
| Direct CMake (no Maven)       | `cmake -S iotdb-client/client-cpp -B build && cmake --build build --target install`                    |

The Maven build sets `cmake.install.prefix` to `target/install/`. Output zips
land at `iotdb-client/client-cpp/target/client-cpp-<version>-<classifier>.zip`
(with `include/` and `lib/` under `client-cpp-<version>-<classifier>/` inside the zip),
where `<classifier>` defaults to the OS name (for example `linux-x86_64`) and
can be overridden with `-Dclient.cpp.package.classifier=...` when building
multiple toolchains on the same platform.

### Release packages (CI)

The [C++ Client package](../../.github/workflows/client-cpp-package.yml) workflow
builds one zip per platform/toolchain. Pick the artifact that matches your
deployment environment:

| Target environment | Zip classifier (suffix) |
|--------------------|-------------------------|
| Linux x86_64, glibc ≥ 2.24, default g++ (cxx11 ABI) | `linux-x86_64-glibc224` |
| Linux aarch64, glibc ≥ 2.24, default g++ (cxx11 ABI) | `linux-aarch64-glibc224` |
| Linux x86_64, glibc ≥ 2.17, legacy libstdc++ ABI | `linux-x86_64-glibc217` |
| Linux aarch64, glibc ≥ 2.17, legacy libstdc++ ABI | `linux-aarch64-glibc217` |
| macOS x86_64 | `mac-x86_64` |
| macOS arm64 | `mac-aarch64` |
| Windows + Visual Studio 2017 | `windows-x86_64-vs2017` |
| Windows + Visual Studio 2019 | `windows-x86_64-vs2019` |
| Windows + Visual Studio 2022 | `windows-x86_64-vs2022` |
| Windows + Visual Studio 2026 | `windows-x86_64-vs2026` |

Example file name:
`client-cpp-2.0.7-SNAPSHOT-linux-x86_64-glibc224.zip`.

**Linux ABI and glibc:** Two Linux families are published.

| Package | manylinux image | Deployment glibc | libstdc++ ABI | How to match when you build your app |
|---------|-----------------|------------------|---------------|--------------------------------------|
| `*-glibc224` | `manylinux_2_24` | **≥ 2.24** | **cxx11** (`nm` shows `__cxx11`) | Default g++ on Ubuntu 22.04+, Kylin V10, etc. — no extra flags |
| `*-glibc217` | `manylinux2014` | **≥ 2.17** | **legacy** (no `__cxx11`) | Add **`-D_GLIBCXX_USE_CXX11_ABI=0`** to your compile flags |

Prefer **`glibc224`** on modern distros. Use **`glibc217`** only when the target
host cannot run glibc 2.24+ binaries, or you must stay on the legacy ABI.
The `manylinux_2_24` image series is **EOL**; it is the oldest manylinux line
used here that can produce a cxx11-ABI SDK with the image toolchain.

CI passes libstdc++ ABI into CMake via **`-Diotdb.extra.cxx.flags=...`** (see
below). Setting `CXXFLAGS` alone is **not** enough for the Maven/CMake build.

Thrift **0.21.0** is compiled from source during the CMake configure step (see
`cmake/FetchThrift.cmake`). Older releases that used pre-built
`iotdb-tools-thrift` Maven artifacts and `-Diotdb-tools-thrift.version=...`
for glibc/MSVC compatibility apply only to the **legacy** client-cpp build;
with the current CMake build, compatibility is determined by the **compiler
and OS used to build** the SDK, not by that Maven property.

### Local build for a specific classifier

Linux x86_64 (**glibc224**, cxx11 ABI — modern distro or `manylinux_2_24`):

```bash
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
  -Dclient.cpp.package.classifier=linux-x86_64-glibc224 \
  -Diotdb.extra.cxx.flags=-D_GLIBCXX_USE_CXX11_ABI=1 package
```

Linux x86_64 (**glibc217**, legacy ABI — CentOS 7 / `manylinux2014`):

```bash
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
  -Dclient.cpp.package.classifier=linux-x86_64-glibc217 \
  -Diotdb.extra.cxx.flags=-D_GLIBCXX_USE_CXX11_ABI=0 package
```

Standalone CMake (same ABI flags):

```bash
cmake -S iotdb-client/client-cpp -B build \
  -DIOTDB_EXTRA_CXX_FLAGS=-D_GLIBCXX_USE_CXX11_ABI=1   # or =0 for glibc217
cmake --build build --target install
```

Windows (match the Visual Studio version you use to build your application):

```powershell
# Visual Studio 2022 (default on recent Windows)
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests package

# Visual Studio 2019
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests `
  -Dcmake.generator="Visual Studio 16 2019" `
  -Dclient.cpp.package.classifier=windows-x86_64-vs2019 package

# Visual Studio 2017 (CMake uses -A x64 on Windows automatically)
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests `
  -Dcmake.generator="Visual Studio 15 2017" `
  -Dclient.cpp.package.classifier=windows-x86_64-vs2017 package
```

On Windows, the build passes `-DCMAKE_GENERATOR_PLATFORM=x64` so Visual Studio
generators target **x64** (VS2017 otherwise defaults to Win32).

## CMake options

The table below lists CMake cache variables. When building through **Maven**,
pass them as Maven properties (the POM maps them to `-D` options for CMake):

| CMake variable | Maven property (`-D...`) |
|----------------|--------------------------|
| `WITH_SSL` | `with.ssl` (e.g. `-Dwith.ssl=ON`) |
| `IOTDB_OFFLINE` | `iotdb.offline` |
| `BUILD_TESTING` | `build.tests` |
| `IOTDB_DEPS_DIR` | `iotdb.deps.dir` |
| `BOOST_INCLUDEDIR` | `boost.include.dir` (legacy alias) |
| `IOTDB_EXTRA_CXX_FLAGS` | `iotdb.extra.cxx.flags` (e.g. `-D_GLIBCXX_USE_CXX11_ABI=0`) |

For a **standalone** `cmake` configure, pass `-DWITH_SSL=ON`, `-DIOTDB_OFFLINE=ON`,
etc. directly.

| Option                | Default                          | Purpose                                                                                                  |
|-----------------------|----------------------------------|----------------------------------------------------------------------------------------------------------|
| `WITH_SSL`            | `OFF`                            | Link against OpenSSL. See *SSL* below.                                                                   |
| `BUILD_TESTING`       | `OFF` (Maven sets `ON` for verify) | Build Catch2 IT executables.                                                                           |
| `IOTDB_OFFLINE`       | `OFF`                            | Disallow any network access during configure.                                                            |
| `IOTDB_DEPS_DIR`      | `<client-cpp>/third-party`       | Override the local tarball cache directory.                                                              |
| `BOOST_VERSION`       | `1.60.0` (`1.84.0` on macOS)     | Boost version that CMake will look for / download.                                                       |
| `THRIFT_VERSION`      | `0.21.0`                         | Apache Thrift version to build from source.                                                              |
| `BOOST_ROOT`          | (unset)                          | Existing Boost install to reuse, equivalent to `-Dboost.include.dir=...` from the legacy build.          |
| `OPENSSL_ROOT_DIR`    | (unset)                          | Existing OpenSSL install when `WITH_SSL=ON`.                                                             |
| `CMAKE_INSTALL_PREFIX`| `<build>/install`                | Install location.                                                                                        |
| `IOTDB_EXTRA_CXX_FLAGS` | (empty)                        | Appended to `CMAKE_CXX_FLAGS` on Linux/macOS (libstdc++ ABI, etc.). Also applied to the Thrift sub-build. |

## Online build (default)

CMake will download any missing tarball at configure time. The first run is
slow (≈100 MB download + a Thrift build); subsequent runs reuse the
extracted artifacts under `build/_deps/`.

```bash
# Linux / macOS
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests package

# Windows (Developer Command Prompt for VS, PowerShell, or cmd)
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests "-Dboost.include.dir=C:\boost_1_88_0" package
```

## Offline build

1. Pre-populate the platform-specific sub-directory under `third-party/`:

   | Platform   | Required files                                                                                                                                                       |
   |------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
   | `linux/`   | `thrift-0.21.0.tar.gz`, `boost_1_60_0.tar.gz`, `m4-1.4.19.tar.gz`, `flex-2.6.4.tar.gz`, `bison-3.8.tar.gz` (and `openssl-3.5.0.tar.gz` when `WITH_SSL=ON`)            |
   | `mac/`     | `thrift-0.21.0.tar.gz`, `boost_1_84_0.tar.gz` (newer Boost for Xcode/Clang; Apple ships m4/flex/bison; `openssl-3.5.0.tar.gz` optional)                               |
   | `windows/` | `thrift-0.21.0.tar.gz`, `boost_1_60_0.tar.gz` (Boost headers only - no `b2` build required for `iotdb_session`)                                                      |

   Reference URLs (the configure step uses the same):
   - Apache Thrift 0.21.0: <https://archive.apache.org/dist/thrift/0.21.0/thrift-0.21.0.tar.gz>
   - Boost 1.60.0:        <https://archives.boost.io/release/1.60.0/source/boost_1_60_0.tar.gz>
   - GNU m4 1.4.19:       <https://ftp.gnu.org/gnu/m4/m4-1.4.19.tar.gz>
   - GNU flex 2.6.4:      <https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz>
   - GNU bison 3.8:       <https://ftp.gnu.org/gnu/bison/bison-3.8.tar.gz>
   - OpenSSL 3.5.0:       <https://www.openssl.org/source/openssl-3.5.0.tar.gz>

2. Run the build with offline mode enabled:

   ```bash
   mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
       -Diotdb.offline=ON package
   ```

   or, going straight through CMake:

   ```bash
   cmake -S iotdb-client/client-cpp -B build -DIOTDB_OFFLINE=ON
   cmake --build build --config Release --target install
   ```

CI environments can share a single cache by setting
`-DIOTDB_DEPS_DIR=/path/to/cache` instead of copying tarballs around.

## Platform-specific notes

### Linux

- Tested with GCC 7+ and Clang 9+. Anything that can compile Apache Thrift
  0.21.0 works.
- Build deps that must already exist on the host (only required when
  CMake auto-builds m4/flex/bison from tarball): `make`, `autoconf`,
  `gcc`, plus the standard C/C++ toolchain. `sudo` is **not** required;
  the helper tools install under `build/tools/`.
- If you would rather use distro-provided tools (`apt install m4 flex
  bison`), CMake will pick them up first.

### macOS

- Xcode Command Line Tools provide `m4`, `flex`, `bison`, and `make`,
  so the auto-build path normally skips them.
- Homebrew users can `brew install boost` to short-circuit `FetchBoost`.

### Windows

Visual Studio **2017, 2019, 2022, or 2026** is supported for building the SDK.
Link your application against the zip built with the **same VS generation** you
use for your project.

Prerequisites:

1. **Boost.** Download and extract
   <https://archives.boost.io/release/1.88.0/source/boost_1_88_0.zip>
   (any 1.60+ release will work). `iotdb_session` only needs Boost
   headers, so running `bootstrap.bat` / `b2` is optional. Pass the
   location with either `-Dboost.include.dir="C:\boost_1_88_0"` (Maven)
   or `-DBOOST_ROOT="C:\boost_1_88_0"` (raw CMake).
2. **flex / bison.** Install <https://sourceforge.net/projects/winflexbison/>
   and rename `win_flex.exe`→`flex.exe`, `win_bison.exe`→`bison.exe` on
   `PATH`.
3. **OpenSSL** *(only when `WITH_SSL=ON`)*: run the Win64 OpenSSL
   installer from <https://slproweb.com/products/Win32OpenSSL.html>, then
   pass `-DOPENSSL_ROOT_DIR=...` to CMake.

On Windows the SDK ships as **`iotdb_session.dll`** plus an import library
**`iotdb_session.lib`**, built with **`/MD`** (dynamic CRT, same as a
default Visual Studio application). Thrift is linked into the DLL; users
do not install separate Thrift headers or libraries. Place
`iotdb_session.dll` next to your `.exe` or on `PATH`.

Auto-building m4/flex/bison from tarball is **not** supported on Windows;
the GNU autotools tarballs assume a POSIX shell environment.

## SSL

Both Thrift and `iotdb_session` build without OpenSSL by default. Enable
SSL with `-Dwith.ssl=ON` (Maven) or `-DWITH_SSL=ON` (standalone CMake).
CMake first calls `find_package(OpenSSL)`;
if nothing is found, it falls back to:

- **Linux / macOS** – use a local `openssl-<ver>.tar.gz` (or download it
  when not in offline mode), configure with `no-shared`, install into
  `build/_deps/openssl/install`, and link statically.
- **Windows** – fail with a friendly message that points at the Win64
  OpenSSL installer. Building OpenSSL from source via MSVC is out of scope.

## Tests

Maven binds `cmake-maven-plugin`'s `test` goal to the `integration-test`
phase and runs `ctest`. `pre-integration-test` spawns a local IoTDB server
from `distribution/target/.../sbin/start-standalone.{sh,bat}`, so make sure
the distribution module is built first:

```bash
mvn clean install -P with-cpp -pl distribution,iotdb-client/client-cpp -am -DskipTests
mvn -P with-cpp -pl iotdb-client/client-cpp -am verify
```

Running ctest directly (after a `mvn ... package` build) is also supported:

```bash
cd iotdb-client/client-cpp/target/build/test
ctest --output-on-failure
```

## Code formatting

We use `clang-format` (pinned by the root POM as `clang.format.version`)
through Maven Spotless. **clang-format 17.0.6** is the version CI runs.

```bash
mvn -P with-cpp -pl iotdb-client/client-cpp spotless:check
mvn -P with-cpp -pl iotdb-client/client-cpp spotless:apply
```

On JDK 8 the C++ Spotless profile is skipped automatically (Spotless's
clang-format integration requires Spotless 2.44+, which itself requires
JDK 11+).

## Package layout

A successful `mvn ... package` produces
`target/client-cpp-<version>-<classifier>.zip` with this layout:

```
client-cpp-<version>-<classifier>/
├── include/
│   ├── Session.h
│   ├── SessionC.h
│   └── ...  (public API headers only; no Thrift/Boost)
└── lib/
    ├── libiotdb_session.{so,dylib}     (Linux / macOS)
    ├── iotdb_session.dll               (Windows – runtime)
    └── iotdb_session.lib               (Windows – import library for linking)
```

Thrift is embedded inside `iotdb_session` on all platforms; it is not shipped
as a separate install artifact.

## Using the C++ client

### Application code

```cpp
#include "Session.h"
#include <iostream>
#include <memory>

int main() {
  auto session = std::make_shared<Session>("127.0.0.1", 6667, "root", "root");
  session->open(false);
  session->setStorageGroup("root.test01");
  if (!session->checkTimeseriesExists("root.test01.d0.s0")) {
    session->createTimeseries(
        "root.test01.d0.s0",
        TSDataType::INT64,
        TSEncoding::RLE,
        CompressionType::SNAPPY);
  }
  session->close();
}
```

### Link against a pre-built zip (Linux)

1. Unzip `client-cpp-<version>-<classifier>.zip` and note `include/` and `lib/`.
2. Pick the classifier that matches your machine (see table above).
3. Compile with the **same libstdc++ ABI** as the zip:

```bash
SDK=/path/to/client-cpp-2.0.7-SNAPSHOT-linux-x86_64-glibc224

# glibc224 zip (default g++ ABI on modern Linux):
g++ -std=c++11 -O2 -o myapp myapp.cpp \
  -I"${SDK}/include" -L"${SDK}/lib" -liotdb_session -lpthread \
  -Wl,-rpath,"${SDK}/lib"

# glibc217 zip (legacy ABI — required):
g++ -std=c++11 -O2 -o myapp myapp.cpp \
  -D_GLIBCXX_USE_CXX11_ABI=0 \
  -I"${SDK}/include" -L"${SDK}/lib" -liotdb_session -lpthread \
  -Wl,-rpath,"${SDK}/lib"
```

If linking fails with `undefined reference to ... __cxx11::basic_string`, the zip
ABI and your compiler flags do not match — switch zip (`glibc224` vs `glibc217`)
or add/remove `-D_GLIBCXX_USE_CXX11_ABI=0`.

At runtime, ensure `libiotdb_session.so` is found (`LD_LIBRARY_PATH` or
`-Wl,-rpath` as above). Thrift is **inside** the SDK shared library; you do not
ship a separate `libthrift.so`.

### Link after a local Maven/CMake build

Install tree: `iotdb-client/client-cpp/target/install/{include,lib}`.

```bash
g++ -std=c++11 -O2 -o myapp myapp.cpp \
  -Iiotdb-client/client-cpp/target/install/include \
  -Liotdb-client/client-cpp/target/install/lib \
  -liotdb_session -lpthread \
  -Wl,-rpath,"$PWD/iotdb-client/client-cpp/target/install/lib"
```

Use the same `-D_GLIBCXX_USE_CXX11_ABI=...` flag you passed when building the SDK
(`-Diotdb.extra.cxx.flags=...`).

### Examples

See [client-cpp-example](../../example/client-cpp-example/README.md) for CMake
projects that consume an unpacked SDK (`-DIOTDB_SDK_ROOT=...`).

For full API documentation see the [C++ Native API guide](https://iotdb.apache.org/UserGuide/latest/API/Programming-Cpp-Native-API.html).
