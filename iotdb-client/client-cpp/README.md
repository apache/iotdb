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
├── src/test/                 # Catch2-based integration tests
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
land at `iotdb-client/client-cpp/target/client-cpp-<version>-cpp-<classifier>.zip`,
where `<classifier>` defaults to the OS name (for example `linux-x86_64`) and
can be overridden with `-Dclient.cpp.package.classifier=...` when building
multiple toolchains on the same platform.

### Release packages (CI)

The [C++ Client package](../../.github/workflows/client-cpp-package.yml) workflow
builds one zip per platform/toolchain. Pick the artifact that matches your
deployment environment:

| Target environment | Zip classifier (suffix) |
|--------------------|-------------------------|
| Linux x86_64, glibc ≥ 2.17 | `linux-x86_64-glibc217` |
| Linux aarch64, glibc ≥ 2.31 | `linux-aarch64-glibc231` |
| macOS x86_64 | `mac-x86_64` |
| macOS arm64 | `mac-aarch64` |
| Windows + Visual Studio 2017 | `windows-x86_64-vs2017` |
| Windows + Visual Studio 2019 | `windows-x86_64-vs2019` |
| Windows + Visual Studio 2022 | `windows-x86_64-vs2022` |
| Windows + Visual Studio 2026 | `windows-x86_64-vs2026` |

Example file name:
`client-cpp-2.0.7-SNAPSHOT-cpp-linux-x86_64-glibc217.zip`.

Thrift **0.21.0** is compiled from source during the CMake configure step (see
`cmake/FetchThrift.cmake`). Older releases that used pre-built
`iotdb-tools-thrift` Maven artifacts and `-Diotdb-tools-thrift.version=...`
for glibc/MSVC compatibility apply only to the **legacy** client-cpp build;
with the current CMake build, compatibility is determined by the **compiler
and OS used to build** the SDK, not by that Maven property.

### Local build for a specific classifier

Linux x86_64 (glibc 2.17 baseline — use CentOS 7 + devtoolset-8, or any host
whose glibc is ≤ your deployment target):

```bash
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
  -Dclient.cpp.package.classifier=linux-x86_64-glibc217 package
```

Windows (match the Visual Studio version you use to build your application):

```powershell
# Visual Studio 2022 (default on recent Windows)
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests package

# Visual Studio 2019
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests `
  -Dcmake.generator="Visual Studio 16 2019" `
  -Dclient.cpp.package.classifier=windows-x86_64-vs2019 package

# Visual Studio 2017
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests `
  -Dcmake.generator="Visual Studio 15 2017" `
  -Dclient.cpp.package.classifier=windows-x86_64-vs2017 package
```

## CMake options

All of these can be set on the Maven command line (`-DWITH_SSL=ON`, etc.) or
passed directly to `cmake`.

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

2. Run the build with `-DIOTDB_OFFLINE=ON`:

   ```bash
   mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
       -DIOTDB_OFFLINE=ON package
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
SSL support with `-DWITH_SSL=ON`. CMake first calls `find_package(OpenSSL)`;
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
cd iotdb-client/client-cpp/target/build/src/test
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
`target/client-cpp-<version>-cpp-<os>.zip` with the historical layout:

```
.
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

```cpp
#include "Session.h"
    #include <memory>
    #include <iostream>

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

Compile against the produced SDK:

```bash
clang++ -O2 user-cpp-code.cpp \
    -I/path/to/sdk/include \
    -L/path/to/sdk/lib \
    -liotdb_session -lpthread \
    -Wl,-rpath,/path/to/sdk/lib \
    -std=c++11
```

For full API documentation see the [C++ Native API guide](https://iotdb.apache.org/UserGuide/latest/API/Programming-Cpp-Native-API.html).
