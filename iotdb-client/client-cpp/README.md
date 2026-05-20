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
├── third-party/              # downloaded tarballs (linux/ mac/ windows/)
├── src/main/                 # handwritten C/C++ sources + public headers
├── src/test/                 # Catch2-based integration tests
└── pom.xml                   # Maven wrapper (cmake-maven-plugin)
```

Third-party tarballs (Boost, Thrift, flex/bison, OpenSSL, ...) are cached
under **`third-party/<os>/`** inside this module. That keeps everything
portable: stage dependencies on a networked machine, **copy the whole IoTDB
tree** to an offline host, then build with `-DIOTDB_OFFLINE=ON`. Archives are
git-ignored; see [`third-party/README.md`](third-party/README.md). Override
the cache root with `-DIOTDB_DEPS_DIR=<path>` (Maven: `-Diotdb.deps.dir=...`).

During configure CMake will, in order:

1. Resolve Boost headers (`find_package` → local `${IOTDB_DEPS_DIR}/<os>/`
   tarball → download from `archives.boost.io` when not in offline mode).
2. On Linux/macOS, ensure `m4` / `flex` / `bison` are available; if not,
   build them from local tarballs into `build/tools/bin` (no `sudo`
   required).
3. Build a static Apache Thrift from source (optional `THRIFT_TARBALL` →
   `${IOTDB_DEPS_DIR}/<os>/` cache with `thrift-*.tar.gz` glob → download
   from Apache archive when not in offline mode; **same on Windows**).
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
land at `iotdb-client/client-cpp/target/client-cpp-<version>-cpp-<os>.zip`.

## CMake options

All of these can be set on the Maven command line (`-DWITH_SSL=ON`, etc.) or
passed directly to `cmake`.

| Option                | Default                          | Purpose                                                                                                  |
|-----------------------|----------------------------------|----------------------------------------------------------------------------------------------------------|
| `WITH_SSL`            | `OFF`                            | Link against OpenSSL. See *SSL* below.                                                                   |
| `BUILD_TESTING`       | `ON` (`OFF` when `-DskipTests`)  | Build Catch2 IT executables.                                                                             |
| `IOTDB_OFFLINE`       | `OFF`                            | Disallow any network access during configure.                                                            |
| `IOTDB_DEPS_DIR`      | `<client-cpp>/third-party`       | Override the third-party cache root (`linux/` / `mac/` / `windows/` appended automatically).             |
| `BOOST_VERSION`       | `1.60.0`                         | Boost version that CMake will look for / download.                                                       |
| `THRIFT_VERSION`      | `0.21.0`                         | Apache Thrift version to build from source.                                                              |
| `THRIFT_TARBALL`      | (unset)                          | Path to a pre-downloaded `thrift-*.tar.gz` anywhere on disk (skips the cache lookup).                    |
| `THRIFT_URL`          | (unset)                          | Override the Apache archive URL used when the thrift tarball is downloaded.                              |
| `BOOST_ROOT`          | (unset)                          | Existing Boost install to reuse, equivalent to `-Dboost.include.dir=...` from the legacy build.          |
| `OPENSSL_ROOT_DIR`    | (unset)                          | Existing OpenSSL install when `WITH_SSL=ON`.                                                             |
| `CMAKE_INSTALL_PREFIX`| `<build>/install`                | Install location.                                                                                        |

## Online build (default)

CMake will download any missing tarball into `third-party/<os>/` at configure
time. The first run is slow (≈100 MB download + a Thrift build); subsequent
runs reuse both the cached archives and the extracted artifacts under
`build/_deps/`.

```bash
# Linux / macOS
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests package

# Windows (Developer Command Prompt for VS, PowerShell, or cmd)
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests "-Dboost.include.dir=C:\boost_1_88_0" package
```

## Offline build (copy whole IoTDB tree)

**Recommended workflow**

1. On a **networked** machine, run one online configure/build so CMake fills
   `iotdb-client/client-cpp/third-party/<os>/` (or copy tarballs there
   manually — see table below).
2. Copy the **entire IoTDB repository** (including `third-party/`) to the
   offline host.
3. Build with `-DIOTDB_OFFLINE=ON` (no downloads attempted).

```bash
# Step 1 (online machine)
cmake -S iotdb-client/client-cpp -B build
# tarballs land in third-party/windows/ (or linux/ / mac/)

# Step 3 (offline machine, after copying the repo)
cmake -S iotdb-client/client-cpp -B build -DIOTDB_OFFLINE=ON
cmake --build build --config Release --target install
```

### Files to place under `third-party/<os>/`

| Platform   | Required files                                                                                                                                                                                |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `linux/`   | `thrift-0.21.0.tar.gz`, `boost_1_60_0.tar.gz`, `m4-1.4.19.tar.gz`, `flex-2.6.4.tar.gz`, `bison-3.8.tar.gz` (and `openssl-3.5.0.tar.gz` when `WITH_SSL=ON`)                                     |
| `mac/`     | `thrift-0.21.0.tar.gz`, `boost_1_60_0.tar.gz` (Apple already ships m4/flex/bison; `openssl-3.5.0.tar.gz` optional)                                                                            |
| `windows/` | `thrift-0.21.0.tar.gz`, `boost_1_60_0.tar.gz` (Boost headers only - no `b2` build required for `iotdb_session`), `win_flex_bison-2.5.25.zip` (any `win_flex_bison*.zip` name is accepted)      |

Reference URLs (the configure step uses the same when downloading online):

- Apache Thrift 0.21.0: <https://archive.apache.org/dist/thrift/0.21.0/thrift-0.21.0.tar.gz>
- Boost 1.60.0:        <https://archives.boost.io/release/1.60.0/source/boost_1_60_0.tar.gz>
- GNU m4 1.4.19:       <https://ftp.gnu.org/gnu/m4/m4-1.4.19.tar.gz>
- GNU flex 2.6.4:      <https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz>
- GNU bison 3.8:       <https://ftp.gnu.org/gnu/bison/bison-3.8.tar.gz>
- winflexbison 2.5.25: <https://github.com/lexxmark/winflexbison/releases/download/v2.5.25/win_flex_bison-2.5.25.zip>
- OpenSSL 3.5.0:       <https://www.openssl.org/source/openssl-3.5.0.tar.gz>

Maven offline equivalent:

```bash
mvn -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
    -DIOTDB_OFFLINE=ON package
```

Shared CI caches can still use `-DIOTDB_DEPS_DIR=/path/to/cache` if the
tarballs should live outside this module (the `<os>/` sub-folder is still
appended automatically).

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

The recommended toolchain is Visual Studio 2019 or 2022.

Prerequisites:

1. **Apache Thrift.** Online builds download `thrift-0.21.0.tar.gz` into
   `third-party/windows/` automatically (or pass
   `-DTHRIFT_TARBALL=D:\path\to\thrift-0.21.0.tar.gz` for a copy elsewhere).
   Offline builds require the tarball under `third-party/windows/` (any
   `thrift-*.tar.gz` name is accepted) or `-DTHRIFT_TARBALL=...`.
2. **Boost.** Download and extract
   <https://archives.boost.io/release/1.88.0/source/boost_1_88_0.zip>
   (any 1.60+ release will work). `iotdb_session` only needs Boost
   headers, so running `bootstrap.bat` / `b2` is optional. Pass the
   location with either `-Dboost.include.dir="C:\boost_1_88_0"` (Maven)
   or `-DBOOST_ROOT="C:\boost_1_88_0"` (raw CMake).
3. **flex / bison.** CMake handles this automatically:
   - If the host already has `flex` / `bison` (or `win_flex` / `win_bison`)
     on `PATH`, they are reused as-is.
   - Otherwise, in online mode the build downloads
     <https://github.com/lexxmark/winflexbison/releases/download/v2.5.25/win_flex_bison-2.5.25.zip>
     into `third-party/windows/` and extracts it into `build\tools\bin\`,
     renaming `win_flex.exe` / `win_bison.exe` to `flex.exe` / `bison.exe`.
   - For offline builds pre-stage any `win_flex_bison*.zip` (e.g.
     `win_flex_bison-latest.zip`) into `third-party/windows/`; CMake picks
     the first match via glob.
4. **OpenSSL** *(only when `WITH_SSL=ON`)*: run the Win64 OpenSSL
   installer from <https://slproweb.com/products/Win32OpenSSL.html>, then
   pass `-DOPENSSL_ROOT_DIR=...` to CMake.

Auto-building m4 from the GNU autotools tarball is **not** supported on
Windows; the bundled winflexbison binaries already cover the flex/bison
needs of Apache Thrift's source build.

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
│   ├── ...  (handwritten + generated headers)
│   └── thrift/
│       └── ... (Thrift runtime headers)
└── lib/
    ├── libiotdb_session.{so,dylib}  (or iotdb_session.lib on Windows)
    └── libthrift.{a,lib}            (or thriftmd.lib on Windows)
```

## Using the C++ client

```cpp
#include "include/Session.h"
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
    -liotdb_session -lthrift -lpthread \
    -Wl,-rpath,/path/to/sdk/lib \
    -std=c++11
```

For full API documentation see the [C++ Native API guide](https://iotdb.apache.org/UserGuide/latest/API/Programming-Cpp-Native-API.html).
