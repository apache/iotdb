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
# IoTDB ODBC driver

The driver builds directly against `../client-cpp`. Each release contains one
driver library with the Session client, Thrift, OpenSSL and curl linked statically.
It does not require a separately installed C++ SDK, OpenSSL or VC redistributable.
The OS libraries and ODBC manager/installer remain system prerequisites.

## Build

Use CMake 3.20+, a C++14 compiler and the C++ SDK build prerequisites. Windows
requires Visual Studio 2022 x64, Strawberry Perl and winflexbison3. Linux requires
unixODBC development headers. CMake downloads curl 8.4.0 and nlohmann/json 3.11.3
into the build directory and verifies their SHA-256 checksums. The C++ SDK keeps
its dependency archives under `../client-cpp/third-party`; `-DIOTDB_OFFLINE=ON`
requires that SDK cache to be populated.

```sh
cmake -S iotdb-client/odbc -B iotdb-client/odbc/target/build -DCMAKE_BUILD_TYPE=Release
cmake --build iotdb-client/odbc/target/build --config Release --parallel 4
ctest --test-dir iotdb-client/odbc/target/build -C Release --output-on-failure
cd iotdb-client/odbc/target/build
cpack -C Release
```

On Windows pass `-A x64` to configure. Maven also supports
`./mvnw package -P with-odbc -pl iotdb-client/odbc`; CMake and CPack must be on PATH.
`-DskipTests` skips test execution. Builds do not register the driver on the host.

For Linux release packages use the same manylinux 2.28 baseline as the C++ SDK:

```sh
source .github/scripts/native-linux-env.sh
docker run --rm -v "$PWD:/workspace" -w /workspace "$NATIVE_LINUX_IMAGE" \
  bash .github/scripts/package-odbc-manylinux228.sh
```

The release script builds all dependencies in this environment, runs tests and
rejects unexpected shared-library dependencies or GLIBC requirements above 2.28.
A build on a newer host is a development build, not evidence of glibc 2.28 compatibility.
Release scope is Linux x86_64 and Windows x64 (Windows 10/11, Server 2016+).
Packages and SHA-512 checksums are written into the CMake build directory.
The dependency URLs, versions and checksums are exposed as `ODBC_CURL_*` and
`ODBC_JSON_*` CMake cache variables for controlled mirrors or version updates.
File logging is excluded from release builds by default; enable it explicitly
with `-DODBC_ENABLE_LOGGING=ON` when troubleshooting.

GNU/Linux binary packages combine GCC's libstdc++ and libgcc runtime object code
under GPL-3.0-or-later WITH GCC-exception-3.1. The
[ASF GCC Runtime Library exception policy](https://www.apache.org/legal/resolved.html#gcc-runtime-library)
addresses this binary combination; the project PMC must review the inclusion.
No GCC runtime source or GPL license text is vendored in the IoTDB source tree.
At install/package time, GNU builds download the two license texts from the GCC
14.2.0 upstream release, verify pinned SHA-256 hashes and include them under
`licenses/gcc`, with a disclosure in the binary's `LICENSE`. See the
[GCC runtime licensing documentation](https://gcc.gnu.org/onlinedocs/libstdc++/manual/license.html).
Windows/MSVC packages do not include GCC runtime notices. Offline GNU packaging
requires prepopulating `<build>/runtime-licenses/GPL-3.0.txt` and
`GCC-Runtime-Exception.txt` with those verified upstream files.

## ODBC conformance

The implementation reference is Microsoft's official
[ODBC Programmer's Reference](https://learn.microsoft.com/en-us/sql/odbc/reference/odbc-programmer-s-reference),
especially the [Core Interface Conformance](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/core-interface-conformance)
function requirements, API reference, state-transition
tables and diagnostic-record rules linked from that page. The driver uses the
ODBC 3.8 ABI and targets core interface conformance.

Capability discovery is treated as a compatibility contract: `SQLGetFunctions`
reports only callable implementations, and `SQLGetInfo` reports only behavior
provided by the driver and IoTDB. Unsupported optional behavior returns a
standard SQLSTATE instead of being silently accepted. Core behavior includes
explicit and implicit descriptors, input parameter arrays (including
data-at-execution parameters),
rowset fetching, and the required catalog result schemas. The current intentional
optional limits are forward-only, read-only cursors; no true transactions, output
parameters, stored procedures, asynchronous execution or bookmarks. Applications
must use `SQLGetFunctions` and `SQLGetInfo` rather than infer optional support from
exported symbols.

`SQL_C_NUMERIC` conversion supports INT32, INT64 and epoch TIMESTAMP values,
including INT64_MIN, with a default precision of 38 and scale of zero.
Bound numeric columns and `SQLGetData(SQL_ARD_TYPE)` honor the application row
descriptor's precision (1–38) and scale (-38–38). Fractional truncation returns
`SQL_SUCCESS_WITH_INFO`/`01S07`; magnitude overflow returns `22003`.
FLOAT/DOUBLE to `SQL_C_NUMERIC` is currently unsupported and returns `07006`
without modifying the output. Retrieve approximate values as `SQL_C_FLOAT` or
`SQL_C_DOUBLE` instead. `SQLDisconnect` drops all connection-owned statement and
descriptor handles; applications must allocate new ones after reconnecting.

## Install

Windows: extract the release, then run `install.bat "C:\Program Files\Apache IoTDB ODBC Driver"`
from an administrator command prompt. Use the **64-bit** ODBC administrator to
configure a DSN. `uninstall.bat` unregisters the driver, preserving DSNs and files.

Linux: install unixODBC, extract the release, update `Driver` in `conf/odbcinst.ini`
to the absolute library path, then register with `odbcinst -i -d -f conf/odbcinst.ini`.
Copy the example DSN into your user `odbc.ini` and provide your credentials.

## Connection and encryption

Existing `SERVER`, `PORT`, `UID`, `PWD`, `DATABASE`, `ISTABLEMODEL`, `LOGLEVEL`,
`SESSIONTIMEOUTMS` and `BATCHSIZE` parameters remain available. `RESTFUL=1` selects
the retained HTTP REST implementation; specify its HTTP port explicitly.
The default is Session RPC on port 6667. Explicit connection attributes override
DSN values. Values containing semicolons can be braced, with `}}` escaping `}`.

```text
DRIVER={Apache IoTDB ODBC Driver};SERVER=localhost;PORT=6667;UID=root;PWD={your password};DATABASE=information_schema;SSL=1;SSLCA={C:\certs\ca.pem};
```

| Attribute | Default | Meaning |
| --- | --- | --- |
| SSL | 0 | Enable Session TLS; accepts 0/1, false/true, no/yes, off/on |
| SSLCA | empty | Trusted CA file in PEM format; required for TLS |
| SSLCERT | empty | PEM client certificate for mutual TLS |
| SSLKEY | empty | PEM client private key; required together with SSLCERT |

These attributes are available in connection strings, DSNs and the Windows setup
dialog. Certificates are external files readable by the application account;
they are not compiled into the driver. The server must enable the corresponding
RPC TLS mode. The client verifies trust and server identity and never falls back
to plaintext. Connection testing and queries use the same Session construction.
Certificate attributes with SSL disabled are rejected. REST combined with these
TLS options is rejected: HTTPS is outside this migration's scope.

## Validation

CTest covers configuration, type conversions, diagnostics and loading through the
system ODBC manager. The manager test deliberately rejects an invalid TLS/REST
combination without contacting a server. Windows tests use a temporary user DSN
and do not install or modify a machine-wide ODBC driver registration.
To test a running server:

```sh
export IOTDB_ODBC_CONNECTION='SERVER=localhost;PORT=6667;UID=root;PWD=your-password;ISTABLEMODEL=0'
target/build/test/odbc_manager /absolute/path/libapache_iotdb_odbc.so
```

Set `IOTDB_ODBC_QUERY` to exercise a query and `IOTDB_ODBC_EXPECT_FAILURE=1` for
negative connection tests. For plain/TLS/mTLS server phases, run
`python test/run_e2e.py --help`; use a disposable server distribution and test data.
The runner requires Java 17+, `keytool`, Python 3 and (on Linux) `lsof` or `netstat`.
Set `IOTDB_TEST_PASSWORD` to the test server password. It copies the supplied
distribution, refuses occupied test ports and removes the temporary copy afterward.

The package workflow also builds an IoTDB server and tests the **extracted release
packages** in plain, TLS and mutual-TLS modes on Linux and Windows. Certificate
rejection tests cover missing trust material, wrong trust, missing/mismatched
client credentials and a trusted server certificate with the wrong identity.
`--windows-consumers` adds real `System.Data.Odbc` and MSDASQL/ADO tests, including
tree/table catalog browsing, parameterized inserts, Unicode, long text, NULL,
empty strings, DATE, BOOLEAN and Session batch boundaries. It creates temporary
user DSNs and disposable databases, then removes them. CTest additionally runs
public-ABI regressions against a local HTTP fixture for catalog schemas,
enumeration, patterns, Unicode lengths and REST type conversion.

## Excel and other Unicode applications

Use **64-bit Microsoft Excel** with the Windows x64 package and a 64-bit user or
system DSN. In Excel choose **Data > Get Data > From Other Sources > From ODBC**,
select the DSN and select a table in Navigator. For an explicit query use the
advanced SQL option, for example `SELECT time,device,val,txt,day FROM samples` in
the DSN's table-model database. SQL must follow IoTDB's dialect; optional ODBC SQL
escapes and transactions are not implemented. Refresh uses a forward-only,
read-only cursor.

The driver exports Unicode `W` entry points and describes STRING/TEXT as
`SQL_WVARCHAR`/`SQL_WLONGVARCHAR`. Unicode applications therefore pass UTF-16 SQL
and retrieve UTF-16 data without the system ANSI code page corrupting Chinese or
other characters. Native applications should use `SQLDriverConnectW`,
`SQLExecDirectW` and `SQLPrepareW` through the Driver Manager. The driver's narrow
entry points accept UTF-8 directly; an ANSI application going through Windows'
Driver Manager is subject to its ANSI code-page conversion. TIMESTAMP query
columns retain epoch-integer semantics; DATE columns use `SQL_TYPE_DATE`.

The Windows application tests exercise .NET and Office's MSDASQL/ADO data path.
GitHub-hosted runners do not include Excel; those tests are not an Excel or Power
Query UI test. Validate Navigator, loading into a worksheet and refresh on the
target Excel installation before claiming that particular Office version has
been tested. WPS registering `Excel.Application` does not establish Microsoft
Excel compatibility.
