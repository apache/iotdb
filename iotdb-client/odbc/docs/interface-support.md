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
# Interface support and verification

## Scope and reference

This driver exposes the ODBC 3.8 ABI on Windows x64 and Linux x86_64. It targets
the [ODBC Core interface](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/core-interface-conformance);
this is a capability checklist, not a claim of certification or full optional
ODBC/SQL grammar support. The
[ODBC function reference](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-function-summary)
defines each API's argument, result and diagnostic contract. Applications must
query `SQLGetFunctions` and `SQLGetInfo` through their Driver Manager.

The default transport embeds this repository's C++ Session client. The retained
`RESTFUL=1` transport uses HTTP and is tested separately. Session TLS settings
are rejected with REST; they do not enable HTTPS.

## ODBC interface checklist

"Implemented" below includes the stated restrictions. Function availability does
not imply that every attribute, orientation or optional SQL feature is supported.
String-bearing APIs have Unicode W wrappers where applicable; narrow driver
entry points use UTF-8. The Windows manager may convert ANSI application strings
through its configured code page.

| Requirement / interface | Behavior and limits | Example / verification |
| --- | --- | --- |
| Handle lifecycle: `SQLAllocHandle`, `SQLFreeHandle`, `SQLFreeStmt` | ENV, DBC, STMT and explicit/implicit descriptors; close, unbind and reset-parameter options. Disconnect drops connection-owned statements/descriptors. | Allocate ENV → DBC → STMT; `unit.cpp` checks ownership and stale child frees. |
| Connect: `SQLConnect`, `SQLDriverConnect`, `SQLDisconnect` | DSN and connection-string configuration; table/tree Session; plain, TLS and mTLS. Successful connection executes SHOW VERSION. | `manager.cpp`, `windows_setup.py`, `run_e2e.py`; credentials supplied locally. |
| Environment/connection/statement attributes: `SQLGetEnvAttr`, `SQLSetEnvAttr`, `SQLGetConnectAttr`, `SQLSetConnectAttr`, `SQLGetStmtAttr`, `SQLSetStmtAttr` | Implemented attributes include descriptor associations, array binding/status pointers, rowset size and metadata-ID mode. Unsupported values return a diagnostic; no asynchronous execution. Clearing a fetch-counter pointer detaches it. | `unit.cpp`, `parameters.py`, `compatibility.py`. Use `SESSIONTIMEOUTMS` for Session query timeout; nonzero `SQL_ATTR_QUERY_TIMEOUT` is unsupported. |
| Capability discovery: `SQLGetFunctions`, `SQLGetInfo` | Reports implemented function IDs and restricted capabilities; forward-only/read-only, no transactions, no bookmarks. | `unit.cpp` checks function bitmap and capability values. |
| Direct execution: `SQLExecDirect`, `SQLNativeSql` | IoTDB SQL dialect. Query verbs SELECT/WITH/SHOW/DESCRIBE/DESC/LIST/COUNT/EXPLAIN use the query API; other statements use the non-query API. NativeSql passes SQL through; no general ODBC escape translation. | `manager.cpp`, `run_e2e.py`: DDL, inserts, SELECT, COUNT DATABASES, EXPLAIN. |
| Prepared execution: `SQLPrepare`, `SQLExecute`, `SQLNumParams` | Client-side preparation counts unquoted/uncommented `?` markers; execution renders typed SQL literals. No server-side prepared-statement object or server parameter metadata. | `parameters.py`: quoted question marks, escaping, NULL, Unicode, numeric/date/binary values and errors. |
| Parameters: `SQLBindParameter`, `SQLParamData`, `SQLPutData` | Input parameters, row/column-wise arrays, status/processed counters and data-at-execution buffers. Array elements execute sequentially; not an atomic transaction. Output/input-output parameters unsupported. | `parameters.py`; real Session parameterized inserts in `run_e2e.py` and `windows-consumers.ps1`. |
| Fetch: `SQLFetch`, `SQLFetchScroll`, `SQLBindCol`, `SQLGetData` | Forward-only NEXT, bound rowsets, incremental character/binary retrieval; any-column/any-order retrieval. No scroll positioning or SQLGetData block-fetch extension. Completed fixed-size/NULL retrieval returns SQL_NO_DATA on the next consecutive call. | `unit.cpp`, `compatibility.py`; real Session batch crossings in Windows consumer tests. |
| Result description: `SQLNumResultCols`, `SQLDescribeCol`, `SQLColAttribute` | Column name, type, size, precision/scale and nullable metadata. Unicode character and byte lengths use their respective API units. | `compatibility.py`; .NET `DataTable.Load` checks nullable/long Unicode metadata in `windows-consumers.ps1`. |
| Descriptors: `SQLGetDescField`, `SQLGetDescRec`, `SQLSetDescField`, `SQLSetDescRec`, `SQLCopyDesc` | ARD/APD/IRD/IPD, explicit associations and descriptor-driven bindings; implementation metadata has write restrictions. TEXT is SQL_WLONGVARCHAR consistently in IRD and result description. | `unit.cpp`, `compatibility.py`; SQL_ARD_TYPE numeric precision/scale checks. |
| Catalogs: `SQLTables`, `SQLColumns` | Fixed ODBC catalog schemas; table-model database/table/column discovery and tree-model device/measurement projection. Patterns are filtered locally, with Unicode-aware `_`, `%` and escaped literals. | `compatibility.py`: enumeration, patterns, schema sizes, ordinal positions; real tree/table browsing in Windows consumer tests. |
| Type/catalog metadata: `SQLGetTypeInfo`, `SQLSpecialColumns`, `SQLStatistics` | Type rows are provided; special-column/statistics calls return correctly shaped empty results where the server has no corresponding ODBC metadata. Empty metadata is not a fabricated key/index. | `unit.cpp`, `compatibility.py`; `Catalog.cpp` defines schemas. |
| Diagnostics: `SQLGetDiagRec`, `SQLGetDiagField`, legacy `SQLError` | SQLSTATE, native error and message records, including conversion overflow, truncation, unsupported options and connection failure. | `unit.cpp`, `parameters.py`, `compatibility.py`, negative TLS tests. |
| Result/cursor completion: `SQLCloseCursor`, `SQLMoreResults`, `SQLRowCount` | Closes Session operation handles. One result set; MoreResults returns SQL_NO_DATA. RowCount returns -1 (unknown), not an invented affected-row count. | `unit.cpp`, manager/consumer execution and cleanup. |
| Cancellation: `SQLCancel`, `SQLCancelHandle` | Cancels local data-at-execution state. Other calls are synchronous; this is not remote interruption of an in-flight Session RPC. | Parameter state tests and `driver.cpp`. |
| Transactions: `SQLEndTran`, `SQLTransact` | SQL_TXN_CAPABLE = SQL_TC_NONE; commit is a no-op, rollback returns HYC00. No atomic batch/rollback guarantee. | `unit.cpp` and capability checks. |
| Cursor names: `SQLGetCursorName`, `SQLSetCursorName` | Local cursor naming only; no positioned update/delete. | `driver.cpp`; no scroll/update capability advertised. |
| Manager-facing enumeration: `SQLDataSources`, `SQLDrivers` | System Driver Manager owns installed driver/DSN enumeration. SQLDataSources has an empty driver stub; SQLDrivers is manager-provided, not a driver export. | `manager.cpp` loads the packaged library through the OS manager. |
| ODBC 2 compatibility: `SQLAllocEnv`, `SQLAllocConnect`, `SQLAllocStmt`, `SQLFreeEnv`, `SQLFreeConnect`, `SQLBindParam`, `SQLSetParam`, `SQLGetConnectOption`, `SQLSetConnectOption`, `SQLGetStmtOption`, `SQLSetStmtOption` | Adapters to the supported ODBC 3 operations, with the same restrictions. | Legacy wrappers in `driver.cpp`; primary application verification uses the ODBC 3 manager path. |

Not supported/advertised: stored procedures and procedure metadata, foreign-key
and primary-key discovery APIs, `SQLDescribeParam`, output parameters, bookmarks,
positioned updates, scrollable/updatable cursors, asynchronous operations,
multiple result sets, distributed/real transactions and general ODBC SQL escapes.
Power BI validation covers Import; DirectQuery and cloud gateway deployment have
not been validated.

## C++ client capabilities versus the ODBC adapter

The public SDK headers under `../client-cpp/src/include` are the source of truth.
Existing C++ applications keep their Session API. ODBC does not expose every
SDK method as a separate ODBC operation.

| C++ capability | ODBC mapping / scope | Verification |
| --- | --- | --- |
| `SessionBuilder` / `TableSessionBuilder`, host/port/user/password/database | `ConnectionHandle::OpenSession` constructs the selected model. `SessionSSLConfig` carries trust and optional client cert/key. | Real tree/table connections in all three encryption modes. |
| `Session::executeQueryStatement` / `TableSession::executeQueryStatement` | Direct/prepared SELECT and catalog source queries; `SESSIONTIMEOUTMS` passed to the query call. | `session_api_client.cpp`, `run_e2e.py`. |
| `executeNonQueryStatement` | DDL and DML, including client-rendered parameter statements. | Real creation/insertion/deletion and prepared inserts. |
| `SessionDataSet` names/types, iterator and typed getters | `ODBCSessionResultSet` maps SDK values to metadata and ODBC C buffers; `BATCHSIZE` controls adapter batches. | Unicode, NULL/empty, DATE/BOOLEAN, numeric limits and batch crossings. |
| `SessionDataSet::closeOperationHandle`, Session close | Cursor/result replacement and connection cleanup. | Handle/cursor unit tests and repeated real queries. |
| Native `insertRecord(s)`, `insertTablet(s)`, aligned inserts, raw/last-data queries and schema APIs | Remain available to C++ clients; not directly wrapped by ODBC. ODBC clients use SQL for server-supported equivalents. Parameter arrays do not call the tablet ingestion API. | SDK tests are separate; ODBC tests are not evidence for every SDK method. |
| Session TLS trust/identity and mutual authentication | SSL/SSLCA/SSLCERT/SSLKEY DSN/connection attributes use the same Session implementation for connect and query; no plaintext fallback. | Missing/wrong trust, missing/mismatched client credentials, wrong server identity rejection. |

## Type contract

| IoTDB result type | ODBC SQL type / normal C representation |
| --- | --- |
| BOOLEAN | SQL_BIT / SQL_C_BIT |
| INT32 | SQL_INTEGER / SQL_C_SLONG |
| INT64 and TIMESTAMP | SQL_BIGINT / SQL_C_SBIGINT; TIMESTAMP preserves the server epoch integer and its precision, without implicit seconds conversion |
| FLOAT / DOUBLE | SQL_REAL / SQL_DOUBLE and corresponding C floating-point values |
| STRING / TEXT | SQL_WVARCHAR / SQL_WLONGVARCHAR; Unicode W entry points use UTF-16 |
| DATE | SQL_TYPE_DATE / SQL_DATE_STRUCT |
| BLOB | SQL_LONGVARBINARY / SQL_C_BINARY |

NULL requires an indicator; empty strings are distinct from NULL. Integer
conversions check target range (22003). Integer and epoch values support
SQL_C_NUMERIC (precision 1–38, scale -38–38 via ARD; defaults 38/0).
Fractional truncation returns 01S07; FLOAT/DOUBLE to SQL_C_NUMERIC is intentionally
unsupported (07006). Query timestamp-as-date/time conversion is an explicit C
conversion, not the default metadata contract.

## Generated files and release contents

Use the release scripts in the [README](../README.md) for the same gates as CI.
Let `<build>` be `iotdb-client/odbc/target/build` by default. A fresh build is
needed when validating changed dependency defaults: old CMake cache values
override new defaults.

| Input / stage | Generated output | Tracking / distribution |
| --- | --- | --- |
| `iotdb-protocol/thrift-commons/src/main/thrift/common.thrift` and `thrift-datanode/src/main/thrift/client.thrift` | Staged copies in `<build>/client-cpp/thrift-protocols`; Thrift `-r --gen cpp:no_skeleton` produces `common_types.{h,cpp}`, `client_types.{h,cpp}`, `IClientRPCService.{h,cpp}` and a stamp under `client-cpp/gen-cpp`. | Generated by SDK CMake at configure/build time; never hand-edit or commit generated RPC code. |
| Pinned SDK/curl/json dependency configuration | SDK archives under `client-cpp/third-party`; extracted/compiled dependency trees under `<build>/client-cpp/_deps` and `<build>/_deps`. | Build caches, not release/source additions. Default dependency versions and archive digests live in CMake. |
| `DriverConfig.h.in` and CMake/git metadata | `<build>/generated/DriverConfig.h`, `<build>/BUILD-INFO.txt`. | Header compiled into driver; BUILD-INFO shipped with version, source SHA, platform, compiler, dependency versions and static-linkage declarations. |
| Driver + embedded Session/Thrift/OpenSSL/curl | Windows `Release/apache_iotdb_odbc.dll`; Linux `libapache_iotdb_odbc.so`; test executables and intermediate objects. | One driver library in the ZIP; no separate SDK/OpenSSL/VC redistributable required. OS ODBC manager/system libraries remain prerequisites. |
| Install rules + CPack | `iotdb-odbc-<version>-windows-x86_64.zip` or `iotdb-odbc-<version>-linux-x86_64-glibc2.28.zip`, plus adjacent `.zip.sha512`. | ZIP root matches filename. Contains driver, manager smoke-test executable, README/docs, conf, LICENSE/NOTICE and dependency notices. Windows also ships install helper and install/uninstall scripts. |
| GNU runtime combination | Verified GCC license/exception texts in `<build>/runtime-licenses`; installed under `licenses/gcc`. | Binary package only; no vendored GPL source/text in the repository. See README for ASF policy/PMC review. |
| Desktop acceptance | Local `.xlsx`, `.pbix`, Power Query M text, screenshots and validation record. | Test evidence, not build outputs or driver package contents. Do not commit credentials or activation/account screens. |

The ODBC module's Maven `with-odbc` profile invokes CMake at generate-resources,
builds at compile, runs CTest at test and CPack at package. It does not copy
generated Thrift C++ into source directories. The embedded Session target uses
C++17 for current Thrift-generated code; the ODBC target remains C++14.

## Reproducible verification

From the repository root:

```powershell
# Windows x64; CMake/CTest/CPack, Python 3.8+, native Perl and VS2022 installed
& .github/scripts/package-odbc-windows.ps1
python -m unittest discover -s iotdb-client/odbc/test -p test_release.py -v
```

```sh
# Linux release baseline, using the checked-in manylinux environment
source .github/scripts/native-linux-env.sh
docker run --rm -v "$PWD:/workspace" -w /workspace "$NATIVE_LINUX_IMAGE" \
  bash .github/scripts/package-odbc-manylinux228.sh
# With clang-format 17 and Java 17+ available
./mvnw -B -P with-odbc -pl iotdb-client/odbc spotless:check
```

Before extraction, run `test/verify_release.py PACKAGE.zip --commit FULL_SHA
--platform CLASSIFIER`. It checks SHA-512, provenance, filename/root, required
nonempty files/notices, duplicate/unsafe entries, one-library packaging and
absence of build intermediates. A checksum is not a cryptographic release
signature and does not assert that the source tree was clean.

| Layer | Gate and evidence | What it does not prove |
| --- | --- | --- |
| CTest | 5 tests on Windows / 4 on Linux: unit, manager load, Windows setup (Windows only), parameters, compatibility. HTTP fixtures exercise exported ABI without a server. | Fixture tests alone do not prove Session RPC integration. |
| Release verifier | 10 positive/negative `test_release.py` cases; validate each produced archive before upload/extraction. | Does not replace runtime loading or publisher signature verification. |
| Binary dependencies | Windows dependency allowlist; Linux shared-library allowlist and GLIBC ≤ 2.28 gate. | Building on a newer ordinary Linux host does not establish the release baseline. |
| Clean runtime | Extracted Linux ZIP loaded by unixODBC on AlmaLinux 8, Ubuntu 22.04 and 24.04. | Not an exhaustive distribution/architecture matrix. |
| Real server | `run_e2e.py MANAGER DRIVER SERVER_DISTRIBUTION`; disposable IoTDB built from the workflow checkout; plain/TLS/mTLS, tree/table SQL, REST and negative certificate cases. | Requires free test ports; never substitutes a production database. |
| Windows consumers | Add `--windows-consumers`: System.Data.Odbc and MSDASQL/ADO, catalog browsing, parameterized inserts, complete DataTable materialization, Unicode/long text/NULL/empty/DATE/BOOLEAN/batching. | Does not launch Excel or Power BI UI. |
| Desktop UI | [Windows acceptance procedure](windows-validation.md): load, insert a fifth row, refresh, save, reopen and refresh. Record exact application and driver versions. | A passed explicit query does not establish Navigator support; old-binary UI evidence is not a rerun on the current head. |

The `.github/workflows/odbc-package.yml` workflow has eight jobs: two package
builds, three Linux runtime jobs, one server/format/verifier job and two real-server
runtime jobs. The server/runtime jobs consume the extracted driver ZIPs produced
by that same workflow run.

Review regressions cover detaching SQL_ATTR_ROWS_FETCHED_PTR, epoch integer
range/units, TEXT IRD consistency, completed fixed/NULL SQLGetData retrieval and
COUNT/EXPLAIN query dispatch. Tests first reproduced the pointer/retrieval defects;
COUNT DATABASES failed against a real server before its dispatch fix and passed
afterward. PR validation results identify the tested commit and workflow run,
rather than treating this checklist as proof that every future commit passes.
