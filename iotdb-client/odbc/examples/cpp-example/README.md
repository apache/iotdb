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
# C++ ODBC Example

This directory contains C++ example programs that connect to IoTDB using the Apache IoTDB ODBC driver.

## Example Files

| File | Description |
|------|-------------|
| **TableTest.cpp** | Table Model example. Uses table-style SQL such as `CREATE TABLE`, `INSERT INTO table`, suitable for IoTDB Table Model. |
| **TreeTest.cpp** | Tree Model example. Uses timeseries-style SQL such as `CREATE DATABASE`, `CREATE TIMESERIES`, `INSERT INTO root.xxx.xxx`, suitable for IoTDB native Tree Model. |

### Table Model vs Tree Model

- **Table Model**: Connection string defaults to or explicitly sets `istablemodel=1`. Uses `use database`, `CREATE TABLE`, `INSERT INTO tablename`; data is stored in tables.
- **Tree Model**: Connection string requires `istablemodel=0`. Uses full paths like `root.xxx.xxx`, creates timeseries via `CREATE TIMESERIES`; data is organized by device/measurement.

## Prerequisites

1. **Build Environment**: Visual Studio 2022 (or x64 Native Tools Command Prompt)
2. **ODBC Driver**: Apache IoTDB ODBC driver installed and configured, with DSN name `Apache IoTDB DSN`
3. **IoTDB Service**: IoTDB is running, default `127.0.0.1:6667`
4. **Dependencies**: `odbc32.lib` (Windows ODBC). When installing Visual Studio with the Windows SDK and C++ development tools options selected, `odbc32.lib` is automatically included.

## Build and Run

### 1. Open Command Prompt

Open **x64 Native Tools Command Prompt for VS 2022** (or the corresponding version of VS Command Prompt).

### 2. Navigate to Example Directory

```bash
cd examples\cpp-example
```

### 3. Build

**Table Model example:**
```bash
cl /EHsc /W4 /O2 TableTest.cpp /link odbc32.lib
```

**Tree Model example:**
```bash
cl /EHsc /W4 /O2 TreeTest.cpp /link odbc32.lib
```

### 4. Run

**Table Model:**
```bash
TableTest
```

**Tree Model:**
```bash
TreeTest
```

## Connection String Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| DSN | Data Source Name | `Apache IoTDB DSN` |
| Server | IoTDB server address | `127.0.0.1` |
| UID | Username | `root` |
| PWD | Password | `root` |
| Database | Database name (Table Model) | `test` |
| loglevel | Log level 0–4 | `4` |
| istablemodel | 0=Tree Model, 1=Table Model | `0` or `1` |

## Encoding Notes

STRING and TEXT columns use UTF-8 encoding. Direct output in the Windows console may show garbled characters; you can redirect output to a file to view correctly:

```bash
TreeTest > output.txt
```

## FAQ

1. **Connection failed**: Check whether IoTDB is running, the DSN is configured correctly, and the Server/port are correct.
2. **Garbled characters**: If SQL statements contain non-ASCII characters, make sure the source file is saved with code page encoding. Alternatively, use English test data to avoid encoding issues.
