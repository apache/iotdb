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
# Python ODBC Example

Python example programs that connect to the Apache IoTDB ODBC driver using pyodbc. For reference, see `examples/cpp-example` and `examples/BasicTest`.

## Example Files

| File | Description |
|------|-------------|
| **TableTest.py** | Table Model example. Uses table-style SQL such as `CREATE TABLE`, `INSERT INTO table`, suitable for IoTDB Table Model. |
| **TreeTest.py** | Tree Model example. Uses timeseries-style SQL such as `CREATE DATABASE`, `CREATE TIMESERIES`, `INSERT INTO root.xxx.xxx`, suitable for IoTDB native Tree Model. |

### Table Model vs Tree Model

- **Table Model**: Connection string defaults to or explicitly sets `istablemodel=1`. Uses `use database`, `CREATE TABLE`, `INSERT INTO tablename`; data is stored in tables.
- **Tree Model**: Connection string requires `istablemodel=0`. Uses full paths like `root.xxx.xxx`, creates timeseries via `CREATE TIMESERIES`; data is organized by device/measurement.

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Prerequisites

1. **ODBC Driver**: Apache IoTDB ODBC driver installed and configured, with DSN name `Apache IoTDB DSN`
2. **IoTDB Service**: IoTDB is running, default `127.0.0.1:6667`

## Run

### Table Model Example

```bash
python TableTest.py
```

### Tree Model Example

```bash
python TreeTest.py
```

## Function Descriptions

- **execute**: Execute non-query SQL (e.g. `CREATE`, `INSERT`, `DELETE`)
- **query**: Execute a SELECT query and display results in table format
- **TableTest**: Demonstrates creation, insertion, and query of fulltable
- **TreeTest**: Demonstrates creation, insertion, and query of root.full.fulldevice

## Connection Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| DSN | Data Source Name | `Apache IoTDB DSN` |
| Server | IoTDB server address | `127.0.0.1` |
| UID | Username | `root` |
| PWD | Password | `root` |
| Database | Database name (Table Model) | `test` |
| loglevel | Log level 0–4 | `4` |
| istablemodel | 0=Tree Model, 1=Table Model | `0` or `1` |

## FAQ

1. **Connection failed**: Check whether IoTDB is running, the DSN is configured correctly, and the Server/port are correct.
2. **Garbled characters**: If SQL statements contain non-ASCII characters, make sure the source file is saved with code page encoding. Alternatively, use English test data to avoid encoding issues.
