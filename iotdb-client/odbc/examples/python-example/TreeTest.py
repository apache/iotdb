#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# -*- coding: utf-8 -*-
"""
Apache IoTDB ODBC Python example - Tree Model
Uses pyodbc to connect to the IoTDB ODBC driver with istablemodel=0 for tree model.
Reference: examples/BasicTest/TreeTest/TreeTest.cs and examples/cpp-example/TreeTest.cpp.
"""

import pyodbc


def execute(conn: pyodbc.Connection, command: str) -> None:
    """Execute a non-query SQL statement (e.g. INSERT; tree model INSERT auto-creates timeseries)"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(command)
            cmd_upper = command.strip().upper()
            if cmd_upper.startswith(("INSERT", "UPDATE", "DELETE")):
                conn.commit()
            print(f"Execute command: {command}")
    except pyodbc.Error as ex:
        print(f"CommandText error: {ex}")


def query(conn: pyodbc.Connection, sql: str) -> None:
    """Execute a SELECT query and display results of root.full.fulldevice in table format"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            col_count = len(cursor.description)
            print(f"fCount = {col_count}")

            if col_count <= 0:
                return

            columns = []
            for i in range(col_count):
                col_name = cursor.description[i][0] or f"Column{i}"
                if "." in str(col_name):
                    col_name = str(col_name).split(".")[-1]
                columns.append(str(col_name))

            rows = cursor.fetchall()

            col_widths = [max(len(str(col)), 4) for col in columns]
            for row in rows:
                for j, val in enumerate(row):
                    if j < len(col_widths):
                        col_widths[j] = max(col_widths[j], len(str(val) if val is not None else "NULL"))

            header = " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(columns))
            print(header)
            print("-" * len(header))

            for row in rows:
                values = []
                for i, val in enumerate(row):
                    if val is None:
                        cell = "NULL"
                    else:
                        cell = str(val)
                    values.append(cell.ljust(col_widths[i]) if i < len(col_widths) else cell)
                print(" | ".join(values))

            print()

    except pyodbc.Error as ex:
        print(f"Query error: {ex}")


def main() -> None:
    dsn = "Apache IoTDB DSN"
    user = "root"
    password = "root"
    server = "127.0.0.1"
    connection_string = (
        f"DSN={dsn};Server={server};UID={user};PWD={password};"
        f"loglevel=4;istablemodel=0"
    )

    print("Start")

    try:
        conn = pyodbc.connect(connection_string)
    except pyodbc.Error as ex:
        print(f"Login failed: {ex}")
        return

    try:
        driver_name = conn.getinfo(6)  # SQL_DRIVER_NAME
        print(f"Successfully opened connection. driver = {driver_name}")
    except Exception:
        print("Successfully opened connection.")

    try:
        insert_statements = [
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735689600001, true, 100, 10000000000, 36.5, 128.689, 'Device running normally', 'DeviceA-Room1', 1735689600000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735689660000, false, 101, 10000000001, 36.6, 128.789, 'Device running normally', 'DeviceA-Room1', 1735689660000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735689720000, true, 102, 10000000002, 36.7, 128.889, 'Device running normally', 'DeviceA-Room1', 1735689720000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735689780000, false, 103, 10000000003, 36.8, 128.989, 'Device high temperature alert', 'DeviceA-Room1', 1735689780000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735689840000, true, 104, 10000000004, 36.9, 129.089, 'Device status restored', 'DeviceA-Room1', 1735689840000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735689900000, false, 105, 10000000005, 37.0, 129.189, 'Device running normally', 'DeviceB-Room2', 1735689900000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735689960000, true, 106, 10000000006, 37.1, 129.289, 'Device running normally', 'DeviceB-Room2', 1735689960000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690020000, false, 107, 10000000007, 37.2, 129.389, 'Device low humidity alert', 'DeviceB-Room2', 1735690020000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690080000, true, 108, 10000000008, 37.3, 129.489, 'Device status restored', 'DeviceB-Room2', 1735690080000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690140000, false, 109, 10000000009, 37.4, 129.589, 'Device running normally', 'DeviceC-Room3', 1735690140000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690200000, true, 110, 10000000010, 37.5, 129.689, 'Device running normally', 'DeviceC-Room3', 1735690200000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690260000, false, 111, 10000000011, 37.6, 129.789, 'Device unstable voltage alert', 'DeviceC-Room3', 1735690260000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690320000, true, 112, 10000000012, 37.7, 129.889, 'Device status restored', 'DeviceC-Room3', 1735690320000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690380000, false, 113, 10000000013, 37.8, 129.989, 'Device running normally', 'DeviceD-Room4', 1735690380000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690440000, true, 114, 10000000014, 37.9, 130.089, 'Device running normally', 'DeviceD-Room4', 1735690440000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690500000, false, 115, 10000000015, 38.0, 130.189, 'Device running normally', 'DeviceD-Room4', 1735690500000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690560000, true, 116, 10000000016, 38.1, 130.289, 'Device signal interrupted alert', 'DeviceD-Room4', 1735690560000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690620000, false, 117, 10000000017, 38.2, 130.389, 'Device running normally', 'DeviceE-Room5', 1735690620000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690680000, true, 118, 10000000018, 38.3, 130.489, 'Device running normally', 'DeviceE-Room5', 1735690680000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690740000, false, 119, 10000000019, 38.4, 130.589, 'Device running normally', 'DeviceE-Room5', 1735690740000, '2026-01-04')",
            "INSERT INTO root.full.fulldevice(timestamp, bool_col, int32_col, int64_col, float_col, double_col, text_col, string_col, timestamp_col, date_col) VALUES (1735690790000, false, 119, 10000000019, 38.4, 130.589, 'Device running normally', 'DeviceE-Room5', 1735690740000, '2026-01-04')",
        ]
        for insert_sql in insert_statements:
            execute(conn, insert_sql)
        print(f"[DEBUG] Inserted {len(insert_statements)} rows. Begin to query.")

        query_sql = "SELECT * FROM root.full.fulldevice WHERE time >= 1735689600000 AND time <= 1735690790000"
        query(conn, query_sql)
        print("Query ok")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
